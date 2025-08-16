"""
CSV Import API endpoints - Core import functionality.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from fastapi.responses import JSONResponse

from shared.database import get_async_session
from shared.kafka_client import get_kafka_manager
from shared.storage import get_storage_manager
from shared.models import (
    UploadResponse, PreviewRequest, PreviewResponse, 
    ExecuteRequest, ExecuteResponse, ProgressResponse
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    client_id: str = Form(...),
    hints: Optional[str] = Form(None),
    session=Depends(get_async_session)
):
    """
    Upload a CSV file for processing.
    
    This endpoint:
    1. Validates the uploaded file
    2. Stores it in object storage
    3. Creates a job record
    4. Triggers the processing pipeline
    """
    try:
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        
        # Check file size
        content = await file.read()
        file_size = len(content)
        
        if file_size == 0:
            raise HTTPException(status_code=400, detail="Empty file")
        
        if file_size > 500 * 1024 * 1024:  # 500MB limit
            raise HTTPException(status_code=400, detail="File too large")
        
        # Generate IDs
        job_id = uuid4()
        ingest_id = f"{client_id}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{job_id.hex[:8]}"
        
        # Store file in object storage
        storage_manager = await get_storage_manager()
        object_key = storage_manager.generate_object_key(
            client_id, ingest_id, file.filename, "uploads"
        )
        
        from io import BytesIO
        file_obj = BytesIO(content)
        uri = await storage_manager.upload_file(
            file_obj,
            object_key,
            content_type=file.content_type,
            metadata={
                "client_id": client_id,
                "ingest_id": ingest_id,
                "original_filename": file.filename,
            }
        )
        
        # Parse hints
        hints_dict = {}
        if hints:
            try:
                import json
                hints_dict = json.loads(hints)
            except json.JSONDecodeError:
                logger.warning(f"Invalid hints JSON: {hints}")
        
        # Create job record
        query = """
            INSERT INTO import_job (job_id, client_id, ingest_id, status, progress, started_at)
            VALUES (:job_id, :client_id, :ingest_id, :status, :progress, :started_at)
        """
        
        await session.execute(query, {
            "job_id": job_id,
            "client_id": client_id,
            "ingest_id": ingest_id,
            "status": "created",
            "progress": {"stage": "uploaded", "percent": 0},
            "started_at": datetime.utcnow(),
        })
        
        # Create staging file record
        file_query = """
            INSERT INTO stg_file (client_id, ingest_id, uri, filename, size_bytes, status)
            VALUES (:client_id, :ingest_id, :uri, :filename, :size_bytes, :status)
        """
        
        await session.execute(file_query, {
            "client_id": client_id,
            "ingest_id": ingest_id,
            "uri": uri,
            "filename": file.filename,
            "size_bytes": file_size,
            "status": "uploaded",
        })
        
        await session.commit()
        
        # Trigger processing pipeline by publishing event
        kafka_manager = await get_kafka_manager()
        
        # Create file upload event
        from shared.events import BaseEvent
        upload_event = BaseEvent(
            event_id=f"upload-{ingest_id}",
            event_type="file.upload.requested",
            trace_id=str(job_id),
            client_id=client_id,
            ingest_id=ingest_id,
            source_agent="api",
            idempotency_key=f"upload-{client_id}-{ingest_id}",
        )
        
        # Add file information to event
        upload_event_dict = upload_event.dict()
        upload_event_dict.update({
            "uri": uri,
            "filename": file.filename,
            "size_bytes": file_size,
            "content_type": file.content_type or "text/csv",
            "hints": hints_dict,
        })
        
        await kafka_manager.publish_event("file.received", upload_event, f"{client_id}:{ingest_id}")
        
        logger.info(f"File uploaded successfully: {ingest_id}")
        
        return UploadResponse(
            job_id=job_id,
            ingest_id=ingest_id,
            status="created"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/preview", response_model=PreviewResponse)
async def preview_file(
    job_id: str,
    request: PreviewRequest,
    session=Depends(get_async_session)
):
    """
    Preview file structure and sample data.
    
    This endpoint returns:
    - Detected file structure (encoding, delimiter, headers)
    - Sample rows
    - Column statistics
    - Suggested mappings
    """
    try:
        # Get job information
        query = """
            SELECT sf.uri, sf.encoding, sf.delimiter, sf.has_header, sf.profile, ij.client_id, ij.ingest_id
            FROM import_job ij
            JOIN stg_file sf ON ij.client_id = sf.client_id AND ij.ingest_id = sf.ingest_id
            WHERE ij.job_id = :job_id
        """
        
        result = await session.execute(query, {"job_id": job_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        
        uri, encoding, delimiter, has_header, profile, client_id, ingest_id = row
        
        # If file hasn't been profiled yet, return basic info
        if not profile:
            # Try to get basic file info
            storage_manager = await get_storage_manager()
            object_key = uri.split('/', 3)[-1] if uri.startswith('s3://') else uri
            
            try:
                file_obj, metadata = await storage_manager.download_file(object_key)
                
                # Basic preview without full profiling
                from shared.storage import detect_file_encoding
                detected_encoding = detect_file_encoding(file_obj)
                
                file_obj.seek(0)
                content = file_obj.read(1000).decode(detected_encoding, errors='replace')
                lines = content.split('\n')[:10]
                
                return PreviewResponse(
                    profile={
                        "encoding": detected_encoding,
                        "delimiter": ",",  # Default guess
                        "has_header": True,
                        "row_count": len(lines) - 1,
                        "column_count": len(lines[0].split(',')) if lines else 0,
                        "columns": [f"col_{i}" for i in range(len(lines[0].split(',')))] if lines else [],
                        "sample_rows": [{"preview": line} for line in lines[:5]],
                        "column_stats": {},
                        "quality_score": 0.5,
                        "issues": ["File not yet profiled - showing basic preview"],
                    },
                    suggested_mappings=[]
                )
                
            except Exception as e:
                logger.error(f"Error creating preview: {e}")
                raise HTTPException(status_code=500, detail="Unable to preview file")
        
        # Return full profile if available
        from shared.models import FileProfile
        file_profile = FileProfile(**profile)
        
        # Generate suggested mappings based on column names
        suggested_mappings = await _generate_mapping_suggestions(file_profile.columns, client_id, session)
        
        return PreviewResponse(
            profile=file_profile,
            suggested_mappings=suggested_mappings
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error previewing file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/execute", response_model=ExecuteResponse)
async def execute_job(
    job_id: str,
    request: ExecuteRequest,
    session=Depends(get_async_session)
):
    """
    Execute the import job.
    
    This triggers the full processing pipeline:
    1. Validation (if not already done)
    2. Normalization
    3. Persistence to canonical tables
    """
    try:
        # Get job information
        query = """
            SELECT client_id, ingest_id, status, template_id
            FROM import_job
            WHERE job_id = :job_id
        """
        
        result = await session.execute(query, {"job_id": job_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        
        client_id, ingest_id, status, template_id = row
        
        # Check if job can be executed
        if status in ["executing", "completed"]:
            raise HTTPException(status_code=400, detail=f"Job is already {status}")
        
        if status == "failed":
            raise HTTPException(status_code=400, detail="Job has failed and cannot be executed")
        
        # Check if template is assigned
        if not template_id:
            raise HTTPException(status_code=400, detail="No mapping template assigned to job")
        
        # Update job status
        update_query = """
            UPDATE import_job 
            SET status = 'executing', 
                progress = :progress,
                updated_at = NOW()
            WHERE job_id = :job_id
        """
        
        await session.execute(update_query, {
            "job_id": job_id,
            "progress": {"stage": "executing", "percent": 0, "dry_run": request.dry_run},
        })
        
        await session.commit()
        
        # Trigger execution by publishing event
        kafka_manager = await get_kafka_manager()
        
        from shared.events import BaseEvent
        execute_event = BaseEvent(
            event_id=f"execute-{ingest_id}",
            event_type="job.execute.requested",
            trace_id=job_id,
            client_id=client_id,
            ingest_id=ingest_id,
            source_agent="api",
            idempotency_key=f"execute-{client_id}-{ingest_id}",
        )
        
        # Add execution parameters
        execute_event_dict = execute_event.dict()
        execute_event_dict.update({
            "dry_run": request.dry_run,
            "batch_size": request.batch_size,
            "quality_gate_override": request.quality_gate_override,
        })
        
        await kafka_manager.publish_event("job.execute.requested", execute_event, f"{client_id}:{ingest_id}")
        
        logger.info(f"Job execution started: {job_id}")
        
        return ExecuteResponse(
            status="executing",
            execution_id=uuid4(),
            estimated_duration=300  # 5 minutes estimate
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/progress", response_model=ProgressResponse)
async def get_job_progress(
    job_id: str,
    session=Depends(get_async_session)
):
    """
    Get job progress and status.
    """
    try:
        query = """
            SELECT ij.status, ij.progress, ij.error_message, ij.started_at, ij.completed_at,
                   sf.status as file_status
            FROM import_job ij
            LEFT JOIN stg_file sf ON ij.client_id = sf.client_id AND ij.ingest_id = sf.ingest_id
            WHERE ij.job_id = :job_id
        """
        
        result = await session.execute(query, {"job_id": job_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        
        status, progress, error_message, started_at, completed_at, file_status = row
        
        # Calculate progress percentage based on status
        progress_dict = progress or {}
        
        stage_progress = {
            "created": 0,
            "profiling": 10,
            "profiled": 20,
            "mapping": 30,
            "mapped": 40,
            "validating": 50,
            "validated": 60,
            "normalizing": 70,
            "normalized": 80,
            "executing": 90,
            "completed": 100,
            "failed": 0,
        }
        
        current_progress = stage_progress.get(status, 0)
        
        # Define processing stages
        all_stages = ["upload", "profiling", "mapping", "validation", "normalization", "execution", "completion"]
        
        completed_stages = []
        remaining_stages = all_stages.copy()
        
        if status in ["profiled", "mapping", "mapped", "validating", "validated", "normalizing", "normalized", "executing", "completed"]:
            completed_stages.append("profiling")
            remaining_stages.remove("profiling")
        
        if status in ["mapped", "validating", "validated", "normalizing", "normalized", "executing", "completed"]:
            completed_stages.append("mapping")
            if "mapping" in remaining_stages:
                remaining_stages.remove("mapping")
        
        if status in ["validated", "normalizing", "normalized", "executing", "completed"]:
            completed_stages.append("validation")
            if "validation" in remaining_stages:
                remaining_stages.remove("validation")
        
        if status in ["normalized", "executing", "completed"]:
            completed_stages.append("normalization")
            if "normalization" in remaining_stages:
                remaining_stages.remove("normalization")
        
        if status == "completed":
            completed_stages.extend(["execution", "completion"])
            remaining_stages = []
        
        # Estimate completion time
        estimated_completion = None
        if status == "executing" and started_at:
            from datetime import timedelta
            estimated_completion = started_at + timedelta(minutes=5)  # Simple estimate
        
        return ProgressResponse(
            job_id=job_id,
            status=status,
            progress=progress_dict,
            current_stage=status,
            stages_completed=completed_stages,
            estimated_completion=estimated_completion,
            error_message=error_message,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job progress: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_jobs(
    client_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    session=Depends(get_async_session)
):
    """
    List import jobs with optional filtering.
    """
    try:
        # Build query with filters
        where_conditions = []
        params = {"limit": limit, "offset": offset}
        
        if client_id:
            where_conditions.append("client_id = :client_id")
            params["client_id"] = client_id
        
        if status:
            where_conditions.append("status = :status")
            params["status"] = status
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT job_id, client_id, ingest_id, status, progress, 
                   started_at, completed_at, error_message
            FROM import_job
            {where_clause}
            ORDER BY started_at DESC
            LIMIT :limit OFFSET :offset
        """
        
        result = await session.execute(query, params)
        rows = result.fetchall()
        
        jobs = []
        for row in rows:
            jobs.append({
                "job_id": row[0],
                "client_id": row[1],
                "ingest_id": row[2],
                "status": row[3],
                "progress": row[4],
                "started_at": row[5],
                "completed_at": row[6],
                "error_message": row[7],
            })
        
        # Get total count
        count_query = f"""
            SELECT COUNT(*) FROM import_job {where_clause}
        """
        
        count_result = await session.execute(count_query, params)
        total_count = count_result.scalar()
        
        return {
            "jobs": jobs,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        }
        
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _generate_mapping_suggestions(columns: list, client_id: str, session) -> list:
    """Generate mapping suggestions based on column names."""
    suggestions = []
    
    # Simple heuristic-based suggestions
    for column in columns:
        col_lower = column.lower().strip()
        
        if any(pattern in col_lower for pattern in ['sku', 'article', 'item']):
            suggestions.append({
                "src_column": column,
                "target_table": "product",
                "target_column": "sku",
                "confidence": 0.9,
                "reason": "Column name suggests product SKU"
            })
        
        elif any(pattern in col_lower for pattern in ['gtin', 'ean', 'barcode']):
            suggestions.append({
                "src_column": column,
                "target_table": "product", 
                "target_column": "gtin",
                "confidence": 0.8,
                "reason": "Column name suggests GTIN/EAN code"
            })
        
        elif any(pattern in col_lower for pattern in ['name', 'title', 'description']):
            suggestions.append({
                "src_column": column,
                "target_table": "product",
                "target_column": "product_name",
                "confidence": 0.7,
                "reason": "Column name suggests product name"
            })
    
    return suggestions