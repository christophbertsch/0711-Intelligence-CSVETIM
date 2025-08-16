"""
Ingest Agent - Handles file upload and initial processing.
"""

import hashlib
import logging
from datetime import datetime
from io import BytesIO
from typing import Dict, Any

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, FileReceivedEvent, FileProfiledEvent
from shared.storage import detect_file_encoding

logger = logging.getLogger(__name__)


@register_agent("ingest")
class IngestAgent(BaseAgent):
    """
    IngestAgent watches for file uploads and initiates the processing pipeline.
    
    Responsibilities:
    - Monitor file uploads (S3 events or API triggers)
    - Validate file format and size
    - Store file metadata in staging tables
    - Emit file.received events
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.max_file_size = self.settings.max_file_size_mb * 1024 * 1024
        self.allowed_extensions = {'.csv', '.tsv', '.txt', '.xlsx', '.xls'}
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events - primarily file upload notifications."""
        if event.event_type == "file.upload.requested":
            await self._handle_file_upload(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _handle_file_upload(self, event: BaseEvent):
        """Process a file upload request."""
        try:
            # Extract file information from event
            file_info = event.dict()
            client_id = event.client_id
            ingest_id = event.ingest_id
            
            logger.info(f"Processing file upload for client {client_id}, ingest {ingest_id}")
            
            # Download file from storage
            object_key = self._extract_object_key(file_info.get("uri", ""))
            file_obj, metadata = await self.storage_manager.download_file(object_key)
            
            # Validate file
            validation_result = await self._validate_file(file_obj, file_info)
            if not validation_result["valid"]:
                await self._handle_validation_error(event, validation_result["errors"])
                return
            
            # Store file metadata in database
            await self._store_file_metadata(event, file_obj, metadata)
            
            # Emit file.received event
            await self._emit_file_received_event(event, file_obj, metadata)
            
            logger.info(f"Successfully processed file upload: {ingest_id}")
            
        except Exception as e:
            logger.error(f"Error processing file upload: {e}")
            await self._handle_processing_error(event, str(e))
    
    def _extract_object_key(self, uri: str) -> str:
        """Extract object key from S3 URI."""
        if uri.startswith("s3://"):
            parts = uri[5:].split("/", 1)
            if len(parts) == 2:
                return parts[1]
        raise ValueError(f"Invalid S3 URI: {uri}")
    
    async def _validate_file(self, file_obj: BytesIO, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate uploaded file."""
        errors = []
        
        # Check file size
        file_obj.seek(0, 2)  # Seek to end
        file_size = file_obj.tell()
        file_obj.seek(0)  # Reset to beginning
        
        if file_size > self.max_file_size:
            errors.append(f"File size {file_size} exceeds maximum {self.max_file_size}")
        
        if file_size == 0:
            errors.append("File is empty")
        
        # Check file extension
        filename = file_info.get("filename", "")
        file_extension = filename.lower().split(".")[-1] if "." in filename else ""
        
        if f".{file_extension}" not in self.allowed_extensions:
            errors.append(f"File extension .{file_extension} not allowed")
        
        # Basic content validation for CSV files
        if file_extension in ["csv", "tsv", "txt"]:
            try:
                # Try to detect encoding
                encoding = detect_file_encoding(file_obj)
                
                # Read first few lines to validate CSV structure
                file_obj.seek(0)
                content = file_obj.read(1024).decode(encoding)
                
                if not content.strip():
                    errors.append("File appears to be empty or contains only whitespace")
                
                # Basic CSV validation
                lines = content.split('\n')[:5]  # Check first 5 lines
                if len(lines) < 2:
                    errors.append("File must contain at least a header and one data row")
                
            except UnicodeDecodeError as e:
                errors.append(f"File encoding error: {e}")
            except Exception as e:
                errors.append(f"File validation error: {e}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "file_size": file_size,
            "encoding": encoding if file_extension in ["csv", "tsv", "txt"] else None,
        }
    
    async def _store_file_metadata(self, event: BaseEvent, file_obj: BytesIO, metadata: Dict[str, Any]):
        """Store file metadata in the staging table."""
        async with self.database_session() as session:
            # Calculate file hash
            file_obj.seek(0)
            file_hash = hashlib.sha256(file_obj.read()).hexdigest()
            file_obj.seek(0)
            
            # Insert into stg_file table
            query = """
                INSERT INTO stg_file (
                    client_id, ingest_id, uri, filename, size_bytes, 
                    encoding, status, created_at
                ) VALUES (
                    :client_id, :ingest_id, :uri, :filename, :size_bytes,
                    :encoding, :status, :created_at
                )
                ON CONFLICT (client_id, ingest_id) 
                DO UPDATE SET 
                    uri = EXCLUDED.uri,
                    filename = EXCLUDED.filename,
                    size_bytes = EXCLUDED.size_bytes,
                    encoding = EXCLUDED.encoding,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
                "uri": event.dict().get("uri", ""),
                "filename": event.dict().get("filename", ""),
                "size_bytes": metadata.get("content_length", 0),
                "encoding": metadata.get("encoding"),
                "status": "received",
                "created_at": datetime.utcnow(),
            })
            
            await session.commit()
    
    async def _emit_file_received_event(self, event: BaseEvent, file_obj: BytesIO, metadata: Dict[str, Any]):
        """Emit file.received event to trigger profiling."""
        file_received_event = FileReceivedEvent(
            event_id=f"received-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"received-{event.client_id}-{event.ingest_id}",
            uri=event.dict().get("uri", ""),
            filename=event.dict().get("filename", ""),
            size_bytes=metadata.get("content_length", 0),
            content_type=metadata.get("content_type", ""),
            hints=event.dict().get("hints", {}),
        )
        
        await self.publish_event(file_received_event)
    
    async def _handle_validation_error(self, event: BaseEvent, errors: list):
        """Handle file validation errors."""
        logger.error(f"File validation failed for {event.ingest_id}: {errors}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'validation_failed', 
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
        
        # Emit error event
        from shared.events import ProcessingErrorEvent
        
        error_event = ProcessingErrorEvent(
            event_id=f"validation-error-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"validation-error-{event.client_id}-{event.ingest_id}",
            error_code="FILE_VALIDATION_FAILED",
            error_message="; ".join(errors),
            failed_stage="ingest",
            is_recoverable=False,
            retry_count=0,
        )
        
        await self.publish_event(error_event)
    
    async def _handle_processing_error(self, event: BaseEvent, error_message: str):
        """Handle general processing errors."""
        logger.error(f"Processing error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'processing_failed', 
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("IngestAgent initialized")
        
        # Could set up file system watchers, S3 event listeners, etc.
        # For now, we rely on API-triggered events
    
    async def start_background_tasks(self):
        """Start background tasks for file monitoring."""
        tasks = []
        
        # Could add tasks for:
        # - Monitoring S3 bucket for new files
        # - Cleaning up old temporary files
        # - Health checks
        
        return tasks