"""
Template management endpoints.
"""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends

from shared.database import get_async_session
from shared.models import (
    TemplateSuggestRequest, TemplateSuggestResponse,
    TemplateCreateRequest, TemplateCreateResponse,
    TemplateApplyRequest, TemplateApplyResponse
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/suggest", response_model=TemplateSuggestResponse)
async def suggest_template(
    request: TemplateSuggestRequest,
    session=Depends(get_async_session)
):
    """
    Suggest mapping templates based on column analysis.
    """
    try:
        columns = request.columns
        sample_data = request.sample_data
        etim_class = request.etim_class
        
        # Analyze columns and suggest mappings
        field_mappings = []
        feature_mappings = []
        confidence_scores = {}
        
        for column in columns:
            col_lower = column.lower().strip()
            
            # Product field suggestions
            if any(pattern in col_lower for pattern in ['sku', 'article', 'item', 'product_id']):
                field_mappings.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'sku',
                    'transform_type': 'direct',
                    'is_required': True,
                })
                confidence_scores[column] = 0.9
                
            elif any(pattern in col_lower for pattern in ['gtin', 'ean', 'barcode']):
                field_mappings.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'gtin',
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.85
                
            elif any(pattern in col_lower for pattern in ['name', 'title', 'bezeichnung', 'description']):
                field_mappings.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'product_name',
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.8
                
            elif any(pattern in col_lower for pattern in ['brand', 'marke', 'hersteller', 'manufacturer']):
                field_mappings.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'brand',
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.75
                
            # ETIM feature suggestions
            elif any(pattern in col_lower for pattern in ['length', 'länge', 'laenge']):
                feature_mappings.append({
                    'src_column': column,
                    'etim_feature': 'EF001001',  # Length
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.8
                
            elif any(pattern in col_lower for pattern in ['diameter', 'durchmesser', 'width', 'breite']):
                feature_mappings.append({
                    'src_column': column,
                    'etim_feature': 'EF001002',  # Diameter
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.8
                
            elif any(pattern in col_lower for pattern in ['material', 'werkstoff']):
                feature_mappings.append({
                    'src_column': column,
                    'etim_feature': 'EF001003',  # Material
                    'transform_type': 'direct',
                    'is_required': False,
                })
                confidence_scores[column] = 0.7
        
        # Look for existing similar templates
        suggested_templates = []
        
        if field_mappings or feature_mappings:
            suggested_templates.append({
                'name': 'Auto-suggested template',
                'description': f'Generated based on {len(columns)} columns',
                'field_mappings': field_mappings,
                'feature_mappings': feature_mappings,
                'confidence': sum(confidence_scores.values()) / len(confidence_scores) if confidence_scores else 0.5,
            })
        
        return TemplateSuggestResponse(
            suggested_templates=suggested_templates,
            confidence_scores=confidence_scores,
            field_mappings=field_mappings,
            feature_mappings=feature_mappings
        )
        
    except Exception as e:
        logger.error(f"Error suggesting template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=TemplateCreateResponse)
async def create_template(
    request: TemplateCreateRequest,
    client_id: str,
    session=Depends(get_async_session)
):
    """
    Create a new mapping template.
    """
    try:
        template_id = uuid4()
        
        # Insert template
        template_query = """
            INSERT INTO map_template (template_id, client_id, name, description, version, is_active)
            VALUES (:template_id, :client_id, :name, :description, :version, :is_active)
        """
        
        await session.execute(template_query, {
            "template_id": template_id,
            "client_id": client_id,
            "name": request.name,
            "description": request.description,
            "version": 1,
            "is_active": True,
        })
        
        # Insert field mappings
        for field_mapping in request.field_mappings:
            field_query = """
                INSERT INTO map_field (
                    template_id, src_column, target_table, target_column,
                    transform_type, transform_config, is_required, default_value
                ) VALUES (
                    :template_id, :src_column, :target_table, :target_column,
                    :transform_type, :transform_config, :is_required, :default_value
                )
            """
            
            await session.execute(field_query, {
                "template_id": template_id,
                "src_column": field_mapping["src_column"],
                "target_table": field_mapping["target_table"],
                "target_column": field_mapping["target_column"],
                "transform_type": field_mapping.get("transform_type", "direct"),
                "transform_config": field_mapping.get("transform_config", {}),
                "is_required": field_mapping.get("is_required", False),
                "default_value": field_mapping.get("default_value"),
            })
        
        # Insert feature mappings
        for feature_mapping in request.feature_mappings:
            feature_query = """
                INSERT INTO map_feature (
                    template_id, src_column, etim_feature, unit_column,
                    transform_type, transform_config, is_required
                ) VALUES (
                    :template_id, :src_column, :etim_feature, :unit_column,
                    :transform_type, :transform_config, :is_required
                )
            """
            
            await session.execute(feature_query, {
                "template_id": template_id,
                "src_column": feature_mapping["src_column"],
                "etim_feature": feature_mapping["etim_feature"],
                "unit_column": feature_mapping.get("unit_column"),
                "transform_type": feature_mapping.get("transform_type", "direct"),
                "transform_config": feature_mapping.get("transform_config", {}),
                "is_required": feature_mapping.get("is_required", False),
            })
        
        await session.commit()
        
        logger.info(f"Template created: {template_id}")
        
        return TemplateCreateResponse(
            template_id=template_id,
            version=1,
            status="created"
        )
        
    except Exception as e:
        logger.error(f"Error creating template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{template_id}/apply/{job_id}", response_model=TemplateApplyResponse)
async def apply_template(
    template_id: str,
    job_id: str,
    request: TemplateApplyRequest,
    session=Depends(get_async_session)
):
    """
    Apply a mapping template to a job.
    """
    try:
        # Verify template exists
        template_query = """
            SELECT client_id, name, version FROM map_template
            WHERE template_id = :template_id AND is_active = true
        """
        
        template_result = await session.execute(template_query, {"template_id": template_id})
        template_row = template_result.fetchone()
        
        if not template_row:
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_client_id, template_name, template_version = template_row
        
        # Verify job exists and get client_id
        job_query = """
            SELECT client_id, ingest_id, status FROM import_job
            WHERE job_id = :job_id
        """
        
        job_result = await session.execute(job_query, {"job_id": job_id})
        job_row = job_result.fetchone()
        
        if not job_row:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job_client_id, ingest_id, job_status = job_row
        
        # Verify client access (template must belong to same client or be global)
        if template_client_id != job_client_id and template_client_id != "global":
            raise HTTPException(status_code=403, detail="Template not accessible for this client")
        
        # Update job with template
        update_query = """
            UPDATE import_job 
            SET template_id = :template_id, 
                status = CASE WHEN status = 'created' THEN 'mapped' ELSE status END,
                updated_at = NOW()
            WHERE job_id = :job_id
        """
        
        await session.execute(update_query, {
            "template_id": template_id,
            "job_id": job_id,
        })
        
        await session.commit()
        
        # Count mappings applied
        field_count_query = """
            SELECT COUNT(*) FROM map_field WHERE template_id = :template_id
        """
        field_result = await session.execute(field_count_query, {"template_id": template_id})
        field_count = field_result.scalar()
        
        feature_count_query = """
            SELECT COUNT(*) FROM map_feature WHERE template_id = :template_id
        """
        feature_result = await session.execute(feature_count_query, {"template_id": template_id})
        feature_count = feature_result.scalar()
        
        total_mappings = field_count + feature_count
        
        logger.info(f"Template {template_id} applied to job {job_id}")
        
        return TemplateApplyResponse(
            status="applied",
            mappings_applied=total_mappings,
            issues=[]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error applying template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def list_templates(
    client_id: str,
    limit: int = 50,
    offset: int = 0,
    session=Depends(get_async_session)
):
    """
    List mapping templates for a client.
    """
    try:
        query = """
            SELECT template_id, name, description, version, is_active, created_at, updated_at
            FROM map_template
            WHERE client_id = :client_id OR client_id = 'global'
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """
        
        result = await session.execute(query, {
            "client_id": client_id,
            "limit": limit,
            "offset": offset,
        })
        
        rows = result.fetchall()
        
        templates = []
        for row in rows:
            templates.append({
                "template_id": row[0],
                "name": row[1],
                "description": row[2],
                "version": row[3],
                "is_active": row[4],
                "created_at": row[5],
                "updated_at": row[6],
            })
        
        return {
            "templates": templates,
            "limit": limit,
            "offset": offset,
        }
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{template_id}")
async def get_template(
    template_id: str,
    session=Depends(get_async_session)
):
    """
    Get detailed template information including mappings.
    """
    try:
        # Get template info
        template_query = """
            SELECT template_id, client_id, name, description, version, is_active, created_at, updated_at
            FROM map_template
            WHERE template_id = :template_id
        """
        
        template_result = await session.execute(template_query, {"template_id": template_id})
        template_row = template_result.fetchone()
        
        if not template_row:
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_info = {
            "template_id": template_row[0],
            "client_id": template_row[1],
            "name": template_row[2],
            "description": template_row[3],
            "version": template_row[4],
            "is_active": template_row[5],
            "created_at": template_row[6],
            "updated_at": template_row[7],
        }
        
        # Get field mappings
        field_query = """
            SELECT src_column, target_table, target_column, transform_type, 
                   transform_config, is_required, default_value
            FROM map_field
            WHERE template_id = :template_id
            ORDER BY src_column
        """
        
        field_result = await session.execute(field_query, {"template_id": template_id})
        field_rows = field_result.fetchall()
        
        field_mappings = []
        for row in field_rows:
            field_mappings.append({
                "src_column": row[0],
                "target_table": row[1],
                "target_column": row[2],
                "transform_type": row[3],
                "transform_config": row[4],
                "is_required": row[5],
                "default_value": row[6],
            })
        
        # Get feature mappings
        feature_query = """
            SELECT src_column, etim_feature, unit_column, transform_type,
                   transform_config, is_required
            FROM map_feature
            WHERE template_id = :template_id
            ORDER BY src_column
        """
        
        feature_result = await session.execute(feature_query, {"template_id": template_id})
        feature_rows = feature_result.fetchall()
        
        feature_mappings = []
        for row in feature_rows:
            feature_mappings.append({
                "src_column": row[0],
                "etim_feature": row[1],
                "unit_column": row[2],
                "transform_type": row[3],
                "transform_config": row[4],
                "is_required": row[5],
            })
        
        template_info.update({
            "field_mappings": field_mappings,
            "feature_mappings": feature_mappings,
        })
        
        return template_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/predefined")
async def list_predefined_templates():
    """
    List predefined templates for common use cases.
    """
    # This would typically load from a configuration file or database
    predefined_templates = [
        {
            "template_id": "fastener-basic",
            "name": "Basic Fastener Template",
            "description": "Standard template for screws, bolts, and fasteners",
            "etim_class": "EC000123",
            "field_mappings": [
                {"src_column": "SKU", "target_column": "sku", "required": True},
                {"src_column": "Name", "target_column": "product_name", "required": False},
                {"src_column": "Brand", "target_column": "brand", "required": False},
            ],
            "feature_mappings": [
                {"src_column": "Length", "etim_feature": "EF001001", "unit": "mm"},
                {"src_column": "Diameter", "etim_feature": "EF001002", "unit": "mm"},
                {"src_column": "Material", "etim_feature": "EF001003"},
            ],
        },
        {
            "template_id": "electronics-basic",
            "name": "Basic Electronics Template", 
            "description": "Standard template for electronic components",
            "etim_class": "EC000456",
            "field_mappings": [
                {"src_column": "PartNumber", "target_column": "sku", "required": True},
                {"src_column": "Description", "target_column": "product_name", "required": False},
                {"src_column": "Manufacturer", "target_column": "brand", "required": False},
            ],
            "feature_mappings": [
                {"src_column": "Voltage", "etim_feature": "EF002001", "unit": "V"},
                {"src_column": "Current", "etim_feature": "EF002002", "unit": "A"},
                {"src_column": "Power", "etim_feature": "EF002003", "unit": "W"},
            ],
        },
    ]
    
    return {"predefined_templates": predefined_templates}


@router.post("/predefined/{template_id}/adapt")
async def adapt_predefined_template(
    template_id: str,
    columns: List[str],
    client_id: str
):
    """
    Adapt a predefined template to match specific column names.
    """
    try:
        # Get predefined template
        predefined = await list_predefined_templates()
        template = None
        
        for t in predefined["predefined_templates"]:
            if t["template_id"] == template_id:
                template = t
                break
        
        if not template:
            raise HTTPException(status_code=404, detail="Predefined template not found")
        
        # Simple column matching logic
        adapted_mappings = []
        
        for mapping in template["field_mappings"]:
            suggested_column = mapping["src_column"]
            
            # Try to find matching column
            for col in columns:
                if col.lower() == suggested_column.lower():
                    adapted_mappings.append({
                        **mapping,
                        "src_column": col,
                        "confidence": 1.0,
                    })
                    break
                elif suggested_column.lower() in col.lower() or col.lower() in suggested_column.lower():
                    adapted_mappings.append({
                        **mapping,
                        "src_column": col,
                        "confidence": 0.8,
                    })
                    break
        
        return {
            "adapted_template": {
                **template,
                "field_mappings": adapted_mappings,
                "client_id": client_id,
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adapting template: {e}")
        raise HTTPException(status_code=500, detail=str(e))