"""
Mapping Agent - Applies field mappings and transformations.
"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from io import StringIO

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, FileProfiledEvent, MappingAppliedEvent
from shared.models import TransformType

logger = logging.getLogger(__name__)


@register_agent("mapping")
class MappingAgent(BaseAgent):
    """
    MappingAgent applies field mappings and transformations to raw data.
    
    Responsibilities:
    - Find or suggest mapping templates
    - Apply field mappings to transform raw data
    - Handle data type conversions
    - Apply business rules and transformations
    - Store mapped data in staging
    - Emit mapping.applied events
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.batch_size = self.config["batch_size"]
        
        # Transformation functions registry
        self.transform_functions = {
            TransformType.DIRECT: self._transform_direct,
            TransformType.LOOKUP: self._transform_lookup,
            TransformType.FORMULA: self._transform_formula,
            TransformType.CONCAT: self._transform_concat,
            TransformType.SPLIT: self._transform_split,
            TransformType.REGEX: self._transform_regex,
            TransformType.UOM_CONVERT: self._transform_uom_convert,
        }
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, FileProfiledEvent):
            await self._process_profiled_file(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _process_profiled_file(self, event: FileProfiledEvent):
        """Process a profiled file by applying mappings."""
        try:
            logger.info(f"Processing mappings for client {event.client_id}, ingest {event.ingest_id}")
            
            # Find or create mapping template
            template = await self._get_mapping_template(event)
            if not template:
                logger.warning(f"No mapping template found for {event.ingest_id}")
                await self._handle_no_template_error(event)
                return
            
            # Load raw data
            raw_data = await self._load_raw_data(event)
            
            # Apply mappings in batches
            total_rows = len(raw_data)
            processed_rows = 0
            
            for batch_start in range(0, total_rows, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_rows)
                batch_data = raw_data[batch_start:batch_end]
                
                # Apply mappings to batch
                mapped_batch, issues = await self._apply_mappings(batch_data, template, event)
                
                # Store mapped data
                await self._store_mapped_data(event, mapped_batch, batch_start)
                
                processed_rows += len(batch_data)
                
                # Emit progress event for large files
                if total_rows > self.batch_size:
                    await self._emit_mapping_progress(event, processed_rows, total_rows)
            
            # Emit mapping.applied event
            await self._emit_mapping_applied_event(event, template, processed_rows)
            
            logger.info(f"Successfully applied mappings to {processed_rows} rows for {event.ingest_id}")
            
        except Exception as e:
            logger.error(f"Error applying mappings: {e}")
            await self._handle_mapping_error(event, str(e))
    
    async def _get_mapping_template(self, event: FileProfiledEvent) -> Optional[Dict[str, Any]]:
        """Get or suggest a mapping template for the file."""
        async with self.database_session() as session:
            # First, try to find an existing active template for this client
            query = """
                SELECT template_id, name, version, 
                       (SELECT json_agg(
                           json_build_object(
                               'src_column', src_column,
                               'target_table', target_table,
                               'target_column', target_column,
                               'transform_type', transform_type,
                               'transform_config', transform_config,
                               'is_required', is_required,
                               'default_value', default_value
                           )
                       ) FROM map_field WHERE template_id = mt.template_id) as field_mappings,
                       (SELECT json_agg(
                           json_build_object(
                               'src_column', src_column,
                               'etim_feature', etim_feature,
                               'unit_column', unit_column,
                               'transform_type', transform_type,
                               'transform_config', transform_config,
                               'is_required', is_required
                           )
                       ) FROM map_feature WHERE template_id = mt.template_id) as feature_mappings
                FROM map_template mt
                WHERE client_id = :client_id AND is_active = true
                ORDER BY version DESC
                LIMIT 1
            """
            
            result = await session.execute(query, {"client_id": event.client_id})
            row = result.fetchone()
            
            if row:
                return {
                    "template_id": row[0],
                    "name": row[1],
                    "version": row[2],
                    "field_mappings": row[3] or [],
                    "feature_mappings": row[4] or [],
                }
            
            # If no template exists, suggest one based on column names
            return await self._suggest_mapping_template(event)
    
    async def _suggest_mapping_template(self, event: FileProfiledEvent) -> Optional[Dict[str, Any]]:
        """Suggest a mapping template based on column analysis."""
        columns = event.columns
        column_stats = event.column_stats
        
        # Common field mappings based on column name patterns
        field_mapping_suggestions = []
        feature_mapping_suggestions = []
        
        for column in columns:
            col_lower = column.lower().strip()
            
            # Product core field mappings
            if any(pattern in col_lower for pattern in ['sku', 'article', 'item', 'product_id']):
                field_mapping_suggestions.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'sku',
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': True,
                })
            
            elif any(pattern in col_lower for pattern in ['gtin', 'ean', 'barcode']):
                field_mapping_suggestions.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'gtin',
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
            
            elif any(pattern in col_lower for pattern in ['name', 'title', 'description', 'bezeichnung']):
                field_mapping_suggestions.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'product_name',
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
            
            elif any(pattern in col_lower for pattern in ['brand', 'marke', 'hersteller', 'manufacturer']):
                field_mapping_suggestions.append({
                    'src_column': column,
                    'target_table': 'product',
                    'target_column': 'brand',
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
            
            # ETIM feature mappings
            elif any(pattern in col_lower for pattern in ['length', 'länge', 'laenge']):
                feature_mapping_suggestions.append({
                    'src_column': column,
                    'etim_feature': 'EF001001',  # Length
                    'unit_column': self._find_unit_column(columns, column),
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
            
            elif any(pattern in col_lower for pattern in ['diameter', 'durchmesser', 'width', 'breite']):
                feature_mapping_suggestions.append({
                    'src_column': column,
                    'etim_feature': 'EF001002',  # Diameter
                    'unit_column': self._find_unit_column(columns, column),
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
            
            elif any(pattern in col_lower for pattern in ['material', 'werkstoff']):
                feature_mapping_suggestions.append({
                    'src_column': column,
                    'etim_feature': 'EF001003',  # Material
                    'transform_type': 'direct',
                    'transform_config': {},
                    'is_required': False,
                })
        
        # Create suggested template
        if field_mapping_suggestions or feature_mapping_suggestions:
            template = {
                "template_id": None,  # Will be created if accepted
                "name": f"Suggested template for {event.client_id}",
                "version": 1,
                "field_mappings": field_mapping_suggestions,
                "feature_mappings": feature_mapping_suggestions,
                "is_suggestion": True,
            }
            
            # Store suggestion for later use
            await self._store_template_suggestion(event, template)
            
            return template
        
        return None
    
    def _find_unit_column(self, columns: List[str], value_column: str) -> Optional[str]:
        """Find a potential unit column for a value column."""
        value_col_lower = value_column.lower()
        
        # Look for columns with similar names but containing 'unit', 'einheit', etc.
        for col in columns:
            col_lower = col.lower()
            if col != value_column and any(unit_word in col_lower for unit_word in ['unit', 'einheit', 'uom']):
                # Check if it's related to the value column
                base_name = value_col_lower.replace('_value', '').replace('_val', '').replace('value', '').replace('val', '')
                if base_name in col_lower or any(word in col_lower for word in base_name.split('_')):
                    return col
        
        return None
    
    async def _store_template_suggestion(self, event: FileProfiledEvent, template: Dict[str, Any]):
        """Store template suggestion for review."""
        # For now, just log the suggestion
        # In a full implementation, this could be stored in a suggestions table
        logger.info(f"Template suggestion created for {event.client_id}: {len(template['field_mappings'])} field mappings, {len(template['feature_mappings'])} feature mappings")
    
    async def _load_raw_data(self, event: FileProfiledEvent) -> List[Dict[str, Any]]:
        """Load raw data from the file."""
        # Get file information
        async with self.database_session() as session:
            query = """
                SELECT uri, encoding, delimiter, has_header
                FROM stg_file
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            result = await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            row = result.fetchone()
            if not row:
                raise ValueError(f"File not found: {event.ingest_id}")
            
            uri, encoding, delimiter, has_header = row
        
        # Download and parse file
        object_key = self._extract_object_key(uri)
        file_obj, metadata = await self.storage_manager.download_file(object_key)
        
        file_obj.seek(0)
        content = file_obj.read().decode(encoding)
        
        # Parse CSV
        df = pd.read_csv(
            StringIO(content),
            delimiter=delimiter,
            header=0 if has_header else None,
            dtype=str,  # Keep everything as string for now
            keep_default_na=False,  # Don't convert to NaN
        )
        
        return df.to_dict('records')
    
    def _extract_object_key(self, uri: str) -> str:
        """Extract object key from S3 URI."""
        if uri.startswith("s3://"):
            parts = uri[5:].split("/", 1)
            if len(parts) == 2:
                return parts[1]
        raise ValueError(f"Invalid S3 URI: {uri}")
    
    async def _apply_mappings(self, raw_data: List[Dict[str, Any]], template: Dict[str, Any], event: FileProfiledEvent) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply mappings to a batch of raw data."""
        mapped_rows = []
        issues = []
        
        field_mappings = template.get("field_mappings", [])
        feature_mappings = template.get("feature_mappings", [])
        
        for row_idx, raw_row in enumerate(raw_data):
            try:
                mapped_row = await self._map_single_row(raw_row, field_mappings, feature_mappings, row_idx)
                mapped_rows.append(mapped_row)
            except Exception as e:
                issue = {
                    "row_index": row_idx,
                    "error": str(e),
                    "raw_data": raw_row,
                }
                issues.append(issue)
                logger.warning(f"Error mapping row {row_idx}: {e}")
        
        return mapped_rows, issues
    
    async def _map_single_row(self, raw_row: Dict[str, Any], field_mappings: List[Dict[str, Any]], feature_mappings: List[Dict[str, Any]], row_idx: int) -> Dict[str, Any]:
        """Map a single row of data."""
        mapped_row = {
            "row_index": row_idx,
            "product": {},
            "features": [],
            "issues": [],
        }
        
        # Apply field mappings (product core data)
        for mapping in field_mappings:
            try:
                value = await self._apply_field_mapping(raw_row, mapping)
                if value is not None:
                    mapped_row["product"][mapping["target_column"]] = value
            except Exception as e:
                mapped_row["issues"].append({
                    "type": "field_mapping_error",
                    "field": mapping["src_column"],
                    "error": str(e),
                })
        
        # Apply feature mappings (ETIM features)
        for mapping in feature_mappings:
            try:
                feature = await self._apply_feature_mapping(raw_row, mapping)
                if feature:
                    mapped_row["features"].append(feature)
            except Exception as e:
                mapped_row["issues"].append({
                    "type": "feature_mapping_error",
                    "feature": mapping["etim_feature"],
                    "error": str(e),
                })
        
        return mapped_row
    
    async def _apply_field_mapping(self, raw_row: Dict[str, Any], mapping: Dict[str, Any]) -> Any:
        """Apply a single field mapping."""
        src_column = mapping["src_column"]
        transform_type = mapping.get("transform_type", "direct")
        transform_config = mapping.get("transform_config", {})
        default_value = mapping.get("default_value")
        
        # Get source value
        raw_value = raw_row.get(src_column)
        
        # Handle missing values
        if raw_value is None or raw_value == "":
            if mapping.get("is_required", False):
                raise ValueError(f"Required field '{src_column}' is missing")
            return default_value
        
        # Apply transformation
        transform_func = self.transform_functions.get(transform_type)
        if not transform_func:
            raise ValueError(f"Unknown transform type: {transform_type}")
        
        return await transform_func(raw_value, transform_config, raw_row)
    
    async def _apply_feature_mapping(self, raw_row: Dict[str, Any], mapping: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply a single feature mapping."""
        src_column = mapping["src_column"]
        etim_feature = mapping["etim_feature"]
        unit_column = mapping.get("unit_column")
        transform_type = mapping.get("transform_type", "direct")
        transform_config = mapping.get("transform_config", {})
        
        # Get source value
        raw_value = raw_row.get(src_column)
        if raw_value is None or raw_value == "":
            if mapping.get("is_required", False):
                raise ValueError(f"Required feature '{src_column}' is missing")
            return None
        
        # Get unit value if specified
        unit_raw = None
        if unit_column:
            unit_raw = raw_row.get(unit_column)
        
        # Apply transformation
        transform_func = self.transform_functions.get(transform_type)
        if not transform_func:
            raise ValueError(f"Unknown transform type: {transform_type}")
        
        transformed_value = await transform_func(raw_value, transform_config, raw_row)
        
        return {
            "etim_feature": etim_feature,
            "value_raw": str(raw_value),
            "unit_raw": unit_raw,
            "value_transformed": transformed_value,
            "source_column": src_column,
        }
    
    # Transformation functions
    
    async def _transform_direct(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> Any:
        """Direct value pass-through with optional cleaning."""
        if isinstance(value, str):
            value = value.strip()
            
            # Apply cleaning rules from config
            if config.get("remove_quotes", False):
                value = value.strip('"\'')
            
            if config.get("uppercase", False):
                value = value.upper()
            elif config.get("lowercase", False):
                value = value.lower()
        
        return value
    
    async def _transform_lookup(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> Any:
        """Transform value using lookup table."""
        lookup_table = config.get("lookup_table", {})
        default_value = config.get("default_value")
        
        # Convert value to string for lookup
        lookup_key = str(value).strip()
        
        # Try exact match first
        if lookup_key in lookup_table:
            return lookup_table[lookup_key]
        
        # Try case-insensitive match
        for key, mapped_value in lookup_table.items():
            if key.lower() == lookup_key.lower():
                return mapped_value
        
        # Return default or original value
        return default_value if default_value is not None else value
    
    async def _transform_formula(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> Any:
        """Transform value using a formula."""
        formula = config.get("formula", "")
        
        if not formula:
            return value
        
        # Simple formula evaluation (could be extended with a proper expression parser)
        try:
            # Replace placeholders with actual values
            eval_formula = formula
            eval_formula = eval_formula.replace("{value}", str(value))
            
            # Replace other column references
            for col_name, col_value in row.items():
                placeholder = f"{{{col_name}}}"
                if placeholder in eval_formula:
                    eval_formula = eval_formula.replace(placeholder, str(col_value))
            
            # Evaluate simple mathematical expressions
            # Note: In production, use a safer expression evaluator
            if re.match(r'^[\d\+\-\*\/\.\(\)\s]+$', eval_formula):
                return eval(eval_formula)
            
        except Exception as e:
            logger.warning(f"Formula evaluation failed: {e}")
        
        return value
    
    async def _transform_concat(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> str:
        """Concatenate multiple values."""
        columns = config.get("columns", [])
        separator = config.get("separator", " ")
        
        values = [str(value)]
        
        for col_name in columns:
            if col_name in row and row[col_name]:
                values.append(str(row[col_name]))
        
        return separator.join(values)
    
    async def _transform_split(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> str:
        """Split value and return specified part."""
        separator = config.get("separator", " ")
        index = config.get("index", 0)
        
        parts = str(value).split(separator)
        
        if 0 <= index < len(parts):
            return parts[index].strip()
        
        return str(value)
    
    async def _transform_regex(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> str:
        """Transform value using regex."""
        pattern = config.get("pattern", "")
        replacement = config.get("replacement", "")
        
        if not pattern:
            return str(value)
        
        try:
            return re.sub(pattern, replacement, str(value))
        except Exception as e:
            logger.warning(f"Regex transformation failed: {e}")
            return str(value)
    
    async def _transform_uom_convert(self, value: Any, config: Dict[str, Any], row: Dict[str, Any]) -> float:
        """Convert units of measure."""
        from_unit = config.get("from_unit", "")
        to_unit = config.get("to_unit", "")
        
        if not from_unit or not to_unit:
            return float(value)
        
        # Get conversion factor from database
        async with self.database_session() as session:
            query = """
                SELECT conversion_factor, conversion_offset
                FROM uom_mapping
                WHERE source_unit = :from_unit AND target_unit = :to_unit
            """
            
            result = await session.execute(query, {
                "from_unit": from_unit,
                "to_unit": to_unit,
            })
            
            row_result = result.fetchone()
            if row_result:
                factor, offset = row_result
                return float(value) * factor + offset
        
        # No conversion found, return original value
        return float(value)
    
    async def _store_mapped_data(self, event: FileProfiledEvent, mapped_data: List[Dict[str, Any]], batch_start: int):
        """Store mapped data in staging tables."""
        async with self.database_session() as session:
            for row_idx, mapped_row in enumerate(mapped_data):
                global_row_idx = batch_start + row_idx
                
                # Store in stg_row table
                query = """
                    INSERT INTO stg_row (
                        client_id, ingest_id, row_num, data, issues
                    ) VALUES (
                        :client_id, :ingest_id, :row_num, :data, :issues
                    )
                    ON CONFLICT (client_id, ingest_id, row_num)
                    DO UPDATE SET
                        data = EXCLUDED.data,
                        issues = EXCLUDED.issues,
                        updated_at = NOW()
                """
                
                await session.execute(query, {
                    "client_id": event.client_id,
                    "ingest_id": event.ingest_id,
                    "row_num": global_row_idx,
                    "data": mapped_row,
                    "issues": mapped_row.get("issues", []),
                })
            
            await session.commit()
    
    async def _emit_mapping_progress(self, event: FileProfiledEvent, processed_rows: int, total_rows: int):
        """Emit progress update for large files."""
        # Could emit a progress event here
        logger.info(f"Mapping progress: {processed_rows}/{total_rows} rows processed")
    
    async def _emit_mapping_applied_event(self, event: FileProfiledEvent, template: Dict[str, Any], processed_rows: int):
        """Emit mapping.applied event."""
        mapping_applied_event = MappingAppliedEvent(
            event_id=f"mapped-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"mapped-{event.client_id}-{event.ingest_id}",
            template_id=template.get("template_id"),
            template_version=template.get("version", 1),
            page_number=1,  # For now, treating as single page
            total_pages=1,
            rows_processed=processed_rows,
            mapped_rows=[],  # Don't include actual data in event
            mapping_issues=[],
        )
        
        await self.publish_event(mapping_applied_event)
    
    async def _handle_no_template_error(self, event: FileProfiledEvent):
        """Handle case where no mapping template is available."""
        logger.error(f"No mapping template available for {event.ingest_id}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'mapping_template_missing', 
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def _handle_mapping_error(self, event: FileProfiledEvent, error_message: str):
        """Handle mapping errors."""
        logger.error(f"Mapping error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'mapping_failed', 
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
        logger.info("MappingAgent initialized")
        
        # Could load common lookup tables, initialize ML models for mapping suggestions, etc.