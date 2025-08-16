"""
Normalization Agent - Handles data normalization and unit conversion.
"""

import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, ValidationResultsEvent, NormalizationDoneEvent

logger = logging.getLogger(__name__)


@register_agent("normalization")
class NormalizationAgent(BaseAgent):
    """
    NormalizationAgent normalizes and standardizes data values.
    
    Responsibilities:
    - Unit of measure conversion
    - Currency normalization
    - Date/time standardization
    - Text normalization (case, encoding)
    - Numeric precision standardization
    - GTIN validation and formatting
    - Locale-specific transformations
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.batch_size = self.config["batch_size"]
        
        # Currency conversion rates (would be loaded from external service)
        self.currency_rates = {
            "USD": 1.0,
            "EUR": 0.85,
            "GBP": 0.73,
            "JPY": 110.0,
        }
        
        # Common unit conversions
        self.unit_conversions = {}
        self._load_unit_conversions()
        
        # Text normalization patterns
        self.text_patterns = {
            "whitespace": re.compile(r'\s+'),
            "special_chars": re.compile(r'[^\w\s\-\.]'),
            "multiple_spaces": re.compile(r'\s{2,}'),
        }
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, ValidationResultsEvent):
            # Only process if validation passed or has warnings
            if event.gate_status in ["PASS", "WARN"]:
                await self._normalize_data(event)
            else:
                logger.info(f"Skipping normalization for failed validation: {event.ingest_id}")
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _normalize_data(self, event: ValidationResultsEvent):
        """Normalize validated data."""
        try:
            logger.info(f"Normalizing data for client {event.client_id}, ingest {event.ingest_id}")
            
            # Load mapped data
            mapped_data = await self._load_mapped_data(event)
            
            # Normalize data in batches
            total_rows = len(mapped_data)
            processed_rows = 0
            normalization_issues = []
            
            for batch_start in range(0, total_rows, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_rows)
                batch_data = mapped_data[batch_start:batch_end]
                
                # Normalize batch
                normalized_batch, batch_issues = await self._normalize_batch(batch_data, event)
                normalization_issues.extend(batch_issues)
                
                # Store normalized data
                await self._store_normalized_data(event, normalized_batch, batch_start)
                
                processed_rows += len(batch_data)
                
                # Emit progress for large files
                if total_rows > self.batch_size:
                    await self._emit_normalization_progress(event, processed_rows, total_rows)
            
            # Update job status
            await self._update_job_status(event, "normalized")
            
            # Emit normalization.done event
            await self._emit_normalization_done_event(event, processed_rows, normalization_issues)
            
            logger.info(f"Successfully normalized {processed_rows} rows for {event.ingest_id}")
            
        except Exception as e:
            logger.error(f"Error normalizing data: {e}")
            await self._handle_normalization_error(event, str(e))
    
    async def _load_mapped_data(self, event: ValidationResultsEvent) -> List[Dict[str, Any]]:
        """Load mapped data from staging."""
        async with self.database_session() as session:
            query = """
                SELECT row_num, data
                FROM stg_row
                WHERE client_id = :client_id AND ingest_id = :ingest_id
                ORDER BY row_num
            """
            
            result = await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            rows = result.fetchall()
            return [{"row_num": row[0], **row[1]} for row in rows]
    
    async def _normalize_batch(self, batch_data: List[Dict[str, Any]], event: ValidationResultsEvent) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Normalize a batch of data."""
        normalized_rows = []
        issues = []
        
        for row_data in batch_data:
            try:
                normalized_row = await self._normalize_single_row(row_data, event)
                normalized_rows.append(normalized_row)
            except Exception as e:
                issue = {
                    "row_num": row_data.get("row_num"),
                    "error": str(e),
                    "type": "normalization_error",
                }
                issues.append(issue)
                logger.warning(f"Error normalizing row {row_data.get('row_num')}: {e}")
                
                # Add row with original data and error flag
                normalized_row = row_data.copy()
                normalized_row["normalization_error"] = str(e)
                normalized_rows.append(normalized_row)
        
        return normalized_rows, issues
    
    async def _normalize_single_row(self, row_data: Dict[str, Any], event: ValidationResultsEvent) -> Dict[str, Any]:
        """Normalize a single row of data."""
        normalized_row = row_data.copy()
        
        # Normalize product core data
        product_data = row_data.get("product", {})
        normalized_product = await self._normalize_product_data(product_data)
        normalized_row["product"] = normalized_product
        
        # Normalize features
        features = row_data.get("features", [])
        normalized_features = []
        
        for feature in features:
            normalized_feature = await self._normalize_feature_data(feature)
            normalized_features.append(normalized_feature)
        
        normalized_row["features"] = normalized_features
        
        # Add normalization metadata
        normalized_row["normalized_at"] = datetime.utcnow().isoformat()
        
        return normalized_row
    
    async def _normalize_product_data(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize product core data."""
        normalized = product_data.copy()
        
        # Normalize SKU
        if "sku" in normalized:
            normalized["sku"] = self._normalize_text(normalized["sku"], uppercase=True)
        
        # Normalize GTIN
        if "gtin" in normalized:
            normalized["gtin"] = self._normalize_gtin(normalized["gtin"])
        
        # Normalize product name
        if "product_name" in normalized:
            normalized["product_name"] = self._normalize_text(normalized["product_name"])
        
        # Normalize brand
        if "brand" in normalized:
            normalized["brand"] = self._normalize_text(normalized["brand"])
        
        # Normalize prices
        for price_field in ["price", "list_price", "cost_price"]:
            if price_field in normalized:
                normalized[price_field] = self._normalize_price(normalized[price_field])
        
        # Normalize currency
        if "currency" in normalized:
            normalized["currency"] = self._normalize_currency(normalized["currency"])
        
        # Normalize dimensions
        for dim_field in ["length", "width", "height", "diameter"]:
            if dim_field in normalized:
                normalized[dim_field] = self._normalize_numeric(normalized[dim_field])
        
        # Normalize weight
        if "weight" in normalized:
            normalized["weight"] = self._normalize_numeric(normalized["weight"])
        
        return normalized
    
    async def _normalize_feature_data(self, feature_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize ETIM feature data."""
        normalized = feature_data.copy()
        
        # Get feature information for proper normalization
        etim_feature = feature_data.get("etim_feature")
        if etim_feature:
            feature_info = await self._get_etim_feature_info(etim_feature)
            
            # Normalize value based on ETIM data type
            raw_value = feature_data.get("value_raw")
            if raw_value is not None:
                normalized_value = await self._normalize_etim_value(
                    raw_value, feature_info, feature_data.get("unit_raw")
                )
                normalized["value_normalized"] = normalized_value
                
                # Convert units if needed
                if feature_data.get("unit_raw") and feature_info.get("standard_unit"):
                    converted_value, converted_unit = await self._convert_unit(
                        normalized_value, feature_data["unit_raw"], feature_info["standard_unit"]
                    )
                    normalized["value_converted"] = converted_value
                    normalized["unit_normalized"] = converted_unit
                else:
                    normalized["value_converted"] = normalized_value
                    normalized["unit_normalized"] = feature_data.get("unit_raw")
        
        return normalized
    
    async def _get_etim_feature_info(self, etim_feature: str) -> Dict[str, Any]:
        """Get ETIM feature information from database."""
        async with self.database_session() as session:
            query = """
                SELECT data_type, unit, value_list
                FROM etim_feature
                WHERE feature_code = :feature_code
            """
            
            result = await session.execute(query, {"feature_code": etim_feature})
            row = result.fetchone()
            
            if row:
                return {
                    "data_type": row[0],
                    "standard_unit": row[1],
                    "value_list": row[2],
                }
            
            return {}
    
    async def _normalize_etim_value(self, value: Any, feature_info: Dict[str, Any], unit: Optional[str]) -> Any:
        """Normalize ETIM feature value based on data type."""
        data_type = feature_info.get("data_type", "A")  # Default to alphanumeric
        
        if data_type == "N":  # Numeric
            return self._normalize_numeric(value)
        elif data_type == "A":  # Alphanumeric
            return self._normalize_text(value)
        elif data_type == "L":  # Logical (boolean)
            return self._normalize_boolean(value)
        else:
            return self._normalize_text(value)
    
    def _normalize_text(self, value: Any, uppercase: bool = False, lowercase: bool = False) -> str:
        """Normalize text values."""
        if value is None:
            return ""
        
        text = str(value).strip()
        
        # Remove extra whitespace
        text = self.text_patterns["multiple_spaces"].sub(" ", text)
        
        # Apply case transformation
        if uppercase:
            text = text.upper()
        elif lowercase:
            text = text.lower()
        
        return text
    
    def _normalize_numeric(self, value: Any, precision: int = 6) -> Optional[Decimal]:
        """Normalize numeric values."""
        if value is None or value == "":
            return None
        
        try:
            # Handle different numeric formats
            str_value = str(value).strip()
            
            # Remove common thousands separators
            str_value = str_value.replace(",", "").replace(" ", "")
            
            # Handle European decimal separator
            if "." in str_value and str_value.count(".") == 1:
                # Check if it's likely a thousands separator
                parts = str_value.split(".")
                if len(parts[1]) == 3 and parts[1].isdigit():
                    # Likely thousands separator, remove it
                    str_value = parts[0] + parts[1]
                # Otherwise treat as decimal separator
            
            decimal_value = Decimal(str_value)
            
            # Round to specified precision
            return decimal_value.quantize(Decimal(10) ** -precision)
            
        except (InvalidOperation, ValueError):
            logger.warning(f"Could not normalize numeric value: {value}")
            return None
    
    def _normalize_price(self, value: Any) -> Optional[Decimal]:
        """Normalize price values."""
        normalized = self._normalize_numeric(value, precision=2)
        
        # Ensure non-negative
        if normalized is not None and normalized < 0:
            logger.warning(f"Negative price normalized to zero: {value}")
            return Decimal("0.00")
        
        return normalized
    
    def _normalize_currency(self, value: Any) -> str:
        """Normalize currency codes."""
        if value is None:
            return "USD"  # Default currency
        
        currency = str(value).strip().upper()
        
        # Map common currency variations
        currency_mappings = {
            "EURO": "EUR",
            "EUROS": "EUR",
            "DOLLAR": "USD",
            "DOLLARS": "USD",
            "POUND": "GBP",
            "POUNDS": "GBP",
            "YEN": "JPY",
        }
        
        return currency_mappings.get(currency, currency)
    
    def _normalize_gtin(self, value: Any) -> Optional[str]:
        """Normalize GTIN values."""
        if value is None or value == "":
            return None
        
        gtin = str(value).strip()
        
        # Remove any non-digit characters
        gtin = re.sub(r'\D', '', gtin)
        
        # Validate length
        if len(gtin) not in [8, 12, 13, 14]:
            logger.warning(f"Invalid GTIN length: {gtin}")
            return None
        
        # Pad with zeros if needed (for UPC-A to GTIN-14)
        if len(gtin) == 12:
            gtin = "0" + gtin  # UPC-A to GTIN-13
        elif len(gtin) == 13:
            gtin = "0" + gtin  # GTIN-13 to GTIN-14
        
        return gtin
    
    def _normalize_boolean(self, value: Any) -> Optional[bool]:
        """Normalize boolean values."""
        if value is None or value == "":
            return None
        
        str_value = str(value).strip().lower()
        
        true_values = {"true", "yes", "y", "1", "on", "enabled", "ja", "oui", "si"}
        false_values = {"false", "no", "n", "0", "off", "disabled", "nein", "non"}
        
        if str_value in true_values:
            return True
        elif str_value in false_values:
            return False
        else:
            logger.warning(f"Could not normalize boolean value: {value}")
            return None
    
    async def _convert_unit(self, value: Decimal, from_unit: str, to_unit: str) -> Tuple[Decimal, str]:
        """Convert units of measure."""
        if from_unit == to_unit or not value:
            return value, to_unit
        
        # Get conversion factor from database
        conversion_factor = await self._get_unit_conversion_factor(from_unit, to_unit)
        
        if conversion_factor:
            converted_value = value * Decimal(str(conversion_factor))
            return converted_value, to_unit
        
        # No conversion available, return original
        logger.warning(f"No unit conversion available: {from_unit} -> {to_unit}")
        return value, from_unit
    
    async def _get_unit_conversion_factor(self, from_unit: str, to_unit: str) -> Optional[float]:
        """Get unit conversion factor from database."""
        async with self.database_session() as session:
            query = """
                SELECT conversion_factor
                FROM uom_mapping
                WHERE source_unit = :from_unit AND target_unit = :to_unit
            """
            
            result = await session.execute(query, {
                "from_unit": from_unit,
                "to_unit": to_unit,
            })
            
            row = result.fetchone()
            return row[0] if row else None
    
    def _load_unit_conversions(self):
        """Load common unit conversions."""
        # Length conversions (to mm)
        length_conversions = {
            ("m", "mm"): 1000.0,
            ("cm", "mm"): 10.0,
            ("in", "mm"): 25.4,
            ("ft", "mm"): 304.8,
        }
        
        # Weight conversions (to g)
        weight_conversions = {
            ("kg", "g"): 1000.0,
            ("lb", "g"): 453.592,
            ("oz", "g"): 28.3495,
        }
        
        # Volume conversions (to ml)
        volume_conversions = {
            ("l", "ml"): 1000.0,
            ("gal", "ml"): 3785.41,
            ("qt", "ml"): 946.353,
        }
        
        self.unit_conversions.update(length_conversions)
        self.unit_conversions.update(weight_conversions)
        self.unit_conversions.update(volume_conversions)
    
    async def _store_normalized_data(self, event: ValidationResultsEvent, normalized_data: List[Dict[str, Any]], batch_start: int):
        """Store normalized data back to staging."""
        async with self.database_session() as session:
            for row_idx, normalized_row in enumerate(normalized_data):
                global_row_idx = batch_start + row_idx
                
                # Update stg_row with normalized data
                query = """
                    UPDATE stg_row 
                    SET data = :data, updated_at = NOW()
                    WHERE client_id = :client_id AND ingest_id = :ingest_id AND row_num = :row_num
                """
                
                await session.execute(query, {
                    "client_id": event.client_id,
                    "ingest_id": event.ingest_id,
                    "row_num": global_row_idx,
                    "data": normalized_row,
                })
            
            await session.commit()
    
    async def _emit_normalization_progress(self, event: ValidationResultsEvent, processed_rows: int, total_rows: int):
        """Emit progress update for large files."""
        logger.info(f"Normalization progress: {processed_rows}/{total_rows} rows processed")
    
    async def _update_job_status(self, event: ValidationResultsEvent, status: str):
        """Update job status."""
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = :status, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "status": status,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            # Update import_job status
            job_query = """
                UPDATE import_job 
                SET status = :status, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(job_query, {
                "status": "normalizing",
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def _emit_normalization_done_event(self, event: ValidationResultsEvent, processed_rows: int, issues: List[Dict[str, Any]]):
        """Emit normalization.done event."""
        normalization_event = NormalizationDoneEvent(
            event_id=f"normalized-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"normalized-{event.client_id}-{event.ingest_id}",
            rows_processed=processed_rows,
            normalization_issues=issues,
            quality_score=event.quality_metrics.get("overall_score", 0.0),
        )
        
        await self.publish_event(normalization_event)
    
    async def _handle_normalization_error(self, event: ValidationResultsEvent, error_message: str):
        """Handle normalization errors."""
        logger.error(f"Normalization error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'normalization_failed', 
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
        logger.info("NormalizationAgent initialized")
        
        # Load unit conversion tables from database
        await self._load_database_conversions()
    
    async def _load_database_conversions(self):
        """Load unit conversions from database."""
        try:
            async with self.database_session() as session:
                query = """
                    SELECT source_unit, target_unit, conversion_factor
                    FROM uom_mapping
                    WHERE is_active = true
                """
                
                result = await session.execute(query)
                rows = result.fetchall()
                
                for row in rows:
                    source_unit, target_unit, factor = row
                    self.unit_conversions[(source_unit, target_unit)] = factor
                
                logger.info(f"Loaded {len(rows)} unit conversions from database")
                
        except Exception as e:
            logger.warning(f"Could not load unit conversions from database: {e}")
            # Continue with hardcoded conversions