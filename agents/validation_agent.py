"""
Validation Agent - Validates mapped data against business rules.
"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, MappingAppliedEvent, ValidationResultsEvent
from shared.models import ValidationSeverity, RuleType

logger = logging.getLogger(__name__)


@register_agent("validation")
class ValidationAgent(BaseAgent):
    """
    ValidationAgent validates mapped data against configurable business rules.
    
    Responsibilities:
    - Load validation rules for client/template
    - Apply validation rules to mapped data
    - Calculate data quality metrics
    - Determine quality gate status
    - Store validation results
    - Emit validation.results events
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.quality_threshold = self.settings.quality_gate_threshold
        
        # Rule validators registry
        self.rule_validators = {
            RuleType.REQUIRED: self._validate_required,
            RuleType.FORMAT: self._validate_format,
            RuleType.RANGE: self._validate_range,
            RuleType.DOMAIN: self._validate_domain,
            RuleType.ETIM: self._validate_etim,
            RuleType.CUSTOM: self._validate_custom,
        }
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, MappingAppliedEvent):
            await self._validate_mapped_data(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _validate_mapped_data(self, event: MappingAppliedEvent):
        """Validate mapped data against business rules."""
        try:
            logger.info(f"Validating data for client {event.client_id}, ingest {event.ingest_id}")
            
            # Load validation rules
            rules = await self._load_validation_rules(event)
            if not rules:
                logger.info(f"No validation rules found for {event.ingest_id}")
                await self._emit_validation_results_event(event, [], {}, "PASS")
                return
            
            # Load mapped data
            mapped_data = await self._load_mapped_data(event)
            
            # Apply validation rules
            validation_results = []
            for row_data in mapped_data:
                row_results = await self._validate_single_row(row_data, rules, event)
                validation_results.extend(row_results)
            
            # Calculate quality metrics
            quality_metrics = self._calculate_quality_metrics(validation_results, len(mapped_data))
            
            # Determine quality gate status
            gate_status = self._determine_gate_status(quality_metrics, validation_results)
            
            # Store validation results
            await self._store_validation_results(event, validation_results)
            
            # Update job status
            await self._update_job_status(event, gate_status)
            
            # Emit validation.results event
            await self._emit_validation_results_event(event, validation_results, quality_metrics, gate_status)
            
            logger.info(f"Validation completed for {event.ingest_id}: {gate_status} with {len(validation_results)} issues")
            
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            await self._handle_validation_error(event, str(e))
    
    async def _load_validation_rules(self, event: MappingAppliedEvent) -> List[Dict[str, Any]]:
        """Load validation rules for the client and template."""
        async with self.database_session() as session:
            # Load active validation rules
            query = """
                SELECT rule_id, name, description, rule_type, target_type, 
                       target_name, rule_config, severity
                FROM validation_rule
                WHERE is_active = true
                ORDER BY severity DESC, name
            """
            
            result = await session.execute(query)
            rows = result.fetchall()
            
            rules = []
            for row in rows:
                rules.append({
                    "rule_id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "rule_type": row[3],
                    "target_type": row[4],
                    "target_name": row[5],
                    "rule_config": row[6],
                    "severity": row[7],
                })
            
            return rules
    
    async def _load_mapped_data(self, event: MappingAppliedEvent) -> List[Dict[str, Any]]:
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
    
    async def _validate_single_row(self, row_data: Dict[str, Any], rules: List[Dict[str, Any]], event: MappingAppliedEvent) -> List[Dict[str, Any]]:
        """Validate a single row against all applicable rules."""
        results = []
        
        for rule in rules:
            try:
                # Check if rule applies to this row
                if not self._rule_applies_to_row(rule, row_data):
                    continue
                
                # Apply validation rule
                validation_result = await self._apply_validation_rule(rule, row_data, event)
                
                if validation_result:
                    results.append(validation_result)
                    
            except Exception as e:
                logger.error(f"Error applying rule {rule['name']}: {e}")
                # Create error result
                results.append({
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["name"],
                    "row_num": row_data.get("row_num"),
                    "severity": "ERROR",
                    "message": f"Rule validation failed: {e}",
                    "target_type": rule["target_type"],
                    "target_name": rule["target_name"],
                    "context": {"error": str(e)},
                })
        
        return results
    
    def _rule_applies_to_row(self, rule: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """Check if a validation rule applies to the given row."""
        target_type = rule["target_type"]
        target_name = rule["target_name"]
        
        if target_type == "field":
            # Check if the field exists in product data
            product_data = row_data.get("product", {})
            return target_name in product_data
        
        elif target_type == "feature":
            # Check if the feature exists in features list
            features = row_data.get("features", [])
            return any(f.get("etim_feature") == target_name for f in features)
        
        elif target_type == "product":
            # Product-level rules always apply
            return True
        
        return False
    
    async def _apply_validation_rule(self, rule: Dict[str, Any], row_data: Dict[str, Any], event: MappingAppliedEvent) -> Optional[Dict[str, Any]]:
        """Apply a single validation rule to a row."""
        rule_type = rule["rule_type"]
        validator = self.rule_validators.get(rule_type)
        
        if not validator:
            logger.warning(f"Unknown rule type: {rule_type}")
            return None
        
        # Get the value to validate
        value = self._extract_value_for_rule(rule, row_data)
        
        # Apply the validator
        is_valid, error_message = await validator(value, rule["rule_config"], row_data, rule)
        
        if not is_valid:
            return {
                "rule_id": rule["rule_id"],
                "rule_name": rule["name"],
                "row_num": row_data.get("row_num"),
                "severity": rule["severity"],
                "message": error_message,
                "target_type": rule["target_type"],
                "target_name": rule["target_name"],
                "context": {
                    "value": str(value) if value is not None else None,
                    "rule_config": rule["rule_config"],
                },
            }
        
        return None
    
    def _extract_value_for_rule(self, rule: Dict[str, Any], row_data: Dict[str, Any]) -> Any:
        """Extract the value to validate based on rule target."""
        target_type = rule["target_type"]
        target_name = rule["target_name"]
        
        if target_type == "field":
            product_data = row_data.get("product", {})
            return product_data.get(target_name)
        
        elif target_type == "feature":
            features = row_data.get("features", [])
            for feature in features:
                if feature.get("etim_feature") == target_name:
                    return feature.get("value_transformed") or feature.get("value_raw")
            return None
        
        elif target_type == "product":
            # For product-level rules, return the entire product data
            return row_data.get("product", {})
        
        return None
    
    # Validation rule implementations
    
    async def _validate_required(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate that a required field has a value."""
        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
            return False, f"Required field '{rule['target_name']}' is missing or empty"
        
        return True, ""
    
    async def _validate_format(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate that a value matches a specific format pattern."""
        if value is None or value == "":
            return True, ""  # Empty values are handled by required rule
        
        pattern = config.get("pattern", "")
        if not pattern:
            return True, ""
        
        try:
            if not re.match(pattern, str(value)):
                return False, f"Value '{value}' does not match required format pattern"
        except re.error as e:
            return False, f"Invalid regex pattern: {e}"
        
        return True, ""
    
    async def _validate_range(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate that a numeric value is within a specified range."""
        if value is None or value == "":
            return True, ""
        
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return False, f"Value '{value}' is not numeric"
        
        min_val = config.get("min")
        max_val = config.get("max")
        exclusive_min = config.get("exclusive_min", False)
        exclusive_max = config.get("exclusive_max", False)
        
        if min_val is not None:
            if exclusive_min and numeric_value <= min_val:
                return False, f"Value {numeric_value} must be greater than {min_val}"
            elif not exclusive_min and numeric_value < min_val:
                return False, f"Value {numeric_value} must be greater than or equal to {min_val}"
        
        if max_val is not None:
            if exclusive_max and numeric_value >= max_val:
                return False, f"Value {numeric_value} must be less than {max_val}"
            elif not exclusive_max and numeric_value > max_val:
                return False, f"Value {numeric_value} must be less than or equal to {max_val}"
        
        return True, ""
    
    async def _validate_domain(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate that a value is from an allowed set of values."""
        if value is None or value == "":
            return True, ""
        
        allowed_values = config.get("values", [])
        if not allowed_values:
            return True, ""
        
        case_sensitive = config.get("case_sensitive", True)
        
        if case_sensitive:
            if str(value) not in allowed_values:
                return False, f"Value '{value}' is not in allowed list: {allowed_values}"
        else:
            if str(value).lower() not in [str(v).lower() for v in allowed_values]:
                return False, f"Value '{value}' is not in allowed list: {allowed_values}"
        
        return True, ""
    
    async def _validate_etim(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate ETIM-specific rules."""
        if value is None or value == "":
            # Check if this is a mandatory ETIM feature
            etim_feature = rule["target_name"]
            
            # Get product's ETIM class
            product_data = row_data.get("product", {})
            etim_class = product_data.get("etim_class")
            
            if etim_class:
                # Check if feature is mandatory for this class
                is_mandatory = await self._is_etim_feature_mandatory(etim_class, etim_feature)
                if is_mandatory:
                    return False, f"ETIM feature '{etim_feature}' is mandatory for class '{etim_class}'"
        
        # Additional ETIM validations could be added here
        # - Data type validation (numeric, alphanumeric, logical)
        # - Unit validation
        # - Value range validation based on ETIM specifications
        
        return True, ""
    
    async def _is_etim_feature_mandatory(self, etim_class: str, etim_feature: str) -> bool:
        """Check if an ETIM feature is mandatory for a given class."""
        async with self.database_session() as session:
            query = """
                SELECT is_mandatory
                FROM etim_class_feature
                WHERE class_code = :class_code AND feature_code = :feature_code
            """
            
            result = await session.execute(query, {
                "class_code": etim_class,
                "feature_code": etim_feature,
            })
            
            row = result.fetchone()
            return row[0] if row else False
    
    async def _validate_custom(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any], rule: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate using custom business logic."""
        # Custom validation logic would be implemented here
        # This could include complex business rules, external API calls, etc.
        
        custom_rule = config.get("custom_rule", "")
        
        if custom_rule == "gtin_checksum":
            return self._validate_gtin_checksum(value)
        elif custom_rule == "sku_format":
            return self._validate_sku_format(value, config)
        
        return True, ""
    
    def _validate_gtin_checksum(self, value: Any) -> Tuple[bool, str]:
        """Validate GTIN checksum."""
        if not value:
            return True, ""
        
        gtin = str(value).strip()
        
        # GTIN must be 8, 12, 13, or 14 digits
        if not gtin.isdigit() or len(gtin) not in [8, 12, 13, 14]:
            return False, f"GTIN '{gtin}' must be 8, 12, 13, or 14 digits"
        
        # Calculate checksum
        try:
            digits = [int(d) for d in gtin]
            check_digit = digits[-1]
            
            # Calculate expected check digit
            sum_odd = sum(digits[i] for i in range(len(digits) - 1) if i % 2 == 0)
            sum_even = sum(digits[i] for i in range(len(digits) - 1) if i % 2 == 1)
            
            total = sum_odd + (sum_even * 3)
            expected_check = (10 - (total % 10)) % 10
            
            if check_digit != expected_check:
                return False, f"GTIN '{gtin}' has invalid checksum"
            
        except Exception as e:
            return False, f"Error validating GTIN checksum: {e}"
        
        return True, ""
    
    def _validate_sku_format(self, value: Any, config: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate SKU format according to client-specific rules."""
        if not value:
            return True, ""
        
        sku = str(value).strip()
        
        # Example: SKU must be alphanumeric and 6-20 characters
        min_length = config.get("min_length", 6)
        max_length = config.get("max_length", 20)
        allow_special_chars = config.get("allow_special_chars", ["-", "_"])
        
        if len(sku) < min_length or len(sku) > max_length:
            return False, f"SKU '{sku}' must be between {min_length} and {max_length} characters"
        
        # Check allowed characters
        allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        allowed_chars.update(allow_special_chars)
        
        if not all(c in allowed_chars for c in sku):
            return False, f"SKU '{sku}' contains invalid characters"
        
        return True, ""
    
    def _calculate_quality_metrics(self, validation_results: List[Dict[str, Any]], total_rows: int) -> Dict[str, float]:
        """Calculate data quality metrics."""
        if total_rows == 0:
            return {
                "completeness": 1.0,
                "accuracy": 1.0,
                "consistency": 1.0,
                "validity": 1.0,
                "overall_score": 1.0,
            }
        
        # Count issues by severity
        error_count = sum(1 for r in validation_results if r["severity"] == "ERROR")
        warning_count = sum(1 for r in validation_results if r["severity"] == "WARN")
        
        # Calculate metrics
        validity = 1.0 - (error_count / total_rows)
        consistency = 1.0 - (warning_count / total_rows)
        
        # For now, use simplified metrics
        # In a full implementation, these would be calculated based on specific criteria
        completeness = max(0.0, 1.0 - (error_count * 0.1))  # Penalize errors
        accuracy = validity  # Simplified: accuracy = validity
        
        overall_score = (completeness + accuracy + consistency + validity) / 4
        
        return {
            "completeness": completeness,
            "accuracy": accuracy,
            "consistency": consistency,
            "validity": validity,
            "overall_score": overall_score,
            "error_count": error_count,
            "warning_count": warning_count,
            "total_rows": total_rows,
        }
    
    def _determine_gate_status(self, quality_metrics: Dict[str, float], validation_results: List[Dict[str, Any]]) -> str:
        """Determine quality gate status."""
        overall_score = quality_metrics["overall_score"]
        error_count = quality_metrics["error_count"]
        
        # Fail if there are critical errors or score is too low
        if error_count > 0 and overall_score < self.quality_threshold:
            return "FAIL"
        elif quality_metrics["warning_count"] > 0:
            return "WARN"
        else:
            return "PASS"
    
    async def _store_validation_results(self, event: MappingAppliedEvent, validation_results: List[Dict[str, Any]]):
        """Store validation results in the database."""
        if not validation_results:
            return
        
        async with self.database_session() as session:
            # Get job_id
            job_query = """
                SELECT job_id FROM import_job
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            job_result = await session.execute(job_query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            job_row = job_result.fetchone()
            if not job_row:
                logger.warning(f"No job found for {event.ingest_id}")
                return
            
            job_id = job_row[0]
            
            # Insert validation results
            for result in validation_results:
                query = """
                    INSERT INTO validation_result (
                        job_id, rule_id, client_id, ingest_id, row_num,
                        severity, message, context
                    ) VALUES (
                        :job_id, :rule_id, :client_id, :ingest_id, :row_num,
                        :severity, :message, :context
                    )
                """
                
                await session.execute(query, {
                    "job_id": job_id,
                    "rule_id": result["rule_id"],
                    "client_id": event.client_id,
                    "ingest_id": event.ingest_id,
                    "row_num": result["row_num"],
                    "severity": result["severity"],
                    "message": result["message"],
                    "context": result["context"],
                })
            
            await session.commit()
    
    async def _update_job_status(self, event: MappingAppliedEvent, gate_status: str):
        """Update job status based on validation results."""
        async with self.database_session() as session:
            # Update stg_file status
            file_status = "validated" if gate_status == "PASS" else "validation_failed"
            
            query = """
                UPDATE stg_file 
                SET status = :status, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "status": file_status,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            # Update import_job status
            job_status = "validating" if gate_status == "PASS" else "failed"
            
            job_query = """
                UPDATE import_job 
                SET status = :status, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(job_query, {
                "status": job_status,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def _emit_validation_results_event(self, event: MappingAppliedEvent, validation_results: List[Dict[str, Any]], quality_metrics: Dict[str, float], gate_status: str):
        """Emit validation.results event."""
        # Summarize issues by severity
        summary = {
            "ERROR": sum(1 for r in validation_results if r["severity"] == "ERROR"),
            "WARN": sum(1 for r in validation_results if r["severity"] == "WARN"),
            "INFO": sum(1 for r in validation_results if r["severity"] == "INFO"),
        }
        
        validation_event = ValidationResultsEvent(
            event_id=f"validated-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"validated-{event.client_id}-{event.ingest_id}",
            page_number=1,
            total_pages=1,
            summary=summary,
            issues=validation_results[:100],  # Limit issues in event
            quality_metrics=quality_metrics,
            gate_status=gate_status,
        )
        
        await self.publish_event(validation_event)
    
    async def _handle_validation_error(self, event: MappingAppliedEvent, error_message: str):
        """Handle validation errors."""
        logger.error(f"Validation error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'validation_error', 
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
        logger.info("ValidationAgent initialized")
        
        # Could load validation rule templates, initialize external validation services, etc.