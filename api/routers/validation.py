"""
Validation and quality analysis endpoints.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends

from shared.database import get_async_session
from shared.models import (
    ValidateRequest, ValidateResponse,
    QualityAnalysisRequest, QualityAnalysisResponse,
    RuleSuggestRequest, RuleSuggestResponse
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/jobs/{job_id}/validate", response_model=ValidateResponse)
async def validate_job(
    job_id: str,
    request: ValidateRequest,
    session=Depends(get_async_session)
):
    """
    Validate job data against business rules.
    """
    try:
        # Get job information
        job_query = """
            SELECT client_id, ingest_id, status FROM import_job
            WHERE job_id = :job_id
        """
        
        job_result = await session.execute(job_query, {"job_id": job_id})
        job_row = job_result.fetchone()
        
        if not job_row:
            raise HTTPException(status_code=404, detail="Job not found")
        
        client_id, ingest_id, status = job_row
        
        # Get validation results if already validated
        results_query = """
            SELECT vr.severity, vr.message, vr.context, vr.row_num
            FROM validation_result vr
            JOIN import_job ij ON vr.job_id = ij.job_id
            WHERE ij.job_id = :job_id
            ORDER BY vr.severity DESC, vr.row_num
        """
        
        results = await session.execute(results_query, {"job_id": job_id})
        result_rows = results.fetchall()
        
        # Summarize results
        summary = {"ERROR": 0, "WARN": 0, "INFO": 0}
        issues = []
        
        for row in result_rows:
            severity, message, context, row_num = row
            summary[severity] = summary.get(severity, 0) + 1
            
            issues.append({
                "severity": severity,
                "message": message,
                "row_num": row_num,
                "context": context or {},
            })
        
        # Calculate quality score
        total_issues = sum(summary.values())
        if total_issues == 0:
            quality_score = 1.0
            gate_status = "PASS"
        else:
            # Simple scoring: penalize errors more than warnings
            error_penalty = summary["ERROR"] * 1.0
            warn_penalty = summary["WARN"] * 0.5
            info_penalty = summary["INFO"] * 0.1
            
            total_penalty = error_penalty + warn_penalty + info_penalty
            quality_score = max(0.0, 1.0 - (total_penalty / 100))  # Normalize to 0-1
            
            if summary["ERROR"] > 0:
                gate_status = "FAIL"
            elif summary["WARN"] > 0:
                gate_status = "WARN"
            else:
                gate_status = "PASS"
        
        # Generate recommendations
        recommendations = []
        if summary["ERROR"] > 0:
            recommendations.append("Fix all ERROR-level issues before proceeding")
        if summary["WARN"] > 10:
            recommendations.append("Consider reviewing WARN-level issues for data quality")
        if quality_score < 0.8:
            recommendations.append("Overall data quality is below recommended threshold")
        
        return ValidateResponse(
            summary=summary,
            quality_score=quality_score,
            gate_status=gate_status,
            issues=issues[:100],  # Limit issues returned
            recommendations=recommendations
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error validating job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/quality", response_model=QualityAnalysisResponse)
async def analyze_quality(
    request: QualityAnalysisRequest,
    job_id: Optional[str] = None,
    client_id: Optional[str] = None,
    session=Depends(get_async_session)
):
    """
    Perform detailed quality analysis on data.
    """
    try:
        # Build query based on parameters
        where_conditions = []
        params = {}
        
        if job_id:
            where_conditions.append("ij.job_id = :job_id")
            params["job_id"] = job_id
        elif client_id:
            where_conditions.append("ij.client_id = :client_id")
            params["client_id"] = client_id
        else:
            raise HTTPException(status_code=400, detail="Either job_id or client_id must be provided")
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Get validation statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as total_issues,
                COUNT(CASE WHEN vr.severity = 'ERROR' THEN 1 END) as error_count,
                COUNT(CASE WHEN vr.severity = 'WARN' THEN 1 END) as warn_count,
                COUNT(CASE WHEN vr.severity = 'INFO' THEN 1 END) as info_count,
                COUNT(DISTINCT vr.row_num) as affected_rows
            FROM validation_result vr
            JOIN import_job ij ON vr.job_id = ij.job_id
            {where_clause}
        """
        
        stats_result = await session.execute(stats_query, params)
        stats_row = stats_result.fetchone()
        
        if not stats_row:
            # No validation results found
            return QualityAnalysisResponse(
                overall_score=1.0,
                metrics={},
                dimension_scores={},
                recommendations=[]
            )
        
        total_issues, error_count, warn_count, info_count, affected_rows = stats_row
        
        # Calculate quality dimensions
        completeness = 1.0 - (error_count / max(total_issues, 1)) * 0.5
        accuracy = 1.0 - (error_count / max(total_issues, 1))
        consistency = 1.0 - (warn_count / max(total_issues, 1)) * 0.3
        validity = 1.0 - ((error_count + warn_count) / max(total_issues, 1)) * 0.4
        
        dimension_scores = {
            "completeness": max(0.0, completeness),
            "accuracy": max(0.0, accuracy),
            "consistency": max(0.0, consistency),
            "validity": max(0.0, validity),
        }
        
        overall_score = sum(dimension_scores.values()) / len(dimension_scores)
        
        # Detailed metrics
        metrics = {
            "total_issues": total_issues,
            "error_count": error_count,
            "warning_count": warn_count,
            "info_count": info_count,
            "affected_rows": affected_rows,
            "error_rate": error_count / max(total_issues, 1),
            "warning_rate": warn_count / max(total_issues, 1),
        }
        
        # Generate recommendations
        recommendations = []
        
        if error_count > 0:
            recommendations.append({
                "type": "critical",
                "message": f"Fix {error_count} critical errors before proceeding",
                "priority": "high"
            })
        
        if warn_count > 10:
            recommendations.append({
                "type": "quality",
                "message": f"Review {warn_count} warnings to improve data quality",
                "priority": "medium"
            })
        
        if overall_score < 0.7:
            recommendations.append({
                "type": "overall",
                "message": "Overall data quality is below acceptable threshold",
                "priority": "high"
            })
        
        if dimension_scores["completeness"] < 0.8:
            recommendations.append({
                "type": "completeness",
                "message": "Improve data completeness by filling missing required fields",
                "priority": "medium"
            })
        
        return QualityAnalysisResponse(
            overall_score=overall_score,
            metrics=metrics,
            dimension_scores=dimension_scores,
            recommendations=recommendations
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/suggest/rules", response_model=RuleSuggestResponse)
async def suggest_rules(
    request: RuleSuggestRequest,
    session=Depends(get_async_session)
):
    """
    Suggest validation rules based on data analysis.
    """
    try:
        target_type = request.target_type
        target_name = request.target_name
        sample_data = request.sample_data
        etim_context = request.etim_context or {}
        
        suggested_rules = []
        confidence_scores = {}
        rationale = {}
        
        # Analyze sample data to suggest rules
        if sample_data:
            # Check for required field
            non_empty_count = sum(1 for val in sample_data if val and str(val).strip())
            if non_empty_count == len(sample_data):
                suggested_rules.append({
                    "rule_type": "required",
                    "rule_config": {},
                    "severity": "ERROR",
                    "description": f"{target_name} appears to always have values"
                })
                confidence_scores["required"] = 0.9
                rationale["required"] = "All sample values are non-empty"
            
            # Check for numeric data
            numeric_count = 0
            numeric_values = []
            for val in sample_data:
                try:
                    num_val = float(str(val))
                    numeric_count += 1
                    numeric_values.append(num_val)
                except (ValueError, TypeError):
                    pass
            
            if numeric_count > len(sample_data) * 0.8:  # 80% numeric
                min_val = min(numeric_values)
                max_val = max(numeric_values)
                
                suggested_rules.append({
                    "rule_type": "range",
                    "rule_config": {
                        "min": min_val * 0.8,  # Allow some variance
                        "max": max_val * 1.2,
                    },
                    "severity": "WARN",
                    "description": f"Values should be between {min_val * 0.8:.2f} and {max_val * 1.2:.2f}"
                })
                confidence_scores["range"] = 0.8
                rationale["range"] = f"Sample values range from {min_val} to {max_val}"
            
            # Check for common patterns
            string_values = [str(val) for val in sample_data if val]
            
            # GTIN pattern
            gtin_pattern_count = sum(1 for val in string_values if val.isdigit() and len(val) in [8, 12, 13, 14])
            if gtin_pattern_count > len(string_values) * 0.7:
                suggested_rules.append({
                    "rule_type": "format",
                    "rule_config": {
                        "pattern": r"^\d{8}|\d{12}|\d{13}|\d{14}$"
                    },
                    "severity": "ERROR",
                    "description": "Value should be a valid GTIN (8, 12, 13, or 14 digits)"
                })
                confidence_scores["gtin_format"] = 0.85
                rationale["gtin_format"] = f"{gtin_pattern_count} values match GTIN pattern"
            
            # Email pattern
            email_pattern_count = sum(1 for val in string_values if "@" in val and "." in val)
            if email_pattern_count > len(string_values) * 0.7:
                suggested_rules.append({
                    "rule_type": "format",
                    "rule_config": {
                        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                    },
                    "severity": "WARN",
                    "description": "Value should be a valid email address"
                })
                confidence_scores["email_format"] = 0.8
                rationale["email_format"] = f"{email_pattern_count} values contain email-like patterns"
            
            # Domain validation for categorical data
            unique_values = list(set(string_values))
            if len(unique_values) <= 10 and len(unique_values) < len(string_values) * 0.5:
                suggested_rules.append({
                    "rule_type": "domain",
                    "rule_config": {
                        "values": unique_values,
                        "case_sensitive": False
                    },
                    "severity": "WARN",
                    "description": f"Value should be one of: {', '.join(unique_values)}"
                })
                confidence_scores["domain"] = 0.7
                rationale["domain"] = f"Only {len(unique_values)} unique values found in sample"
        
        # ETIM-specific rules
        if etim_context and target_type == "feature":
            etim_feature = etim_context.get("etim_feature")
            etim_class = etim_context.get("etim_class")
            
            if etim_feature and etim_class:
                # Check if feature is mandatory for this class
                etim_query = """
                    SELECT is_mandatory, ef.data_type, ef.unit
                    FROM etim_class_feature ecf
                    JOIN etim_feature ef ON ecf.feature_code = ef.feature_code
                    WHERE ecf.class_code = :class_code AND ecf.feature_code = :feature_code
                """
                
                etim_result = await session.execute(etim_query, {
                    "class_code": etim_class,
                    "feature_code": etim_feature,
                })
                
                etim_row = etim_result.fetchone()
                if etim_row:
                    is_mandatory, data_type, unit = etim_row
                    
                    if is_mandatory:
                        suggested_rules.append({
                            "rule_type": "required",
                            "rule_config": {},
                            "severity": "ERROR",
                            "description": f"ETIM feature {etim_feature} is mandatory for class {etim_class}"
                        })
                        confidence_scores["etim_required"] = 1.0
                        rationale["etim_required"] = "ETIM specification requires this feature"
                    
                    if data_type == "N":  # Numeric
                        suggested_rules.append({
                            "rule_type": "format",
                            "rule_config": {
                                "pattern": r"^\d+(\.\d+)?$"
                            },
                            "severity": "ERROR",
                            "description": "ETIM numeric feature must contain only numbers"
                        })
                        confidence_scores["etim_numeric"] = 0.95
                        rationale["etim_numeric"] = "ETIM data type is numeric"
        
        return RuleSuggestResponse(
            suggested_rules=suggested_rules,
            confidence_scores=confidence_scores,
            rationale=rationale
        )
        
    except Exception as e:
        logger.error(f"Error suggesting rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))