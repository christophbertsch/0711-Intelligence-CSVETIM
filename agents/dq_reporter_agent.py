"""
Data Quality Reporter Agent - Generates comprehensive data quality reports.
"""

import json
import logging
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional
from uuid import uuid4

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, ValidationResultsEvent, PersistenceUpsertedEvent, DQReportReadyEvent

logger = logging.getLogger(__name__)


@register_agent("dq_reporter")
class DQReporterAgent(BaseAgent):
    """
    DQReporterAgent generates comprehensive data quality reports and analytics.
    
    Responsibilities:
    - Aggregate validation results into comprehensive reports
    - Generate HTML/PDF reports with visualizations
    - Calculate quality trends and benchmarks
    - Provide actionable recommendations
    - Track quality metrics over time
    - Generate executive dashboards
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.report_formats = ["html", "json", "csv"]
        self.quality_dimensions = [
            "completeness", "accuracy", "consistency", "validity", 
            "uniqueness", "timeliness", "conformity"
        ]
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, ValidationResultsEvent):
            await self._process_validation_results(event)
        elif isinstance(event, PersistenceUpsertedEvent):
            await self._process_persistence_results(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _process_validation_results(self, event: ValidationResultsEvent):
        """Process validation results and generate quality report."""
        try:
            logger.info(f"Generating DQ report for validation results: {event.ingest_id}")
            
            # Generate comprehensive quality report
            report_data = await self._generate_quality_report(event)
            
            # Generate report in multiple formats
            reports = {}
            for format_type in self.report_formats:
                report_content = await self._generate_report_format(report_data, format_type)
                reports[format_type] = report_content
            
            # Upload reports to storage
            report_urls = await self._upload_reports(event, reports)
            
            # Store report metadata in database
            await self._store_report_metadata(event, report_data, report_urls)
            
            # Emit DQ report ready event
            await self._emit_dq_report_ready_event(event, report_data, report_urls)
            
            logger.info(f"DQ report generated successfully for {event.ingest_id}")
            
        except Exception as e:
            logger.error(f"Error generating DQ report: {e}")
    
    async def _process_persistence_results(self, event: PersistenceUpsertedEvent):
        """Process persistence results for trend analysis."""
        try:
            logger.info(f"Processing persistence results for trend analysis: {event.ingest_id}")
            
            # Update quality trends
            await self._update_quality_trends(event)
            
            # Generate trend report if needed
            if await self._should_generate_trend_report(event):
                await self._generate_trend_report(event)
            
        except Exception as e:
            logger.error(f"Error processing persistence results: {e}")
    
    async def _generate_quality_report(self, event: ValidationResultsEvent) -> Dict[str, Any]:
        """Generate comprehensive quality report data."""
        # Load detailed validation results
        validation_details = await self._load_validation_details(event)
        
        # Load file and job information
        file_info = await self._load_file_info(event)
        
        # Calculate quality metrics
        quality_metrics = await self._calculate_detailed_quality_metrics(validation_details, file_info)
        
        # Generate recommendations
        recommendations = await self._generate_recommendations(quality_metrics, validation_details)
        
        # Get historical comparison
        historical_comparison = await self._get_historical_comparison(event.client_id, quality_metrics)
        
        # Generate executive summary
        executive_summary = self._generate_executive_summary(quality_metrics, recommendations)
        
        report_data = {
            "report_id": str(uuid4()),
            "client_id": event.client_id,
            "ingest_id": event.ingest_id,
            "generated_at": datetime.utcnow().isoformat(),
            "file_info": file_info,
            "quality_metrics": quality_metrics,
            "validation_summary": {
                "total_issues": len(validation_details),
                "by_severity": event.summary,
                "by_rule_type": self._group_issues_by_rule_type(validation_details),
                "by_column": self._group_issues_by_column(validation_details),
            },
            "detailed_issues": validation_details[:100],  # Limit for report size
            "recommendations": recommendations,
            "historical_comparison": historical_comparison,
            "executive_summary": executive_summary,
            "quality_score": quality_metrics.get("overall_score", 0.0),
            "gate_status": event.gate_status,
        }
        
        return report_data
    
    async def _load_validation_details(self, event: ValidationResultsEvent) -> List[Dict[str, Any]]:
        """Load detailed validation results from database."""
        async with self.database_session() as session:
            query = """
                SELECT vr.rule_id, vr.severity, vr.message, vr.context, vr.row_num,
                       vr_rule.name as rule_name, vr_rule.rule_type, vr_rule.target_type, vr_rule.target_name
                FROM validation_result vr
                JOIN import_job ij ON vr.job_id = ij.job_id
                LEFT JOIN validation_rule vr_rule ON vr.rule_id = vr_rule.rule_id
                WHERE ij.client_id = :client_id AND ij.ingest_id = :ingest_id
                ORDER BY vr.severity DESC, vr.row_num
            """
            
            result = await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            rows = result.fetchall()
            
            details = []
            for row in rows:
                details.append({
                    "rule_id": row[0],
                    "severity": row[1],
                    "message": row[2],
                    "context": row[3] or {},
                    "row_num": row[4],
                    "rule_name": row[5],
                    "rule_type": row[6],
                    "target_type": row[7],
                    "target_name": row[8],
                })
            
            return details
    
    async def _load_file_info(self, event: ValidationResultsEvent) -> Dict[str, Any]:
        """Load file information from database."""
        async with self.database_session() as session:
            query = """
                SELECT sf.filename, sf.size_bytes, sf.encoding, sf.delimiter,
                       sf.has_header, sf.row_count, sf.column_count, sf.profile,
                       ij.started_at, ij.completed_at
                FROM stg_file sf
                JOIN import_job ij ON sf.client_id = ij.client_id AND sf.ingest_id = ij.ingest_id
                WHERE sf.client_id = :client_id AND sf.ingest_id = :ingest_id
            """
            
            result = await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            row = result.fetchone()
            if row:
                return {
                    "filename": row[0],
                    "size_bytes": row[1],
                    "encoding": row[2],
                    "delimiter": row[3],
                    "has_header": row[4],
                    "row_count": row[5],
                    "column_count": row[6],
                    "profile": row[7] or {},
                    "started_at": row[8].isoformat() if row[8] else None,
                    "completed_at": row[9].isoformat() if row[9] else None,
                }
            
            return {}
    
    async def _calculate_detailed_quality_metrics(self, validation_details: List[Dict[str, Any]], file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate detailed quality metrics."""
        total_rows = file_info.get("row_count", 1)
        total_issues = len(validation_details)
        
        # Count issues by severity
        error_count = sum(1 for issue in validation_details if issue["severity"] == "ERROR")
        warn_count = sum(1 for issue in validation_details if issue["severity"] == "WARN")
        info_count = sum(1 for issue in validation_details if issue["severity"] == "INFO")
        
        # Calculate quality dimensions
        completeness = self._calculate_completeness(validation_details, total_rows)
        accuracy = self._calculate_accuracy(validation_details, total_rows)
        consistency = self._calculate_consistency(validation_details, total_rows)
        validity = self._calculate_validity(validation_details, total_rows)
        uniqueness = self._calculate_uniqueness(validation_details, total_rows)
        timeliness = self._calculate_timeliness(file_info)
        conformity = self._calculate_conformity(validation_details, total_rows)
        
        # Overall score (weighted average)
        weights = {
            "completeness": 0.2,
            "accuracy": 0.25,
            "consistency": 0.15,
            "validity": 0.2,
            "uniqueness": 0.1,
            "timeliness": 0.05,
            "conformity": 0.05,
        }
        
        dimension_scores = {
            "completeness": completeness,
            "accuracy": accuracy,
            "consistency": consistency,
            "validity": validity,
            "uniqueness": uniqueness,
            "timeliness": timeliness,
            "conformity": conformity,
        }
        
        overall_score = sum(score * weights[dim] for dim, score in dimension_scores.items())
        
        return {
            "overall_score": overall_score,
            "dimension_scores": dimension_scores,
            "issue_counts": {
                "total": total_issues,
                "error": error_count,
                "warning": warn_count,
                "info": info_count,
            },
            "rates": {
                "error_rate": error_count / total_rows,
                "warning_rate": warn_count / total_rows,
                "issue_rate": total_issues / total_rows,
            },
            "affected_rows": len(set(issue["row_num"] for issue in validation_details if issue.get("row_num"))),
            "data_coverage": (total_rows - len(set(issue["row_num"] for issue in validation_details if issue.get("row_num")))) / total_rows,
        }
    
    def _calculate_completeness(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate completeness score."""
        missing_value_issues = [
            issue for issue in validation_details 
            if issue.get("rule_type") == "required" or "missing" in issue.get("message", "").lower()
        ]
        
        return max(0.0, 1.0 - (len(missing_value_issues) / total_rows))
    
    def _calculate_accuracy(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate accuracy score."""
        accuracy_issues = [
            issue for issue in validation_details 
            if issue.get("severity") == "ERROR" and issue.get("rule_type") in ["format", "range", "domain"]
        ]
        
        return max(0.0, 1.0 - (len(accuracy_issues) / total_rows))
    
    def _calculate_consistency(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate consistency score."""
        consistency_issues = [
            issue for issue in validation_details 
            if issue.get("severity") == "WARN" or "inconsistent" in issue.get("message", "").lower()
        ]
        
        return max(0.0, 1.0 - (len(consistency_issues) / total_rows * 0.5))  # Warnings have less impact
    
    def _calculate_validity(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate validity score."""
        validity_issues = [
            issue for issue in validation_details 
            if issue.get("rule_type") in ["format", "domain", "etim"]
        ]
        
        return max(0.0, 1.0 - (len(validity_issues) / total_rows))
    
    def _calculate_uniqueness(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate uniqueness score."""
        duplicate_issues = [
            issue for issue in validation_details 
            if "duplicate" in issue.get("message", "").lower()
        ]
        
        return max(0.0, 1.0 - (len(duplicate_issues) / total_rows))
    
    def _calculate_timeliness(self, file_info: Dict[str, Any]) -> float:
        """Calculate timeliness score based on processing time."""
        started_at = file_info.get("started_at")
        completed_at = file_info.get("completed_at")
        
        if not started_at or not completed_at:
            return 0.8  # Default score
        
        try:
            start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
            processing_time = (end_time - start_time).total_seconds()
            
            # Score based on processing time (faster is better)
            # Assume target is < 5 minutes for good score
            target_time = 300  # 5 minutes
            if processing_time <= target_time:
                return 1.0
            else:
                # Decay score for longer processing times
                return max(0.0, 1.0 - (processing_time - target_time) / (target_time * 4))
        
        except Exception:
            return 0.8  # Default score
    
    def _calculate_conformity(self, validation_details: List[Dict[str, Any]], total_rows: int) -> float:
        """Calculate conformity score (adherence to standards)."""
        conformity_issues = [
            issue for issue in validation_details 
            if issue.get("rule_type") == "etim" or "standard" in issue.get("message", "").lower()
        ]
        
        return max(0.0, 1.0 - (len(conformity_issues) / total_rows))
    
    def _group_issues_by_rule_type(self, validation_details: List[Dict[str, Any]]) -> Dict[str, int]:
        """Group issues by rule type."""
        groups = {}
        for issue in validation_details:
            rule_type = issue.get("rule_type", "unknown")
            groups[rule_type] = groups.get(rule_type, 0) + 1
        return groups
    
    def _group_issues_by_column(self, validation_details: List[Dict[str, Any]]) -> Dict[str, int]:
        """Group issues by target column."""
        groups = {}
        for issue in validation_details:
            target_name = issue.get("target_name", "unknown")
            groups[target_name] = groups.get(target_name, 0) + 1
        return groups
    
    async def _generate_recommendations(self, quality_metrics: Dict[str, Any], validation_details: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate actionable recommendations."""
        recommendations = []
        
        dimension_scores = quality_metrics.get("dimension_scores", {})
        issue_counts = quality_metrics.get("issue_counts", {})
        
        # Completeness recommendations
        if dimension_scores.get("completeness", 1.0) < 0.8:
            recommendations.append({
                "category": "completeness",
                "priority": "high",
                "title": "Improve Data Completeness",
                "description": "Several required fields are missing values",
                "action": "Review data collection processes and add validation at source",
                "impact": "High - Missing data affects downstream processing",
            })
        
        # Accuracy recommendations
        if dimension_scores.get("accuracy", 1.0) < 0.7:
            recommendations.append({
                "category": "accuracy",
                "priority": "high",
                "title": "Fix Data Accuracy Issues",
                "description": "Multiple format and range validation errors detected",
                "action": "Implement data validation rules at data entry points",
                "impact": "High - Inaccurate data leads to poor decisions",
            })
        
        # Error-specific recommendations
        if issue_counts.get("error", 0) > 0:
            recommendations.append({
                "category": "errors",
                "priority": "critical",
                "title": "Resolve Critical Errors",
                "description": f"{issue_counts['error']} critical errors must be fixed",
                "action": "Review and fix all ERROR-level validation issues",
                "impact": "Critical - Errors prevent successful processing",
            })
        
        # Column-specific recommendations
        column_issues = self._group_issues_by_column(validation_details)
        problematic_columns = [col for col, count in column_issues.items() if count > 10]
        
        if problematic_columns:
            recommendations.append({
                "category": "data_quality",
                "priority": "medium",
                "title": "Focus on Problematic Columns",
                "description": f"Columns with most issues: {', '.join(problematic_columns[:3])}",
                "action": "Review data quality for these specific columns",
                "impact": "Medium - Targeted fixes will improve overall quality",
            })
        
        # Performance recommendations
        if quality_metrics.get("dimension_scores", {}).get("timeliness", 1.0) < 0.7:
            recommendations.append({
                "category": "performance",
                "priority": "low",
                "title": "Optimize Processing Performance",
                "description": "File processing took longer than expected",
                "action": "Consider file size optimization or processing improvements",
                "impact": "Low - Affects user experience but not data quality",
            })
        
        return recommendations
    
    async def _get_historical_comparison(self, client_id: str, current_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Get historical quality metrics for comparison."""
        async with self.database_session() as session:
            # Get average metrics from last 30 days
            query = """
                SELECT AVG(quality_score) as avg_score,
                       COUNT(*) as total_files,
                       AVG(error_count) as avg_errors,
                       AVG(warning_count) as avg_warnings
                FROM dq_report
                WHERE client_id = :client_id 
                  AND created_at >= NOW() - INTERVAL '30 days'
                  AND created_at < NOW() - INTERVAL '1 day'
            """
            
            result = await session.execute(query, {"client_id": client_id})
            row = result.fetchone()
            
            if row and row[0] is not None:
                historical_avg = float(row[0])
                current_score = current_metrics.get("overall_score", 0.0)
                
                return {
                    "historical_average": historical_avg,
                    "current_score": current_score,
                    "improvement": current_score - historical_avg,
                    "trend": "improving" if current_score > historical_avg else "declining",
                    "total_historical_files": int(row[1]) if row[1] else 0,
                    "avg_historical_errors": float(row[2]) if row[2] else 0,
                    "avg_historical_warnings": float(row[3]) if row[3] else 0,
                }
            
            return {
                "historical_average": None,
                "current_score": current_metrics.get("overall_score", 0.0),
                "improvement": None,
                "trend": "no_history",
                "total_historical_files": 0,
            }
    
    def _generate_executive_summary(self, quality_metrics: Dict[str, Any], recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate executive summary."""
        overall_score = quality_metrics.get("overall_score", 0.0)
        issue_counts = quality_metrics.get("issue_counts", {})
        
        # Determine overall status
        if overall_score >= 0.9:
            status = "excellent"
            status_message = "Data quality is excellent with minimal issues"
        elif overall_score >= 0.8:
            status = "good"
            status_message = "Data quality is good with minor issues to address"
        elif overall_score >= 0.7:
            status = "fair"
            status_message = "Data quality is fair but needs improvement"
        elif overall_score >= 0.6:
            status = "poor"
            status_message = "Data quality is poor and requires immediate attention"
        else:
            status = "critical"
            status_message = "Data quality is critical and must be addressed before processing"
        
        # Key findings
        key_findings = []
        
        if issue_counts.get("error", 0) > 0:
            key_findings.append(f"{issue_counts['error']} critical errors detected")
        
        if issue_counts.get("warning", 0) > 50:
            key_findings.append(f"{issue_counts['warning']} warnings may impact quality")
        
        dimension_scores = quality_metrics.get("dimension_scores", {})
        low_dimensions = [dim for dim, score in dimension_scores.items() if score < 0.7]
        if low_dimensions:
            key_findings.append(f"Low scores in: {', '.join(low_dimensions)}")
        
        # Priority actions
        priority_actions = [
            rec["title"] for rec in recommendations 
            if rec.get("priority") in ["critical", "high"]
        ][:3]
        
        return {
            "overall_status": status,
            "status_message": status_message,
            "quality_score": overall_score,
            "key_findings": key_findings,
            "priority_actions": priority_actions,
            "total_issues": issue_counts.get("total", 0),
            "critical_issues": issue_counts.get("error", 0),
        }
    
    async def _generate_report_format(self, report_data: Dict[str, Any], format_type: str) -> str:
        """Generate report in specified format."""
        if format_type == "json":
            return json.dumps(report_data, indent=2, default=str)
        
        elif format_type == "html":
            return self._generate_html_report(report_data)
        
        elif format_type == "csv":
            return self._generate_csv_report(report_data)
        
        else:
            raise ValueError(f"Unsupported report format: {format_type}")
    
    def _generate_html_report(self, report_data: Dict[str, Any]) -> str:
        """Generate HTML report."""
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - {client_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ background-color: #e8f5e8; padding: 15px; margin: 20px 0; border-radius: 5px; }}
        .metrics {{ display: flex; flex-wrap: wrap; gap: 20px; margin: 20px 0; }}
        .metric {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; min-width: 200px; }}
        .recommendations {{ margin: 20px 0; }}
        .recommendation {{ background-color: #fff3cd; padding: 10px; margin: 10px 0; border-left: 4px solid #ffc107; }}
        .issues {{ margin: 20px 0; }}
        .issue {{ padding: 5px; margin: 5px 0; border-left: 3px solid #dc3545; }}
        .score {{ font-size: 2em; font-weight: bold; color: {score_color}; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <p><strong>Client:</strong> {client_id}</p>
        <p><strong>File:</strong> {filename}</p>
        <p><strong>Generated:</strong> {generated_at}</p>
    </div>
    
    <div class="summary">
        <h2>Executive Summary</h2>
        <div class="score">{quality_score:.1%}</div>
        <p><strong>Status:</strong> {status_message}</p>
        <p><strong>Key Findings:</strong></p>
        <ul>
            {key_findings}
        </ul>
    </div>
    
    <div class="metrics">
        <div class="metric">
            <h3>Overall Score</h3>
            <div class="score">{quality_score:.1%}</div>
        </div>
        <div class="metric">
            <h3>Total Issues</h3>
            <div>{total_issues}</div>
        </div>
        <div class="metric">
            <h3>Critical Errors</h3>
            <div>{error_count}</div>
        </div>
        <div class="metric">
            <h3>Warnings</h3>
            <div>{warning_count}</div>
        </div>
    </div>
    
    <h2>Quality Dimensions</h2>
    <table>
        <tr><th>Dimension</th><th>Score</th><th>Status</th></tr>
        {dimension_rows}
    </table>
    
    <div class="recommendations">
        <h2>Recommendations</h2>
        {recommendation_items}
    </div>
    
    <div class="issues">
        <h2>Top Issues</h2>
        {issue_items}
    </div>
</body>
</html>
        """
        
        # Prepare template variables
        executive_summary = report_data.get("executive_summary", {})
        quality_metrics = report_data.get("quality_metrics", {})
        file_info = report_data.get("file_info", {})
        
        quality_score = quality_metrics.get("overall_score", 0.0)
        score_color = "#28a745" if quality_score >= 0.8 else "#ffc107" if quality_score >= 0.6 else "#dc3545"
        
        # Key findings
        key_findings_html = "".join(f"<li>{finding}</li>" for finding in executive_summary.get("key_findings", []))
        
        # Dimension rows
        dimension_scores = quality_metrics.get("dimension_scores", {})
        dimension_rows = ""
        for dim, score in dimension_scores.items():
            status = "Good" if score >= 0.8 else "Fair" if score >= 0.6 else "Poor"
            dimension_rows += f"<tr><td>{dim.title()}</td><td>{score:.1%}</td><td>{status}</td></tr>"
        
        # Recommendations
        recommendations = report_data.get("recommendations", [])
        recommendation_items = ""
        for rec in recommendations[:5]:  # Top 5 recommendations
            recommendation_items += f"""
            <div class="recommendation">
                <h4>{rec.get('title', '')}</h4>
                <p><strong>Priority:</strong> {rec.get('priority', '').title()}</p>
                <p>{rec.get('description', '')}</p>
                <p><strong>Action:</strong> {rec.get('action', '')}</p>
            </div>
            """
        
        # Issues
        detailed_issues = report_data.get("detailed_issues", [])
        issue_items = ""
        for issue in detailed_issues[:10]:  # Top 10 issues
            issue_items += f"""
            <div class="issue">
                <strong>{issue.get('severity', '')}:</strong> {issue.get('message', '')}
                (Row {issue.get('row_num', 'N/A')})
            </div>
            """
        
        return html_template.format(
            client_id=report_data.get("client_id", ""),
            filename=file_info.get("filename", ""),
            generated_at=report_data.get("generated_at", ""),
            quality_score=quality_score,
            score_color=score_color,
            status_message=executive_summary.get("status_message", ""),
            key_findings=key_findings_html,
            total_issues=quality_metrics.get("issue_counts", {}).get("total", 0),
            error_count=quality_metrics.get("issue_counts", {}).get("error", 0),
            warning_count=quality_metrics.get("issue_counts", {}).get("warning", 0),
            dimension_rows=dimension_rows,
            recommendation_items=recommendation_items,
            issue_items=issue_items,
        )
    
    def _generate_csv_report(self, report_data: Dict[str, Any]) -> str:
        """Generate CSV summary report."""
        from io import StringIO
        import csv
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow(["Data Quality Report Summary"])
        writer.writerow(["Client", report_data.get("client_id", "")])
        writer.writerow(["File", report_data.get("file_info", {}).get("filename", "")])
        writer.writerow(["Generated", report_data.get("generated_at", "")])
        writer.writerow([])
        
        # Quality metrics
        writer.writerow(["Quality Metrics"])
        writer.writerow(["Metric", "Value"])
        
        quality_metrics = report_data.get("quality_metrics", {})
        writer.writerow(["Overall Score", f"{quality_metrics.get('overall_score', 0.0):.1%}"])
        
        dimension_scores = quality_metrics.get("dimension_scores", {})
        for dim, score in dimension_scores.items():
            writer.writerow([dim.title(), f"{score:.1%}"])
        
        writer.writerow([])
        
        # Issue summary
        writer.writerow(["Issue Summary"])
        writer.writerow(["Severity", "Count"])
        
        issue_counts = quality_metrics.get("issue_counts", {})
        writer.writerow(["Errors", issue_counts.get("error", 0)])
        writer.writerow(["Warnings", issue_counts.get("warning", 0)])
        writer.writerow(["Info", issue_counts.get("info", 0)])
        
        return output.getvalue()
    
    async def _upload_reports(self, event: ValidationResultsEvent, reports: Dict[str, str]) -> Dict[str, str]:
        """Upload reports to storage and return URLs."""
        report_urls = {}
        
        for format_type, content in reports.items():
            # Generate filename
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"dq_report_{event.client_id}_{timestamp}.{format_type}"
            
            # Upload to storage
            object_key = self.storage_manager.generate_object_key(
                event.client_id, event.ingest_id, filename, "reports"
            )
            
            file_obj = BytesIO(content.encode('utf-8'))
            
            uri = await self.storage_manager.upload_file(
                file_obj,
                object_key,
                content_type=f"text/{format_type}" if format_type != "html" else "text/html",
                metadata={
                    "client_id": event.client_id,
                    "ingest_id": event.ingest_id,
                    "report_type": "data_quality",
                    "format": format_type,
                }
            )
            
            # Generate signed download URL
            download_url = await self.storage_manager.generate_presigned_url(
                object_key, expiration=86400 * 7  # 7 days
            )
            
            report_urls[format_type] = download_url
        
        return report_urls
    
    async def _store_report_metadata(self, event: ValidationResultsEvent, report_data: Dict[str, Any], report_urls: Dict[str, str]):
        """Store report metadata in database."""
        async with self.database_session() as session:
            query = """
                INSERT INTO dq_report (
                    report_id, client_id, ingest_id, quality_score,
                    error_count, warning_count, info_count,
                    report_urls, created_at
                ) VALUES (
                    :report_id, :client_id, :ingest_id, :quality_score,
                    :error_count, :warning_count, :info_count,
                    :report_urls, :created_at
                )
            """
            
            quality_metrics = report_data.get("quality_metrics", {})
            issue_counts = quality_metrics.get("issue_counts", {})
            
            await session.execute(query, {
                "report_id": report_data["report_id"],
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
                "quality_score": quality_metrics.get("overall_score", 0.0),
                "error_count": issue_counts.get("error", 0),
                "warning_count": issue_counts.get("warning", 0),
                "info_count": issue_counts.get("info", 0),
                "report_urls": report_urls,
                "created_at": datetime.utcnow(),
            })
            
            await session.commit()
    
    async def _emit_dq_report_ready_event(self, event: ValidationResultsEvent, report_data: Dict[str, Any], report_urls: Dict[str, str]):
        """Emit DQ report ready event."""
        dq_event = DQReportReadyEvent(
            event_id=f"dq-report-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"dq-report-{event.client_id}-{event.ingest_id}",
            report_id=report_data["report_id"],
            quality_score=report_data["quality_score"],
            report_urls=report_urls,
        )
        
        await self.publish_event(dq_event)
    
    async def _update_quality_trends(self, event: PersistenceUpsertedEvent):
        """Update quality trends for analytics."""
        # This would update trend tables for dashboard analytics
        logger.info(f"Updating quality trends for {event.client_id}")
    
    async def _should_generate_trend_report(self, event: PersistenceUpsertedEvent) -> bool:
        """Determine if a trend report should be generated."""
        # Generate weekly trend reports
        return datetime.utcnow().weekday() == 0  # Monday
    
    async def _generate_trend_report(self, event: PersistenceUpsertedEvent):
        """Generate quality trend report."""
        logger.info(f"Generating trend report for {event.client_id}")
        # Implementation would generate trend analysis reports
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("DQReporterAgent initialized")
        
        # Could initialize report templates, charting libraries, etc.