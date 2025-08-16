"""
Event contracts for the CSV Import Guardian Agent System.

These define the message schemas for inter-agent communication via Kafka.
All events include tracing information for observability.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    """Base event with common fields for all messages."""
    
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(..., description="Type of event")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    trace_id: str = Field(..., description="Distributed tracing ID")
    client_id: str = Field(..., description="Tenant/client identifier")
    ingest_id: str = Field(..., description="Batch/file identifier")
    source_agent: str = Field(..., description="Agent that produced this event")
    idempotency_key: str = Field(..., description="Idempotency key for deduplication")


# File Processing Events

class FileReceivedEvent(BaseEvent):
    """Emitted when a new file is uploaded and ready for processing."""
    
    event_type: str = Field(default="file.received", const=True)
    uri: str = Field(..., description="Storage URI of the uploaded file")
    filename: str = Field(..., description="Original filename")
    size_bytes: int = Field(..., description="File size in bytes")
    content_type: str = Field(..., description="MIME type")
    hints: Dict[str, Any] = Field(default_factory=dict, description="Processing hints")


class FileProfiledEvent(BaseEvent):
    """Emitted when file profiling is complete."""
    
    event_type: str = Field(default="file.profiled", const=True)
    columns: List[str] = Field(..., description="Detected column names")
    encoding: str = Field(..., description="Character encoding")
    delimiter: str = Field(..., description="CSV delimiter")
    has_header: bool = Field(..., description="Whether file has header row")
    row_count: int = Field(..., description="Total number of data rows")
    column_count: int = Field(..., description="Number of columns")
    sample_rows: List[Dict[str, Any]] = Field(..., description="Sample data rows")
    column_stats: Dict[str, Dict[str, Any]] = Field(..., description="Column statistics")
    quality_score: float = Field(..., description="Overall data quality score")
    issues: List[Dict[str, Any]] = Field(default_factory=list, description="Profiling issues found")


# Mapping Events

class MappingAppliedEvent(BaseEvent):
    """Emitted when field mappings have been applied to data."""
    
    event_type: str = Field(default="mapping.applied", const=True)
    template_id: UUID = Field(..., description="Mapping template used")
    template_version: int = Field(..., description="Template version")
    page_number: int = Field(..., description="Page number for large files")
    total_pages: int = Field(..., description="Total number of pages")
    rows_processed: int = Field(..., description="Number of rows in this page")
    mapped_rows: List[Dict[str, Any]] = Field(..., description="Mapped product data")
    mapping_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Mapping issues")


# Validation Events

class ValidationResultsEvent(BaseEvent):
    """Emitted when data validation is complete."""
    
    event_type: str = Field(default="validation.results", const=True)
    page_number: int = Field(..., description="Page number for large files")
    total_pages: int = Field(..., description="Total number of pages")
    summary: Dict[str, int] = Field(..., description="Issue count by severity")
    issues: List[Dict[str, Any]] = Field(..., description="Detailed validation issues")
    quality_metrics: Dict[str, float] = Field(..., description="Quality metrics")
    gate_status: str = Field(..., description="Quality gate status: PASS, WARN, FAIL")


# Normalization Events

class NormalizationDoneEvent(BaseEvent):
    """Emitted when data normalization is complete."""
    
    event_type: str = Field(default="normalization.done", const=True)
    page_number: int = Field(..., description="Page number for large files")
    total_pages: int = Field(..., description="Total number of pages")
    rows_normalized: int = Field(..., description="Number of rows normalized")
    transformations_applied: List[str] = Field(..., description="List of transformations applied")
    normalization_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Normalization issues")


# Normalization Events

class NormalizationDoneEvent(BaseEvent):
    """Emitted when data normalization is completed."""
    
    event_type: str = Field(default="normalization.done", const=True)
    rows_processed: int = Field(..., description="Number of rows processed")
    normalization_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Normalization issues")
    quality_score: float = Field(..., description="Quality score after normalization")


# Persistence Events

class PersistenceUpsertedEvent(BaseEvent):
    """Emitted when data has been persisted to canonical storage."""
    
    event_type: str = Field(default="persistence.upserted", const=True)
    products_created: int = Field(..., description="Number of new products created")
    products_updated: int = Field(..., description="Number of existing products updated")
    features_upserted: int = Field(..., description="Number of feature values upserted")
    product_ids: List[UUID] = Field(..., description="List of affected product IDs")
    crosswalk_entries: int = Field(..., description="Number of crosswalk entries created/updated")


# Export Events

class ExportRequestedEvent(BaseEvent):
    """Emitted when an export is requested."""
    
    event_type: str = Field(default="export.requested", const=True)
    export_format: str = Field(..., description="Export format: bmecat, etim, csv")
    export_profile: str = Field(..., description="Export profile/template")
    filter_criteria: Dict[str, Any] = Field(default_factory=dict, description="Export filters")
    etim_version: Optional[str] = Field(None, description="ETIM version for ETIM exports")


class ExportCompletedEvent(BaseEvent):
    """Emitted when an export is complete."""
    
    event_type: str = Field(default="export.completed", const=True)
    export_uri: str = Field(..., description="URI of the generated export file")
    export_format: str = Field(..., description="Export format")
    file_size: int = Field(..., description="Export file size in bytes")
    product_count: int = Field(..., description="Number of products exported")
    checksum: str = Field(..., description="File checksum for integrity")
    validation_report: Optional[Dict[str, Any]] = Field(None, description="XSD validation report")


# Data Quality Events

class DQReportReadyEvent(BaseEvent):
    """Emitted when a data quality report is ready."""
    
    event_type: str = Field(default="dq.report.ready", const=True)
    report_uri: str = Field(..., description="URI of the generated report")
    report_format: str = Field(..., description="Report format: html, csv, json")
    overall_score: float = Field(..., description="Overall data quality score")
    metrics: Dict[str, float] = Field(..., description="Detailed quality metrics")
    recommendations: List[str] = Field(default_factory=list, description="Improvement recommendations")


# Lineage Events

class LineageUpdatedEvent(BaseEvent):
    """Emitted when lineage information is updated."""
    
    event_type: str = Field(default="lineage.updated", const=True)
    nodes_created: List[Dict[str, Any]] = Field(..., description="Lineage nodes created")
    edges_created: List[Dict[str, Any]] = Field(..., description="Lineage edges created")
    lineage_depth: int = Field(..., description="Current lineage depth")


# Error Events

class ProcessingErrorEvent(BaseEvent):
    """Emitted when an error occurs during processing."""
    
    event_type: str = Field(default="processing.error", const=True)
    error_code: str = Field(..., description="Error code")
    error_message: str = Field(..., description="Human-readable error message")
    error_details: Dict[str, Any] = Field(default_factory=dict, description="Additional error context")
    retry_count: int = Field(default=0, description="Number of retry attempts")
    is_recoverable: bool = Field(..., description="Whether the error is recoverable")
    failed_stage: str = Field(..., description="Processing stage where error occurred")


# Job Status Events

class JobStatusUpdatedEvent(BaseEvent):
    """Emitted when job status changes."""
    
    event_type: str = Field(default="job.status.updated", const=True)
    job_id: UUID = Field(..., description="Job identifier")
    old_status: str = Field(..., description="Previous status")
    new_status: str = Field(..., description="New status")
    progress: Dict[str, Any] = Field(default_factory=dict, description="Progress information")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")


# Event type mapping for deserialization
EVENT_TYPE_MAP = {
    "file.received": FileReceivedEvent,
    "file.profiled": FileProfiledEvent,
    "mapping.applied": MappingAppliedEvent,
    "validation.results": ValidationResultsEvent,
    "normalization.done": NormalizationDoneEvent,
    "persistence.upserted": PersistenceUpsertedEvent,
    "export.requested": ExportRequestedEvent,
    "export.completed": ExportCompletedEvent,
    "dq.report.ready": DQReportReadyEvent,
    "lineage.updated": LineageUpdatedEvent,
    "processing.error": ProcessingErrorEvent,
    "job.status.updated": JobStatusUpdatedEvent,
}


def deserialize_event(event_data: Dict[str, Any]) -> BaseEvent:
    """Deserialize event data to appropriate event class."""
    event_type = event_data.get("event_type")
    event_class = EVENT_TYPE_MAP.get(event_type, BaseEvent)
    return event_class(**event_data)


# Topic configuration
KAFKA_TOPICS = {
    "file.received": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}  # 7 days
    },
    "file.profiled": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
    "mapping.applied": {
        "partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
    "validation.results": {
        "partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
    "normalization.done": {
        "partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
    "persistence.upserted": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": 2592000000}  # 30 days
    },
    "export.requested": {
        "partitions": 2,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
    "export.completed": {
        "partitions": 2,
        "replication_factor": 1,
        "config": {"retention.ms": 2592000000}
    },
    "dq.report.ready": {
        "partitions": 2,
        "replication_factor": 1,
        "config": {"retention.ms": 2592000000}
    },
    "lineage.updated": {
        "partitions": 2,
        "replication_factor": 1,
        "config": {"retention.ms": 2592000000}
    },
    "processing.error": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": 2592000000}
    },
    "job.status.updated": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": 604800000}
    },
}