"""
Data models for the CSV Import Guardian Agent System.
These models represent the core domain objects and API contracts.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator


# Enums

class JobStatus(str, Enum):
    CREATED = "created"
    PROFILING = "profiling"
    MAPPING = "mapping"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ValidationSeverity(str, Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"


class RuleType(str, Enum):
    REQUIRED = "required"
    FORMAT = "format"
    RANGE = "range"
    DOMAIN = "domain"
    ETIM = "etim"
    CUSTOM = "custom"


class TransformType(str, Enum):
    DIRECT = "direct"
    LOOKUP = "lookup"
    FORMULA = "formula"
    CONCAT = "concat"
    SPLIT = "split"
    REGEX = "regex"
    UOM_CONVERT = "uom_convert"


class ExportFormat(str, Enum):
    BMECAT = "bmecat"
    ETIM = "etim"
    CSV = "csv"
    JSON = "json"
    XML = "xml"


class LineageNodeType(str, Enum):
    FILE = "file"
    JOB = "job"
    TEMPLATE = "template"
    PRODUCT = "product"
    EXPORT = "export"
    RULE = "rule"


class LineageEdgeType(str, Enum):
    PROCESSES = "processes"
    GENERATES = "generates"
    USES = "uses"
    DERIVES_FROM = "derives_from"
    VALIDATES = "validates"


# Base Models

class TimestampedModel(BaseModel):
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ClientScopedModel(BaseModel):
    client_id: str = Field(..., description="Tenant/client identifier")


# API Request/Response Models

class UploadRequest(BaseModel):
    """File upload request."""
    
    client_id: str
    filename: str
    content_type: str = "text/csv"
    hints: Dict[str, Any] = Field(default_factory=dict)


class UploadResponse(BaseModel):
    """File upload response."""
    
    job_id: UUID
    ingest_id: str
    upload_url: Optional[str] = None
    status: str = "created"


class PreviewRequest(BaseModel):
    """File preview request."""
    
    sample_size: int = 100
    detect_encoding: bool = True
    detect_delimiter: bool = True


class FileProfile(BaseModel):
    """File profiling results."""
    
    encoding: str
    delimiter: str
    has_header: bool
    row_count: int
    column_count: int
    columns: List[str]
    sample_rows: List[Dict[str, Any]]
    column_stats: Dict[str, Dict[str, Any]]
    quality_score: float
    issues: List[Dict[str, Any]] = Field(default_factory=list)


class PreviewResponse(BaseModel):
    """File preview response."""
    
    profile: FileProfile
    suggested_mappings: List[Dict[str, Any]] = Field(default_factory=list)


class TemplateSuggestRequest(BaseModel):
    """Template suggestion request."""
    
    columns: List[str]
    sample_data: List[Dict[str, Any]]
    etim_class: Optional[str] = None


class TemplateSuggestResponse(BaseModel):
    """Template suggestion response."""
    
    suggested_templates: List[Dict[str, Any]]
    confidence_scores: Dict[str, float]
    field_mappings: List[Dict[str, Any]]
    feature_mappings: List[Dict[str, Any]]


class TemplateCreateRequest(BaseModel):
    """Template creation request."""
    
    name: str
    description: Optional[str] = None
    field_mappings: List[Dict[str, Any]]
    feature_mappings: List[Dict[str, Any]]


class TemplateCreateResponse(BaseModel):
    """Template creation response."""
    
    template_id: UUID
    version: int
    status: str = "created"


class TemplateApplyRequest(BaseModel):
    """Template application request."""
    
    template_id: UUID
    overrides: Dict[str, Any] = Field(default_factory=dict)


class TemplateApplyResponse(BaseModel):
    """Template application response."""
    
    status: str
    mappings_applied: int
    issues: List[Dict[str, Any]] = Field(default_factory=list)


class ValidateRequest(BaseModel):
    """Validation request."""
    
    rule_sets: List[str] = Field(default_factory=list)
    quality_gates: Dict[str, Any] = Field(default_factory=dict)


class ValidateResponse(BaseModel):
    """Validation response."""
    
    summary: Dict[str, int]
    quality_score: float
    gate_status: str
    issues: List[Dict[str, Any]]
    recommendations: List[str] = Field(default_factory=list)


class ExecuteRequest(BaseModel):
    """Execution request."""
    
    dry_run: bool = False
    batch_size: int = 1000
    quality_gate_override: bool = False


class ExecuteResponse(BaseModel):
    """Execution response."""
    
    status: str
    execution_id: UUID
    estimated_duration: Optional[int] = None


class ProgressResponse(BaseModel):
    """Progress response."""
    
    job_id: UUID
    status: JobStatus
    progress: Dict[str, Any]
    current_stage: str
    stages_completed: List[str]
    estimated_completion: Optional[datetime] = None
    error_message: Optional[str] = None


class QualityAnalysisRequest(BaseModel):
    """Quality analysis request."""
    
    metrics: List[str] = Field(default_factory=list)
    include_recommendations: bool = True


class QualityAnalysisResponse(BaseModel):
    """Quality analysis response."""
    
    overall_score: float
    metrics: Dict[str, float]
    dimension_scores: Dict[str, float]
    recommendations: List[Dict[str, Any]]
    benchmark_comparison: Optional[Dict[str, Any]] = None


class RuleSuggestRequest(BaseModel):
    """Rule suggestion request."""
    
    target_type: str
    target_name: str
    sample_data: List[Any]
    etim_context: Optional[Dict[str, Any]] = None


class RuleSuggestResponse(BaseModel):
    """Rule suggestion response."""
    
    suggested_rules: List[Dict[str, Any]]
    confidence_scores: Dict[str, float]
    rationale: Dict[str, str]


class ExportRequest(BaseModel):
    """Export request."""
    
    export_format: ExportFormat
    export_profile: str
    filter_criteria: Dict[str, Any] = Field(default_factory=dict)
    etim_version: Optional[str] = None
    include_validation: bool = True


class ExportResponse(BaseModel):
    """Export response."""
    
    export_id: UUID
    status: str = "requested"
    estimated_duration: Optional[int] = None


class ExportStatusResponse(BaseModel):
    """Export status response."""
    
    export_id: UUID
    status: str
    progress: Dict[str, Any]
    download_url: Optional[str] = None
    file_size: Optional[int] = None
    checksum: Optional[str] = None
    validation_report: Optional[Dict[str, Any]] = None


# Search and Graph Models

class SearchRequest(BaseModel):
    """Search request."""
    
    query: str
    filters: Dict[str, Any] = Field(default_factory=dict)
    limit: int = 50
    offset: int = 0


class SearchResponse(BaseModel):
    """Search response."""
    
    results: List[Dict[str, Any]]
    total_count: int
    facets: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)


class GraphQueryRequest(BaseModel):
    """Graph query request."""
    
    query_type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    max_depth: int = 3


class GraphQueryResponse(BaseModel):
    """Graph query response."""
    
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    stats: Dict[str, Any]


# Health and Monitoring Models

class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str
    components: Dict[str, Dict[str, Any]]


class MetricsResponse(BaseModel):
    """Metrics response."""
    
    metrics: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Configuration Models

class AgentConfig(BaseModel):
    """Agent configuration."""
    
    agent_name: str
    input_topics: List[str]
    output_topics: List[str]
    batch_size: int = 100
    max_retries: int = 3
    timeout_seconds: int = 300
    concurrency: int = 1
    config: Dict[str, Any] = Field(default_factory=dict)


class SystemConfig(BaseModel):
    """System configuration."""
    
    database_url: str
    kafka_bootstrap_servers: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    redis_url: str
    jaeger_endpoint: str
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    environment: str = "development"