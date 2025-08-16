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


# File and Staging Models

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


class StagingFile(ClientScopedModel, TimestampedModel):
    """Staging file metadata."""
    
    ingest_id: str
    uri: str
    filename: str
    size_bytes: int
    encoding: Optional[str] = None
    delimiter: Optional[str] = None
    has_header: bool = True
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    profile: Optional[FileProfile] = None
    status: str = "uploaded"


class StagingRow(ClientScopedModel, TimestampedModel):
    """Individual row in staging."""
    
    ingest_id: str
    row_num: int
    data: Dict[str, Any]
    row_hash: Optional[bytes] = None
    issues: List[Dict[str, Any]] = Field(default_factory=list)


# Product Models

class Product(ClientScopedModel, TimestampedModel):
    """Canonical product model."""
    
    product_id: UUID = Field(default_factory=uuid4)
    sku: str
    gtin: Optional[str] = None
    brand: Optional[str] = None
    manufacturer: Optional[str] = None
    product_name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    etim_class: Optional[str] = None
    lifecycle_status: str = "active"


class FeatureValue(TimestampedModel):
    """ETIM feature value for a product."""
    
    product_id: UUID
    etim_feature: str
    value_raw: Optional[str] = None
    unit_raw: Optional[str] = None
    value_norm: Optional[float] = None
    unit_norm: Optional[str] = None
    confidence: float = 1.0
    source_column: Optional[str] = None


# Mapping Models

class MapTemplate(ClientScopedModel, TimestampedModel):
    """Mapping template for field transformations."""
    
    template_id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    version: int = 1
    is_active: bool = True


class MapField(BaseModel):
    """Field mapping configuration."""
    
    template_id: UUID
    src_column: str
    target_table: str
    target_column: str
    transform_type: TransformType = TransformType.DIRECT
    transform_config: Dict[str, Any] = Field(default_factory=dict)
    is_required: bool = False
    default_value: Optional[str] = None
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)


class MapFeature(BaseModel):
    """ETIM feature mapping configuration."""
    
    template_id: UUID
    src_column: str
    etim_feature: str
    unit_column: Optional[str] = None
    transform_type: TransformType = TransformType.DIRECT
    transform_config: Dict[str, Any] = Field(default_factory=dict)
    is_required: bool = False
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)


# Validation Models

class ValidationRule(TimestampedModel):
    """Data validation rule."""
    
    rule_id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    rule_type: RuleType
    target_type: str  # 'field', 'feature', 'product'
    target_name: str
    rule_config: Dict[str, Any]
    severity: ValidationSeverity = ValidationSeverity.ERROR
    is_active: bool = True


class ValidationResult(ClientScopedModel, TimestampedModel):
    """Individual validation result."""
    
    result_id: UUID = Field(default_factory=uuid4)
    job_id: UUID
    rule_id: UUID
    ingest_id: str
    row_num: Optional[int] = None
    severity: ValidationSeverity
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)


# Job Models

class ImportJob(ClientScopedModel, TimestampedModel):
    """Import job tracking."""
    
    job_id: UUID = Field(default_factory=uuid4)
    ingest_id: str
    template_id: Optional[UUID] = None
    status: JobStatus = JobStatus.CREATED
    progress: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


# Lineage Models

class LineageNode(ClientScopedModel, TimestampedModel):
    """Lineage graph node."""
    
    node_id: UUID = Field(default_factory=uuid4)
    node_type: LineageNodeType
    node_name: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LineageEdge(TimestampedModel):
    """Lineage graph edge."""
    
    edge_id: UUID = Field(default_factory=uuid4)
    from_node_id: UUID
    to_node_id: UUID
    edge_type: LineageEdgeType
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ETIM Models

class ETIMClass(BaseModel):
    """ETIM classification."""
    
    class_code: str
    class_name: str
    parent_class: Optional[str] = None
    description: Optional[str] = None
    version: str = "9.0"


class ETIMFeature(BaseModel):
    """ETIM feature definition."""
    
    feature_code: str
    feature_name: str
    data_type: str  # 'N', 'A', 'L'
    unit: Optional[str] = None
    description: Optional[str] = None
    version: str = "9.0"


class ETIMClassFeature(BaseModel):
    """ETIM class-feature relationship."""
    
    class_code: str
    feature_code: str
    is_mandatory: bool = False
    sort_order: Optional[int] = None


# Unit of Measure Models

class UOMMapping(BaseModel):
    """Unit of measure conversion mapping."""
    
    source_unit: str
    target_unit: str
    conversion_factor: float
    conversion_offset: float = 0.0
    unit_type: Optional[str] = None


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
    """Response model for file preview."""
    
    columns: List[str] = Field(..., description="Column names")
    sample_rows: List[Dict[str, Any]] = Field(..., description="Sample data")
    encoding: str = Field(..., description="Detected encoding")
    delimiter: str = Field(..., description="Detected delimiter")
    has_header: bool = Field(..., description="Whether file has header")
    row_count: int = Field(..., description="Total row count")
    column_stats: Dict[str, Dict[str, Any]] = Field(..., description="Column statistics")


class TemplateCreateRequest(BaseModel):
    """Request model for creating mapping templates."""
    
    name: str = Field(..., description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    field_mappings: List[Dict[str, Any]] = Field(..., description="Field mapping definitions")
    feature_mappings: List[Dict[str, Any]] = Field(..., description="Feature mapping definitions")


class TemplateResponse(BaseModel):
    """Response model for mapping templates."""
    
    template_id: UUID = Field(..., description="Template identifier")
    client_id: str = Field(..., description="Client identifier")
    name: str = Field(..., description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    version: int = Field(..., description="Template version")
    is_active: bool = Field(..., description="Whether template is active")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")


class ValidationRequest(BaseModel):
    """Request model for data validation."""
    
    rule_sets: List[str] = Field(default_factory=list, description="Rule sets to apply")
    severity_threshold: str = Field(default="ERROR", description="Minimum severity to report")
    sample_only: bool = Field(default=False, description="Validate sample only")


class ValidationResponse(BaseModel):
    """Response model for validation results."""
    
    summary: Dict[str, int] = Field(..., description="Issue count by severity")
    issues: List[Dict[str, Any]] = Field(..., description="Validation issues")
    quality_score: float = Field(..., description="Overall quality score")
    gate_status: str = Field(..., description="Quality gate status")


class ExecuteRequest(BaseModel):
    """Request model for job execution."""
    
    dry_run: bool = Field(default=False, description="Whether to perform dry run")
    batch_size: int = Field(default=1000, description="Processing batch size")
    continue_on_error: bool = Field(default=False, description="Continue processing on errors")


class ExecuteResponse(BaseModel):
    """Response model for job execution."""
    
    execution_id: UUID = Field(..., description="Execution identifier")
    status: str = Field(..., description="Execution status")
    estimated_duration: Optional[int] = Field(None, description="Estimated duration in seconds")


class ProgressResponse(BaseModel):
    """Response model for job progress."""
    
    job_id: UUID = Field(..., description="Job identifier")
    status: str = Field(..., description="Current status")
    progress_percent: float = Field(..., description="Progress percentage")
    current_stage: str = Field(..., description="Current processing stage")
    stages_completed: List[str] = Field(..., description="Completed stages")
    stages_remaining: List[str] = Field(..., description="Remaining stages")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")
    error_message: Optional[str] = Field(None, description="Error message if failed")


# Database Models

class FileProfile(BaseModel):
    """File profiling information."""
    
    encoding: str = Field(..., description="Character encoding")
    delimiter: str = Field(..., description="CSV delimiter")
    has_header: bool = Field(..., description="Whether file has header")
    columns: List[str] = Field(..., description="Column names")
    row_count: int = Field(..., description="Total rows")
    column_count: int = Field(..., description="Total columns")
    sample_hash: str = Field(..., description="Hash of sample data")
    column_stats: Dict[str, Dict[str, Any]] = Field(..., description="Column statistics")
    quality_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Quality issues")


class ProductData(BaseModel):
    """Canonical product data structure."""
    
    sku: str = Field(..., description="Stock keeping unit")
    gtin: Optional[str] = Field(None, description="Global trade item number")
    brand: Optional[str] = Field(None, description="Brand name")
    manufacturer: Optional[str] = Field(None, description="Manufacturer name")
    product_name: Optional[str] = Field(None, description="Product name")
    description: Optional[str] = Field(None, description="Product description")
    category: Optional[str] = Field(None, description="Product category")
    etim_class: Optional[str] = Field(None, description="ETIM classification")


class FeatureValue(BaseModel):
    """ETIM feature value."""
    
    etim_feature: str = Field(..., description="ETIM feature code")
    value_raw: str = Field(..., description="Raw value from source")
    unit_raw: Optional[str] = Field(None, description="Raw unit from source")
    value_norm: Optional[float] = Field(None, description="Normalized numeric value")
    unit_norm: Optional[str] = Field(None, description="Normalized unit")
    confidence: float = Field(default=1.0, description="Confidence score")
    source_column: Optional[str] = Field(None, description="Source column name")


class MappedRow(BaseModel):
    """Mapped row data structure."""
    
    src_key: str = Field(..., description="Source row identifier")
    product: ProductData = Field(..., description="Product core data")
    features: List[FeatureValue] = Field(default_factory=list, description="Feature values")
    mapping_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Mapping issues")


class ValidationIssue(BaseModel):
    """Validation issue details."""
    
    rule_id: UUID = Field(..., description="Validation rule ID")
    rule_name: str = Field(..., description="Rule name")
    severity: str = Field(..., description="Issue severity")
    message: str = Field(..., description="Issue description")
    src_key: Optional[str] = Field(None, description="Source row identifier")
    field_name: Optional[str] = Field(None, description="Field name")
    feature_code: Optional[str] = Field(None, description="ETIM feature code")
    context: Dict[str, Any] = Field(default_factory=dict, description="Additional context")


class QualityMetrics(BaseModel):
    """Data quality metrics."""
    
    completeness: float = Field(..., description="Data completeness score")
    accuracy: float = Field(..., description="Data accuracy score")
    consistency: float = Field(..., description="Data consistency score")
    validity: float = Field(..., description="Data validity score")
    uniqueness: float = Field(..., description="Data uniqueness score")
    overall_score: float = Field(..., description="Overall quality score")


class LineageNode(BaseModel):
    """Data lineage node."""
    
    node_id: UUID = Field(..., description="Node identifier")
    node_type: str = Field(..., description="Node type")
    node_name: str = Field(..., description="Node name")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Node metadata")


class LineageEdge(BaseModel):
    """Data lineage edge."""
    
    from_node_id: UUID = Field(..., description="Source node ID")
    to_node_id: UUID = Field(..., description="Target node ID")
    edge_type: str = Field(..., description="Relationship type")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Edge metadata")


# ETIM Reference Models

class ETIMClass(BaseModel):
    """ETIM classification."""
    
    class_code: str = Field(..., description="ETIM class code")
    class_name: str = Field(..., description="Class name")
    parent_class: Optional[str] = Field(None, description="Parent class code")
    description: Optional[str] = Field(None, description="Class description")
    version: str = Field(default="9.0", description="ETIM version")


class ETIMFeature(BaseModel):
    """ETIM feature definition."""
    
    feature_code: str = Field(..., description="ETIM feature code")
    feature_name: str = Field(..., description="Feature name")
    data_type: str = Field(..., description="Data type: N, A, L")
    unit: Optional[str] = Field(None, description="Unit of measure")
    description: Optional[str] = Field(None, description="Feature description")
    version: str = Field(default="9.0", description="ETIM version")


class UnitMapping(BaseModel):
    """Unit of measure mapping."""
    
    source_unit: str = Field(..., description="Source unit")
    target_unit: str = Field(..., description="Target unit")
    conversion_factor: float = Field(..., description="Conversion factor")
    conversion_offset: float = Field(default=0.0, description="Conversion offset")
    unit_type: Optional[str] = Field(None, description="Unit type category")


# Export Models

class ExportRequest(BaseModel):
    """Export request model."""
    
    export_format: str = Field(..., description="Export format")
    export_profile: str = Field(..., description="Export profile")
    filter_criteria: Dict[str, Any] = Field(default_factory=dict, description="Export filters")
    etim_version: Optional[str] = Field(None, description="ETIM version")
    include_features: bool = Field(default=True, description="Include feature values")
    include_media: bool = Field(default=False, description="Include media references")


class ExportResponse(BaseModel):
    """Export response model."""
    
    export_id: UUID = Field(..., description="Export identifier")
    status: str = Field(..., description="Export status")
    download_url: Optional[str] = Field(None, description="Download URL when ready")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion")


# Configuration Models

class AgentConfig(BaseModel):
    """Agent configuration."""
    
    agent_name: str = Field(..., description="Agent name")
    input_topics: List[str] = Field(..., description="Input Kafka topics")
    output_topics: List[str] = Field(..., description="Output Kafka topics")
    batch_size: int = Field(default=100, description="Processing batch size")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: int = Field(default=5, description="Retry delay in seconds")
    health_check_interval: int = Field(default=30, description="Health check interval")


class DatabaseConfig(BaseModel):
    """Database configuration."""
    
    url: str = Field(..., description="Database connection URL")
    pool_size: int = Field(default=10, description="Connection pool size")
    max_overflow: int = Field(default=20, description="Maximum pool overflow")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    pool_recycle: int = Field(default=3600, description="Pool recycle time")


class KafkaConfig(BaseModel):
    """Kafka configuration."""
    
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: Optional[str] = Field(None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(None, description="SASL username")
    sasl_password: Optional[str] = Field(None, description="SASL password")
    consumer_group_prefix: str = Field(default="csv-guardian", description="Consumer group prefix")
    auto_offset_reset: str = Field(default="earliest", description="Auto offset reset")
    enable_auto_commit: bool = Field(default=False, description="Enable auto commit")


class StorageConfig(BaseModel):
    """Object storage configuration."""
    
    endpoint_url: str = Field(..., description="S3-compatible endpoint URL")
    access_key: str = Field(..., description="Access key")
    secret_key: str = Field(..., description="Secret key")
    bucket_name: str = Field(default="csv-guardian", description="Default bucket name")
    region: str = Field(default="us-east-1", description="Region")
    use_ssl: bool = Field(default=True, description="Use SSL")