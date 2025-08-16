# CSV Import Guardian Agent System - Architecture

## Overview

The CSV Import Guardian Agent System is a production-ready, micro-agent architecture designed for processing CSV imports with ETIM support, data quality validation, and BMEcat export generation. The system implements event-driven choreography with Kafka as the message bus, PostgreSQL for canonical data storage, and S3-compatible object storage for files.

## Architecture Principles

### Micro-Agent Architecture
- **Single Responsibility**: Each agent handles one specific aspect of the pipeline
- **Event-Driven**: Agents communicate via Kafka topics using well-defined event contracts
- **Autonomous**: Agents can be deployed, scaled, and updated independently
- **Resilient**: Built-in retry logic, dead letter queues, and circuit breakers

### Event-Driven Choreography
- **Loose Coupling**: Agents don't directly call each other
- **Scalability**: Easy to scale individual agents based on load
- **Reliability**: At-least-once delivery with idempotent processing
- **Observability**: Full distributed tracing and metrics

## System Components

### Core Agents

#### 1. IngestAgent
**Responsibility**: File upload and initial validation
- Monitors file uploads (S3 events or API triggers)
- Validates file format, size, and basic structure
- Stores file metadata in staging tables
- Emits `file.received` events

**Input**: File upload notifications
**Output**: `file.received` events

#### 2. ProfilingAgent
**Responsibility**: File structure analysis and data quality assessment
- Detects encoding, delimiter, and column structure
- Analyzes column types and generates statistics
- Calculates initial data quality metrics
- Stores profiling results and sample data

**Input**: `file.received` events
**Output**: `file.profiled` events

#### 3. MappingAgent
**Responsibility**: Field mapping and data transformation
- Finds or suggests mapping templates
- Applies field mappings to transform raw data
- Handles data type conversions and business rules
- Supports multiple transformation types (direct, lookup, formula, etc.)

**Input**: `file.profiled` events
**Output**: `mapping.applied` events

#### 4. ValidationAgent
**Responsibility**: Data validation against business rules
- Loads configurable validation rules
- Applies rules to mapped data (required fields, formats, ranges, domains)
- Calculates comprehensive quality metrics
- Determines quality gate status (PASS/WARN/FAIL)

**Input**: `mapping.applied` events
**Output**: `validation.results` events

#### 5. NormalizationAgent
**Responsibility**: Data normalization and unit conversion
- Normalizes units of measure using conversion tables
- Standardizes data formats and values
- Applies locale-specific transformations
- Handles currency and date normalization

**Input**: `validation.results` events (PASS/WARN only)
**Output**: `normalization.done` events

#### 6. PersistenceAgent
**Responsibility**: Canonical data storage
- Performs idempotent upserts into canonical tables
- Maintains product crosswalk tables
- Handles product deduplication and merging
- Updates data lineage information

**Input**: `normalization.done` events
**Output**: `persistence.upserted` events

### Supporting Agents

#### 7. ExportAgent
**Responsibility**: BMEcat/ETIM export generation
- Generates exports in multiple formats (BMEcat, ETIM, CSV, JSON)
- Validates exports against XSD schemas
- Handles large dataset streaming
- Supports incremental and full exports

**Input**: `export.requested` events
**Output**: `export.completed` events

#### 8. DQReporterAgent
**Responsibility**: Data quality reporting
- Aggregates validation results into comprehensive reports
- Generates HTML/PDF reports with visualizations
- Calculates quality trends and benchmarks
- Provides actionable recommendations

**Input**: `validation.results`, `persistence.upserted` events
**Output**: `dq.report.ready` events

#### 9. LineageAgent
**Responsibility**: End-to-end data lineage tracking
- Builds lineage graphs from file to export
- Tracks data transformations and quality changes
- Enables impact analysis and root cause investigation
- Supports compliance and audit requirements

**Input**: All processing events
**Output**: `lineage.updated` events

#### 10. ETIMGraphAgent (Optional)
**Responsibility**: ETIM taxonomy management
- Maintains ETIM class hierarchies and relationships
- Provides graph-based queries for classification
- Handles ETIM version migrations
- Supports semantic search and recommendations

**Input**: ETIM reference data updates
**Output**: Classification suggestions

## Data Flow

```
File Upload → IngestAgent → ProfilingAgent → MappingAgent → ValidationAgent
                                                                    ↓
ExportAgent ← PersistenceAgent ← NormalizationAgent ←──────────────┘
     ↓              ↓                    ↓
Export Files   Canonical Data    DQReporterAgent → Quality Reports
                    ↓
              LineageAgent → Lineage Graph
```

## Event Contracts

### Core Events

#### file.received
```json
{
  "event_id": "received-{ingest_id}",
  "event_type": "file.received",
  "client_id": "acme",
  "ingest_id": "acme-20240115-001",
  "uri": "s3://bucket/path/file.csv",
  "filename": "products.csv",
  "size_bytes": 1024000,
  "content_type": "text/csv",
  "hints": {"etim_class": "EC000123"}
}
```

#### file.profiled
```json
{
  "event_id": "profiled-{ingest_id}",
  "event_type": "file.profiled",
  "client_id": "acme",
  "ingest_id": "acme-20240115-001",
  "columns": ["SKU", "Name", "Length", "Material"],
  "encoding": "utf-8",
  "delimiter": ",",
  "has_header": true,
  "row_count": 10000,
  "column_count": 4,
  "quality_score": 0.85,
  "sample_rows": [...],
  "column_stats": {...}
}
```

#### mapping.applied
```json
{
  "event_id": "mapped-{ingest_id}",
  "event_type": "mapping.applied",
  "client_id": "acme",
  "ingest_id": "acme-20240115-001",
  "template_id": "uuid",
  "template_version": 1,
  "rows_processed": 10000,
  "mapping_issues": [...]
}
```

#### validation.results
```json
{
  "event_id": "validated-{ingest_id}",
  "event_type": "validation.results",
  "client_id": "acme",
  "ingest_id": "acme-20240115-001",
  "summary": {"ERROR": 5, "WARN": 23, "INFO": 0},
  "quality_score": 0.78,
  "gate_status": "WARN",
  "issues": [...]
}
```

## Database Schema

### Staging Tables
- `stg_file`: File metadata and profiling results
- `stg_row`: Raw row data with mapping results

### Canonical Tables
- `product`: Core product information
- `feature_value`: ETIM feature values
- `import_job`: Job tracking and status
- `validation_result`: Detailed validation results

### Mapping Configuration
- `map_template`: Mapping template definitions
- `map_field`: Field mapping rules
- `map_feature`: ETIM feature mapping rules
- `validation_rule`: Configurable validation rules

### Reference Data
- `etim_class`: ETIM classification hierarchy
- `etim_feature`: ETIM feature definitions
- `etim_class_feature`: Class-feature relationships
- `uom_mapping`: Unit of measure conversions

### Lineage and Audit
- `lineage_node`: Data lineage nodes
- `lineage_edge`: Data lineage relationships

## API Endpoints

### Core Import API (0711 Compatible)
- `POST /v1/csv-import/upload` - Upload CSV file
- `POST /v1/csv-import/jobs/{job_id}/preview` - Preview file structure
- `POST /v1/csv-import/templates/suggest` - Suggest mapping templates
- `POST /v1/csv-import/jobs/{job_id}/validate` - Validate data
- `POST /v1/csv-import/jobs/{job_id}/execute` - Execute import
- `GET /v1/csv-import/jobs/{job_id}/progress` - Check progress

### Template Management
- `POST /v1/csv-import/templates` - Create template
- `PUT /v1/csv-import/templates/{template_id}` - Update template
- `POST /v1/csv-import/templates/{template_id}/apply/{job_id}` - Apply template
- `GET /v1/csv-import/templates/predefined` - List predefined templates

### Quality and Validation
- `POST /v1/csv-import/suggest/rules` - Suggest validation rules
- `POST /v1/csv-import/analyze/quality` - Analyze data quality

### Export Management
- `POST /v1/exports` - Request export
- `GET /v1/exports/{export_id}` - Get export status
- `GET /v1/exports/formats` - List supported formats

## Deployment Architecture

### Container Strategy
- **API Gateway**: FastAPI application with Uvicorn
- **Agents**: Individual Python containers with Kafka consumers
- **Infrastructure**: Docker Compose for local development
- **Production**: Kubernetes with Helm charts

### Scaling Strategy
- **Horizontal**: Scale agents independently based on queue depth
- **Vertical**: Adjust memory/CPU per agent type
- **Auto-scaling**: Kubernetes HPA based on Kafka lag metrics

### High Availability
- **Database**: PostgreSQL with streaming replication
- **Message Bus**: Kafka cluster with multiple brokers
- **Storage**: S3 with cross-region replication
- **Agents**: Multiple replicas with leader election where needed

## Security

### Authentication & Authorization
- **API**: JWT tokens with role-based access control
- **Row-Level Security**: PostgreSQL RLS by client_id
- **Object Storage**: Signed URLs with expiration

### Data Protection
- **Encryption**: TLS in transit, AES-256 at rest
- **PII Handling**: Automatic detection and masking
- **Audit Logging**: All operations logged with user context

### Network Security
- **VPC**: Isolated network with private subnets
- **Firewall**: Restrictive security groups
- **Secrets**: Vault/KMS for credential management

## Monitoring & Observability

### Distributed Tracing
- **OpenTelemetry**: Full request tracing across agents
- **Jaeger**: Trace visualization and analysis
- **Correlation**: Trace ID propagation through events

### Metrics & Alerting
- **Prometheus**: Time-series metrics collection
- **Grafana**: Dashboards and visualization
- **AlertManager**: Intelligent alerting rules

### Logging
- **Structured**: JSON logs with correlation IDs
- **Centralized**: ELK stack or similar
- **Retention**: Configurable retention policies

### Health Checks
- **Liveness**: Basic agent responsiveness
- **Readiness**: Dependency health checks
- **Deep Health**: End-to-end pipeline validation

## Quality Assurance

### Testing Strategy
- **Unit Tests**: Individual agent logic
- **Integration Tests**: Agent-to-agent communication
- **End-to-End Tests**: Full pipeline validation
- **Performance Tests**: Load and stress testing

### Quality Gates
- **Code Quality**: SonarQube analysis
- **Security**: SAST/DAST scanning
- **Dependencies**: Vulnerability scanning
- **Performance**: Benchmark regression testing

## Configuration Management

### Environment Variables
- **Database**: Connection strings and pool settings
- **Kafka**: Bootstrap servers and security config
- **Storage**: S3 endpoints and credentials
- **Agents**: Batch sizes, timeouts, and retry policies

### Feature Flags
- **Processing**: Enable/disable specific transformations
- **Quality Gates**: Adjust thresholds dynamically
- **Exports**: Control format availability
- **Experimental**: A/B testing for new features

## Disaster Recovery

### Backup Strategy
- **Database**: Continuous WAL archiving
- **Files**: Cross-region S3 replication
- **Configuration**: GitOps with version control

### Recovery Procedures
- **RTO**: 4 hours for full system recovery
- **RPO**: 15 minutes maximum data loss
- **Testing**: Monthly DR drills
- **Documentation**: Runbooks for all scenarios

## Performance Characteristics

### Throughput
- **Files**: 1000+ files per hour
- **Rows**: 1M+ rows per hour per agent
- **Exports**: 100K+ products per export

### Latency
- **Upload to Profile**: < 2 minutes
- **Profile to Validation**: < 5 minutes
- **Validation to Export**: < 10 minutes
- **API Response**: < 200ms (95th percentile)

### Scalability
- **Horizontal**: Linear scaling with agent replicas
- **Data Volume**: Tested up to 10M products
- **Concurrent Users**: 1000+ simultaneous uploads

## Future Enhancements

### Machine Learning
- **Auto-Mapping**: ML-based field mapping suggestions
- **Quality Prediction**: Predictive quality scoring
- **Anomaly Detection**: Automated data quality monitoring

### Advanced Features
- **Real-time Processing**: Stream processing for live data
- **Multi-format Support**: Excel, XML, JSON imports
- **Collaborative Mapping**: Web-based mapping editor
- **API Integrations**: Direct ERP/PIM system connections

### Ecosystem Integration
- **Marketplace**: Template and rule sharing
- **Plugins**: Custom transformation plugins
- **Webhooks**: External system notifications
- **GraphQL**: Advanced query capabilities