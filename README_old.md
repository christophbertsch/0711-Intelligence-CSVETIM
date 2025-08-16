# CSV Import Guardian Agent System

A micro-agent architecture for CSV import processing with event-driven communication, designed to integrate with the 0711 Agent System API.

## Architecture Overview

This system implements a choreographed micro-agent architecture where small, single-purpose agents communicate via Kafka topics:

```
IngestAgent → ProfilingAgent → MappingAgent → ValidationAgent
→ NormalizationAgent → PersistenceAgent → ExportAgent(BMEcat/ETIM)
         ↘ DQReporterAgent ↗       ↘ LineageAgent ↗
```

## Core Components

### Agents
- **IngestAgent**: File upload and initial processing
- **ProfilingAgent**: Schema detection and column profiling
- **MappingAgent**: Field mapping using templates
- **ValidationAgent**: Data validation with configurable rules
- **NormalizationAgent**: Data normalization and unit conversion
- **PersistenceAgent**: Canonical data storage
- **ExportAgent**: BMEcat/ETIM export generation
- **DQReporterAgent**: Data quality reporting
- **LineageAgent**: End-to-end data lineage tracking

### Infrastructure
- **API Gateway**: HTTP endpoints mapped to 0711 Agent System API
- **Event Bus**: Kafka for inter-agent communication
- **Storage**: PostgreSQL for canonical data, S3-compatible for files
- **Monitoring**: OpenTelemetry tracing and metrics

## Quick Start

1. **Start the infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Initialize the database**:
   ```bash
   docker-compose exec api python scripts/init_db.py
   ```

3. **Upload a CSV file**:
   ```bash
   curl -X POST http://localhost:8000/v1/csv-import/upload \
     -F "file=@sample.csv" \
     -F "client_id=acme"
   ```

4. **Monitor processing**:
   ```bash
   # Check job status
   curl http://localhost:8000/v1/csv-import/jobs/{job_id}
   
   # View logs
   docker-compose logs -f
   ```

## API Endpoints

The system exposes the 0711 Agent System API endpoints:

- `POST /v1/csv-import/upload` - Upload CSV file
- `POST /v1/csv-import/jobs/{job_id}/preview` - Preview file structure
- `POST /v1/csv-import/templates/suggest` - Suggest mapping templates
- `POST /v1/csv-import/jobs/{job_id}/validate` - Validate data
- `POST /v1/csv-import/jobs/{job_id}/execute` - Execute import
- `GET /v1/csv-import/jobs/{job_id}/progress` - Check progress

## Event Topics

- `file.received` - New file uploaded
- `file.profiled` - Schema and stats detected
- `mapping.applied` - Field mappings applied
- `validation.results` - Validation completed
- `normalization.done` - Data normalized
- `persistence.upserted` - Data persisted
- `export.completed` - Export generated
- `dq.report.ready` - Quality report available

## Configuration

Environment variables:
- `DATABASE_URL` - PostgreSQL connection string
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses
- `S3_ENDPOINT_URL` - S3-compatible storage endpoint
- `API_HOST` - API server host (default: 0.0.0.0)
- `API_PORT` - API server port (default: 8000)

## Development

### Adding New Agents

1. Create agent class inheriting from `BaseAgent`
2. Implement `process_message` method
3. Define input/output topics
4. Add to docker-compose.yml
5. Update event contracts in `shared/events.py`

### Testing

```bash
# Run unit tests
docker-compose exec api pytest tests/

# Run integration tests
docker-compose exec api pytest tests/integration/

# Load test with sample data
python scripts/load_test.py
```

## Monitoring

- **Metrics**: Prometheus metrics at `/metrics`
- **Health**: Health checks at `/health`
- **Tracing**: Jaeger UI at http://localhost:16686
- **Logs**: Structured JSON logs via docker-compose logs

## Security

- Row-level security by client_id
- Signed URLs for file access
- Input validation and sanitization
- Audit logging for all operations