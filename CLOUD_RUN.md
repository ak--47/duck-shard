# 🦆 Duck Shard Cloud Run API

A Cloud Run wrapper for duck-shard that provides a REST API for DuckDB ETL operations with jq transformations.

## Quick Deploy

1. **Set up your project:**
   ```bash
   echo "GCP_PROJECT_ID=your-gcp-project-id" > .env
   echo "GCS_KEY_ID=your-gcs-key" >> .env
   echo "GCS_SECRET=your-gcs-secret" >> .env
   ```

2. **Deploy to Cloud Run:**
   ```bash
   ./deploy.sh
   ```

## API Usage

### Endpoints

- `GET /` - API documentation and status
- `GET /health` - Health check
- `POST /run` - Execute duck-shard operations

### Example Requests

**Preview Mode (test transformations):**
```bash
curl -X POST https://your-service-url/run \
  -H "Content-Type: application/json" \
  -d '{
    "input_path": "gs://bucket/data.parquet",
    "format": "ndjson",
    "preview": 10,
    "jq": ".user_id = (.user_id | tonumber)"
  }'
```

**Full Processing with jq transformation:**
```bash
curl -X POST https://your-service-url/run \
  -H "Content-Type: application/json" \
  -d '{
    "input_path": "gs://bucket/input/",
    "format": "ndjson",
    "output": "gs://bucket/output/",
    "jq": "select(.event == \"purchase\") | {user: .user_id, amount: (.revenue | tonumber)}",
    "rows": 1000,
    "gcs_key": "your-key",
    "gcs_secret": "your-secret"
  }'
```

**Stream to HTTP API:**
```bash
curl -X POST https://your-service-url/run \
  -H "Content-Type: application/json" \
  -d '{
    "input_path": "gs://bucket/events.csv",
    "format": "ndjson",
    "jq": "select(.event == \"click\")",
    "url": "https://your-api.com/webhook",
    "header": ["Authorization: Bearer token", "X-Source: duck-shard"],
    "rows": 500
  }'
```

### Supported Parameters

All duck-shard CLI parameters are supported via JSON:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input_path` | string | Input file/directory path (required) |
| `format` | string | Output format: ndjson, csv, parquet |
| `output` | string | Output directory |
| `single_file` | string | Merge into single file |
| `cols` | string | Column selection (comma-separated) |
| `dedupe` | boolean | Remove duplicates |
| `rows` | number | Rows per output file |
| `sql` | string | SQL file path for transformations |
| `jq` | string | jq expression for JSON transformations |
| `preview` | number | Preview mode (first N rows) |
| `url` | string | HTTP endpoint for streaming |
| `header` | array | HTTP headers |
| `log` | boolean | Log HTTP responses |
| `verbose` | boolean | Verbose output |
| `gcs_key` | string | GCS HMAC key |
| `gcs_secret` | string | GCS HMAC secret |
| `s3_key` | string | S3 access key |
| `s3_secret` | string | S3 secret key |

## Response Format

```json
{
  "status": "success|error|timeout",
  "code": 0,
  "signal": null,
  "logs": "Duck shard output...",
  "error_logs": null,
  "request_id": "abc123",
  "duration": 1500,
  "timestamp": "2024-01-01T00:00:00.000Z"
}
```

## Environment Variables

Set these in your `.env` file or Cloud Run environment:

- `GCP_PROJECT_ID` - Your GCP project ID (required for deployment)
- `GCS_KEY_ID` - GCS HMAC access key (optional, can be passed in requests)
- `GCS_SECRET` - GCS HMAC secret (optional, can be passed in requests)
- `S3_KEY_ID` - S3 access key (optional)
- `S3_SECRET` - S3 secret key (optional)

## Limits

- **Request timeout:** 850 seconds (Cloud Run limit: 900s)
- **Memory:** 4GB (configurable in deploy.sh)
- **CPU:** 2 vCPUs (configurable in deploy.sh)
- **Max instances:** 10 (configurable in deploy.sh)
- **Request size:** 10MB max

## Local Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Test locally
node example-client.mjs http://localhost:8080
```

## Monitoring

- Health check: `GET /health`
- Logs are output to Cloud Run logs
- Request IDs are included for tracking

## Security

- Runs as non-root user in container
- Environment variables for sensitive data
- Request timeouts to prevent resource exhaustion
- Memory limits to prevent OOM issues

## Cost Optimization

- **Preview mode**: Test transformations on sample data before full processing
- **Request-based pricing**: Only pay when processing data
- **Auto-scaling**: Scales to zero when not in use
- **Configurable limits**: Adjust CPU/memory based on workload