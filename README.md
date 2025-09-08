# 🦆 duck-shard 

**Universal data pipeline:** Convert, transform, and stream data with zero DevOps overhead.

Convert **Parquet**, **CSV**, **NDJSON** ↔ Stream to **HTTP APIs** ↔ Apply **SQL/jq transforms**

Cross-platform. No Python, no JVM, no drama.

---

## Quick Start

**Install:**
```bash
brew install duckdb jq
curl -O https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh
chmod +x duck-shard.sh
```

**Convert files:**
```bash
./duck-shard.sh ./data/ --format csv --output ./output/
```

**Stream to API:**
```bash
./duck-shard.sh ./data/ --url https://api.example.com/events --rows 1000
```

**Web Interface:**
```bash
./duck-shard.sh --ui  # Start web UI at http://localhost:8080
```

---

## Key Features

- **🔥 API Streaming** - POST to HTTP endpoints with batching & retry logic
- **🚀 Parallel Processing** - Utilize all CPU cores automatically  
- **🛠️ SQL Transforms** - Apply custom SQL with DuckDB's engine
- **☁️ Cloud Native** - Read/write GCS, S3, local storage
- **🎯 jq Transforms** - Powerful JSON transformations
- **🌐 Web Interface** - Visual configuration with real-time progress
- **🔍 Preview Mode** - Test on sample data first
- **📦 Zero Dependencies** - Just DuckDB + jq + bash

---

## Examples

```bash
# Basic conversion
./duck-shard.sh data/ -f csv -o ./out/

# SQL transformation  
./duck-shard.sh data/ --sql transform.sql -f ndjson -o ./processed/

# Stream to webhook with auth
./duck-shard.sh data/ --url https://api.example.com/webhook \
  --header "Authorization: Bearer token" -r 1000

# JSON transformation
./duck-shard.sh events.csv -f ndjson \
  --jq 'select(.event == "purchase") | {user: .user_id, amount: .revenue}' \
  -o ./purchases/

# Preview before processing
./duck-shard.sh large_dataset.parquet --preview 10 -f csv

# Cloud storage
./duck-shard.sh gs://bucket/data/ -f csv -o s3://other-bucket/output/
```

---

## CLI Reference

```bash
./duck-shard.sh <input_path> [max_parallel_jobs] [options]
```

**Core Options:**
- `-f, --format` - Output format: `ndjson`, `csv`, `parquet`
- `-o, --output` - Output directory (local or cloud)
- `-r, --rows` - Split into batches of N rows per file
- `-s, --single-file` - Merge everything into one file
- `--sql` - Apply SQL transformation
- `--jq` - Apply jq transformation to JSON
- `--preview` - Preview mode (don't write files)
- `--ui` - Start web interface

**API Streaming:**
- `--url` - POST to HTTP endpoint
- `--header` - Add HTTP header (repeatable)
- `--log` - Log responses to file

**Cloud Storage:**
- `--gcs-key/--gcs-secret` - GCS credentials
- `--s3-key/--s3-secret` - S3 credentials

Run `./duck-shard.sh --help` for complete options.

---

## License

MIT — go wild with your data!

**PRs welcome:** [github.com/ak--47/duck-shard](https://github.com/ak--47/duck-shard)