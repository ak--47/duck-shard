# 🦆 duck-shard

## 🤨 wat ?

duck-shard brings together three of my favorite high-performance tools: **DuckDB**, **jq**, and **curl**. Pipe them together and you get insane local performance even on modest hardware.

The idea is simple: instead of spinning up clusters or dealing with JVM heap tuning, just use the right tool for each job. DuckDB handles the heavy SQL lifting, jq transforms JSON like magic, and curl moves data to APIs. All running in parallel on your machine.

No Python environments. No Spark clusters. No Docker containers. Just fast, reliable data processing that fits in a single shell script.

## 👔 tldr;

Convert massive datasets between formats, apply SQL/jq transforms, stream to APIs. Built on DuckDB + bash + curl. Has a web UI. Stupid fast.

```bash
# Install and run
npx duck-shard

# CLI usage
npx duck-shard data.parquet -f csv -o ./clean/
npx duck-shard events/ --sql transform.sql --url https://api.company.com/ingest
```

---

## Install

### Option 1: npx (recommended)
```bash
npx duck-shard
```
No installation needed! Just run it. Dependencies are checked automatically.

### Option 2: npm global install
```bash
npm install -g duck-shard
duck-shard --ui
```

### Option 3: Homebrew (legacy)
```bash
brew tap ak--47/tap && brew install duck-shard
```

**Dependencies**: DuckDB and curl are required. jq is optional.
- **macOS**: `brew install duckdb jq`
- **Linux**: `apt-get install duckdb jq curl` or `yum install duckdb jq curl`
- **Windows**: `winget install DuckDB.cli jqlang.jq`

## Quick Start

### Web Interface (Recommended)
```bash
npx duck-shard          # Starts web UI at http://localhost:8080
npx duck-shard --ui     # Same as above
```

### Command Line
```bash
# Convert files
npx duck-shard ./data/ --format csv --output ./processed/

# Transform and stream to API
npx duck-shard ./events.json --sql "SELECT * WHERE event='purchase'" --url https://api.company.com/ingest
```

---

## What it does

**File conversion:** Parquet ↔ CSV ↔ NDJSON
**SQL transforms:** Full DuckDB power on any file format
**JSON transforms:** jq expressions for reshaping data
**API streaming:** POST results directly to webhooks
**Cloud storage:** Read/write GCS and S3 buckets
**Column selection:** Pick specific fields, handle tricky names like `$email`
**Progress bars:** See exactly what's happening during long operations

---

## Examples

```bash
# Basic conversion
duck-shard data/ -f csv -o ./output/

# Select specific columns (use single quotes for $ names)
duck-shard data.json -f csv --cols 'user_id,$email,timestamp' -o ./clean/

# SQL transformation (ETL mode)
duck-shard events.parquet --sql ./transform.sql -f ndjson -o ./processed/

# Analytical mode (no --format = display results + save CSV)  
duck-shard sales.parquet --sql ./monthly_analysis.sql -o ./reports/

# Stream to API with batching
duck-shard data/ --url https://api.example.com/ingest \
  --header "Authorization: Bearer token" --rows 1000

# JSON transformation with jq
duck-shard events.csv -f ndjson \
  --jq 'select(.event == "purchase") | {user: .user_id, revenue}' \
  -o ./purchases/

# Preview before processing
duck-shard huge-file.parquet --preview 10 -f csv

# Cloud storage
duck-shard gs://my-bucket/data/ -f csv -o s3://other-bucket/results/

# Single file output with custom name
duck-shard data/ --single-file -o ./merged-data.ndjson
```

---

## SQL Files & Analytical Mode

duck-shard has two SQL modes depending on whether you specify `--format`:

**ETL Mode** (with `--format`): Transform data and output in specified format
```bash
duck-shard data.parquet --sql ./transform.sql -f ndjson -o ./processed/
```

**Analytical Mode** (no `--format`): Display results in terminal + save as CSV
```bash
duck-shard data.parquet --sql ./analysis.sql -o ./reports/
# Shows formatted table in terminal AND saves query_result.csv
```

Your SQL files get an `input_data` view automatically:

```sql
-- monthly_analysis.sql
SELECT 
  DATE_TRUNC('month', created_at) as month,
  COUNT(*) as orders,
  SUM(revenue) as total_revenue,
  AVG(revenue) as avg_order_value
FROM input_data 
WHERE created_at >= '2024-01-01'
GROUP BY DATE_TRUNC('month', created_at)
ORDER BY month;
```

Works with any file format - duck-shard handles the loading, you write the analysis.

---

## Web UI

Launch the web interface for visual configuration:

```bash
duck-shard --ui
```

Features:
- Drag-and-drop file selection
- Visual column picker
- Real-time progress tracking via WebSockets
- Command preview and copy
- Built-in examples and validation

Perfect for prototyping transforms before scripting them.

---

## Performance Philosophy

The magic happens when you combine:

**DuckDB** for columnar analytics - processes GB files in seconds
**Parallel execution** across all CPU cores automatically
**Streaming architecture** - no loading entire datasets into memory
**Smart batching** - optimal chunk sizes for both storage and APIs

Result: Spark-like performance without the operational complexity. I've processed 100GB+ datasets on a MacBook faster than most "big data" stacks.

---

## CLI Reference

```bash
duck-shard <input> [options]
```

**Core:**
- `-f, --format` - Output: `ndjson`, `csv`, `parquet`
- `-o, --output` - Directory or file path (detects automatically in `--single-file` mode)
- `-s, --single-file` - Merge all inputs into one output
- `--cols` - Column selection: `'col1,$email,col3'` (use single quotes for $ names)
- `--sql` - Custom SQL file (gets `input_data` view)
- `--jq` - JSON transformation expression
- `--preview` - Test on first N rows without writing files
- `--verbose` - Show SQL commands and progress bars

**API Streaming:**
- `--url` - POST endpoint for results
- `--header` - HTTP headers (repeatable)
- `--rows` - Batch size (default: 1000 with `--url`)
- `--log` - Save all HTTP responses

**Cloud:**
- `--gcs-key/--gcs-secret` - Google Cloud Storage HMAC
- `--s3-key/--s3-secret` - AWS S3 credentials

**UI:**
- `--ui` - Launch web interface at localhost:8080

Run `duck-shard --help` for the complete list.

---

## License

MIT — AK

[Issues & PRs welcome](https://github.com/ak--47/duck-shard)
