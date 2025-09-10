# 🦆 duck-shard

## 🤨 wat ?

duck-shard brings together three of my favorite high-performance tools: **DuckDB**, **jq**, and **curl**. Pipe them together and you get insane local performance even on modest hardware.

The idea is simple: instead of spinning up clusters or dealing with JVM heap tuning, just use the right tool for each job. DuckDB handles the heavy SQL lifting, jq transforms JSON like magic, and curl moves data to APIs. All running in parallel on your machine.

No Python environments. No Spark clusters. No Docker containers. Just fast, reliable data processing that fits in a single shell script.

## 👔 tldr;

Convert massive datasets between formats, apply SQL/jq transforms, stream to APIs. Built on DuckDB + bash + curl. Has a web UI. Stupid fast.

```bash
# Get it
curl -O https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh && chmod +x duck-shard.sh

# Use it
./duck-shard.sh data.parquet -f csv -o ./clean/
./duck-shard.sh events/ --sql transform.sql --url https://api.company.com/ingest
./duck-shard.sh --ui  # Web interface at localhost:8080
```

---

## Install & Run

```bash
# Install dependencies
brew install duckdb jq

# Download duck-shard
curl -O https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh
chmod +x duck-shard.sh

# Convert some files
./duck-shard.sh ./data/ --format csv --output ./processed/

# Or use the web UI
./duck-shard.sh --ui
```

Open http://localhost:8080 for a visual interface with real-time progress bars and drag-and-drop configuration.

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
./duck-shard.sh data/ -f csv -o ./output/

# Select specific columns (use single quotes for $ names)
./duck-shard.sh data.json -f csv --cols 'user_id,$email,timestamp' -o ./clean/

# SQL transformation (ETL mode)
./duck-shard.sh events.parquet --sql ./transform.sql -f ndjson -o ./processed/

# Analytical mode (no --format = display results + save CSV)  
./duck-shard.sh sales.parquet --sql ./monthly_analysis.sql -o ./reports/

# Stream to API with batching
./duck-shard.sh data/ --url https://api.example.com/ingest \
  --header "Authorization: Bearer token" --rows 1000

# JSON transformation with jq
./duck-shard.sh events.csv -f ndjson \
  --jq 'select(.event == "purchase") | {user: .user_id, revenue}' \
  -o ./purchases/

# Preview before processing
./duck-shard.sh huge-file.parquet --preview 10 -f csv

# Cloud storage
./duck-shard.sh gs://my-bucket/data/ -f csv -o s3://other-bucket/results/

# Single file output with custom name
./duck-shard.sh data/ --single-file -o ./merged-data.ndjson
```

---

## SQL Files & Analytical Mode

duck-shard has two SQL modes depending on whether you specify `--format`:

**ETL Mode** (with `--format`): Transform data and output in specified format
```bash
./duck-shard.sh data.parquet --sql ./transform.sql -f ndjson -o ./processed/
```

**Analytical Mode** (no `--format`): Display results in terminal + save as CSV
```bash
./duck-shard.sh data.parquet --sql ./analysis.sql -o ./reports/
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
./duck-shard.sh --ui
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
./duck-shard.sh <input> [options]
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

Run `./duck-shard.sh --help` for the complete list.

---

## License

MIT — AK

[Issues & PRs welcome](https://github.com/ak--47/duck-shard)
