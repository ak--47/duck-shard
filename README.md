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

### Option 3: Homebrew (legacy - not recommended)
```bash
brew tap ak--47/tap && brew install duck-shard
```
*Note: Homebrew installation is deprecated. Use npx for the latest features.*

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

**File conversion:** Parquet ↔ CSV ↔ TSV ↔ NDJSON ↔ XML ↔ JSON (supports .gz, .bz2, .xz, .zst compression)
**Compressed output:** Write gzip-compressed files with `--compressed` flag (adds .gz extension)
**Fast mode:** Skip JSON parsing for NDJSON→NDJSON operations (25-50% faster for large files)
**SQL transforms:** Full DuckDB power on any file format
**JSON transforms:** jq expressions for reshaping data
**API streaming:** POST results directly to webhooks
**Cloud storage:** Read/write GCS and S3 buckets
**Column selection:** Pick specific fields, handle tricky names like `$email`
**XML processing:** Custom root elements, auto-detection, complex structures
**Progress bars:** See exactly what's happening during long operations

---

## Examples

```bash
# Basic conversion
duck-shard data/ -f csv -o ./output/

# Convert TSV files to CSV/Parquet
duck-shard data.tsv -f csv -o ./output/
duck-shard data/ -f tsv -o ./tab_files/

# Compressed files (auto-detected by extension)
duck-shard data.csv.gz -f parquet -o ./output/
duck-shard events.json.gz --preview 10
duck-shard gs://bucket/data.tsv.gz -f csv -o ./clean/

# Write compressed output (adds .gz extension)
duck-shard data/ -f parquet --compressed -o ./output/
duck-shard events.csv -f ndjson --compressed -o ./compressed/
duck-shard data/ --single-file --compressed -o ./merged.csv.gz

# Fast mode for NDJSON splitting (25-50% faster, no JSON parsing)
duck-shard huge.ndjson --fast-mode -f ndjson --rows 10000 -o ./chunks/
duck-shard events/*.jsonl --fast-mode -f ndjson -s merged.ndjson -o ./output/

# Convert XML to CSV with custom root element
duck-shard data.xml --xml-root 'records' -f csv -o ./output/

# Process XML with column selection and deduplication
duck-shard employees.xml --xml-root 'employees' -c 'id,name,department' --dedupe -f ndjson -o ./clean/

# XML from cloud storage with transformation
duck-shard gs://bucket/events.xml --xml-root 'events' --sql ./transform.sql -f parquet -o ./processed/

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

# XML from cloud to local CSV with specific root
duck-shard gs://bucket/data.xml --xml-root 'transactions' -f csv -o ./output/

# Merge multiple XML files with custom naming
duck-shard ./xml-files/ --xml-root 'records' --single-file --prefix 'merged_' --suffix '_clean' -f ndjson -o ./output/

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

## XML Processing

duck-shard provides first-class XML support with automatic schema detection and configurable parsing:

### Basic XML Processing
```bash
# Convert XML to any format
duck-shard data.xml -f csv -o ./output/
duck-shard events.xml -f ndjson -o ./processed/
duck-shard records.xml -f parquet -o ./warehouse/
```

### Custom Root Elements
By default, duck-shard expects XML with a `<root>` element. Use `--xml-root` to specify different structures:

```bash
# For XML with <employees> root
duck-shard staff.xml --xml-root 'employees' -f csv -o ./hr/

# For XML with <transactions> root
duck-shard sales.xml --xml-root 'transactions' -f ndjson -o ./finance/
```

### XML Example Structure
duck-shard works best with flat XML structures:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<root>
  <row>
    <id>1</id>
    <name>John Doe</name>
    <department>Engineering</department>
    <salary>75000</salary>
  </row>
  <row>
    <id>2</id>
    <name>Jane Smith</name>
    <department>Marketing</department>
    <salary>68000</salary>
  </row>
</root>
```

### Advanced XML Operations
```bash
# Column selection with XML
duck-shard employees.xml --xml-root 'staff' -c 'id,name,department' -f csv

# Deduplication
duck-shard events.xml --dedupe -f ndjson -o ./clean/

# SQL transformations on XML
duck-shard data.xml --sql ./transform.sql -f parquet -o ./processed/

# Merge multiple XML files
duck-shard ./xml-files/ --xml-root 'records' --single-file -o ./merged.ndjson

# Stream XML to API with batching
duck-shard events.xml --url https://api.company.com/ingest --rows 500
```

### Cloud Storage & XML
```bash
# Process XML from cloud storage
duck-shard gs://bucket/data.xml --xml-root 'events' -f csv -o ./local/

# Upload XML results to cloud
duck-shard local.xml -f parquet -o gs://warehouse/processed/
```

XML processing uses DuckDB's webbed extension for robust parsing and automatic type inference.

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
- `-f, --format` - Output: `ndjson`, `csv`, `tsv`, `parquet`, `xml`, `json`
- `-o, --output` - Directory or file path (detects automatically in `--single-file` mode)
- `-s, --single-file` - Merge all inputs into one output
- `--compressed` - Write gzip-compressed output files (adds `.gz` extension)
- `--fast-mode` - Skip JSON parsing for NDJSON→NDJSON (25-50% faster, no transformations)
- `--cols` - Column selection: `'col1,$email,col3'` (use single quotes for $ names)
- `--sql` - Custom SQL file (gets `input_data` view)
- `--jq` - JSON transformation expression
- `--preview` - Test on first N rows without writing files
- `--verbose` - Show SQL commands and progress bars

**XML Processing:**
- `--xml-root` - XML root element name for parsing (default: 'root')
- `--dedupe` - Remove duplicate rows (works with XML)
- `--prefix/--suffix` - Add prefixes/suffixes to output filenames

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
