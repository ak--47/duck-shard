# 🦆 duck-shard 🚀

A DuckDB CLI wrapper offered as swiss army knife which allows you to "batch everything to everything" for your data lake (or local).

convert, transform, and stream data to HTTP APIs with zero DevOps overhead. No cloud. No Python, no JVM, no drama.

convert folders or files of **Parquet**, **CSV**, or **NDJSON** into **NDJSON**, **CSV**, or **Parquet**.
*Stream processed data directly to HTTP endpoints. Apply JSON transformations with jq. Preview data before processing. Deduplicate. Merge. Split into shards. Parallelize across all CPU cores.*  It's great fun!

**Cross-platform, no Python, no JVM, no drama.**

---
## ⚡ Key Features

**🔥 HTTP API Streaming:** POST processed data directly to any HTTP endpoint with automatic batching, rate limiting, retry logic, and throughput monitoring.

**🚀 Spark Performance:** Process massive datasets in parallel across all CPU cores without cluster management.

**🛠️ SQL Transforms:** Apply custom SQL transformations using DuckDB's powerful engine.

**☁️ Cloud Native:** Read from and write to GCS, S3, or local storage seamlessly.

**🎯 JSON Transformations:** Apply powerful jq transformations to JSON data in real-time.

**🔍 Preview Mode:** Test transformations on sample data before full processing.

**📦 Zero Dependencies:** Just DuckDB + jq + bash. No Python environments, no JVM heap tuning.

---

## 🚀 **Quick Start**

**Install:**
```bash
brew install duckdb jq  # or download from duckdb.org and jqlang.org
curl -O https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh
chmod +x duck-shard.sh
```

**Basic File conversion:**
```bash
./duck-shard.sh ./data/ --format csv --output ./output/
```
formats supported: `ndjson`, `csv`, `parquet` ... all interchangeable

**Stream to HTTP API:**
```bash
./duck-shard.sh ./data/ --url https://api.example.com/events \
  --header "Authorization: Bearer token123" --rows 1000 # Stream 1k rows per batch
```

**JSON Transformations:**
```bash
./duck-shard.sh ./events.csv -f ndjson \
  --jq 'select(.event == "purchase") | {user: .user_id, amount: (.revenue | tonumber)}' \
  -o ./processed/
```

**Preview Mode:**
```bash
./duck-shard.sh ./large_dataset.parquet --preview 10 -f csv  # Preview first 10 rows
```

## 🌐 **HTTP API Streaming**

Duck-shard can POST processed data directly to HTTP endpoints, making it perfect for real-time data integration:

### **Basic API Streaming**
```bash
# Stream CSV data as JSON batches to a webhook
./duck-shard.sh ./sales_data.csv \
  --url https://webhook.site/abc123 \
  -f ndjson -r 1000
```

### **With Authentication & Headers**
```bash
# Post to API with custom headers
./duck-shard.sh ./events/ \
  --url https://api.analytics.com/ingest \
  --header "Authorization: Bearer sk-1234567890" \
  --header "Content-Type: application/json" \
  --header "X-Source: data-pipeline" \
  -r 500
```

### **SQL Transform + API**
```bash
# Transform data with SQL then stream to API
./duck-shard.sh ./raw_data.parquet \
  --sql ./transform.sql \
  --url https://api.example.com/processed \
  -f ndjson -r 1000
```

*Example transform.sql:*
```sql
SELECT
  user_id,
  event_name,
  CAST(timestamp AS VARCHAR) as event_time,
  JSON_EXTRACT(properties, '$.revenue') as revenue
FROM input_data
WHERE event_name IN ('purchase', 'signup')
  AND timestamp >= '2024-01-01'
```

### **Logging & Monitoring**
```bash
# Log all HTTP responses and monitor throughput
./duck-shard.sh ./data/ \
  --url https://api.example.com/webhook \
  --log \
  -r 1000

# Monitor output:
# ✅ Posted part-1-1.ndjson (HTTP 200) | 15.2 req/s, 15,200 rec/s
# ✅ Posted part-1-2.ndjson (HTTP 200) | 16.1 req/s, 16,100 rec/s

# Check response logs:
cat response-logs.json | jq '.[].http_code'
```

---

## 💻 **CLI Reference**

```bash
./duck-shard.sh <input_path> [max_parallel_jobs] [options]
```

### **Core Options**

| Option             | Description                                        |
| ------------------ | -------------------------------------------------- |
| `-f ndjson`        | Output format: `ndjson`, `csv`, `parquet`         |
| `-o output_dir`    | Output directory (local or cloud)                 |
| `-r N`             | Split into batches of N rows per file             |
| `-s [filename]`    | Merge everything into single file                 |
| `-c col1,col2`     | Select only specific columns                       |
| `--dedupe`         | Remove duplicate rows                              |
| `--sql file.sql`   | Apply SQL transformation (or analytical query)    |
| `--jq <expression>`| Apply jq transformation to JSON output            |
| `--preview [N]`    | Preview first N rows (default 10), don't write    |
| `--prefix <text>`  | Add prefix to all output filenames                |
| `--suffix <text>`  | Add suffix to output filenames (before extension) |

### **HTTP API Options**

| Option             | Description                                        |
| ------------------ | -------------------------------------------------- |
| `--url <api_url>`  | POST processed data to HTTP endpoint               |
| `--header <header>`| Add custom HTTP header (repeatable)               |
| `--log`            | Log HTTP responses to `response-logs.json`        |

### **Cloud Storage Options**

| Option             | Description                                        |
| ------------------ | -------------------------------------------------- |
| `--gcs-key KEY`    | Google Cloud Storage HMAC key                     |
| `--gcs-secret SEC` | Google Cloud Storage HMAC secret                  |
| `--s3-key KEY`     | AWS S3 access key                                 |
| `--s3-secret SEC`  | AWS S3 secret key                                 |

---

## 🎯 **Real-World Examples**

### **E-commerce Analytics Pipeline**
```bash
# Process daily sales, apply transforms, stream to analytics API
./duck-shard.sh gs://data-lake/sales/2024-06-11/ \
  --sql ./sql/clean_sales.sql \
  --url https://analytics.company.com/api/events \
  --header "Authorization: Bearer ${API_TOKEN}" \
  --header "X-Pipeline: daily-sales" \
  --log \
  -f ndjson -r 1000
```

### **Event Stream Processing**
```bash
# Convert Parquet event logs to JSON and stream to multiple endpoints
./duck-shard.sh ./events.parquet \
  --url https://webhook1.example.com/events \
  --header "X-Source: event-processor" \
  -r 500 &

./duck-shard.sh ./events.parquet \
  --url https://webhook2.example.com/backup \
  --header "X-Source: event-processor" \
  -r 500 &
```

### **Data Lake to API Integration**
```bash
# Stream processed customer data to CRM API
./duck-shard.sh s3://company-datalake/customers/ \
  --sql ./sql/customer_enrichment.sql \
  --url https://api.crm.com/customers/bulk \
  --header "Authorization: Bearer ${CRM_TOKEN}" \
  --header "Content-Type: application/json" \
  --log \
  -f ndjson -r 100
```

### **Local Development & Testing**
```bash
# Test API integration with local data
./duck-shard.sh ./test_data.csv \
  --url https://httpbin.org/post \
  --header "X-Test: true" \
  --log \
  -f ndjson -r 10 --verbose
```

### **Custom Filename Organization**
```bash
# Add prefixes and suffixes for organized output
./duck-shard.sh ./daily_events/ \
  --prefix "processed_" \
  --suffix "_clean" \
  -f csv -o ./output/
# Creates: processed_events-1_clean.csv, processed_events-2_clean.csv, etc.

# Analytical query with custom naming
./duck-shard.sh ./sales_data.parquet \
  --sql ./reports/quarterly_analysis.sql \
  --prefix "Q2_2024_" \
  --suffix "_report" \
  -o ./reports/
# Creates: Q2_2024_query_result_report.csv
```

---

## 🌩️ **Cloud Storage Support**

Read from and write to **Google Cloud Storage** and **Amazon S3**:

```bash
# GCS to local
./duck-shard.sh gs://my-bucket/data/ \
  --gcs-key YOUR_KEY --gcs-secret YOUR_SECRET \
  -f csv -o ./local_output/

# S3 to API
./duck-shard.sh s3://data-bucket/events/ \
  --s3-key AWS_KEY --s3-secret AWS_SECRET \
  --url https://api.example.com/ingest \
  -r 1000

# Local to GCS
./duck-shard.sh ./processed/ \
  --gcs-key YOUR_KEY --gcs-secret YOUR_SECRET \
  -f parquet -o gs://output-bucket/results/
```

---

## 📊 **Analytical Query Mode**

When you provide `--sql` without specifying `--format`, duck-shard enters analytical query mode. This mode executes your SQL query and displays results as a formatted table in the console while also saving the results as a CSV file.

**Perfect for:**
- Data exploration and analysis
- Quick ad-hoc queries on large datasets
- Generating reports for stakeholders
- Data quality checks

### **Basic Analytical Queries**
```bash
# Analyze sales data without specifying output format
./duck-shard.sh ./sales_data.parquet --sql ./analysis/monthly_summary.sql -o ./reports/

# Results will be displayed in console AND saved as query_result.csv
# Use --prefix/--suffix to customize the output filename
./duck-shard.sh ./events/ --sql ./analysis/user_behavior.sql --prefix "june_" --suffix "_analysis" -o ./reports/
# Saves as: reports/june_query_result_analysis.csv
```

### **Complex Analytics Example**
```sql
-- monthly_revenue_analysis.sql
SELECT 
  DATE_TRUNC('month', order_date) as month,
  COUNT(*) as total_orders,
  SUM(revenue) as total_revenue,
  AVG(revenue) as avg_order_value,
  COUNT(DISTINCT customer_id) as unique_customers
FROM input_data 
WHERE order_date >= '2024-01-01'
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;
```

```bash
# Run analytical query and see results immediately
./duck-shard.sh gs://data-lake/orders/ \
  --sql ./analysis/monthly_revenue_analysis.sql \
  --gcs-key YOUR_KEY --gcs-secret YOUR_SECRET \
  -o ./reports/
```

**Analytical Mode Features:**
- ✅ Pretty table output in console for immediate viewing
- ✅ Automatic CSV export for further analysis
- ✅ Works with both single files and directories
- ✅ Supports cloud storage (GCS, S3) input and output
- ✅ Custom filename prefix/suffix support
- ✅ No format conversion overhead - pure analytical focus

---

## 🦆 **SQL Transformations**

Apply any SQL transformation using DuckDB's powerful engine:

**Example: E-commerce event enrichment**
```sql
-- enrich_events.sql
SELECT
  event_id,
  user_id,
  event_type,
  CAST(timestamp AS VARCHAR) as event_time,

  -- Extract revenue from JSON properties
  CAST(JSON_EXTRACT(properties, '$.revenue') AS DECIMAL(10,2)) as revenue,
  JSON_EXTRACT(properties, '$.product_id') as product_id,

  -- Add calculated fields
  CASE
    WHEN event_type = 'purchase' AND revenue > 100 THEN 'high_value'
    WHEN event_type = 'purchase' THEN 'standard'
    ELSE 'non_purchase'
  END as customer_segment,

  -- Date partitioning
  DATE_TRUNC('day', timestamp) as event_date

FROM input_data
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAY
  AND event_type IN ('page_view', 'purchase', 'signup')
ORDER BY timestamp;
```

```bash
./duck-shard.sh ./raw_events.parquet \
  --sql ./enrich_events.sql \
  --url https://api.analytics.com/events \
  --header "Authorization: Bearer token" \
  -r 1000
```

---

## 🎯 **JSON Transformations with jq**

Apply powerful JSON transformations using [jq](https://jqlang.org/) expressions. Works with any JSON output format (`ndjson`, `json`, `jsonl`).

### **Filter & Transform**
```bash
# Filter only purchase events and reshape structure
./duck-shard.sh ./events.csv -f ndjson \
  --jq 'select(.event_type == "purchase") | {
    user: .user_id,
    revenue: (.amount | tonumber),
    timestamp: .created_at
  }' -o ./purchases/
```

### **Data Type Conversions**
```bash
# Convert string numbers to actual numbers
./duck-shard.sh ./analytics.parquet -f ndjson \
  --jq '.user_id = (.user_id | tonumber) | .revenue = (.revenue | tonumber)' \
  -o ./typed_data/
```

### **Complex Filtering**
```bash
# Filter high-value customers and add calculated fields
./duck-shard.sh ./customers.csv -f ndjson \
  --jq 'select(.lifetime_value | tonumber > 1000) | 
        . + {segment: "premium", processed_at: now}' \
  -o ./premium_customers/
```

### **Combine with SQL + jq**
```bash
# SQL transformation followed by jq reshaping
./duck-shard.sh ./raw_events.parquet \
  --sql ./sql/aggregate_by_user.sql \
  -f ndjson \
  --jq '{
    user_id: .user_id,
    metrics: {
      total_events: .event_count,
      revenue: .total_revenue,
      last_active: .last_event_time
    },
    tags: [.segment, .region]
  }' \
  --url https://api.analytics.com/users
```

### **Stream Filtered Data to APIs**
```bash
# Filter and stream only error events to monitoring system
./duck-shard.sh ./app_logs.ndjson \
  --jq 'select(.level == "ERROR") | {
    message: .msg,
    timestamp: .time,
    service: .service_name,
    stack_trace: .stack
  }' \
  --url https://monitoring.company.com/errors \
  --header "Authorization: Bearer ${MONITOR_TOKEN}" \
  -r 100
```

---

## 🔍 **Preview Mode**

Test your transformations on sample data before processing entire datasets.

### **Basic Preview**
```bash
# Preview first 10 rows (default)
./duck-shard.sh ./large_dataset.parquet --preview -f csv

# Preview specific number of rows
./duck-shard.sh ./events.csv --preview 5 -f ndjson
```

### **Preview with Transformations**
```bash
# Test SQL transformations
./duck-shard.sh ./raw_data.parquet \
  --preview 20 \
  --sql ./complex_transform.sql \
  -f ndjson

# Test jq transformations
./duck-shard.sh ./events.csv \
  --preview 3 \
  -f ndjson \
  --jq 'select(.event == "click") | {user: .user_id, page: .page_url}'
```

### **Preview for Development**
```bash
# Test complete pipeline before production run
./duck-shard.sh ./production_data.parquet \
  --preview 50 \
  --sql ./transforms/clean_data.sql \
  -f ndjson \
  --jq 'select(.is_valid == true) | del(.internal_fields)' \
  -c user_id,event_type,timestamp
```

**Preview mode:**
- ✅ Processes only the first N rows (much faster)
- ✅ Shows exact output format you'll get
- ✅ Works with all transformations (SQL, jq, column selection)
- ✅ No files written to disk
- ✅ Perfect for testing and development

---

## 🚀 **Performance & Features**

* **🔥 Parallel Processing:** Utilize all CPU cores automatically
* **⚡ Streaming:** Real-time HTTP POST with batching and rate limiting
* **🛡️ Reliability:** Automatic retries with exponential backoff
* **📊 Monitoring:** Live throughput stats (requests/sec, records/sec)
* **📝 Logging:** Complete HTTP response logging to JSON
* **🌍 Universal:** Works on macOS, Linux, and cloud containers
* **💾 Memory Efficient:** Stream processing without loading entire datasets
* **🔄 Format Agnostic:** Parquet ↔ CSV ↔ NDJSON ↔ JSON seamlessly

---

## 📦 **Installation**

### **Homebrew (macOS/Linux)**
```bash
brew install duckdb jq
brew tap ak--47/duck-shard
brew install duck-shard
```

### **Manual Installation**
```bash
# Install DuckDB
curl -L https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip -o duckdb.zip
unzip duckdb.zip && sudo mv duckdb /usr/local/bin/

# Install jq
sudo apt-get install jq  # Ubuntu/Debian
# or
brew install jq  # macOS

# Install duck-shard
curl -O https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh
chmod +x duck-shard.sh
```

### **Docker**
```bash
docker run --rm -v $(pwd):/data ak47/duck-shard \
  /data/input.parquet --url https://api.example.com/webhook -r 1000
```

---

## 🧪 **Testing**

Run the comprehensive test suite:

```bash
make test
```

Tests cover:
- All format conversions (Parquet ↔ CSV ↔ NDJSON)
- HTTP API streaming with various configurations
- Cloud storage integration (GCS, S3)
- SQL transformations
- jq JSON transformations and filtering
- Preview mode functionality
- Error handling and edge cases
- Performance and parallel processing

---

## 🤷 **Why duck-shard?**

Sometimes you need Spark-level data processing but don't want to:
- Manage cluster infrastructure
- Configure resource allocation
- Debug JVM memory issues
- Write complex streaming code
- Set up API integration manually

Duck-shard gives you the power of distributed data processing with the simplicity of a single binary. Perfect for:

- **Startups** that need enterprise-grade data processing without the DevOps overhead
- **Data engineers** who want to prototype pipelines quickly
- **API integrations** that require reliable data streaming
- **Cloud migrations** where you need format conversion + API delivery
- **Local development** where Spark is overkill

---

## 🪧 **License**

MIT — go wild with your data!

**PRs, feedback, and wild data dreams welcome.**
[Raise an issue or open a PR!](https://github.com/ak--47/duck-shard/issues)

---

**Happy sharding!** 🦆✨
