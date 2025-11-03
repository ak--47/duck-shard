#!/usr/bin/env bash
# shellcheck disable=SC2034,SC2155
# duck-shard.sh – DuckDB-based ETL/conversion for local/cloud files, cross-platform.
# Auto-updated via GitHub Actions

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

set -euo pipefail

command -v duckdb >/dev/null 2>&1 || {
  echo "Error: duckdb not installed. See https://duckdb.org/docs/installation/" >&2
  exit 1
}

SUPPORTED_EXTENSIONS="parquet csv tsv ndjson jsonl json xml"
COMPRESSION_EXTENSIONS="gz bz2 xz zst"
MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""
FORMAT=""
FORMAT_EXPLICITLY_SET=false
SELECT_COLUMNS="*"
DEDUPE=false
OUTPUT_DIR=""
ROWS_PER_FILE=0
COMPRESSED=false
GCS_KEY_ID="${GCS_KEY_ID:-}"
GCS_SECRET="${GCS_SECRET:-}"
S3_KEY_ID="${S3_KEY_ID:-}"
S3_SECRET="${S3_SECRET:-}"
SQL_FILE=""
VERBOSE=false
POST_URL=""
HTTP_HEADERS=()
HTTP_RATE_LIMIT_DELAY=0.1  # seconds between requests
LOG_RESPONSES=false
RESPONSE_LOG_FILE="response-logs.json"
HTTP_START_TIME=""
JQ_EXPRESSION=""
PREVIEW_ROWS=0
FILE_PREFIX=""
FILE_SUFFIX=""
XML_ROOT="root"

print_help() {
  cat <<EOF

     ██████╗ ██╗   ██╗ ██████╗██╗  ██╗      ███████╗██╗  ██╗ █████╗ ██████╗ ██████╗
     ██╔══██╗██║   ██║██╔════╝██║ ██╔╝      ██╔════╝██║  ██║██╔══██╗██╔══██╗██╔══██╗
     ██║  ██║██║   ██║██║     █████╔╝ █████╗███████╗███████║███████║██████╔╝██║  ██║
     ██║  ██║██║   ██║██║     ██╔═██╗ ╚════╝╚════██║██╔══██║██╔══██║██╔══██╗██║  ██║
     ██████╔╝╚██████╔╝╚██████╗██║  ██╗      ███████║██║  ██║██║  ██║██║  ██║██████╔╝
     ╚═════╝  ╚═════╝  ╚═════╝╚═╝  ╚═╝      ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝
            by AK

Usage: $0 <input_path> [max_parallel_jobs] [options]

Options:
  -s, --single-file [output_filename]   Merge into one output file (optional: filename or gs://...)
  -f, --format <ndjson|parquet|csv|tsv|xml> Output format (default: ndjson)
  -c, --cols <col1,col2,...>            Only include specific columns
  --dedupe                              Remove duplicate rows (by chosen columns)
  --compressed                          Write gzip-compressed output files (adds .gz extension)
  -o, --output <output_dir>             Output directory (local or gs://... or s3://...)
  -r, --rows <rows_per_file>            Split output files with N rows each (not for --single-file)
  --sql <sql_file>                      Use custom SQL SELECT (on temp view input_data)
  --gcs-key <key> --gcs-secret <secret> GCS HMAC credentials
  --s3-key <key> --s3-secret <secret>   S3 HMAC credentials
  --url <api_url>                       POST processed data to API URL in batches
  --header <header>                     Add custom HTTP header (can be used multiple times)
  --log                                 Log HTTP responses to response-logs.json
  --jq <expression>                     Apply jq transformation to JSON output (requires json/ndjson/jsonl format)
  --xml-root <element>                  XML root element name for parsing (default: root)
  --preview <N>                         Preview mode: process only first N rows (default 10), don't write files
  --prefix <prefix>                     Add prefix to output filenames
  --suffix <suffix>                     Add suffix to output filenames (before extension)
  --verbose                             Print all DuckDB SQL commands before running them
  --ui                                  Start web interface server (requires Node.js)
  -h, --help                            Print this help

Examples:
  $0 data/ -f csv -o ./out/
  $0 data/ -f tsv -o ./out/
  $0 data.csv.gz -f parquet -o ./out/      # Gzip compressed files auto-detected
  $0 data/ -f parquet --compressed -o ./out/  # Output compressed parquet.gz files
  $0 data.tsv.gz --preview 5               # Preview compressed TSV files
  $0 data/ -s merged.ndjson
  $0 data/ -f xml -o ./converted/
  $0 data.xml --xml-root 'records' -f csv -o ./out/
  $0 gs://bucket/data.csv.gz -f csv -o gs://other-bucket/output/
  $0 data/ --sql my_query.sql -f csv -o ./out/
  $0 data/ --url https://api.example.com/webhook --header "Authorization: Bearer token" -r 1000
  $0 data/ -f ndjson --jq '.user_id = (.user_id | tonumber)' -o ./out/
  $0 data/ --preview 5 -f csv
  $0 data/ -f ndjson --jq 'select(.event == "click")' --url https://api.example.com/data
  $0 data/ -f csv --prefix "processed_" --suffix "_clean" -o ./out/
  $0 data/ --sql analysis.sql -o ./results/  # Analytical query mode (no --format)
  $0 --ui  # Start web interface at http://localhost:8080


EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--single-file) SINGLE_FILE=true; shift
      if [[ $# -gt 0 && ! $1 =~ ^- ]]; then OUTPUT_FILENAME="$1"; shift; fi ;;
    -f|--format) [[ $# -ge 2 ]] || { echo "Error: --format needs an argument"; exit 1; }
      FORMAT="$2"; FORMAT_EXPLICITLY_SET=true; shift 2 ;;
    -c|--cols) [[ $# -ge 2 ]] || { echo "Error: --cols needs an argument"; exit 1; }
      SELECT_COLUMNS="$(echo "$2" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
      SELECT_COLUMNS="$(echo "$SELECT_COLUMNS" | paste -sd, -)"
      [[ -z "$SELECT_COLUMNS" ]] && SELECT_COLUMNS="*"
      shift 2 ;;
    --dedupe) DEDUPE=true; shift ;;
    --compressed) COMPRESSED=true; shift ;;
    -o|--output) [[ $# -ge 2 ]] || { echo "Error: --output needs an argument"; exit 1; }
      OUTPUT_DIR="$2"; shift 2 ;;
    -r|--rows) [[ $# -ge 2 ]] || { echo "Error: --rows needs an integer argument"; exit 1; }
      [[ "$2" =~ ^[0-9]+$ ]] || { echo "Error: --rows must be integer"; exit 1; }
      ROWS_PER_FILE="$2"; shift 2 ;;
    --sql) [[ $# -ge 2 ]] || { echo "Error: --sql needs an argument"; exit 1; }
      SQL_FILE="$2"; [[ -f "$SQL_FILE" ]] || { echo "Error: SQL file $SQL_FILE not found"; exit 1; }
      shift 2 ;;
    --gcs-key) [[ $# -ge 2 ]] || { echo "Error: --gcs-key needs an argument"; exit 1; }
      GCS_KEY_ID="$2"; shift 2 ;;
    --gcs-secret) [[ $# -ge 2 ]] || { echo "Error: --gcs-secret needs an argument"; exit 1; }
      GCS_SECRET="$2"; shift 2 ;;
    --s3-key) [[ $# -ge 2 ]] || { echo "Error: --s3-key needs an argument"; exit 1; }
      S3_KEY_ID="$2"; shift 2 ;;
    --s3-secret) [[ $# -ge 2 ]] || { echo "Error: --s3-secret needs an argument"; exit 1; }
      S3_SECRET="$2"; shift 2 ;;
    --url) [[ $# -ge 2 ]] || { echo "Error: --url needs an argument"; exit 1; }
      POST_URL="$2"; shift 2 ;;
    --header) [[ $# -ge 2 ]] || { echo "Error: --header needs an argument"; exit 1; }
      HTTP_HEADERS+=("$2"); shift 2 ;;
    --log) LOG_RESPONSES=true; shift ;;
    --jq) [[ $# -ge 2 ]] || { echo "Error: --jq needs an argument"; exit 1; }
      JQ_EXPRESSION="$2"; shift 2 ;;
    --xml-root) [[ $# -ge 2 ]] || { echo "Error: --xml-root needs an argument"; exit 1; }
      XML_ROOT="$2"; shift 2 ;;
    --preview) PREVIEW_ROWS=10; shift
      if [[ $# -gt 0 && $1 =~ ^[0-9]+$ ]]; then PREVIEW_ROWS="$1"; shift; fi ;;
    --prefix) [[ $# -ge 2 ]] || { echo "Error: --prefix needs an argument"; exit 1; }
      FILE_PREFIX="$2"; shift 2 ;;
    --suffix) [[ $# -ge 2 ]] || { echo "Error: --suffix needs an argument"; exit 1; }
      FILE_SUFFIX="$2"; shift 2 ;;
    --verbose) VERBOSE=true; shift ;;
    --ui) 
      echo "🦆 Starting Duck Shard Web Interface..."
      command -v node >/dev/null 2>&1 || {
        echo "Error: Node.js not installed. Please install Node.js to use the web interface." >&2
        exit 1
      }
      cd "$(dirname "$0")"
      echo "Installing dependencies..."
      npm install --silent || {
        echo "Error: Failed to install Node.js dependencies." >&2
        exit 1
      }
      echo "Starting server..."
      exec node server.mjs
      ;;
    -h|--help) print_help; exit 0 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]+"${POSITIONAL[@]}"}"

export GCS_KEY_ID GCS_SECRET S3_KEY_ID S3_SECRET

# Check for jq if JQ_EXPRESSION is provided
if [[ -n "$JQ_EXPRESSION" ]]; then
  command -v jq >/dev/null 2>&1 || {
    echo "Error: jq not installed. jq is required for --jq functionality. See https://jqlang.org/" >&2
    exit 1
  }
fi

if [[ $# -eq 0 ]]; then print_help; exit 0; fi

[[ $# -ge 1 ]] || { echo "Error: Missing <input_path>"; print_help >&2; exit 1; }
INPUT_PATH="$1"
if [[ "${INPUT_PATH}" =~ ^(gs|s3):// ]]; then :; else [[ -e "$INPUT_PATH" ]] || { echo "Error: '$INPUT_PATH' not found" >&2; exit 1; }; fi

to_abs() {
  case "$1" in /*) echo "$1" ;; *) echo "$PWD/${1#./}" ;; esac
}
# For local paths only, resolve to absolute
if [[ ! "${INPUT_PATH}" =~ ^(gs|s3):// ]]; then
  INPUT_PATH=$(to_abs "$INPUT_PATH")
fi

if [[ $# -ge 2 && $2 =~ ^[0-9]+$ ]]; then MAX_PARALLEL_JOBS="$2"; fi

if [[ -n "${OUTPUT_DIR:-}" && ! "${OUTPUT_DIR}" =~ ^(gs|s3):// ]]; then
  OUTPUT_DIR=$(to_abs "$OUTPUT_DIR")
  mkdir -p "$OUTPUT_DIR"
fi

# Detect analytical query mode (no --format specified but --sql is provided)
ANALYTICAL_MODE=false
if [[ $FORMAT_EXPLICITLY_SET == false && -n "$SQL_FILE" ]]; then
  ANALYTICAL_MODE=true
  # Set default output directory if not specified
  if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="."
  fi
else
  # Set default format if not explicitly set and not in analytical mode
  if [[ $FORMAT_EXPLICITLY_SET == false ]]; then
    FORMAT="ndjson"
  fi
fi

# Set format-specific variables (skip in analytical mode)
if ! $ANALYTICAL_MODE; then
  case "$FORMAT" in
    ndjson)  EXT="ndjson";  COPY_OPTS="FORMAT JSON" ;;
    parquet) EXT="parquet"; COPY_OPTS="FORMAT PARQUET" ;;
    csv)     EXT="csv";     COPY_OPTS="FORMAT CSV, HEADER" ;;
    tsv)     EXT="tsv";     COPY_OPTS="FORMAT CSV, HEADER, DELIMITER '\t'" ;;
    xml)     EXT="xml";     COPY_OPTS="FORMAT JSON" ;;
    jsonl|json) EXT="json"; COPY_OPTS="FORMAT JSON" ;;
    "") echo "Error: --format must be specified or use --sql without --format for analytical mode"; exit 1 ;;
    *) echo "Error: --format must be ndjson, parquet, json, csv, tsv, or xml"; exit 1 ;;
  esac

  # Add compression if requested
  if $COMPRESSED; then
    COPY_OPTS="$COPY_OPTS, COMPRESSION GZIP"
  fi
fi

# Validation for --jq usage
if [[ -n "$JQ_EXPRESSION" ]]; then
  if $ANALYTICAL_MODE; then
    echo "Error: --jq cannot be used in analytical query mode (when --format is not specified)" >&2; exit 1
  fi
  case "$FORMAT" in
    ndjson|jsonl|json) ;;
    *) echo "Error: --jq can only be used with JSON output formats (ndjson, json, jsonl)" >&2; exit 1 ;;
  esac
fi

# Validation for --url usage
if [[ -n "$POST_URL" ]]; then
  # When using --url, we need local output to POST files
  if [[ -n "$OUTPUT_DIR" && "$OUTPUT_DIR" =~ ^(gs|s3):// ]]; then
    echo "Error: --url cannot be used with cloud storage output directories (gs:// or s3://)" >&2
    echo "Use a local output directory with -o flag when using --url" >&2
    exit 1
  fi
  # Default rows per file to 1000 if not specified for batching
  if [[ $ROWS_PER_FILE -eq 0 && ! $SINGLE_FILE ]]; then
    ROWS_PER_FILE=1000
    echo "📦 --url specified: defaulting to --rows 1000 for batching"
  fi
fi

if (( ROWS_PER_FILE > 0 )) && $SINGLE_FILE; then
  echo "Error: --rows cannot be used with --single-file mode"; exit 1
fi

# Validation for analytical mode
if $ANALYTICAL_MODE; then
  if [[ -n "$POST_URL" ]]; then
    echo "Error: --url cannot be used in analytical query mode" >&2; exit 1
  fi
  if $SINGLE_FILE; then
    echo "Error: --single-file cannot be used in analytical query mode" >&2; exit 1
  fi
  if (( ROWS_PER_FILE > 0 )); then
    echo "Error: --rows cannot be used in analytical query mode" >&2; exit 1
  fi
fi

# Validation for --preview usage
if (( PREVIEW_ROWS > 0 )); then
  if [[ -n "$POST_URL" ]]; then
    echo "Error: --preview cannot be used with --url (preview mode doesn't POST data)" >&2; exit 1
  fi
  if $ANALYTICAL_MODE; then
    echo "Error: --preview cannot be used in analytical query mode" >&2; exit 1
  fi
  if [[ -n "${OUTPUT_DIR:-}" || -n "${OUTPUT_FILENAME:-}" ]]; then
    echo "Warning: --preview mode specified - no files will be written to disk" >&2
  fi
fi

get_file_format() {
  local file_path="$1"
  local basename_file=$(basename "$file_path")

  # Handle compressed files (remove .gz, .bz2, .xz, .zst extensions)
  if [[ "$basename_file" =~ \.(gz|bz2|xz|zst)$ ]]; then
    basename_file="${basename_file%.*}"
  fi

  # Extract the actual format extension
  local ext="${basename_file##*.}"
  echo "$ext"
}

get_duckdb_func() {
  local ext="$1"
  case "$ext" in
    parquet) echo "read_parquet" ;;
    csv)     echo "read_csv_auto" ;;
    tsv)     echo "read_csv" ;;
    ndjson|jsonl|json) echo "read_json_auto" ;;
    xml)
      if ! $XML_SUPPORTED; then
        echo "Error: XML file detected, but webbed extension is unavailable for this platform" >&2
        echo "XML files cannot be processed without the webbed extension" >&2
        exit 1
      fi
      echo "read_xml"
      ;;
    *) echo "Error: Unsupported extension: $ext" >&2; exit 1 ;;
  esac
}

build_duckdb_call() {
  local func="$1"
  local file_path="$2"
  if [[ "$func" == "read_xml" ]]; then
    # Use read_xml with proper configuration for complex XML structures
    echo "read_xml('$file_path', root_element='$XML_ROOT', auto_detect=true, maximum_file_size=52428800, ignore_errors=true)"
  elif [[ "$func" == "read_csv" ]]; then
    # For TSV files, use read_csv with tab delimiter
    echo "read_csv('$file_path', delim='\t')"
  elif [[ "$func" == "read_json_auto" ]]; then
    # Use read_json_auto with robust error handling for schema variations
    echo "read_json_auto('$file_path', ignore_errors=true, union_by_name=true, maximum_depth=-1)"
  else
    echo "$func('$file_path')"
  fi
}

build_duckdb_array_call() {
  local func="$1"
  local file_array="$2"
  if [[ "$func" == "read_xml" ]]; then
    # For XML arrays, read_xml doesn't support arrays, so we need to UNION individual calls
    # file_array comes in as "ARRAY['file1','file2']" format, extract individual files
    local files_str=$(echo "$file_array" | sed 's/ARRAY\[\(.*\)\]/\1/')
    local files_list=""
    local first=true

    # Parse files from comma-separated list
    echo "$files_str" | tr ',' '\n' | while read -r file; do
      file=$(echo "$file" | sed "s/^'//" | sed "s/'$//")
      if [[ -n "$file" ]]; then
        if [[ "$first" == "true" ]]; then
          echo "SELECT * FROM read_xml('$file', root_element='$XML_ROOT', auto_detect=true, maximum_file_size=52428800, ignore_errors=true)"
          first=false
        else
          echo " UNION ALL SELECT * FROM read_xml('$file', root_element='$XML_ROOT', auto_detect=true, maximum_file_size=52428800, ignore_errors=true)"
        fi
      fi
    done | tr -d '\n'
  elif [[ "$func" == "read_csv" ]]; then
    # For TSV arrays, read_csv with delim doesn't support arrays, so we need to UNION individual calls
    local files_str=$(echo "$file_array" | sed 's/ARRAY\[\(.*\)\]/\1/')
    local first=true

    # Parse files from comma-separated list
    echo "$files_str" | tr ',' '\n' | while read -r file; do
      file=$(echo "$file" | sed "s/^'//" | sed "s/'$//")
      if [[ -n "$file" ]]; then
        if [[ "$first" == "true" ]]; then
          echo "SELECT * FROM read_csv('$file', delim='\t')"
          first=false
        else
          echo " UNION ALL SELECT * FROM read_csv('$file', delim='\t')"
        fi
      fi
    done | tr -d '\n'
  elif [[ "$func" == "read_json_auto" ]]; then
    # For JSON arrays, add robust error handling for schema variations
    echo "$func($file_array, ignore_errors=true, union_by_name=true, maximum_depth=-1)"
  else
    echo "$func($file_array)"
  fi
}

cloud_secret_sql=""
XML_SUPPORTED=false

load_cloud_creds() {
  if $VERBOSE; then
    echo "Using GCS_KEY_ID=$GCS_KEY_ID"
    echo "Using GCS_SECRET=$GCS_SECRET"
  fi

  # Always install httpfs for cloud storage support
  cloud_secret_sql="INSTALL httpfs; LOAD httpfs;"

  # Try to install webbed for XML support, but don't fail if unavailable
  if duckdb -c "INSTALL webbed FROM community; LOAD webbed;" 2>/dev/null; then
    cloud_secret_sql="$cloud_secret_sql INSTALL webbed FROM community; LOAD webbed;"
    XML_SUPPORTED=true
    if $VERBOSE; then
      echo "✓ XML support enabled (webbed extension loaded)"
    fi
  else
    if $VERBOSE; then
      echo "⚠ XML support disabled (webbed extension unavailable for this platform)"
    fi
  fi

  if [[ -n "$GCS_KEY_ID" && -n "$GCS_SECRET" ]]; then
    cloud_secret_sql="$cloud_secret_sql CREATE OR REPLACE SECRET gcs_creds (TYPE gcs, KEY_ID '$GCS_KEY_ID', SECRET '$GCS_SECRET');"
  fi
  if [[ -n "$S3_KEY_ID" && -n "$S3_SECRET" ]]; then
    cloud_secret_sql="$cloud_secret_sql SET s3_access_key_id='$S3_KEY_ID'; SET s3_secret_access_key='$S3_SECRET';"
  fi
}

run_duckdb() {
  local cmd="$1"
  # Enable progress bar and add to cloud credentials setup
  local final_cmd="$cloud_secret_sql SET enable_progress_bar=true; $cmd"
  if $VERBOSE; then
    echo -e "\n[duck-shard:VERBOSE] duckdb -c \"$final_cmd\"\n"
  fi
  
  # Create a temporary file to capture stderr
  local temp_err=$(mktemp)
  local exit_code
  
  # Execute DuckDB with real-time stdout (for progress bars) but capture stderr
  duckdb -c "$final_cmd" 2>"$temp_err"
  exit_code=$?
  
  # Read stderr content
  local stderr_content
  stderr_content=$(cat "$temp_err" 2>/dev/null || true)
  rm -f "$temp_err"
  
  # Check for errors
  if [ $exit_code -ne 0 ] || [[ "$stderr_content" =~ "Error:" ]] || [[ "$stderr_content" =~ "IO Error:" ]] || [[ "$stderr_content" =~ "Permission denied" ]]; then
    echo "Error: DuckDB operation failed" >&2
    if [[ -n "$stderr_content" ]]; then
      echo "$stderr_content" >&2
    fi
    return 1
  fi
  
  return 0
}

find_input_files() {
  local path="$1"
  if [[ "$path" =~ ^gs:// || "$path" =~ ^s3:// ]]; then
    # Check if it's a single file (ends with a supported extension, optionally compressed)
    if [[ "$path" =~ \.(parquet|csv|tsv|ndjson|jsonl|json|xml)(\.(gz|bz2|xz|zst))?$ ]]; then
      echo "$path"
    else
      # Use glob to find files in cloud storage (including compressed)
      for ext in parquet csv tsv ndjson jsonl json xml; do
        # Find uncompressed files
        run_duckdb "COPY (SELECT file FROM glob('${path%/}/*.$ext')) TO '/dev/stdout' (FORMAT CSV, HEADER false);" 2>/dev/null | \
          grep -E "^(gs|s3)://" || true
        # Find compressed files
        for comp_ext in gz bz2 xz zst; do
          run_duckdb "COPY (SELECT file FROM glob('${path%/}/*.$ext.$comp_ext')) TO '/dev/stdout' (FORMAT CSV, HEADER false);" 2>/dev/null | \
            grep -E "^(gs|s3)://" || true
        done
      done | sort -u
    fi
  else
    # Local filesystem - find both compressed and uncompressed files
    find "$path" -type f \( \
      -iname '*.parquet' -o -iname '*.parquet.gz' -o -iname '*.parquet.bz2' -o -iname '*.parquet.xz' -o -iname '*.parquet.zst' -o \
      -iname '*.csv' -o -iname '*.csv.gz' -o -iname '*.csv.bz2' -o -iname '*.csv.xz' -o -iname '*.csv.zst' -o \
      -iname '*.tsv' -o -iname '*.tsv.gz' -o -iname '*.tsv.bz2' -o -iname '*.tsv.xz' -o -iname '*.tsv.zst' -o \
      -iname '*.ndjson' -o -iname '*.ndjson.gz' -o -iname '*.ndjson.bz2' -o -iname '*.ndjson.xz' -o -iname '*.ndjson.zst' -o \
      -iname '*.jsonl' -o -iname '*.jsonl.gz' -o -iname '*.jsonl.bz2' -o -iname '*.jsonl.xz' -o -iname '*.jsonl.zst' -o \
      -iname '*.json' -o -iname '*.json.gz' -o -iname '*.json.bz2' -o -iname '*.json.xz' -o -iname '*.json.zst' -o \
      -iname '*.xml' -o -iname '*.xml.gz' -o -iname '*.xml.bz2' -o -iname '*.xml.xz' -o -iname '*.xml.zst' \
    \) | sort
  fi
}

select_clause() {
  if [[ -z "${SELECT_COLUMNS:-}" || "${SELECT_COLUMNS// /}" == "" || "$SELECT_COLUMNS" == "*" ]]; then
    echo "*"
  else
    # Quote column names that contain special characters like $, spaces, etc.
    local quoted_columns=""
    local col
    echo "$SELECT_COLUMNS" | tr ',' '\n' | while read -r col; do
      col=$(echo "$col" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')  # trim whitespace
      # Check if column needs quoting (contains $ or other special chars)
      if echo "$col" | grep -q '[$@#. -]'; then
        printf '"%s",' "$col"
      else
        printf '%s,' "$col"
      fi
    done | sed 's/,$//'  # Remove trailing comma
  fi
}

dedupe_select_clause() {
  local sel; sel=$(select_clause)
  if [[ "$sel" == "*" ]]; then
    $DEDUPE && echo "SELECT DISTINCT *" || echo "SELECT *"
  else
    $DEDUPE && echo "SELECT DISTINCT $sel" || echo "SELECT $sel"
  fi
}

output_base_name() {
  local file="$1"
  local base="$(basename "$file")"

  # Handle compressed files by removing compression extension first
  if [[ "$base" =~ \.(gz|bz2|xz|zst)$ ]]; then
    base="${base%.*}"
  fi

  # Remove the format extension
  local outbase="${base%.*}"
  echo "$outbase"
}

build_output_filename() {
  local base_name="$1"
  local extension="$2"
  local filename="${FILE_PREFIX}${base_name}${FILE_SUFFIX}.${extension}"
  if $COMPRESSED; then
    filename="${filename}.gz"
  fi
  echo "$filename"
}

fix_glob_name() {
  local name="$1"
  # Replace asterisks and other problematic glob characters with safe alternatives
  name="${name//\*/merged}"
  name="${name//\?/unknown}"
  name="${name//\[/}"
  name="${name//\]/}"
  echo "$name"
}

get_sql_stmt() {
  cat "$1" | sed -e 's/[[:space:]]*$//' -e ':a' -e 's/;$//;ta' -e 's/[[:space:]]*$//'
}

check_output_safety() {
  local infile="$1"
  local outfile="$2"

  # Skip check for cloud storage paths
  if [[ "$infile" =~ ^(gs|s3):// || "$outfile" =~ ^(gs|s3):// ]]; then
    return 0
  fi

  # Convert to absolute paths for comparison
  local abs_infile abs_outfile
  abs_infile=$(to_abs "$infile")
  abs_outfile=$(to_abs "$outfile")

  if [[ "$abs_infile" == "$abs_outfile" ]]; then
    echo "Error: Output file '$outfile' would overwrite input file '$infile'" >&2
    echo "Use -o flag to specify a different output directory" >&2
    return 1
  fi
  return 0
}

log_http_response() {
  local file="$1"
  local url="$2"
  local http_code="$3"
  local response="$4"
  local duration="$5"

  if [[ "$LOG_RESPONSES" == "true" ]]; then
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local log_entry=$(cat <<EOF
{
  "timestamp": "$timestamp",
  "file": "$file",
  "url": "$url",
  "http_code": $http_code,
  "response": $(echo "$response" | jq -R . 2>/dev/null || echo "\"$response\""),
  "duration_ms": $duration
}
EOF
)
    # Append to log file with proper JSON array formatting
    if [[ ! -f "$RESPONSE_LOG_FILE" ]]; then
      echo "[$log_entry]" > "$RESPONSE_LOG_FILE"
    else
      # Remove last ] and add new entry
      sed -i '$ s/]$//' "$RESPONSE_LOG_FILE" 2>/dev/null || sed -i '' '$ s/]$//' "$RESPONSE_LOG_FILE" 2>/dev/null
      echo ",$log_entry]" >> "$RESPONSE_LOG_FILE"
    fi
  fi
}

apply_jq_transform() {
  local file="$1"
  local jq_expr="$2"

  if [[ -z "$jq_expr" ]]; then
    return 0
  fi

  # Check if file exists and is readable
  if [[ ! -f "$file" || ! -r "$file" ]]; then
    echo "Error: Cannot read file $file for jq transformation" >&2
    return 1
  fi

  local temp_file="${file}.jq.tmp"
  local line_count=$(wc -l < "$file" 2>/dev/null || echo 0)

  echo "🔄 Applying jq transformation to $file ($line_count lines)..."

  # Apply jq transformation
  if jq -c "$jq_expr" "$file" > "$temp_file" 2>/dev/null; then
    local new_line_count=$(wc -l < "$temp_file" 2>/dev/null || echo 0)
    echo "✅ jq transformation complete: $line_count → $new_line_count lines"
    mv "$temp_file" "$file"
    return 0
  else
    echo "❌ jq transformation failed for $file" >&2
    rm -f "$temp_file"
    return 1
  fi
}

preview_file() {
  local infile="$1"
  local preview_rows="$2"
  local ext; ext=$(get_file_format "$infile")
  local duckdb_func; duckdb_func=$(get_duckdb_func "$ext")
  local duckdb_call; duckdb_call=$(build_duckdb_call "$duckdb_func" "$infile")

  echo "🔍 Preview mode: showing first $preview_rows rows from $infile"

  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_call; COPY ( $sql_stmt LIMIT $preview_rows ) TO '/dev/stdout' ($COPY_OPTS);"
  else
    local sel; sel=$(dedupe_select_clause)
    run_duckdb "COPY ($sel FROM $duckdb_call LIMIT $preview_rows) TO '/dev/stdout' ($COPY_OPTS);"
  fi | {
    if [[ -n "$JQ_EXPRESSION" && "$FORMAT" =~ ^(ndjson|json|jsonl)$ ]]; then
      echo "🔄 Applying jq transformation: $JQ_EXPRESSION"
      jq -c "$JQ_EXPRESSION" 2>/dev/null || {
        echo "❌ jq transformation failed in preview mode" >&2
        cat  # fallback to showing untransformed data
      }
    else
      cat
    fi
  }

  echo -e "\n✅ Preview complete (first $preview_rows rows shown)"
}

post_file_to_url() {
  local file="$1"
  local url="$2"
  local max_retries=3
  local retry_count=0
  local last_http_code=""

  # Initialize HTTP tracking on first call
  if [[ -z "$HTTP_START_TIME" ]]; then
    HTTP_START_TIME=$(date +%s)
    HTTP_REQUEST_COUNT=0
    HTTP_RECORD_COUNT=0
  fi

  # Check if curl is available
  command -v curl >/dev/null 2>&1 || {
    echo "Error: curl not found. curl is required for --url functionality" >&2
    return 1
  }

  # Build curl command with headers
  local curl_args=("-X" "POST" "-f" "-s" "-S" "--connect-timeout" "10" "--max-time" "30")

  # Reconstruct HTTP_HEADERS array from exported variables (for subprocesses)
  local headers=()
  if [[ -n "${HTTP_HEADERS_COUNT:-}" ]]; then
    for (( i=0; i<HTTP_HEADERS_COUNT; i++ )); do
      local header_var="HTTP_HEADER_$i"
      headers+=("${!header_var}")
    done
  else
    # Direct access to array (when not in subprocess)
    headers=("${HTTP_HEADERS[@]}")
  fi

  # Add custom headers or default Content-Type
  local has_content_type=false
  for header in "${headers[@]}"; do
    curl_args+=("-H" "$header")
    if [[ "$header" =~ ^[Cc]ontent-[Tt]ype: ]]; then
      has_content_type=true
    fi
  done

  # Add default Content-Type if not specified
  if [[ "$has_content_type" == "false" ]]; then
    curl_args+=("-H" "Content-Type: application/json")
  fi

  # Add the data file
  curl_args+=("--data-binary" "@$file" "$url")

  # Count records in file for throughput calculation
  local record_count=0
  if [[ -f "$file" ]]; then
    record_count=$(wc -l < "$file" 2>/dev/null || echo 0)
  fi

  while (( retry_count < max_retries )); do
    local http_code
    local response
    local start_time=$(date +%s)

    # Run curl and capture both output and HTTP status code
    response=$(curl "${curl_args[@]}" -w "%{http_code}" 2>/dev/null || echo "000curl_failed")

    if [[ "$response" != "000curl_failed" && -n "$response" ]]; then
      local end_time=$(date +%s)
      local duration=$((end_time - start_time))

      http_code="${response: -3}"  # Last 3 characters
      last_http_code="$http_code"  # Track for final error message
      response="${response%???}"   # Everything except last 3 characters

      # Log response if requested
      log_http_response "$file" "$url" "$http_code" "$response" "$duration"

      case "$http_code" in
        2??)
          # Update counters for throughput
          ((HTTP_REQUEST_COUNT++))
          HTTP_RECORD_COUNT=$((HTTP_RECORD_COUNT + record_count))

          # Calculate and display throughput
          local elapsed=$(($(date +%s) - HTTP_START_TIME))
          if (( elapsed > 0 )); then
            local req_per_sec=$(( HTTP_REQUEST_COUNT * 100 / elapsed ))  # *100 for 2 decimal precision
            local rec_per_sec=$(( HTTP_RECORD_COUNT * 100 / elapsed ))
            printf "✅ Posted %s to %s (HTTP %s) | %d.%02d req/s, %d.%02d rec/s\n" \
              "$(basename "$file")" "$url" "$http_code" \
              $((req_per_sec / 100)) $((req_per_sec % 100)) \
              $((rec_per_sec / 100)) $((rec_per_sec % 100))
          else
            echo "✅ Posted $(basename "$file") to $url (HTTP $http_code)"
          fi

          if $VERBOSE && [[ -n "$response" ]]; then
            echo "Response: $response"
          fi
          return 0
          ;;
        429)
          ((retry_count++))
          local delay=$((retry_count * 2))  # Exponential backoff
          echo "⚠️  Rate limited (HTTP 429), retrying in ${delay}s... (attempt $retry_count/$max_retries)"
          sleep "$delay"
          ;;
        5??)
          # Server errors - retry
          ((retry_count++))
          echo "❌ Server error HTTP $http_code posting $(basename "$file") to $url (attempt $retry_count/$max_retries)"
          if $VERBOSE && [[ -n "$response" ]]; then
            echo "Response: $response"
          fi
          if (( retry_count < max_retries )); then
            sleep "$((retry_count * 1))"  # Linear backoff for server errors
          fi
          ;;
        4??)
          # Client errors - don't retry (except 429 handled above)
          echo "❌ Failed to post $(basename "$file") to $url: HTTP $http_code (client error, not retrying)"
          if $VERBOSE && [[ -n "$response" ]]; then
            echo "Response: $response"
          fi
          log_http_response "$file" "$url" "$http_code" "$response" "$duration"
          return 1
          ;;
        *)
          ((retry_count++))
          echo "❌ HTTP $http_code error posting $(basename "$file") to $url (attempt $retry_count/$max_retries)"
          if $VERBOSE && [[ -n "$response" ]]; then
            echo "Response: $response"
          fi
          if (( retry_count < max_retries )); then
            sleep "$((retry_count * 1))"  # Linear backoff for other errors
          fi
          ;;
      esac
    else
      ((retry_count++))
      echo "❌ Network error posting $(basename "$file") to $url (attempt $retry_count/$max_retries)"
      if (( retry_count < max_retries )); then
        sleep "$((retry_count * 1))"
      fi
    fi
  done

  if [[ -n "$last_http_code" ]]; then
    echo "❌ Failed to post $file to $url after $max_retries attempts (last HTTP status: $last_http_code)" >&2
  else
    echo "❌ Failed to post $file to $url after $max_retries attempts (network error)" >&2
  fi
  return 1
}

split_convert_file() {
  local infile="$1"
  local ext; ext=$(get_file_format "$infile")
  local duckdb_func; duckdb_func=$(get_duckdb_func "$ext")
  local duckdb_call; duckdb_call=$(build_duckdb_call "$duckdb_func" "$infile")
  local outbase; outbase="$(output_base_name "$infile")"
  local row_count
  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    row_count=$(run_duckdb "CREATE TEMP VIEW input_data AS SELECT * FROM $duckdb_call; SELECT COUNT(*) FROM ( $sql_stmt );" | grep -Eo '[0-9]+' | tail -1)
  else
    local sel; sel=$(dedupe_select_clause)
    row_count=$(run_duckdb "SELECT COUNT(*) FROM $duckdb_call;" | grep -Eo '[0-9]+' | tail -1)
  fi
  local splits=$(( (row_count + ROWS_PER_FILE - 1) / ROWS_PER_FILE ))
  local i=1; local offset=0
  while (( offset < row_count )); do
    local out
    local filename=$(build_output_filename "$outbase-$i" "$EXT")
    if [[ -n "${OUTPUT_DIR:-}" ]]; then
      out="${OUTPUT_DIR%/}/$filename"
    else
      out="$(dirname "$infile")/$filename"
    fi

    # Safety check to prevent overwriting source file
    if ! check_output_safety "$infile" "$out"; then
      return 1
    fi

    [[ ! "$out" =~ ^(gs|s3):// ]] && [[ -f "$out" ]] && rm -f "$out"
    echo "Converting $infile rows $((offset+1))-$((offset+ROWS_PER_FILE>row_count?row_count:offset+ROWS_PER_FILE)) → $out"
    if [[ -n "$SQL_FILE" ]]; then
      sql_stmt=$(get_sql_stmt "$SQL_FILE")
      run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_call; COPY ( $sql_stmt LIMIT $ROWS_PER_FILE OFFSET $offset ) TO '$out' ($COPY_OPTS);"
    else
      local sel; sel=$(dedupe_select_clause)
      run_duckdb "COPY (
        $sel FROM $duckdb_call
        LIMIT $ROWS_PER_FILE OFFSET $offset
      ) TO '$out' ($COPY_OPTS);"
    fi
    echo -e "\n✅ $out\n"

    # Apply jq transformation if specified
    if [[ -n "$JQ_EXPRESSION" && ! "$out" =~ ^(gs|s3):// ]]; then
      apply_jq_transform "$out" "$JQ_EXPRESSION" || {
        echo "Warning: jq transformation failed for $out, continuing..." >&2
      }
    fi

    # POST to URL if specified
    if [[ -n "$POST_URL" && ! "$out" =~ ^(gs|s3):// ]]; then
      sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
      post_file_to_url "$out" "$POST_URL" || true  # Don't exit on POST failure
    fi

    ((i++)); ((offset+=ROWS_PER_FILE))
  done
}

convert_file() {
  if (( ROWS_PER_FILE > 0 )); then
    split_convert_file "$1"
    return
  fi
  local infile="$1"
  local ext; ext=$(get_file_format "$infile")
  local duckdb_func; duckdb_func=$(get_duckdb_func "$ext")
  local duckdb_call; duckdb_call=$(build_duckdb_call "$duckdb_func" "$infile")
  local outbase; outbase="$(output_base_name "$infile")"
  local out
  local filename=$(build_output_filename "$outbase" "$EXT")
  if [[ -n "${OUTPUT_DIR:-}" ]]; then
    out="${OUTPUT_DIR%/}/$filename"
  else
    out="$(dirname "$infile")/$filename"
  fi

  # Safety check to prevent overwriting source file
  if ! check_output_safety "$infile" "$out"; then
    return 1
  fi

  [[ ! "$out" =~ ^(gs|s3):// ]] && [[ -f "$out" ]] && rm -f "$out"
  echo "Converting $infile → $out"
  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_call; COPY ( $sql_stmt ) TO '$out' ($COPY_OPTS);"
  else
    local sel; sel=$(dedupe_select_clause)
    run_duckdb "COPY ($sel FROM $duckdb_call) TO '$out' ($COPY_OPTS);"
  fi
  echo "✅ $out"

  # Apply jq transformation if specified
  if [[ -n "$JQ_EXPRESSION" && ! "$out" =~ ^(gs|s3):// ]]; then
    apply_jq_transform "$out" "$JQ_EXPRESSION" || {
      echo "Warning: jq transformation failed for $out, continuing..." >&2
    }
  fi

  # POST to URL if specified
  if [[ -n "$POST_URL" && ! "$out" =~ ^(gs|s3):// ]]; then
    sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
    post_file_to_url "$out" "$POST_URL" || true  # Don't exit on POST failure
  fi
}

export -f convert_file split_convert_file dedupe_select_clause select_clause get_file_format get_duckdb_func output_base_name get_sql_stmt run_duckdb check_output_safety to_abs post_file_to_url log_http_response apply_jq_transform preview_file build_output_filename fix_glob_name build_duckdb_call build_duckdb_array_call

load_cloud_creds

# Validate XML support before proceeding
if ! $XML_SUPPORTED; then
  # Check if user requested XML output format
  if [[ "$FORMAT" == "xml" ]]; then
    echo "Error: XML output format requested, but webbed extension is unavailable for this platform" >&2
    echo "Please choose a different output format: ndjson, parquet, csv, tsv, or json" >&2
    exit 1
  fi

  # Check if input contains XML files
  if [[ -f "$INPUT_PATH" && "$INPUT_PATH" =~ \.xml(\.(gz|bz2|xz|zst))?$ ]]; then
    echo "Error: XML input file detected, but webbed extension is unavailable for this platform" >&2
    echo "XML files cannot be processed without the webbed extension" >&2
    exit 1
  elif [[ -d "$INPUT_PATH" ]] || [[ "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
    # For directories or cloud paths, we'll check during file discovery
    # This will be caught by get_duckdb_func if XML files are found
    :
  fi
fi

if $ANALYTICAL_MODE; then
  echo -e "\n🦆  DUCK SHARD ANALYTICAL QUERY\n📊 sql_file=$SQL_FILE  output_dir=${OUTPUT_DIR}  prefix=${FILE_PREFIX:-}  suffix=${FILE_SUFFIX:-}\n"
else
  echo -e "\n🦆  DUCK SHARD JOB START\n🚀 format=$FORMAT  cols=${SELECT_COLUMNS:-*}  parallel=$MAX_PARALLEL_JOBS  single_file=$SINGLE_FILE  dedupe=$DEDUPE  output_dir=${OUTPUT_DIR:-<src dir>}  rows_per_file=${ROWS_PER_FILE:-0}  sql_file=${SQL_FILE:-}  jq=${JQ_EXPRESSION:-}  preview=${PREVIEW_ROWS:-0}  prefix=${FILE_PREFIX:-}  suffix=${FILE_SUFFIX:-}\n"
fi

# Handle analytical query mode
if $ANALYTICAL_MODE; then
  echo "🔍 Running analytical query on data..."
  
  # Determine input files
  if [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
    FILES=()
    while IFS= read -r line; do [[ -n "$line" ]] && FILES+=("$line"); done < <(find_input_files "$INPUT_PATH")
    [[ ${#FILES[@]} -gt 0 ]] || { echo "No supported files found in $INPUT_PATH"; exit 1; }

    # Use the first file's extension to determine the duckdb function
    first_ext=$(get_file_format "${FILES[0]}")
    duckdb_func=$(get_duckdb_func "$first_ext")
    SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
    duckdb_array_call=$(build_duckdb_array_call "$duckdb_func" "ARRAY[$SQL_PATHS]")
    if [[ "$duckdb_func" == "read_xml" ]] || [[ "$duckdb_func" == "read_csv" ]]; then
      # For XML and TSV, the array call returns a complete SELECT with UNION ALL, wrap it properly
      VIEW_CREATION="CREATE OR REPLACE TEMP VIEW input_data AS $duckdb_array_call;"
    else
      VIEW_CREATION="CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_array_call;"
    fi
  else
    ext=$(get_file_format "$INPUT_PATH")
    duckdb_func=$(get_duckdb_func "$ext")
    duckdb_call=$(build_duckdb_call "$duckdb_func" "$INPUT_PATH")
    VIEW_CREATION="CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_call;"
  fi
  
  # Get the SQL query
  sql_stmt=$(get_sql_stmt "$SQL_FILE")
  
  # Execute query and display results
  echo "📊 Executing query and displaying results..."
  query_output=$(run_duckdb "$VIEW_CREATION SELECT * FROM ( $sql_stmt );")
  
  # Display results as a nice table in console
  echo "$query_output"
  
  # Save results to CSV file
  output_filename="query_result.csv"
  if [[ -n "$FILE_PREFIX" || -n "$FILE_SUFFIX" ]]; then
    output_filename=$(build_output_filename "query_result" "csv")
  fi
  
  output_path="$OUTPUT_DIR/$output_filename"
  
  # Ensure output directory exists for local paths
  if [[ ! "$OUTPUT_DIR" =~ ^(gs|s3):// ]]; then
    mkdir -p "$OUTPUT_DIR"
  fi
  
  echo "💾 Saving results to $output_path..."
  run_duckdb "$VIEW_CREATION COPY ( $sql_stmt ) TO '$output_path' (FORMAT CSV, HEADER);" > /dev/null
  
  echo -e "\n✅ Analytical query complete!"
  echo "📊 Results displayed above and saved to: $output_path"
  echo -e "\n🦆DUCK SHARD ANALYSIS 💯 DONE\n"
  exit 0
fi

if [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
  FILES=()
  while IFS= read -r line; do [[ -n "$line" ]] && FILES+=("$line"); done < <(find_input_files "$INPUT_PATH")
  [[ ${#FILES[@]} -gt 0 ]] || { echo "No supported files found in $INPUT_PATH"; exit 1; }

  first_ext=$(get_file_format "${FILES[0]}")
  if $SINGLE_FILE; then
    for f in "${FILES[@]}"; do
      ext=$(get_file_format "$f")
      [[ "$ext" == "$first_ext" ]] || { echo "Error: All files must have the same extension for --single-file"; exit 1; }
    done
  fi
  duckdb_func=$(get_duckdb_func "$first_ext")

  # Handle preview mode for directories
  if (( PREVIEW_ROWS > 0 )); then
    echo "🔍 Preview mode: processing first file only"
    preview_file "${FILES[0]}" "$PREVIEW_ROWS"
    echo -e "🦆  DUCK SHARD PREVIEW 💯 DONE\n"
    exit 0
  fi

  if $SINGLE_FILE; then
    # In single-file mode, check if OUTPUT_DIR looks like a file path
    if [[ -n "${OUTPUT_DIR:-}" ]]; then
      # If OUTPUT_DIR has an extension or ends with a filename, treat it as a file path
      if [[ "${OUTPUT_DIR##*/}" == *.* ]] && [[ ! -d "${OUTPUT_DIR}" ]]; then
        # OUTPUT_DIR is a file path, extract directory and filename
        OUTPUT_FILENAME="${OUTPUT_DIR}"
        default_output_dir="$(dirname "${OUTPUT_DIR}")"
      else
        # OUTPUT_DIR is a directory path
        default_output_dir="${OUTPUT_DIR}"
      fi
    elif [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
      if [[ "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
        default_output_dir="$(dirname "$INPUT_PATH")"
      else
        default_output_dir="$INPUT_PATH"
      fi
    else
      default_output_dir="$(dirname "$INPUT_PATH")"
    fi

    if [[ -z "${OUTPUT_FILENAME:-}" ]]; then
      if [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
        base_name="$(basename "${INPUT_PATH%/}")"
        base_name=$(fix_glob_name "$base_name")
        # Don't add _merged if already contains merged
        if [[ "$base_name" == *"merged"* ]]; then
          merged_filename=$(build_output_filename "$base_name" "$EXT")
        else
          merged_filename=$(build_output_filename "${base_name}_merged" "$EXT")
        fi
        OUTPUT_FILENAME="${default_output_dir%/}/$merged_filename"
      else
        base_name="$(basename "$INPUT_PATH")"
        # Handle files without extensions or with multiple dots properly
        if [[ "$base_name" == *.* ]]; then
          base_name="${base_name%.*}"
        fi
        # Don't add _merged if already contains merged
        if [[ "$base_name" == *"merged"* ]]; then
          merged_filename=$(build_output_filename "$base_name" "$EXT")
        else
          merged_filename=$(build_output_filename "${base_name}_merged" "$EXT")
        fi
        OUTPUT_FILENAME="${default_output_dir%/}/$merged_filename"
      fi
    elif [[ "${OUTPUT_FILENAME}" != /* && ! "${OUTPUT_FILENAME}" =~ ^(gs|s3):// ]]; then
      # Apply the default output directory when filename is relative
      OUTPUT_FILENAME="${default_output_dir%/}/${OUTPUT_FILENAME}"
    fi

    # Add .gz extension if compression is enabled and not already present
    if $COMPRESSED && [[ ! "$OUTPUT_FILENAME" =~ \.gz$ ]]; then
      OUTPUT_FILENAME="${OUTPUT_FILENAME}.gz"
    fi

    [[ ! "$OUTPUT_FILENAME" =~ ^(gs|s3):// ]] && [[ -f "$OUTPUT_FILENAME" ]] && rm -f "$OUTPUT_FILENAME"
    if [[ -n "$SQL_FILE" ]]; then
      SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
      duckdb_array_call=$(build_duckdb_array_call "$duckdb_func" "ARRAY[$SQL_PATHS]")
      sql_stmt=$(get_sql_stmt "$SQL_FILE")
      echo "Merging ${#FILES[@]} files → $OUTPUT_FILENAME"
      if [[ "$duckdb_func" == "read_xml" ]] || [[ "$duckdb_func" == "read_csv" ]]; then
        run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS $duckdb_array_call; COPY ( $sql_stmt ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
      else
        run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_array_call; COPY ( $sql_stmt ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
      fi
    else
      SEL=$(dedupe_select_clause)
      SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
      duckdb_array_call=$(build_duckdb_array_call "$duckdb_func" "ARRAY[$SQL_PATHS]")
      echo "Merging ${#FILES[@]} files → $OUTPUT_FILENAME"
      if [[ "$duckdb_func" == "read_xml" ]] || [[ "$duckdb_func" == "read_csv" ]]; then
        # For XML and TSV, the array call returns a complete SELECT statement with UNION ALL
        if [[ "$SEL" == "SELECT *" ]]; then
          run_duckdb "COPY ( $duckdb_array_call ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
        else
          # For column selection with XML/TSV, wrap the UNION in a subquery
          run_duckdb "COPY ( $SEL FROM ( $duckdb_array_call ) ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
        fi
      else
        # For other formats, use the normal pattern
        run_duckdb "COPY (
          $SEL FROM $duckdb_array_call
        ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
      fi
    fi
    echo "✅ Merged → $OUTPUT_FILENAME"

    # Apply jq transformation if specified for merged file
    if [[ -n "$JQ_EXPRESSION" && ! "$OUTPUT_FILENAME" =~ ^(gs|s3):// ]]; then
      apply_jq_transform "$OUTPUT_FILENAME" "$JQ_EXPRESSION" || {
        echo "Warning: jq transformation failed for $OUTPUT_FILENAME, continuing..." >&2
      }
    fi

    # POST to URL if specified for merged file
    if [[ -n "$POST_URL" && ! "$OUTPUT_FILENAME" =~ ^(gs|s3):// ]]; then
      sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
      post_file_to_url "$OUTPUT_FILENAME" "$POST_URL" || true  # Don't exit on POST failure
    fi
  else
    export -f convert_file
    export EXT COPY_OPTS DEDUPE SELECT_COLUMNS OUTPUT_DIR ROWS_PER_FILE cloud_secret_sql SQL_FILE VERBOSE POST_URL HTTP_RATE_LIMIT_DELAY LOG_RESPONSES RESPONSE_LOG_FILE HTTP_START_TIME HTTP_REQUEST_COUNT HTTP_RECORD_COUNT JQ_EXPRESSION FILE_PREFIX FILE_SUFFIX XML_ROOT COMPRESSED
    # Export HTTP_HEADERS array elements as individual variables for subprocesses
    for i in "${!HTTP_HEADERS[@]}"; do
      export "HTTP_HEADER_$i=${HTTP_HEADERS[$i]}"
    done
    export HTTP_HEADERS_COUNT="${#HTTP_HEADERS[@]}"
    printf '%s\n' "${FILES[@]}" | xargs -n1 -P "$MAX_PARALLEL_JOBS" bash -c 'convert_file "$0"'
    echo -e "\n🎉 All individual conversions complete.\n"
  fi

elif [[ -f "$INPUT_PATH" ]] || [[ "$INPUT_PATH" =~ ^(gs|s3)://.+\.(parquet|csv|tsv|json|jsonl|ndjson|xml)(\.(gz|bz2|xz|zst))?$ ]]; then
  # Handle preview mode for single file
  if (( PREVIEW_ROWS > 0 )); then
    preview_file "$INPUT_PATH" "$PREVIEW_ROWS"
    echo -e "🦆  DUCK SHARD PREVIEW 💯 DONE\n"
    exit 0
  fi

  convert_file "$INPUT_PATH"
  echo -e "\n🎉 Conversion complete.\n"
else
  echo "Error: '$INPUT_PATH' is not a supported file or directory" >&2
  exit 1
fi

echo -e "🦆   DUCK SHARD JOB 💯 DONE\n"
