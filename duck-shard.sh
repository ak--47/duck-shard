#!/usr/bin/env bash
# shellcheck disable=SC2034,SC2155
# duck-shard.sh – DuckDB-based ETL/conversion for local/cloud files, cross-platform.

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

SUPPORTED_EXTENSIONS="parquet csv ndjson jsonl json"
MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""
FORMAT="ndjson"
SELECT_COLUMNS="*"
DEDUPE=false
OUTPUT_DIR=""
ROWS_PER_FILE=0
GCS_KEY_ID="${GCS_KEY_ID:-}"
GCS_SECRET="${GCS_SECRET:-}"
S3_KEY_ID="${S3_KEY_ID:-}"
S3_SECRET="${S3_SECRET:-}"
SQL_FILE=""
VERBOSE=false
POST_URL=""
HTTP_HEADERS=()
HTTP_RATE_LIMIT_DELAY=0.1  # seconds between requests

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
  -f, --format <ndjson|parquet|csv>     Output format (default: ndjson)
  -c, --cols <col1,col2,...>            Only include specific columns
  --dedupe                              Remove duplicate rows (by chosen columns)
  -o, --output <output_dir>             Output directory (local or gs://... or s3://...)
  -r, --rows <rows_per_file>            Split output files with N rows each (not for --single-file)
  --sql <sql_file>                      Use custom SQL SELECT (on temp view input_data)
  --gcs-key <key> --gcs-secret <secret> GCS HMAC credentials
  --s3-key <key> --s3-secret <secret>   S3 HMAC credentials
  --url <api_url>                       POST processed data to API URL in batches
  --header <header>                     Add custom HTTP header (can be used multiple times)
  --verbose                             Print all DuckDB SQL commands before running them
  -h, --help                            Print this help

Examples:
  $0 data/ -f csv -o ./out/
  $0 data/ -s merged.ndjson
  $0 gs://bucket/data/ -f csv -o gs://other-bucket/output/
  $0 data/ --sql my_query.sql -f csv -o ./out/
  $0 data/ --url https://api.example.com/webhook --header "Authorization: Bearer token" -r 1000


EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--single-file) SINGLE_FILE=true; shift
      if [[ $# -gt 0 && ! $1 =~ ^- ]]; then OUTPUT_FILENAME="$1"; shift; fi ;;
    -f|--format) [[ $# -ge 2 ]] || { echo "Error: --format needs an argument"; exit 1; }
      FORMAT="$2"; shift 2 ;;
    -c|--cols) [[ $# -ge 2 ]] || { echo "Error: --cols needs an argument"; exit 1; }
      SELECT_COLUMNS="$(echo "$2" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
      SELECT_COLUMNS="$(echo "$SELECT_COLUMNS" | paste -sd, -)"
      [[ -z "$SELECT_COLUMNS" ]] && SELECT_COLUMNS="*"
      shift 2 ;;
    --dedupe) DEDUPE=true; shift ;;
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
    --verbose) VERBOSE=true; shift ;;
    -h|--help) print_help; exit 0 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]}"

export GCS_KEY_ID GCS_SECRET S3_KEY_ID S3_SECRET

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

case "$FORMAT" in
  ndjson)  EXT="ndjson";  COPY_OPTS="FORMAT JSON" ;;
  parquet) EXT="parquet"; COPY_OPTS="FORMAT PARQUET" ;;
  csv)     EXT="csv";     COPY_OPTS="FORMAT CSV, HEADER" ;;
  jsonl|json) EXT="json"; COPY_OPTS="FORMAT JSON" ;;
  *) echo "Error: --format must be ndjson, parquet, json, or csv"; exit 1 ;;
esac

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

get_duckdb_func() {
  local ext="$1"
  case "$ext" in
    parquet) echo "read_parquet" ;;
    csv)     echo "read_csv_auto" ;;
    ndjson|jsonl|json) echo "read_json_auto" ;;
    *) echo "Error: Unsupported extension: $ext" >&2; exit 1 ;;
  esac
}

cloud_secret_sql=""
load_cloud_creds() {
  if $VERBOSE; then
    echo "Using GCS_KEY_ID=$GCS_KEY_ID"
    echo "Using GCS_SECRET=$GCS_SECRET"
  fi
  cloud_secret_sql="INSTALL httpfs; LOAD httpfs;"
  if [[ -n "$GCS_KEY_ID" && -n "$GCS_SECRET" ]]; then
    cloud_secret_sql="$cloud_secret_sql CREATE OR REPLACE SECRET gcs_creds (TYPE gcs, KEY_ID '$GCS_KEY_ID', SECRET '$GCS_SECRET');"
  fi
  if [[ -n "$S3_KEY_ID" && -n "$S3_SECRET" ]]; then
    cloud_secret_sql="$cloud_secret_sql SET s3_access_key_id='$S3_KEY_ID'; SET s3_secret_access_key='$S3_SECRET';"
  fi
}

run_duckdb() {
  local cmd="$1"
  local final_cmd="$cloud_secret_sql $cmd"
  if $VERBOSE; then
    echo -e "\n[duck-shard:VERBOSE] duckdb -c \"$final_cmd\"\n"
  fi
  duckdb -c "$final_cmd"
}

find_input_files() {
  local path="$1"
  if [[ "$path" =~ ^gs:// || "$path" =~ ^s3:// ]]; then
    if [[ "$path" =~ \.(parquet|csv|ndjson|jsonl|json)$ ]]; then
      echo "$path"
    else
      # Use glob to find files in cloud storage
      for ext in parquet csv ndjson jsonl json; do
        run_duckdb "COPY (SELECT file FROM glob('${path%/}/*.$ext')) TO '/dev/stdout' (FORMAT CSV, HEADER false);" | \
          grep -E "^(gs|s3)://"
      done | sort
    fi
  else
    find "$path" -type f \( -iname '*.parquet' -o -iname '*.csv' -o -iname '*.ndjson' -o -iname '*.jsonl' -o -iname '*.json' \) | sort
  fi
}

select_clause() {
  if [[ -z "${SELECT_COLUMNS:-}" || "${SELECT_COLUMNS// /}" == "" || "$SELECT_COLUMNS" == "*" ]]; then
    echo "*"
  else
    echo "$SELECT_COLUMNS"
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
  local outbase="${base%.*}"
  echo "$outbase"
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

post_file_to_url() {
  local file="$1"
  local url="$2"
  local max_retries=3
  local retry_count=0
  
  # Check if curl is available
  command -v curl >/dev/null 2>&1 || {
    echo "Error: curl not found. curl is required for --url functionality" >&2
    return 1
  }
  
  # Build curl command with headers
  local curl_args=("-X" "POST" "-f" "-s" "-S")
  
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
  
  while (( retry_count < max_retries )); do
    local http_code
    local response
    
    # Run curl and capture both output and HTTP status code
    if response=$(curl "${curl_args[@]}" -w "%{http_code}" 2>/dev/null); then
      http_code="${response: -3}"  # Last 3 characters
      response="${response%???}"   # Everything except last 3 characters
      
      case "$http_code" in
        2??) 
          echo "✅ Posted $file to $url (HTTP $http_code)"
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
        *)
          ((retry_count++))
          echo "❌ HTTP $http_code error posting $file to $url (attempt $retry_count/$max_retries)"
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
      echo "❌ Network error posting $file to $url (attempt $retry_count/$max_retries)"
      if (( retry_count < max_retries )); then
        sleep "$((retry_count * 1))"
      fi
    fi
  done
  
  echo "❌ Failed to post $file to $url after $max_retries attempts" >&2
  return 1
}

split_convert_file() {
  local infile="$1"
  local ext="${infile##*.}"
  local duckdb_func; duckdb_func=$(get_duckdb_func "$ext")
  local outbase; outbase="$(output_base_name "$infile")"
  local row_count
  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    row_count=$(run_duckdb "CREATE TEMP VIEW input_data AS SELECT * FROM $duckdb_func('$infile'); SELECT COUNT(*) FROM ( $sql_stmt );" | grep -Eo '[0-9]+' | tail -1)
  else
    local sel; sel=$(dedupe_select_clause)
    row_count=$(run_duckdb "SELECT COUNT(*) FROM $duckdb_func('$infile');" | grep -Eo '[0-9]+' | tail -1)
  fi
  local splits=$(( (row_count + ROWS_PER_FILE - 1) / ROWS_PER_FILE ))
  local i=1; local offset=0
  while (( offset < row_count )); do
    local out
    if [[ -n "${OUTPUT_DIR:-}" ]]; then
      out="${OUTPUT_DIR%/}/$outbase-$i.$EXT"
    else
      out="$(dirname "$infile")/$outbase-$i.$EXT"
    fi

    # Safety check to prevent overwriting source file
    if ! check_output_safety "$infile" "$out"; then
      return 1
    fi

    [[ ! "$out" =~ ^(gs|s3):// ]] && [[ -f "$out" ]] && rm -f "$out"
    echo "Converting $infile rows $((offset+1))-$((offset+ROWS_PER_FILE>row_count?row_count:offset+ROWS_PER_FILE)) → $out"
    if [[ -n "$SQL_FILE" ]]; then
      sql_stmt=$(get_sql_stmt "$SQL_FILE")
      run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_func('$infile'); COPY ( $sql_stmt LIMIT $ROWS_PER_FILE OFFSET $offset ) TO '$out' ($COPY_OPTS);"
    else
      local sel; sel=$(dedupe_select_clause)
      run_duckdb "COPY (
        $sel FROM $duckdb_func('$infile')
        LIMIT $ROWS_PER_FILE OFFSET $offset
      ) TO '$out' ($COPY_OPTS);"
    fi
    echo "✅ $out"
    
    # POST to URL if specified
    if [[ -n "$POST_URL" && ! "$out" =~ ^(gs|s3):// ]]; then
      sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
      post_file_to_url "$out" "$POST_URL"
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
  local ext="${infile##*.}"
  local duckdb_func; duckdb_func=$(get_duckdb_func "$ext")
  local outbase; outbase="$(output_base_name "$infile")"
  local out
  if [[ -n "${OUTPUT_DIR:-}" ]]; then
    out="${OUTPUT_DIR%/}/$outbase.$EXT"
  else
    out="$(dirname "$infile")/$outbase.$EXT"
  fi

  # Safety check to prevent overwriting source file
  if ! check_output_safety "$infile" "$out"; then
    return 1
  fi

  [[ ! "$out" =~ ^(gs|s3):// ]] && [[ -f "$out" ]] && rm -f "$out"
  echo "Converting $infile → $out"
  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_func('$infile'); COPY ( $sql_stmt ) TO '$out' ($COPY_OPTS);"
  else
    local sel; sel=$(dedupe_select_clause)
    run_duckdb "COPY ($sel FROM $duckdb_func('$infile')) TO '$out' ($COPY_OPTS);"
  fi
  echo "✅ $out"
  
  # POST to URL if specified
  if [[ -n "$POST_URL" && ! "$out" =~ ^(gs|s3):// ]]; then
    sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
    post_file_to_url "$out" "$POST_URL"
  fi
}

export -f convert_file split_convert_file dedupe_select_clause select_clause get_duckdb_func output_base_name get_sql_stmt run_duckdb check_output_safety to_abs post_file_to_url

load_cloud_creds

echo "🚀 format=$FORMAT  cols=${SELECT_COLUMNS:-*}  parallel=$MAX_PARALLEL_JOBS  single_file=$SINGLE_FILE  dedupe=$DEDUPE  output_dir=${OUTPUT_DIR:-<src dir>}  rows_per_file=${ROWS_PER_FILE:-0}  sql_file=${SQL_FILE:-}"

if [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
  FILES=()
  while IFS= read -r line; do [[ -n "$line" ]] && FILES+=("$line"); done < <(find_input_files "$INPUT_PATH")
  [[ ${#FILES[@]} -gt 0 ]] || { echo "No supported files found in $INPUT_PATH"; exit 1; }

  first_ext="${FILES[0]##*.}"
  for f in "${FILES[@]}"; do
    ext="${f##*.}"
    [[ "$ext" == "$first_ext" ]] || { echo "Error: All files must have the same extension for --single-file"; exit 1; }
  done
  duckdb_func=$(get_duckdb_func "$first_ext")

  if $SINGLE_FILE; then
    # Determine default output directory - use source directory when OUTPUT_DIR not specified
    default_output_dir=""
    if [[ -n "${OUTPUT_DIR:-}" ]]; then
      default_output_dir="${OUTPUT_DIR}"
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
        OUTPUT_FILENAME="${default_output_dir%/}/$(basename "${INPUT_PATH%/}")_merged.$EXT"
      else
        base_name="$(basename "$INPUT_PATH")"
        # Handle files without extensions or with multiple dots properly
        if [[ "$base_name" == *.* ]]; then
          base_name="${base_name%.*}"
        fi
        OUTPUT_FILENAME="${default_output_dir%/}/${base_name}_merged.$EXT"
      fi
    elif [[ "${OUTPUT_FILENAME}" != /* && ! "${OUTPUT_FILENAME}" =~ ^(gs|s3):// ]]; then
      # Apply the default output directory when filename is relative
      OUTPUT_FILENAME="${default_output_dir%/}/${OUTPUT_FILENAME}"
    fi
    [[ ! "$OUTPUT_FILENAME" =~ ^(gs|s3):// ]] && [[ -f "$OUTPUT_FILENAME" ]] && rm -f "$OUTPUT_FILENAME"
    if [[ -n "$SQL_FILE" ]]; then
      SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
      sql_stmt=$(get_sql_stmt "$SQL_FILE")
      echo "Merging ${#FILES[@]} files → $OUTPUT_FILENAME"
      run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_func(ARRAY[$SQL_PATHS]); COPY ( $sql_stmt ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    else
      SEL=$(dedupe_select_clause)
      SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
      echo "Merging ${#FILES[@]} files → $OUTPUT_FILENAME"
      run_duckdb "COPY (
        $SEL FROM $duckdb_func(ARRAY[$SQL_PATHS])
      ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    fi
    echo "✅ Merged → $OUTPUT_FILENAME"
    
    # POST to URL if specified for merged file
    if [[ -n "$POST_URL" && ! "$OUTPUT_FILENAME" =~ ^(gs|s3):// ]]; then
      sleep "$HTTP_RATE_LIMIT_DELAY"  # Rate limiting
      post_file_to_url "$OUTPUT_FILENAME" "$POST_URL"
    fi
  else
    export -f convert_file
    export EXT COPY_OPTS DEDUPE SELECT_COLUMNS OUTPUT_DIR ROWS_PER_FILE cloud_secret_sql SQL_FILE VERBOSE POST_URL HTTP_RATE_LIMIT_DELAY
    # Export HTTP_HEADERS array elements as individual variables for subprocesses
    for i in "${!HTTP_HEADERS[@]}"; do
      export "HTTP_HEADER_$i=${HTTP_HEADERS[$i]}"
    done
    export HTTP_HEADERS_COUNT="${#HTTP_HEADERS[@]}"
    printf '%s\n' "${FILES[@]}" | xargs -n1 -P "$MAX_PARALLEL_JOBS" bash -c 'convert_file "$0"'
    echo "🎉 All individual conversions complete."
  fi

elif [[ -f "$INPUT_PATH" ]] || [[ "$INPUT_PATH" =~ ^(gs|s3)://.+\.(parquet|csv|json|jsonl|ndjson)$ ]]; then
  convert_file "$INPUT_PATH"
  echo "🎉 Conversion complete."
else
  echo "Error: '$INPUT_PATH' is not a supported file or directory" >&2
  exit 1
fi

echo "💯 Done!"
