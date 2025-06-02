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

print_help() {
  cat <<EOF

Usage: $0 <input_path> [max_parallel_jobs] [options]

Options:
  -s, --single-file [output_filename]   Merge into one output file (optional: filename)
  -f, --format <ndjson|parquet|csv>     Output format (default: ndjson)
  -c, --cols <col1,col2,...>            Only include specific columns
  --dedupe                              Remove duplicate rows (by chosen columns)
  -o, --output <output_dir>             Output directory (per-file mode only)
  -r, --rows <rows_per_file>            Split output files with N rows each (not for --single-file)
  --sql <sql_file>                      Use custom SQL SELECT (on temp view input_data)
  --gcs-key <key> --gcs-secret <secret> GCS HMAC credentials
  --s3-key <key> --s3-secret <secret>   S3 HMAC credentials
  --verbose                             Print all DuckDB SQL commands before running them
  -h, --help                            Print this help

Examples:
  $0 data/ -f csv -o ./out/
  $0 data/ -s merged.ndjson
  $0 gs://bucket/data/ -f csv --gcs-key ... --gcs-secret ... -o ./out/
  $0 data/ --sql my_query.sql -f csv -o ./out/
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
if [[ ! "${INPUT_PATH}" =~ ^(gs|s3):// ]]; then
  INPUT_PATH=$(to_abs "$INPUT_PATH")
fi

if [[ $# -ge 2 && $2 =~ ^[0-9]+$ ]]; then MAX_PARALLEL_JOBS="$2"; fi

if [[ -n "${OUTPUT_DIR:-}" ]]; then
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

# This will hold the full prefix for every DuckDB -c invocation
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
      run_duckdb "SELECT filename FROM list_files('$path', recursive=true);" | \
        awk '/^gs:\/\// || /^s3:\/\// {if (match($0, /\.(parquet|csv|ndjson|jsonl|json)$/)) print $0}' | sort
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
  # If input ends with * (glob), make output name "merged"
  if [[ "$file" == *'*'* ]]; then
    echo "merged"
    return
  fi
  local base="$(basename "$file")"
  local outbase="${base%.*}"
  echo "$outbase"
}

get_sql_stmt() {
  cat "$1" | sed -e 's/[[:space:]]*$//' -e ':a' -e 's/;$//;ta' -e 's/[[:space:]]*$//'
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
    local out="${OUTPUT_DIR:-$(dirname "$infile")}/$outbase-$i.$EXT"
    [[ -f "$out" ]] && rm -f "$out"
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
  local out="${OUTPUT_DIR:-$(dirname "$infile")}/$outbase.$EXT"
  [[ -f "$out" ]] && rm -f "$out"
  echo "Converting $infile → $out"
  if [[ -n "$SQL_FILE" ]]; then
    sql_stmt=$(get_sql_stmt "$SQL_FILE")
    run_duckdb "CREATE OR REPLACE TEMP VIEW input_data AS SELECT * FROM $duckdb_func('$infile'); COPY ( $sql_stmt ) TO '$out' ($COPY_OPTS);"
  else
    local sel; sel=$(dedupe_select_clause)
    run_duckdb "COPY ($sel FROM $duckdb_func('$infile')) TO '$out' ($COPY_OPTS);"
  fi
  echo "✅ $out"
}

export -f convert_file split_convert_file dedupe_select_clause select_clause get_duckdb_func output_base_name get_sql_stmt run_duckdb

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
    if [[ -z "${OUTPUT_FILENAME:-}" ]]; then
      if [[ -d "$INPUT_PATH" || "$INPUT_PATH" =~ ^(gs|s3):// ]]; then
        OUTPUT_FILENAME="${OUTPUT_DIR:-.}/$(basename "${INPUT_PATH%/}")_merged.$EXT"
      else
        OUTPUT_FILENAME="${OUTPUT_DIR:-.}/$(basename "${INPUT_PATH%.*}")_merged.$EXT"
      fi
    elif [[ -n "${OUTPUT_DIR:-}" && "${OUTPUT_FILENAME}" != /* ]]; then
      OUTPUT_FILENAME="${OUTPUT_DIR}/${OUTPUT_FILENAME}"
    fi
    [[ -f "$OUTPUT_FILENAME" ]] && rm -f "$OUTPUT_FILENAME"
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
  else
    export -f convert_file
    export EXT COPY_OPTS DEDUPE SELECT_COLUMNS OUTPUT_DIR ROWS_PER_FILE cloud_secret_sql SQL_FILE VERBOSE
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
