#!/usr/bin/env bash

# parquet-to.sh - Convert Parquet files to NDJSON, Parquet, or CSV using DuckDB, with optional deduplication.
# Usage: ./parquet-to.sh <input_path> [max_parallel_jobs] \
#   [-s|--single-file [output_filename]] \
#   [-f|--format <ndjson|parquet|csv>] \
#   [-c|--cols <column1,column2,...>] \
#   [--dedupe]

set -euo pipefail

### 1) Preconditions
if ! command -v duckdb &> /dev/null; then
  echo "Error: duckdb not installed. See https://duckdb.org/docs/installation/" >&2
  exit 1
fi

### 2) Defaults
MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""
FORMAT="ndjson"
COLUMNS="*"        # default = all columns
DEDUPE=false

print_help() {
  cat <<EOF
Usage: $0 <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]] \\
       [-f|--format <ndjson|parquet|csv>] [-c|--cols <col1,col2,...>] [--dedupe]

  <input_path>          Path to a Parquet file or directory
  [max_parallel_jobs]   Parallel jobs when not single-file
  -s, --single-file     Merge into one output file
     [output_filename]  Optional: name for merged file
  -f, --format          ndjson (default) | parquet | csv
  -c, --cols            Comma-separated list of columns (default=all)
  --dedupe              Remove duplicate rows based on selected columns

Examples:
  # Single-file parquet → NDJSON, deduplicated on all columns
  $0 data/file.parquet --dedupe

  # Directory → CSV in 8 jobs, deduplicated by id,name
  $0 data/ 8 -f csv -c id,name --dedupe

  # Directory → one Parquet, dedupe on all columns
  $0 data/ -s combined.parquet -f parquet --dedupe
EOF
}

### 3) Parse flags
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--single-file)
      SINGLE_FILE=true; shift
      if [[ $# -gt 0 && ! $1 =~ ^- ]]; then
        OUTPUT_FILENAME="$1"; shift
      fi
      ;;
    -f|--format)
      [[ $# -ge 2 ]] || { echo "Error: --format needs an argument"; exit 1; }
      FORMAT="$2"; shift 2
      ;;
    -c|--cols)
      [[ $# -ge 2 ]] || { echo "Error: --cols needs an argument"; exit 1; }
      # Just store as-is (comma separated)
      COLUMNS_RAW="$2"
      COLUMNS=$(echo "$2" \
        | tr ',' '\n' \
        | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
        | awk '{ printf "%s,", $0 }' \
        | sed 's/,$//')
      shift 2
      ;;
    --dedupe)
      DEDUPE=true; shift
      ;;
    -h|--help)
      print_help; exit 0
      ;;
    *)
      POSITIONAL+=("$1"); shift
      ;;
  esac
done
set -- "${POSITIONAL[@]}"

### 4) Validate positional args
[[ $# -ge 1 ]] || { echo "Error: Missing <input_path>"; print_help; exit 1; }
INPUT_PATH="$1"
if [[ $# -ge 2 && $2 =~ ^[0-9]+$ ]]; then
  MAX_PARALLEL_JOBS="$2"
fi

### 5) Pick format
case "$FORMAT" in
  ndjson)  EXT="ndjson";  COPY_OPTS="FORMAT JSON"      ;;
  parquet) EXT="parquet"; COPY_OPTS="FORMAT PARQUET"   ;;
  csv)     EXT="csv";     COPY_OPTS="FORMAT CSV, HEADER";;
  *)
    echo "Error: --format must be ndjson, parquet, or csv"; exit 1
    ;;
esac

export EXT COPY_OPTS

echo "🚀 format=$FORMAT  cols=${COLUMNS:-*}  parallel=$MAX_PARALLEL_JOBS  single_file=$SINGLE_FILE  dedupe=$DEDUPE"

### 6) Default output filename for single-file
if $SINGLE_FILE && [[ -z "${OUTPUT_FILENAME:-}" ]]; then
  if [[ -d "$INPUT_PATH" ]]; then
    DIR="${INPUT_PATH%/}"
    OUTPUT_FILENAME="$(basename "$DIR").$EXT"
  else
    OUTPUT_FILENAME="$(basename "${INPUT_PATH%.*}").$EXT"
  fi
fi

### 7) Build SELECT clause (for column subset)
# Returns "col1, col2, ..." or "*"
select_clause() {
  if [[ "${COLUMNS:-*}" == "*" ]]; then
    echo "*"
  else
    echo "$COLUMNS"
  fi
}

### 8) Build DEDUPE clause (DISTINCT or SELECT)
# If dedupe, use SELECT DISTINCT on the chosen columns
#    - If columns specified, select those columns DISTINCT (but must output only those columns)
#    - If columns not specified, DISTINCT *
# If not dedupe, just SELECT columns (could be *)
dedupe_select_clause() {
  local select_cols
  select_cols=$(select_clause)
  if $DEDUPE; then
    echo "SELECT DISTINCT $select_cols"
  else
    echo "SELECT $select_cols"
  fi
}

### 9) Per-file converter (when NOT single-file)
convert_file() {
  local infile="$1"
  local out="${infile%.*}.$EXT"
  local sel
  sel=$(dedupe_select_clause)
  echo "Converting $infile → $out"
  duckdb -c "COPY ($sel FROM read_parquet('$infile')) TO '$out' ($COPY_OPTS);"
  echo "✅ $out"
}
export -f convert_file dedupe_select_clause select_clause
export EXT COPY_OPTS DEDUPE


### 10) Main logic
if [[ -d "$INPUT_PATH" ]]; then
  # directory mode
  mapfile -t files < <(find "$INPUT_PATH" -type f -name '*.parquet')
  [[ ${#files[@]} -gt 0 ]] || { echo "No Parquet files found in $INPUT_PATH"; exit 1; }

  if $SINGLE_FILE; then
    # merge all via a single DuckDB command
    sel=$(dedupe_select_clause)
    # build ARRAY literal of parquet files
    SQL_PATHS=$(printf "'%s'," "${files[@]}")
    SQL_PATHS=${SQL_PATHS%,}
    echo "Merging ${#files[@]} files → $OUTPUT_FILENAME"
    duckdb -c "COPY (
      $sel
      FROM read_parquet(ARRAY[${SQL_PATHS}])
    ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    echo "✅ Merged → $OUTPUT_FILENAME"
  else
    # parallel per-file
    printf '%s\n' "${files[@]}" \
      | xargs -n1 -P "$MAX_PARALLEL_JOBS" \
          bash -c 'convert_file "$0"'
    echo "🎉 All individual conversions complete."
  fi

elif [[ -f "$INPUT_PATH" && "$INPUT_PATH" == *.parquet ]]; then
  # single-file Parquet input
  if $SINGLE_FILE; then
    sel=$(dedupe_select_clause)
    echo "Converting single file → $OUTPUT_FILENAME"
    duckdb -c "COPY ($sel FROM read_parquet('$INPUT_PATH')) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    echo "✅ $OUTPUT_FILENAME"
  else
    convert_file "$INPUT_PATH"
    echo "🎉 Conversion complete."
  fi

else
  echo "Error: '$INPUT_PATH' is not a Parquet file or directory"; exit 1
fi

echo "💯 Done!"
