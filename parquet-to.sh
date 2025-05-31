#!/usr/bin/env bash
# parquet-to.sh – Portable, works on macOS & Linux. Output directory & row splitting supported.

set -euo pipefail

command -v duckdb >/dev/null 2>&1 || {
  echo "Error: duckdb not installed. See https://duckdb.org/docs/installation/" >&2
  exit 1
}

MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""
FORMAT="ndjson"
COLUMNS="*"
DEDUPE=false
OUTPUT_DIR=""
ROWS_PER_FILE=0

print_help() {
  cat <<EOF

Usage: $0 <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]] \\
       [-f|--format <ndjson|parquet|csv>] [-c|--cols <col1,col2,...>] [--dedupe] \\
       [-o|--output <output_dir>] [-r|--rows <rows_per_file>]

  <input_path>          Path to a Parquet file or directory
  [max_parallel_jobs]   Parallel jobs when not single-file (default = CPU cores)
  -s, --single-file     Merge output into one file
     [output_filename]  Optional: name for merged file
  -f, --format          ndjson (default) | parquet | csv
  -c, --cols            Comma-separated list of columns (default = all)
  --dedupe              Remove duplicate rows based on selected columns
  -o, --output          Output directory for results (per-file mode only)
  -r, --rows            Split output files with N rows each (incompatible with --single-file)

EOF
}

if [[ $# -eq 0 ]]; then
  print_help; exit 0
fi

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--single-file)
      SINGLE_FILE=true; shift
      if [[ $# -gt 0 && ! $1 =~ ^- ]]; then OUTPUT_FILENAME="$1"; shift; fi ;;
    -f|--format)
      [[ $# -ge 2 ]] || { echo "Error: --format needs an argument"; exit 1; }
      FORMAT="$2"; shift 2 ;;
    -c|--cols)
      [[ $# -ge 2 ]] || { echo "Error: --cols needs an argument"; exit 1; }
      COLUMNS=$(echo "$2" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' |
                paste -sd, -); shift 2 ;;
    --dedupe) DEDUPE=true; shift ;;
    -o|--output)
      [[ $# -ge 2 ]] || { echo "Error: --output needs an argument"; exit 1; }
      OUTPUT_DIR="$2"; shift 2 ;;
    -r|--rows)
      [[ $# -ge 2 ]] || { echo "Error: --rows needs an integer argument"; exit 1; }
      [[ "$2" =~ ^[0-9]+$ ]] || { echo "Error: --rows must be an integer"; exit 1; }
      ROWS_PER_FILE="$2"; shift 2 ;;
    -h|--help) print_help; exit 0 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]}"

[[ $# -ge 1 ]] || { echo "Error: Missing <input_path>"; print_help >&2; exit 1; }
INPUT_PATH="$1"
[[ -e "$INPUT_PATH" ]] || { echo "Error: '$INPUT_PATH' not found" >&2; exit 1; }

to_abs() {
  case "$1" in
    /*) echo "$1" ;;
    *) echo "$PWD/${1#./}" ;;
  esac
}

INPUT_PATH=$(to_abs "$INPUT_PATH")

if [[ $# -ge 2 && $2 =~ ^[0-9]+$ ]]; then MAX_PARALLEL_JOBS="$2"; fi

if [[ -n "${OUTPUT_DIR:-}" ]]; then
  OUTPUT_DIR=$(to_abs "$OUTPUT_DIR")
  mkdir -p "$OUTPUT_DIR"
fi

case "$FORMAT" in
  ndjson)  EXT="ndjson";  COPY_OPTS="FORMAT JSON" ;;
  parquet) EXT="parquet"; COPY_OPTS="FORMAT PARQUET" ;;
  csv)     EXT="csv";     COPY_OPTS="FORMAT CSV, HEADER" ;;
  *) echo "Error: --format must be ndjson, parquet, or csv"; exit 1 ;;
esac

if (( ROWS_PER_FILE > 0 )) && $SINGLE_FILE; then
  echo "Error: --rows cannot be used with --single-file mode"; exit 1
fi

echo "🚀 format=$FORMAT  cols=${COLUMNS:-*}  parallel=$MAX_PARALLEL_JOBS  single_file=$SINGLE_FILE  dedupe=$DEDUPE  output_dir=${OUTPUT_DIR:-<src dir>}  rows_per_file=${ROWS_PER_FILE:-0}"

if $SINGLE_FILE && [[ -z "${OUTPUT_FILENAME:-}" ]]; then
  if [[ -d "$INPUT_PATH" ]]; then
    OUTPUT_FILENAME="$(basename "${INPUT_PATH%/}").$EXT"
  else
    OUTPUT_FILENAME="$(basename "${INPUT_PATH%.*}").$EXT"
  fi
fi

select_clause() {
  [[ "$COLUMNS" == "*" ]] && echo "*" || echo "$COLUMNS"
}

dedupe_select_clause() {
  local sel; sel=$(select_clause)
  $DEDUPE && echo "SELECT DISTINCT $sel" || echo "SELECT $sel"
}

split_convert_file() {
  local infile="$1"
  local base="$(basename "${infile%.*}")"
  local sel; sel=$(dedupe_select_clause)
  local row_count
  # Only get the last number output (the count)
  row_count=$(duckdb -c "SELECT COUNT(*) FROM read_parquet('$infile');" | grep -Eo '[0-9]+' | tail -1)
  local splits=$(( (row_count + ROWS_PER_FILE - 1) / ROWS_PER_FILE ))
  local i=1
  local offset=0
  while (( offset < row_count )); do
    local outbase="${base}-${i}.$EXT"
    local out
    if [[ -n "${OUTPUT_DIR:-}" ]]; then
      out="$OUTPUT_DIR/$outbase"
    else
      out="$(dirname "$infile")/$outbase"
    fi
    [[ -f "$out" ]] && rm -f "$out"
    echo "Converting $infile rows $((offset+1))-$((offset+ROWS_PER_FILE>row_count?row_count:offset+ROWS_PER_FILE)) → $out"
    duckdb -c "COPY (
      $sel FROM read_parquet('$infile')
      LIMIT $ROWS_PER_FILE OFFSET $offset
    ) TO '$out' ($COPY_OPTS);"
    echo "✅ $out"
    ((i++))
    ((offset+=ROWS_PER_FILE))
  done
}


convert_file() {
  if (( ROWS_PER_FILE > 0 )); then
    split_convert_file "$1"
    return
  fi
  local infile="$1"
  local outbase
  outbase="$(basename "${infile%.*}").$EXT"
  local out
  if [[ -n "${OUTPUT_DIR:-}" ]]; then
    out="$OUTPUT_DIR/$outbase"
  else
    out="$(dirname "$infile")/$outbase"
  fi
  [[ -f "$out" ]] && rm -f "$out"
  local sel; sel=$(dedupe_select_clause)
  echo "Converting $infile → $out"
  duckdb -c "COPY ($sel FROM read_parquet('$infile')) TO '$out' ($COPY_OPTS);"
  echo "✅ $out"
}

export -f convert_file split_convert_file dedupe_select_clause select_clause
export EXT COPY_OPTS DEDUPE COLUMNS OUTPUT_DIR ROWS_PER_FILE

if [[ -d "$INPUT_PATH" ]]; then
  FILES=()
  while IFS= read -r f; do FILES+=("$f"); done < <(find "$INPUT_PATH" -type f -name '*.parquet' | sort)
  [[ ${#FILES[@]} -gt 0 ]] || { echo "No Parquet files found in $INPUT_PATH"; exit 1; }

  if $SINGLE_FILE; then
    [[ -f "$OUTPUT_FILENAME" ]] && rm -f "$OUTPUT_FILENAME"
    SEL=$(dedupe_select_clause)
    SQL_PATHS=$(for f in "${FILES[@]}"; do printf "'%s'," "$f"; done); SQL_PATHS=${SQL_PATHS%,}
    echo "Merging ${#FILES[@]} files → $OUTPUT_FILENAME"
    duckdb -c "COPY (
      $SEL
      FROM read_parquet(ARRAY[${SQL_PATHS}])
    ) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    echo "✅ Merged → $OUTPUT_FILENAME"
  else
    # Parallel execution (limited jobs)
    n=0
    for f in "${FILES[@]}"; do
      bash -c 'convert_file "$0"' "$f" &
      ((n++))
      if [[ "$n" -ge "$MAX_PARALLEL_JOBS" ]]; then
        wait -n 2>/dev/null || wait
        ((n--))
      fi
    done
    wait
    echo "🎉 All individual conversions complete."
  fi

elif [[ -f "$INPUT_PATH" && "$INPUT_PATH" == *.parquet ]]; then
  if $SINGLE_FILE; then
    [[ -f "$OUTPUT_FILENAME" ]] && rm -f "$OUTPUT_FILENAME"
    SEL=$(dedupe_select_clause)
    echo "Converting single file → $OUTPUT_FILENAME"
    duckdb -c "COPY ($SEL FROM read_parquet('$INPUT_PATH')) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
    echo "✅ $OUTPUT_FILENAME"
  else
    convert_file "$INPUT_PATH"
    echo "🎉 Conversion complete."
  fi

else
  echo "Error: '$INPUT_PATH' is not a Parquet file or directory" >&2
  exit 1
fi

echo "💯 Done!"
