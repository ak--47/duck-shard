#!/usr/bin/env bats

# Robust test suite for duck-shard.sh (portable DuckDB data converter)

setup() {
    export SCRIPT_PATH="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/duck-shard.sh"
    export TEST_DATA_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/testData" && pwd)"
    export TEST_OUTPUT_DIR="$(mktemp -d)"
    chmod +x "$SCRIPT_PATH"
    verify_test_data
}

teardown() {
    [[ -n "$TEST_OUTPUT_DIR" && -d "$TEST_OUTPUT_DIR" ]] && rm -rf "$TEST_OUTPUT_DIR"
}

verify_test_data() {
    for format in parquet csv ndjson; do
        [[ -d "$TEST_DATA_DIR/$format" ]] || { echo "Missing: $TEST_DATA_DIR/$format" >&2; exit 1; }
        [[ $(find "$TEST_DATA_DIR/$format" -name "*.$format" | wc -l) -gt 0 ]] || {
            echo "No $format files in $TEST_DATA_DIR/$format" >&2; exit 1; }
    done
}

count_lines() { wc -l < "$1"; }
file_exists_and_not_empty() { [[ -f "$1" && -s "$1" ]]; }
get_first_file() { find "$1" -type f -name "*.$2" | head -1; }
count_files() { find "$1" -type f -name "*.$2" | wc -l; }

##### ==== HELP AND BASIC SANITY TESTS ====

@test "script exists and is executable" {
    [[ -x "$SCRIPT_PATH" ]]
}

@test "show help with -h" {
    run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

@test "show help with no args" {
    run "$SCRIPT_PATH"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

##### ==== PARQUET INPUT ====

@test "parquet file > ndjson output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

@test "parquet file > csv output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.csv"
    run "$SCRIPT_PATH" "$in_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
    run head -1 "$expected"
    [[ "$output" =~ , ]]
}

@test "parquet dir > ndjson output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
    local first_ndjson=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | head -1)
    file_exists_and_not_empty "$first_ndjson"
}

@test "parquet dir > merged single csv file" {
    cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all.csv -f csv
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "all.csv"
    run head -1 "all.csv"
    [[ "$output" =~ , ]]
}

@test "parquet file > chunked ndjson with --rows" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local chunk_size=1000
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.ndjson" | wc -l)
    [ "$chunks_found" -ge 1 ]
    for f in "$TEST_OUTPUT_DIR"/${base}-*.ndjson; do file_exists_and_not_empty "$f"; done
}

##### ==== CSV INPUT ====

@test "csv file > ndjson output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

@test "csv dir > ndjson output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/csv" "csv")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
}

@test "csv file > chunked ndjson with --rows" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local chunk_size=1000
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.ndjson" | wc -l)
    [ "$chunks_found" -ge 1 ]
    for f in "$TEST_OUTPUT_DIR"/${base}-*.ndjson; do file_exists_and_not_empty "$f"; done
}

##### ==== NDJSON INPUT ====

@test "ndjson file > csv output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local expected="$TEST_OUTPUT_DIR/$base.csv"
    run "$SCRIPT_PATH" "$in_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
    run head -1 "$expected"
    [[ "$output" =~ , ]]
}

@test "ndjson dir > csv output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/ndjson" "ndjson")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
}

@test "ndjson file > chunked csv with --rows" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local chunk_size=1000
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.csv" | wc -l)
    [ "$chunks_found" -ge 1 ]
    for f in "$TEST_OUTPUT_DIR"/${base}-*.csv; do file_exists_and_not_empty "$f"; done
}

##### ==== COLUMN SELECTION TEST ====

@test "specific columns from parquet file > csv" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    [ -n "$in_file" ]
    local base=$(basename "$in_file" .parquet)
    local columns="event,time,user_id"
    run "$SCRIPT_PATH" "$in_file" -c "$columns" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.csv"
    file_exists_and_not_empty "$output_file"
    run head -1 "$output_file"
    # Expect at least those column headers in the first line
    [[ "$output" =~ event ]]
    [[ "$output" =~ time ]]
    [[ "$output" =~ user_id ]]
}

##### ==== ERROR CASES ====

@test "error: non-existent input file" {
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/nope.parquet"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error:" ]]
}

@test "error: empty directory" {
    mkdir -p "$TEST_OUTPUT_DIR/empty_dir"
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/empty_dir/"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No" ]]
}

@test "error: invalid format" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" -f invalid_format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format must be" ]]
}

@test "error: missing argument for --format" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" --format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format needs an argument" ]]
}

@test "error: missing argument for --cols" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" --cols
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --cols needs an argument" ]]
}

@test "error: --rows with --single-file" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" --rows 1000 --single-file merged.ndjson
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --rows cannot be used with --single-file mode" ]]
}

##### ==== PERFORMANCE CHECK ====

@test "performance check on parquet dir" {
    cd "$TEST_OUTPUT_DIR"
    local start_time=$(date +%s)
    run timeout 60s "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    [[ $duration -lt 60 ]]
}

