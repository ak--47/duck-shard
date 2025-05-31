#!/usr/bin/env bats

setup() {
    export SCRIPT_PATH="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/parquet-to.sh"
    export TEST_DATA_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/testData" && pwd)"
    export EVENT_DATA_DIR="$TEST_DATA_DIR/eventData"
    export OBJECT_DATA_DIR="$TEST_DATA_DIR/objectData"
    export TEST_OUTPUT_DIR="$(mktemp -d)"
    chmod +x "$SCRIPT_PATH"
    verify_test_data
}

teardown() {
    [[ -n "$TEST_OUTPUT_DIR" && -d "$TEST_OUTPUT_DIR" ]] && rm -rf "$TEST_OUTPUT_DIR"
    # Clean up only test output, not source data
}

verify_test_data() {
    [[ -d "$EVENT_DATA_DIR" ]] || { echo "Error: $EVENT_DATA_DIR not found" >&2; exit 1; }
    [[ -d "$OBJECT_DATA_DIR" ]] || { echo "Error: $OBJECT_DATA_DIR not found" >&2; exit 1; }
    [[ $(find "$EVENT_DATA_DIR" -name "*.parquet" | wc -l) -gt 0 ]] || {
        echo "No parquet files found in $EVENT_DATA_DIR" >&2; exit 1; }
    [[ $(find "$OBJECT_DATA_DIR" -name "*.parquet" | wc -l) -gt 0 ]] || {
        echo "No parquet files found in $OBJECT_DATA_DIR" >&2; exit 1; }
}

count_lines() { wc -l < "$1"; }
file_exists_and_not_empty() { [[ -f "$1" && -s "$1" ]]; }
get_first_parquet_file() { find "$1" -name "*.parquet" | head -1; }
count_parquet_files() { find "$1" -name "*.parquet" | wc -l; }

@test "script exists and is executable" { [[ -x "$SCRIPT_PATH" ]]; }

@test "test data directories exist and contain parquet files" {
    [[ -d "$EVENT_DATA_DIR" ]]
    [[ -d "$OBJECT_DATA_DIR" ]]
    [[ $(count_parquet_files "$EVENT_DATA_DIR") -gt 0 ]]
    [[ $(count_parquet_files "$OBJECT_DATA_DIR") -gt 0 ]]
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

@test "parquet > ndjson (default)" {
    local in_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
    run head -1 "$expected"
    [[ "$output" =~ ^\{.*\}$ ]]
}

@test "parquet > csv" {
    local in_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.csv"
    run "$SCRIPT_PATH" "$in_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
    run head -1 "$expected"
    [[ "$output" =~ , ]]
}

@test "parquet dir > ndjson" {
    local original_count=$(count_parquet_files "$EVENT_DATA_DIR")
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
    local first_ndjson=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | head -1)
    file_exists_and_not_empty "$first_ndjson"
}

@test "parquet dir > csv" {
    local original_count=$(count_parquet_files "$OBJECT_DATA_DIR")
    run "$SCRIPT_PATH" "$OBJECT_DATA_DIR" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
    local first_csv=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | head -1)
    file_exists_and_not_empty "$first_csv"
    run head -1 "$first_csv"
    [[ "$output" =~ , ]]
}

@test "parquet dir > single ndjson" {
    cd "$TEST_OUTPUT_DIR"
    local original_count=$(count_parquet_files "$EVENT_DATA_DIR")
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" -s merged_events.ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "merged_events.ndjson"
    run head -1 "merged_events.ndjson"
    [[ "$output" =~ ^\{.*\}$ ]]
}

@test "parquet dir > single parquet" {
    cd "$TEST_OUTPUT_DIR"
    local original_count=$(count_parquet_files "$OBJECT_DATA_DIR")
    run "$SCRIPT_PATH" "$OBJECT_DATA_DIR" -s merged_objects.parquet -f parquet
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "merged_objects.parquet"
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('merged_objects.parquet');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ [0-9]+ ]]
}

@test "parquet dir > single csv" {
    cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" -s -f csv
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "eventData.csv"
    run head -1 "eventData.csv"
    [[ "$output" =~ , ]]
}

@test "specific columns only" {
    local in_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
	[ -n "$in_file" ]
    local base=$(basename "$in_file" .parquet)
    run duckdb -c "PRAGMA table_info(read_parquet('$in_file'));" >/tmp/out
    local columns="distinct_id,person_id"
    run "$SCRIPT_PATH" "$in_file" -c "$columns" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.csv"
    file_exists_and_not_empty "$output_file"
}

@test "parallel processing" {
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" 2 -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "parallel=2" ]]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -gt 0 ]
}

@test "errors: non-existent input file" {
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/nonexistent.parquet"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error:" ]]
}

@test "errors: directory with no parquet" {
    mkdir -p "$TEST_OUTPUT_DIR/empty_dir"
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/empty_dir/"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No Parquet files found" ]]
}

@test "errors: invalid format" {
    local in_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    run "$SCRIPT_PATH" "$in_file" -f invalid_format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format must be ndjson, parquet, or csv" ]]
}

@test "errors: missing argument for --format" {
    local in_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    run "$SCRIPT_PATH" "$in_file" --format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format needs an argument" ]]
}

@test "errors: missing argument for --cols" {
    local in_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    run "$SCRIPT_PATH" "$in_file" --cols
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --cols needs an argument" ]]
}

@test "data integrity (events)" {
    local in_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    local base=$(basename "$in_file" .parquet)
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('$in_file');"
    [ "$status" -eq 0 ]
    local original_count=$(echo "$output" | grep -o '[0-9]\+' | head -1)
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_file="$TEST_OUTPUT_DIR/$base.ndjson"
    local ndjson_lines=$(count_lines "$ndjson_file")
    [[ $ndjson_lines -gt 0 ]]
}

@test "data integrity (objects)" {
    local in_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
    local base=$(basename "$in_file" .parquet)
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('$in_file');"
    [ "$status" -eq 0 ]
    local original_count=$(echo "$output" | grep -o '[0-9]\+' | head -1)
    run "$SCRIPT_PATH" "$in_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_file="$TEST_OUTPUT_DIR/$base.csv"
    local csv_lines=$(count_lines "$csv_file")
    [[ $csv_lines -gt $original_count ]]
}

@test "chunking small files with --rows" {
    # Use the first object file as test
    local first_object_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
    local basename=$(basename "$first_object_file" .parquet)
    local chunk_size=1000

    # Use TEST_OUTPUT_DIR for clean output
    run "$SCRIPT_PATH" "$first_object_file" --rows "$chunk_size" -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅" ]]
    [[ "$output" =~ "rows_per_file=$chunk_size" ]]

    # Should produce multiple files like person-01-1.ndjson, person-01-2.ndjson, ...
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${basename}-*.ndjson" | wc -l)
    [ "$chunks_found" -ge 2 ] # Should be at least 2 chunks if file is large enough

    # Each chunk file should be non-empty
    for f in "$TEST_OUTPUT_DIR"/${basename}-*.ndjson; do
        file_exists_and_not_empty "$f"
    done

    # Total lines across all chunks should equal the original file's row count
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('$first_object_file');"
    [ "$status" -eq 0 ]
    local total_rows=$(echo "$output" | grep -Eo '[0-9]+' | tail -1)
    local lines_in_chunks=$(cat "$TEST_OUTPUT_DIR"/${basename}-*.ndjson | wc -l)
    [ "$lines_in_chunks" -eq "$total_rows" ]

    echo "Chunked $first_object_file into $chunks_found NDJSON files, $lines_in_chunks lines total" >&3
}


@test "performance check" {
    cd "$TEST_OUTPUT_DIR"
    local start_time=$(date +%s)
    run timeout 60s "$SCRIPT_PATH" "$EVENT_DATA_DIR" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    [[ $duration -lt 60 ]]
}
