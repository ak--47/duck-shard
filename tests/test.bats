#!/usr/bin/env bats
# shellcheck disable=SC2034,SC2155
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

# test -f ./duck-shard.sh && echo 'File exists' || echo 'File does not exist'
@test "script exists and is executable" {
    [[ -x "$SCRIPT_PATH" ]]
}

# ./duck-shard.sh --help
@test "show help with -h" {
    run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

# ./duck-shard.sh
@test "show help with no args" {
    run "$SCRIPT_PATH"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

##### ==== PARQUET INPUT ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f ndjson -o ./tmp
@test "parquet file > ndjson output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f csv -o ./tmp
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

# ./duck-shard.sh ./tests/testData/parquet -f ndjson -o ./tmp
@test "parquet dir > ndjson output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
    local first_ndjson=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | head -1)
    file_exists_and_not_empty "$first_ndjson"
}

# cd ./tmp && ../duck-shard.sh ../tests/testData/parquet -s all.csv -f csv
@test "parquet dir > merged single csv file" {
    cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all.csv -f csv
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "all.csv"
    run head -1 "all.csv"
    [[ "$output" =~ , ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --rows 1000 -o ./tmp
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

# ./duck-shard.sh ./tests/testData/csv/part-1.csv -f ndjson -o ./tmp
@test "csv file > ndjson output" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/csv -f ndjson -o ./tmp
@test "csv dir > ndjson output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/csv" "csv")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --rows 1000 -o ./tmp
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

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson -f csv -o ./tmp
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

# ./duck-shard.sh ./tests/testData/ndjson -f csv -o ./tmp
@test "ndjson dir > csv output for all files" {
    local original_count=$(count_files "$TEST_DATA_DIR/ndjson" "ndjson")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson --rows 1000 -f csv -o ./tmp
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

# cd ./tmp && ../duck-shard.sh ../tests/testData/parquet -s all_str.csv -f csv --stringify
@test "parquet dir > merged single csv file with --stringify" {
    cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all_str.csv -f csv --stringify
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "all_str.csv"
    # Check that there are no obvious type errors in CSV output
    run grep '\$organic' all_str.csv
    [ "$status" -eq 0 ] # value should appear as string, not fail on INT128
}

##### ==== COLUMN SELECTION TEST ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -c event,time,user_id -f csv -o ./tmp
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

# ./duck-shard.sh ./tests/testData/nope.parquet
@test "error: non-existent input file" {
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/nope.parquet"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error:" ]]
}

# ./duck-shard.sh ./tmp/empty_dir/
@test "error: empty directory" {
    mkdir -p "$TEST_OUTPUT_DIR/empty_dir"
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/empty_dir/"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No" ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f invalid_format
@test "error: invalid format" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" -f invalid_format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format must be" ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --format
@test "error: missing argument for --format" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    [ -f "$in_file" ]
    run "$SCRIPT_PATH" "$in_file" --format
    echo "status: $status"
    echo "output: $output"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Error: --format needs an argument"* ]]
}


# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --cols
@test "error: missing argument for --cols" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    [ -f "$in_file" ] # ensure file exists
    run bash -c "$SCRIPT_PATH $in_file --cols 2>&1"
    echo "status: $status"
    echo "output: $output"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Error: --cols needs an argument"* ]]
}


# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --rows 1000 --single-file merged.ndjson
@test "error: --rows with --single-file" {
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" --rows 1000 --single-file merged.ndjson
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --rows cannot be used with --single-file mode" ]]
}

# ./duck-shard.sh gs://totally-fake-bucket/myfile.parquet
@test "error: GCS URI without credentials" {
    # This will only work if you do NOT have env vars set or default creds
    local fake_gcs="gs://totally-fake-bucket/myfile.parquet"
    run "$SCRIPT_PATH" "$fake_gcs"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Error" ]]
}

# ./duck-shard.sh s3://totally-fake-bucket/myfile.parquet
@test "error: S3 URI without credentials" {
    local fake_s3="s3://totally-fake-bucket/myfile.parquet"
    run "$SCRIPT_PATH" "$fake_s3"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Error" ]]
}

# mkdir -p ./tmp/mixed; cp ./tests/testData/parquet/part-1.parquet ./tmp/mixed/file1.parquet; cp ./tests/testData/csv/part-1.csv ./tmp/mixed/file2.csv; ./duck-shard.sh ./tmp/mixed -s merged.ndjson
@test "error: mixing file types for --single-file" {
    # Create a temp dir with a parquet and a csv
    mkdir -p "$TEST_OUTPUT_DIR/mixed"
    cp "$(get_first_file "$TEST_DATA_DIR/parquet" parquet)" "$TEST_OUTPUT_DIR/mixed/file1.parquet"
    cp "$(get_first_file "$TEST_DATA_DIR/csv" csv)" "$TEST_OUTPUT_DIR/mixed/file2.csv"
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/mixed" -s merged.ndjson
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: All files must have the same extension for --single-file" ]]
}

# mkdir -p ./tmp/unwritable; chmod -w ./tmp/unwritable; ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -o ./tmp/unwritable
@test "error: output directory not writable" {
    local unwritable_dir="$TEST_OUTPUT_DIR/unwritable"
    mkdir -p "$unwritable_dir"
    chmod -w "$unwritable_dir"
    local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" parquet)
    run "$SCRIPT_PATH" "$in_file" -o "$unwritable_dir"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Error" ]]
    chmod +w "$unwritable_dir" # restore permissions
}

# echo 'randomdata' > ./tmp/file.bogus; ./duck-shard.sh ./tmp/file.bogus
@test "error: unsupported file extension" {
    local bogus="$TEST_OUTPUT_DIR/file.bogus"
    echo 'randomdata' > "$bogus"
    run "$SCRIPT_PATH" "$bogus"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Unsupported extension" ]]
}

# ./duck-shard.sh -h
@test "help output mentions GCS and S3" {
    run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "GCS HMAC credentials" ]]
    [[ "$output" =~ "S3 HMAC credentials" ]]
}


##### ==== PERFORMANCE CHECK ====

# cd ./tmp && timeout 60s ../duck-shard.sh ../tests/testData/parquet -f ndjson -o ./tmp
@test "performance check on parquet dir" {
    cd "$TEST_OUTPUT_DIR"
    local start_time=$(date +%s)
    run timeout 60s "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    [[ $duration -lt 60 ]]
}

