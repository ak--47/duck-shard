#!/usr/bin/env bats
# shellcheck disable=SC2034,SC2155
# Robust test suite for duck-shard.sh (portable DuckDB data converter)

setup() {
	PROJECT_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
	if [ -f "$PROJECT_ROOT/.env" ]; then
		# shellcheck disable=SC1091
		set -a
		source "$PROJECT_ROOT/.env"
		set +a
	fi
    export SCRIPT_PATH="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/duck-shard.sh"
    export TEST_DATA_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/testData" && pwd)"
    export TEST_OUTPUT_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/tmp"

	# Create output directory if it doesn't exist
	mkdir -p "$TEST_OUTPUT_DIR"

	# Safe safe_cleanup_output_dir_alternative - only if directory exists and is not empty
	safe_safe_cleanup_output_dir_alternative_output_dir

    chmod +x "$SCRIPT_PATH"
    verify_test_data
}

teardown() {
	# Safe safe_cleanup_output_dir_alternative in teardown too
	safe_safe_cleanup_output_dir_alternative_output_dir
}

# Safe safe_cleanup_output_dir_alternative function that prevents accidental deletion
safe_safe_cleanup_output_dir_alternative_output_dir() {
	# Multiple safety checks
	if [[ -n "$TEST_OUTPUT_DIR" ]] && [[ -d "$TEST_OUTPUT_DIR" ]] && [[ "$TEST_OUTPUT_DIR" != "/" ]]; then
		# Method 1: Use find to be more explicit
		find "$TEST_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type f ! -name '.gitkeep' -delete 2>/dev/null || true
		find "$TEST_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} + 2>/dev/null || true
	fi
}

# Alternative even safer safe_cleanup_output_dir_alternative (choose one approach)
safe_cleanup_output_dir_alternative() {
	if [[ -n "$TEST_OUTPUT_DIR" ]] && [[ -d "$TEST_OUTPUT_DIR" ]] && [[ "$TEST_OUTPUT_DIR" =~ /tmp$ ]]; then
		# Only proceed if path ends with /tmp for extra safety
		for item in "$TEST_OUTPUT_DIR"/*; do
			if [[ -e "$item" ]] && [[ "$(basename "$item")" != ".gitkeep" ]]; then
				rm -rf "$item"
			fi
		done
	fi
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
    safe_cleanup_output_dir_alternative
	[[ -x "$SCRIPT_PATH" ]]
}

# ./duck-shard.sh --help
@test "show help with -h" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

# ./duck-shard.sh
@test "show help with no args" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

##### ==== PARQUET INPUT ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f ndjson -o ./tmp
@test "parquet file > ndjson output" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f csv -o ./tmp
@test "parquet file > csv output" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all.csv -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "all.csv"
    run head -1 "all.csv"
    [[ "$output" =~ , ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --rows 1000 -o ./tmp
@test "parquet file > chunked ndjson with --rows" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/csv -f ndjson -o ./tmp
@test "csv dir > ndjson output for all files" {
    safe_cleanup_output_dir_alternative
	local original_count=$(count_files "$TEST_DATA_DIR/csv" "csv")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --rows 1000 -o ./tmp
@test "csv file > chunked ndjson with --rows" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	local original_count=$(count_files "$TEST_DATA_DIR/ndjson" "ndjson")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
}

# cd ./tmp && ../duck-shard.sh ../tests/testData/parquet -s all_str.csv -f csv --stringify
@test "parquet dir > merged single csv file with --stringify" {
    safe_cleanup_output_dir_alternative
	cd "$TEST_OUTPUT_DIR"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all_str.csv -f csv --stringify -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "all_str.csv"
    # Check that there are no obvious type errors in CSV output
    run grep '\$organic' all_str.csv
    [ "$status" -eq 0 ] # value should appear as string, not fail on INT128
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson --rows 1000 -f csv -o ./tmp
@test "ndjson file > chunked csv with --rows" {
    safe_cleanup_output_dir_alternative
	[ "$(find "$TEST_OUTPUT_DIR" -type f ! -name '.gitkeep' | wc -l)" -eq 0 ]
    local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local chunk_size=1000
    run timeout 60s "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.csv" | wc -l)
    echo "Chunks found: $chunks_found"
    [ "$chunks_found" -ge 1 ]
    for f in "$TEST_OUTPUT_DIR"/${base}-*.csv; do file_exists_and_not_empty "$f"; done
}



##### ==== COLUMN SELECTION TEST ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -c event,time,user_id -f csv -o ./tmp
@test "specific columns from parquet file > csv" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/nope.parquet"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error:" ]]
}

# ./duck-shard.sh ./tmp/empty_dir/
@test "error: empty directory" {
    safe_cleanup_output_dir_alternative
	mkdir -p "$TEST_OUTPUT_DIR/empty_dir"
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/empty_dir/"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No" ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f invalid_format
@test "error: invalid format" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" -f invalid_format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format must be" ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --format
@test "error: missing argument for --format" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$in_file" --rows 1000 --single-file merged.ndjson
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --rows cannot be used with --single-file mode" ]]
}

# # ./duck-shard.sh gs://totally-fake-bucket/myfile.parquet
# @test "error: GCS URI without credentials" {
safe_cleanup_output_dir_alternative
#     # This will only work if you do NOT have env vars set or default creds
#     local fake_gcs="gs://totally-fake-bucket/myfile.parquet"
#     run "$SCRIPT_PATH" "$fake_gcs"
#     [ "$status" -ne 0 ]
#     [[ "$output" =~ "Error" ]]
# }

# # ./duck-shard.sh s3://totally-fake-bucket/myfile.parquet
# @test "error: S3 URI without credentials" {
safe_cleanup_output_dir_alternative
#     local fake_s3="s3://totally-fake-bucket/myfile.parquet"
#     run "$SCRIPT_PATH" "$fake_s3"
#     [ "$status" -ne 0 ]
#     [[ "$output" =~ "Error" ]]
# }

# mkdir -p ./tmp/mixed; cp ./tests/testData/parquet/part-1.parquet ./tmp/mixed/file1.parquet; cp ./tests/testData/csv/part-1.csv ./tmp/mixed/file2.csv; ./duck-shard.sh ./tmp/mixed -s merged.ndjson
@test "error: mixing file types for --single-file" {
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
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
    safe_cleanup_output_dir_alternative
	local bogus="$TEST_OUTPUT_DIR/file.bogus"
    echo 'randomdata' > "$bogus"
    run "$SCRIPT_PATH" "$bogus"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Unsupported extension" ]]
}

# ./duck-shard.sh -h
@test "help output mentions GCS and S3" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "GCS HMAC credentials" ]]
    [[ "$output" =~ "S3 HMAC credentials" ]]
}


##### ==== PERFORMANCE CHECK ====

# cd ./tmp && timeout 60s ../duck-shard.sh ../tests/testData/parquet -f ndjson -o ./tmp
@test "performance check on parquet dir" {
    safe_cleanup_output_dir_alternative
	cd "$TEST_OUTPUT_DIR"
    local start_time=$(date +%s)
    run timeout 60s "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    [[ $duration -lt 60 ]]
}



# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --sql ./tests/ex-query.sql -f csv -o ./tmp
@test "parquet file > csv output using --sql script (with semicolon)" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.csv"
    run "$SCRIPT_PATH" "$in_file" --sql "$sql_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
    # Should have headers "event,user_id,time_str" in the first row
    run head -1 "$expected"
    [[ "$output" =~ event ]]
    [[ "$output" =~ user_id ]]
    [[ "$output" =~ time_str ]]
}

# ./duck-shard.sh ./tests/testData/parquet/ --sql ./tests/ex-query-no-semicolon.sql -f json -o ./tmp
@test "parquet file > ndjson output using --sql script (no semicolon)" {
	safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
	# Path to SQL file is ../ex-query.sql from $TEST_DATA_DIR
	local base_sql="$TEST_DATA_DIR/../ex-query.sql"
	local sql_file="$(mktemp "$TEST_OUTPUT_DIR/ex-query-no-semi-XXXXXX.sql")"
	cp "$base_sql" "$sql_file"
	# Remove trailing semicolon (cross-platform: macOS and GNU sed)
	sed -i'' -e 's/;[[:space:]]*$//' "$sql_file"
	local base=$(basename "$in_file" .parquet)
	local expected="$TEST_OUTPUT_DIR/$base.ndjson"
	run "$SCRIPT_PATH" "$in_file" --sql "$sql_file" -f ndjson -o "$TEST_OUTPUT_DIR"
	[ "$status" -eq 0 ]
	file_exists_and_not_empty "$expected"
	run head -1 "$expected"
	[[ "$output" =~ event ]]
	[[ "$output" =~ user_id ]]
	[[ "$output" =~ time_str ]]
	rm -f "$sql_file"

}

##### ==== GCS AND S3 TESTS ====
# Ensure you have GCS_ACCESS_KEY and GCS_SECRET_KEY set in your .env file or environment
# Ensure you have S3_ACCESS_KEY and S3_SECRET_KEY set in your .env file or environment

# ./duck-shard.sh gs://duck-shard/testData/parquet/part-1.parquet -f ndjson -o ./tmp
@test "GCS parquet file > local ndjson output" {
    safe_cleanup_output_dir_alternative
	# Ensure credentials are available in the environment for the script
    [ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_file="gs://duck-shard/testData/parquet/part-1.parquet"
    local base="part-1"
    local expected="$TEST_OUTPUT_DIR/$base.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}
# ./duck-shard.sh gs://duck-shard/testData/parquet/*.parquet -f ndjson -o ./tmp/
@test "GCS parquet dir > local ndjson output for all files" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_dir="gs://duck-shard/testData/parquet/*.parquet"
    run "$SCRIPT_PATH" "$in_dir" -f ndjson -o "$TEST_OUTPUT_DIR/merged.ndjson"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
}


# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f ndjson -o gs://duck-shard/testData/writeHere
@test "local parquet file > ndjson output on GCS" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_file="$TEST_DATA_DIR/parquet/part-1.parquet"
    local out_gcs="gs://duck-shard/testData/writeHere/test-ndjson-1.ndjson"
    run "$SCRIPT_PATH" "$in_file" -f ndjson -o "$out_gcs"
    [ "$status" -eq 0 ]
    # Verify that DuckDB reports successful output to GCS (look for ✅ or output name)
    [[ "$output" == *"$out_gcs"* ]]
}

# ./duck-shard.sh ./tests/testData/parquet -f parquet -o gs://duck-shard/testData/writeHere/
@test "local parquet dir > per-file parquet output on GCS" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_dir="$TEST_DATA_DIR/parquet"
    run "$SCRIPT_PATH" "$in_dir" -f parquet -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"gs://duck-shard/testData/writeHere"* ]]
}

# ./duck-shard.sh gs://duck-shard/testData/parquet/part-1.parquet -f ndjson -o gs://duck-shard/testData/writeHere/
@test "GCS parquet file > GCS ndjson output" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_gcs="gs://duck-shard/testData/parquet/part-1.parquet"
    local out_gcs="gs://duck-shard/testData/writeHere/part-1.ndjson"
    run "$SCRIPT_PATH" "$in_gcs" -f ndjson -o "$out_gcs"
    [ "$status" -eq 0 ]
    [[ "$output" == *"$out_gcs"* ]]
}

# ./duck-shard.sh gs://duck-shard/testData/parquet -s merged-all.ndjson -f ndjson -o gs://duck-shard/testData/writeHere/
@test "GCS parquet dir > single merged ndjson output on GCS" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_gcs_dir="gs://duck-shard/testData/parquet"
    local merged_gcs="gs://duck-shard/testData/writeHere/merged-all.ndjson"
    run "$SCRIPT_PATH" "$in_gcs_dir" -s merged-all.ndjson -f ndjson -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"$merged_gcs"* ]]
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv -f parquet -o gs://duck-shard/testData/writeHere/
@test "local CSV file > GCS parquet output" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    local out_gcs="gs://duck-shard/testData/writeHere/part-1.parquet"
    run "$SCRIPT_PATH" "$in_file" -f parquet -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"$out_gcs"* ]]
}

# ./duck-shard.sh gs://duck-shard/testData/parquet/part-1.parquet -f csv -o ./tmp/
@test "GCS parquet file > local CSV output" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_gcs="gs://duck-shard/testData/parquet/part-1.parquet"
    local expected="$TEST_OUTPUT_DIR/part-1.csv"
    run "$SCRIPT_PATH" "$in_gcs" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh gs://duck-shard/testData/parquet/part-1.parquet --rows 1000 -f ndjson -o gs://duck-shard/testData/writeHere/
@test "GCS parquet file > GCS chunked ndjson output" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_gcs="gs://duck-shard/testData/parquet/part-1.parquet"
    run "$SCRIPT_PATH" "$in_gcs" --rows 1000 -f ndjson -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"gs://duck-shard/testData/writeHere/part-1-"* ]]
}

# ./duck-shard.sh gs://duck-shard/testData/parquet/part-1.parquet --sql ./tests/ex-query.sql -f ndjson -o gs://duck-shard/testData/writeHere/
@test "GCS parquet file > GCS ndjson using --sql" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_gcs="gs://duck-shard/testData/parquet/part-1.parquet"
    local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    local out_gcs="gs://duck-shard/testData/writeHere/part-1.ndjson"
    run "$SCRIPT_PATH" "$in_gcs" --sql "$sql_file" -f ndjson -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"$out_gcs"* ]]
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson -f csv -o gs://duck-shard/testData/writeHere/
@test "local ndjson file > GCS csv output" {
    safe_cleanup_output_dir_alternative
	[ -n "$GCS_KEY_ID" ]
    [ -n "$GCS_SECRET" ]
    local in_file="$TEST_DATA_DIR/ndjson/part-1.ndjson"
    local out_gcs="gs://duck-shard/testData/writeHere/part-1.csv"
    run "$SCRIPT_PATH" "$in_file" -f csv -o "gs://duck-shard/testData/writeHere/"
    [ "$status" -eq 0 ]
    [[ "$output" == *"$out_gcs"* ]]
}

##### ==== FILE NAMING TESTS ====

# ./duck-shard.sh ./tmp/test_file.parquet -f ndjson
@test "single file conversion without -o uses source directory" {
    safe_cleanup_output_dir_alternative
	local temp_file="$TEST_OUTPUT_DIR/test_file.parquet"
    local expected_out="$TEST_OUTPUT_DIR/test_file.ndjson"
    cp "$TEST_DATA_DIR/parquet/part-1.parquet" "$temp_file"

    run "$SCRIPT_PATH" "$temp_file" -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/test_parquet_dir -s -f ndjson
@test "single file merge without -o uses source directory" {
    safe_cleanup_output_dir_alternative
	local temp_dir="$TEST_OUTPUT_DIR/test_parquet_dir"
    local expected_out="$temp_dir/test_parquet_dir_merged.ndjson"
    mkdir -p "$temp_dir"
    cp "$TEST_DATA_DIR/parquet"/*.parquet "$temp_dir/"

    run "$SCRIPT_PATH" "$temp_dir" -s -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/test_parquet_dir -s custom_name.ndjson -f ndjson
@test "single file merge with specific filename without -o uses source directory" {
    safe_cleanup_output_dir_alternative
	local temp_dir="$TEST_OUTPUT_DIR/test_parquet_dir"
    local expected_out="$temp_dir/custom_name.ndjson"
    mkdir -p "$temp_dir"
    cp "$TEST_DATA_DIR/parquet"/*.parquet "$temp_dir/"

    run "$SCRIPT_PATH" "$temp_dir" -s custom_name.ndjson -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/test_file.csv -f csv
@test "prevents overwriting source file when converting to same format in same directory" {
    safe_cleanup_output_dir_alternative
	local temp_file="$TEST_OUTPUT_DIR/test_file.csv"
    cp "$TEST_DATA_DIR/csv/part-1.csv" "$temp_file"

    run "$SCRIPT_PATH" "$temp_file" -f csv
    [ "$status" -eq 1 ]
    [[ "$output" == *"Error: Output file"* ]]
    [[ "$output" == *"would overwrite input file"* ]]
}

# ./duck-shard.sh ./tmp/test_file-1.ndjson -f ndjson -r 2000 (would create test_file-1-1.ndjson, etc.)
@test "split mode creates appropriately named files" {
    safe_cleanup_output_dir_alternative
	local temp_file="$TEST_OUTPUT_DIR/test_file.ndjson"
    cp "$TEST_DATA_DIR/ndjson/part-1.ndjson" "$temp_file"

    run "$SCRIPT_PATH" "$temp_file" -f ndjson -r 2000
    [ "$status" -eq 0 ]
    # Check that split files were created (should be test_file-1.ndjson, test_file-2.ndjson, etc.)
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/test_file-1.ndjson"
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/test_file-2.ndjson"
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv -f csv -o ./tmp/
@test "allows same format conversion when -o is specified" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    local expected_out="$TEST_OUTPUT_DIR/part-1.csv"
    run "$SCRIPT_PATH" "$in_file" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/no_extension.csv -f ndjson (test extension-based output naming)
@test "handles files without dots in name properly" {
    safe_cleanup_output_dir_alternative
	local test_file="$TEST_OUTPUT_DIR/file_no_dots.csv"
    local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    local expected_out="$TEST_OUTPUT_DIR/file_no_dots.ndjson"

    # Copy a test file with no dots in name but proper extension
    cp "$in_file" "$test_file"

    run "$SCRIPT_PATH" "$test_file" -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/data.backup.csv -f ndjson
@test "handles files with multiple dots properly" {
    safe_cleanup_output_dir_alternative
	local test_file="$TEST_OUTPUT_DIR/data.backup.csv"
    local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    local expected_out="$TEST_OUTPUT_DIR/data.backup.ndjson"

    # Copy a test file with multiple dots
    cp "$in_file" "$test_file"

    run "$SCRIPT_PATH" "$test_file" -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/data*special.csv -f ndjson
@test "handles filenames with asterisks properly" {
    safe_cleanup_output_dir_alternative
	local test_file="$TEST_OUTPUT_DIR/data*special.csv"
    local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    local expected_out="$TEST_OUTPUT_DIR/data*special.ndjson"

    # Copy a test file with asterisk in name
    cp "$in_file" "$test_file"

    run "$SCRIPT_PATH" "$test_file" -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

# ./duck-shard.sh ./tmp/subdir -s -f ndjson
@test "directory merge without -o uses directory itself as output location" {
    safe_cleanup_output_dir_alternative
	# Create a temporary subdirectory for this test
    local temp_dir="$TEST_OUTPUT_DIR/subdir"
    mkdir -p "$temp_dir"
    cp "$TEST_DATA_DIR/parquet"/*.parquet "$temp_dir/"

    local expected_out="$temp_dir/subdir_merged.ndjson"
    run "$SCRIPT_PATH" "$temp_dir" -s -f ndjson
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected_out"
}

##### ==== FORMAT CONVERSION MATRIX TESTS ====

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson -f parquet -o ./tmp
@test "ndjson file > parquet output" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local expected="$TEST_OUTPUT_DIR/$base.parquet"
    run "$SCRIPT_PATH" "$in_file" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -f parquet -o ./tmp
@test "parquet file > parquet output (rewrite)" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local expected="$TEST_OUTPUT_DIR/$base.parquet"
    run "$SCRIPT_PATH" "$in_file" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv -f parquet -o ./tmp
@test "csv file > parquet output" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local expected="$TEST_OUTPUT_DIR/$base.parquet"
    run "$SCRIPT_PATH" "$in_file" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$expected"
}

# ./duck-shard.sh ./tests/testData/ndjson -f parquet -o ./tmp
@test "ndjson dir > parquet output for all files" {
    safe_cleanup_output_dir_alternative
	local original_count=$(count_files "$TEST_DATA_DIR/ndjson" "ndjson")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local parquet_count=$(find "$TEST_OUTPUT_DIR" -name "*.parquet" | wc -l)
    [ "$parquet_count" -eq "$original_count" ]
}

# ./duck-shard.sh ./tests/testData/parquet -f csv -o ./tmp
@test "parquet dir > csv output for all files" {
    safe_cleanup_output_dir_alternative
	local original_count=$(count_files "$TEST_DATA_DIR/parquet" "parquet")
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
}

##### ==== COLUMN SELECTION COMBINATIONS ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -c event,time -f csv -o ./tmp
@test "parquet file > csv with column selection" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local columns="event,time"
    run "$SCRIPT_PATH" "$in_file" -c "$columns" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.csv"
    file_exists_and_not_empty "$output_file"
    run head -1 "$output_file"
    [[ "$output" =~ event ]]
    [[ "$output" =~ time ]]
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv -c user_id,event -f ndjson -o ./tmp
@test "csv file > ndjson with column selection" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local columns="user_id,event"
    run "$SCRIPT_PATH" "$in_file" -c "$columns" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.ndjson"
    file_exists_and_not_empty "$output_file"
    run head -1 "$output_file"
    [[ "$output" =~ user_id ]]
    [[ "$output" =~ event ]]
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson -c event,user_id -f parquet -o ./tmp
@test "ndjson file > parquet with column selection" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local columns="event,user_id"
    run "$SCRIPT_PATH" "$in_file" -c "$columns" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.parquet"
    file_exists_and_not_empty "$output_file"
}

##### ==== DEDUPLICATION TESTS ====

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --dedupe -f ndjson -o ./tmp
@test "csv file > ndjson with deduplication" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    run "$SCRIPT_PATH" "$in_file" --dedupe -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.ndjson"
    file_exists_and_not_empty "$output_file"
}

# ./duck-shard.sh ./tests/testData/parquet -c event,user_id --dedupe -f csv -o ./tmp
@test "parquet dir > csv with column selection and deduplication" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -c "event,user_id" --dedupe -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | wc -l)
    [ "$csv_count" -ge 1 ]
    local first_csv=$(find "$TEST_OUTPUT_DIR" -name "*.csv" | head -1)
    file_exists_and_not_empty "$first_csv"
}

# ./duck-shard.sh ./tests/testData/ndjson -s dedupe_all.parquet --dedupe -f parquet -o ./tmp
@test "ndjson dir > merged parquet with deduplication" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -s dedupe_all.parquet --dedupe -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/dedupe_all.parquet"
}

##### ==== CHUNKING TESTS ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --rows 500 -f csv -o ./tmp
@test "parquet file > chunked csv with 500 rows" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    local chunk_size=500
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.csv" | wc -l)
    [ "$chunks_found" -ge 1 ]
    for f in "$TEST_OUTPUT_DIR"/${base}-*.csv; do file_exists_and_not_empty "$f"; done
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --rows 250 -f parquet -o ./tmp
@test "csv file > chunked parquet with 250 rows" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local base=$(basename "$in_file" .csv)
    local chunk_size=250
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.parquet" | wc -l)
    [ "$chunks_found" -ge 1 ]
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson --rows 100 -c event,user_id -f csv -o ./tmp
@test "ndjson file > chunked csv with column selection" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/ndjson" "ndjson")
    local base=$(basename "$in_file" .ndjson)
    local chunk_size=100
    run "$SCRIPT_PATH" "$in_file" --rows "$chunk_size" -c "event,user_id" -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.csv" | wc -l)
    [ "$chunks_found" -ge 1 ]
}

##### ==== SINGLE FILE MERGE TESTS ====

# ./duck-shard.sh ./tests/testData/ndjson -s all.parquet -f parquet -o ./tmp
@test "ndjson dir > merged single parquet file" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -s all.parquet -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/all.parquet"
}

# ./duck-shard.sh ./tests/testData/parquet -s all.ndjson -f ndjson -o ./tmp
@test "parquet dir > merged single ndjson file" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/parquet" -s all.ndjson -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/all.ndjson"
}

# ./duck-shard.sh ./tests/testData/csv -s merged_with_cols.ndjson -c event,user_id -f ndjson -o ./tmp
@test "csv dir > merged ndjson with column selection" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" -s merged_with_cols.ndjson -c "event,user_id" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/merged_with_cols.ndjson"
    run head -1 "$TEST_OUTPUT_DIR/merged_with_cols.ndjson"
    [[ "$output" =~ event ]]
    [[ "$output" =~ user_id ]]
}

##### ==== SQL TRANSFORMATION TESTS ====

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --sql ./ex-query.sql -f parquet -o ./tmp
@test "csv file > parquet with SQL transformation" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/csv" "csv")
    local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    local base=$(basename "$in_file" .csv)
    run "$SCRIPT_PATH" "$in_file" --sql "$sql_file" -f parquet -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.parquet"
    file_exists_and_not_empty "$output_file"
}

# ./duck-shard.sh ./tests/testData/ndjson --sql ./ex-query.sql -s transformed.csv -f csv -o ./tmp
@test "ndjson dir > merged csv with SQL transformation" {
    safe_cleanup_output_dir_alternative
	local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" --sql "$sql_file" -s transformed.csv -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/transformed.csv"
    run head -1 "$TEST_OUTPUT_DIR/transformed.csv"
    [[ "$output" =~ event ]]
    [[ "$output" =~ user_id ]]
    [[ "$output" =~ time_str ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --sql ./ex-query.sql --rows 200 -f ndjson -o ./tmp
@test "parquet file > chunked ndjson with SQL transformation" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    local base=$(basename "$in_file" .parquet)
    run "$SCRIPT_PATH" "$in_file" --sql "$sql_file" --rows 200 -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local chunks_found=$(find "$TEST_OUTPUT_DIR" -name "${base}-*.ndjson" | wc -l)
    [ "$chunks_found" -ge 1 ]
}

##### ==== STRINGIFY TESTS ====

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --stringify -f csv -o ./tmp
@test "parquet file > csv with stringify" {
    safe_cleanup_output_dir_alternative
	local in_file=$(get_first_file "$TEST_DATA_DIR/parquet" "parquet")
    local base=$(basename "$in_file" .parquet)
    run "$SCRIPT_PATH" "$in_file" --stringify -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local output_file="$TEST_OUTPUT_DIR/$base.csv"
    file_exists_and_not_empty "$output_file"
    # Check that complex values are stringified
    run grep "\$organic" "$output_file"
    [ "$status" -eq 0 ]
}

# ./duck-shard.sh ./tests/testData/ndjson -s stringified.csv --stringify -f csv -o ./tmp
@test "ndjson dir > merged csv with stringify" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -s stringified.csv --stringify -f csv -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/stringified.csv"
}

##### ==== HTTP POST TESTS ====

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --url https://eop7f8y0fywsefw.m.pipedream.net -f ndjson -o ./tmp -r 1000
@test "single file > POST to URL" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    run "$SCRIPT_PATH" "$in_file" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f ndjson -o "$TEST_OUTPUT_DIR" -r 1000
    [ "$status" -eq 0 ]
    # Should create multiple batch files and post each one
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
    [[ "$output" == *"https://eop7f8y0fywsefw.m.pipedream.net"* ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --url https://eop7f8y0fywsefw.m.pipedream.net -f csv -r 500 -o ./tmp
@test "parquet file > csv POST to URL" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/parquet/part-1.parquet"
    run "$SCRIPT_PATH" "$in_file" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f csv -r 500 -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.csv" | wc -l)
    [ "$csv_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/ndjson/part-1.ndjson --url https://eop7f8y0fywsefw.m.pipedream.net -f parquet -r 300 -o ./tmp
@test "ndjson file > parquet POST to URL" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/ndjson/part-1.ndjson"
    run "$SCRIPT_PATH" "$in_file" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f parquet -r 300 -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local parquet_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.parquet" | wc -l)
    [ "$parquet_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv/part-1.csv --url https://eop7f8y0fywsefw.m.pipedream.net --header "X-Custom: test" -f ndjson -o ./tmp -r 1000
@test "single file > POST to URL with custom header" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/csv/part-1.csv"
    run "$SCRIPT_PATH" "$in_file" --url "https://eop7f8y0fywsefw.m.pipedream.net" --header "X-Custom: test" -f ndjson -o "$TEST_OUTPUT_DIR" -r 1000
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv --url https://eop7f8y0fywsefw.m.pipedream.net -r 500 -f ndjson -o ./tmp
@test "directory > batched POST to URL" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" --url "https://eop7f8y0fywsefw.m.pipedream.net" -r 500 -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    # Should create multiple batch files and post each one
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv -s merged.ndjson --url https://eop7f8y0fywsefw.m.pipedream.net -f ndjson -o ./tmp
@test "merged single file > POST to URL" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" -s merged.ndjson --url "https://eop7f8y0fywsefw.m.pipedream.net" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/merged.ndjson"
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet -c event,user_id --url https://eop7f8y0fywsefw.m.pipedream.net -f ndjson -r 400 -o ./tmp
@test "parquet file > POST to URL with column selection" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/parquet/part-1.parquet"
    run "$SCRIPT_PATH" "$in_file" -c "event,user_id" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f ndjson -r 400 -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv --url https://eop7f8y0fywsefw.m.pipedream.net --header "Authorization: Bearer token123" --header "X-Source: duck-shard" -f ndjson -o ./tmp
@test "directory > POST to URL with multiple headers" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" --url "https://eop7f8y0fywsefw.m.pipedream.net" --header "Authorization: Bearer token123" --header "X-Source: duck-shard" -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --sql ./ex-query.sql --url https://eop7f8y0fywsefw.m.pipedream.net -f ndjson -o ./tmp -r 1000
@test "SQL transform > POST to URL" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/parquet/part-1.parquet"
    local sql_file="$PROJECT_ROOT/tests/ex-query.sql"
    run "$SCRIPT_PATH" "$in_file" --sql "$sql_file" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f ndjson -o "$TEST_OUTPUT_DIR" -r 1000
    [ "$status" -eq 0 ]
    local ndjson_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.ndjson" | wc -l)
    [ "$ndjson_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/ndjson -c event,user_id --dedupe --url https://eop7f8y0fywsefw.m.pipedream.net -s deduped.ndjson -f ndjson -o ./tmp
@test "ndjson dir > POST to URL with deduplication and column selection" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/ndjson" -c "event,user_id" --dedupe --url "https://eop7f8y0fywsefw.m.pipedream.net" -s deduped.ndjson -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    file_exists_and_not_empty "$TEST_OUTPUT_DIR/deduped.ndjson"
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv --url https://eop7f8y0fywsefw.m.pipedream.net --log -r 1000 -f ndjson -o ./tmp
@test "POST with logging creates response-logs.json" {
    safe_cleanup_output_dir_alternative
	rm -f response-logs.json  # Clean up any existing log
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" --url "https://eop7f8y0fywsefw.m.pipedream.net" --log -r 1000 -f ndjson -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    [[ "$output" == *"✅ Posted"* ]]
    [ -f "response-logs.json" ]
    # Check that log contains JSON
    run jq length response-logs.json
    [ "$status" -eq 0 ]
}

# ./duck-shard.sh ./tests/testData/parquet/part-1.parquet --stringify --url https://eop7f8y0fywsefw.m.pipedream.net -f csv -r 600 -o ./tmp
@test "parquet file > POST to URL with stringify" {
    safe_cleanup_output_dir_alternative
	local in_file="$TEST_DATA_DIR/parquet/part-1.parquet"
    run "$SCRIPT_PATH" "$in_file" --stringify --url "https://eop7f8y0fywsefw.m.pipedream.net" -f csv -r 600 -o "$TEST_OUTPUT_DIR"
    [ "$status" -eq 0 ]
    local csv_count=$(find "$TEST_OUTPUT_DIR" -name "part-1-*.csv" | wc -l)
    [ "$csv_count" -ge 1 ]
    [[ "$output" == *"✅ Posted"* ]]
}

# ./duck-shard.sh ./tests/testData/csv --url https://invalid-domain-that-should-fail.nonexistent -f ndjson -o ./tmp
# @test "error: invalid URL should fail with retries" {
safe_cleanup_output_dir_alternative
#     run timeout 60s "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" --url "https://invalid-domain-that-should-fail.nonexistent" -f ndjson -o "$TEST_OUTPUT_DIR"
#     [ "$status" -eq 0 ]  # File conversion should succeed
#     # But POST should fail and show retry attempts
#     [[ "$output" == *"❌"* ]]
#     [[ "$output" == *"client error, not retrying"* || "$output" == *"Network error"* ]]
# }

# ./duck-shard.sh ./tests/testData/csv --url https://eop7f8y0fywsefw.m.pipedream.net -f ndjson -o gs://bucket/
@test "error: --url with cloud storage output should fail" {
    safe_cleanup_output_dir_alternative
	run "$SCRIPT_PATH" "$TEST_DATA_DIR/csv" --url "https://eop7f8y0fywsefw.m.pipedream.net" -f ndjson -o "gs://bucket/"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Error: --url cannot be used with cloud storage output directories"* ]]
}

##### ==== CLEANUP ====

# Global cleanup to remove test data from GCS writeHere directory
# @test "cleanup: remove test data from GCS writeHere directory" {
#     # Only run cleanup if we have GCS credentials
#     if [[ -n "${GCS_KEY_ID:-}" && -n "${GCS_SECRET:-}" ]]; then
#         # Clean up the writeHere directory, but don't fail if it doesn't work
#         gsutil -m rm -rf gs://duck-shard/testData/writeHere/* 2>/dev/null || true
#         echo "✅ GCS writeHere directory cleanup completed (or skipped if empty)"
#     else
#         echo "⏭️  Skipping GCS cleanup - no credentials available"
#     fi
# }
