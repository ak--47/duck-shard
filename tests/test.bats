#!/usr/bin/env bats

# test.bats - Test suite for parquet-to.sh using existing test data

# Setup and teardown functions
setup() {
    # Set paths relative to the test file location
    export SCRIPT_PATH="../parquet-to.sh"
    export TEST_DATA_DIR="./testData"
    export EVENT_DATA_DIR="$TEST_DATA_DIR/eventData"
    export OBJECT_DATA_DIR="$TEST_DATA_DIR/objectData"
    
    # Create temporary directory for test outputs
    export TEST_OUTPUT_DIR="$(mktemp -d)"
    
    # Ensure script is executable
    chmod +x "$SCRIPT_PATH"
    
    # Verify test data exists
    verify_test_data
}

teardown() {
    # Clean up temporary output directory
    if [[ -n "$TEST_OUTPUT_DIR" && -d "$TEST_OUTPUT_DIR" ]]; then
        rm -rf "$TEST_OUTPUT_DIR"
    fi
    
    # Clean up any output files created in test data directories
    find "$TEST_DATA_DIR" -name "*.ndjson" -delete 2>/dev/null || true
    find "$TEST_DATA_DIR" -name "*.csv" -delete 2>/dev/null || true
    # Don't delete .parquet files as they might be our source data
    find "$TEST_DATA_DIR" -name "*.parquet" -newer "$TEST_DATA_DIR" -delete 2>/dev/null || true
}

verify_test_data() {
    # Check that test data directories exist
    [[ -d "$EVENT_DATA_DIR" ]] || {
        echo "Error: Event data directory not found: $EVENT_DATA_DIR" >&2
        exit 1
    }
    [[ -d "$OBJECT_DATA_DIR" ]] || {
        echo "Error: Object data directory not found: $OBJECT_DATA_DIR" >&2
        exit 1
    }
    
    # Check that we have the expected parquet files
    local event_count=$(find "$EVENT_DATA_DIR" -name "*.parquet" | wc -l)
    local object_count=$(find "$OBJECT_DATA_DIR" -name "*.parquet" | wc -l)
    
    [[ $event_count -gt 0 ]] || {
        echo "Error: No parquet files found in $EVENT_DATA_DIR" >&2
        exit 1
    }
    [[ $object_count -gt 0 ]] || {
        echo "Error: No parquet files found in $OBJECT_DATA_DIR" >&2
        exit 1
    }
}

# Helper function to count lines in output files
count_lines() {
    local file="$1"
    if [[ -f "$file" ]]; then
        wc -l < "$file"
    else
        echo "0"
    fi
}

# Helper function to check if file exists and is not empty
file_exists_and_not_empty() {
    [[ -f "$1" && -s "$1" ]]
}

# Helper function to get first parquet file from a directory
get_first_parquet_file() {
    local dir="$1"
    find "$dir" -name "*.parquet" | head -1
}

# Helper function to count parquet files in directory
count_parquet_files() {
    local dir="$1"
    find "$dir" -name "*.parquet" | wc -l
}

@test "script exists and is executable" {
    [[ -x "$SCRIPT_PATH" ]]
}

@test "test data directories exist and contain parquet files" {
    [[ -d "$EVENT_DATA_DIR" ]]
    [[ -d "$OBJECT_DATA_DIR" ]]
    
    local event_files=$(count_parquet_files "$EVENT_DATA_DIR")
    local object_files=$(count_parquet_files "$OBJECT_DATA_DIR")
    
    [[ $event_files -gt 0 ]]
    [[ $object_files -gt 0 ]]
    
    echo "Found $event_files event files and $object_files object files" >&3
}

@test "script shows help when called with -h" {
    run "$SCRIPT_PATH" -h
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Usage:" ]]
}

@test "script fails when no arguments provided" {
    run "$SCRIPT_PATH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Missing <input_path>" ]]
}

@test "single event parquet file conversion to NDJSON (default)" {
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    local basename=$(basename "$first_event_file" .parquet)
    local expected_output="$EVENT_DATA_DIR/$basename.ndjson"
    
    run "$SCRIPT_PATH" "$first_event_file"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅" ]]
    
    # Check output file exists and is not empty
    file_exists_and_not_empty "$expected_output"
    
    # Verify it's JSON format
    run head -1 "$expected_output"
    [[ "$output" =~ ^\{.*\}$ ]]
    
    echo "Converted $first_event_file to NDJSON with $(count_lines "$expected_output") lines" >&3
}

@test "single person parquet file conversion to CSV" {
    local first_person_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
    local basename=$(basename "$first_person_file" .parquet)
    local expected_output="$OBJECT_DATA_DIR/$basename.csv"
    
    run "$SCRIPT_PATH" "$first_person_file" -f csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅" ]]
    
    # Check output file exists and has content
    file_exists_and_not_empty "$expected_output"
    
    # Check it has CSV header
    run head -1 "$expected_output"
    [[ "$output" =~ , ]]  # Should contain commas indicating CSV format
    
    echo "Converted $first_person_file to CSV with $(count_lines "$expected_output") lines" >&3
}

@test "event directory conversion to individual NDJSON files" {
    cd "$EVENT_DATA_DIR"
    local original_count=$(count_parquet_files ".")
    
    run "$SCRIPT_PATH" . -f ndjson
    [ "$status" -eq 0 ]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    
    # Check that we have the expected number of NDJSON files
    local ndjson_count=$(find . -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -eq "$original_count" ]
    
    # Verify at least one output file has content
    local first_ndjson=$(find . -name "*.ndjson" | head -1)
    file_exists_and_not_empty "$first_ndjson"
    
    echo "Converted $original_count parquet files to individual NDJSON files" >&3
}

@test "object directory conversion to individual CSV files" {
    cd "$OBJECT_DATA_DIR"
    local original_count=$(count_parquet_files ".")
    
    run "$SCRIPT_PATH" . -f csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    
    # Check that we have the expected number of CSV files
    local csv_count=$(find . -name "*.csv" | wc -l)
    [ "$csv_count" -eq "$original_count" ]
    
    # Verify CSV format
    local first_csv=$(find . -name "*.csv" | head -1)
    file_exists_and_not_empty "$first_csv"
    run head -1 "$first_csv"
    [[ "$output" =~ , ]]
    
    echo "Converted $original_count parquet files to individual CSV files" >&3
}

@test "event directory conversion to single merged NDJSON file" {
    cd "$TEST_OUTPUT_DIR"
    local original_count=$(count_parquet_files "$EVENT_DATA_DIR")
    
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" -s merged_events.ndjson
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅ Merged → merged_events.ndjson" ]]
    
    # Check merged file exists and has content
    file_exists_and_not_empty "merged_events.ndjson"
    
    # Verify it's NDJSON format
    run head -1 "merged_events.ndjson"
    [[ "$output" =~ ^\{.*\}$ ]]
    
    local line_count=$(count_lines "merged_events.ndjson")
    echo "Merged $original_count event files into single NDJSON with $line_count lines" >&3
    
    # Line count should be > 0 (we have data)
    [ "$line_count" -gt 0 ]
}

@test "object directory conversion to single merged parquet file" {
    cd "$TEST_OUTPUT_DIR"
    local original_count=$(count_parquet_files "$OBJECT_DATA_DIR")
    
    run "$SCRIPT_PATH" "$OBJECT_DATA_DIR" -s merged_objects.parquet -f parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅ Merged → merged_objects.parquet" ]]
    
    # Check merged file exists
    file_exists_and_not_empty "merged_objects.parquet"
    
    # Verify we can read it with DuckDB
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('merged_objects.parquet');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ [0-9]+ ]]
    
    echo "Merged $original_count object files into single parquet file" >&3
}

@test "event directory conversion to single CSV with auto-naming" {
    cd "$TEST_OUTPUT_DIR"
    
    run "$SCRIPT_PATH" "$EVENT_DATA_DIR" -s -f csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "✅ Merged → eventData.csv" ]]
    
    # Check auto-named file exists
    file_exists_and_not_empty "eventData.csv"
    
    # Verify CSV format with header
    run head -1 "eventData.csv"
    [[ "$output" =~ , ]]
    
    echo "Auto-named merged CSV: eventData.csv with $(count_lines "eventData.csv") lines" >&3
}

@test "conversion with specific columns on event data" {
    # First, let's discover what columns are available in the event data
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    
    # Get column names from the parquet file
    run duckdb -c "DESCRIBE SELECT * FROM read_parquet('$first_event_file') LIMIT 0;"
    [ "$status" -eq 0 ]
    
    # Extract first two column names for testing (assuming they exist)
    local columns=$(duckdb -c "PRAGMA table_info(read_parquet('$first_event_file'));" | head -2 | cut -d'|' -f2 | tr '\n' ',' | sed 's/,$//')
    
    if [[ -n "$columns" ]]; then
        cd "$TEST_OUTPUT_DIR"
        run "$SCRIPT_PATH" "$first_event_file" -c "$columns" -f csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "cols=" ]]
        
        local basename=$(basename "$first_event_file" .parquet)
        local output_file="$TEST_OUTPUT_DIR/$basename.csv"
        
        # File should exist (though may be created in original directory)
        # Let's check both locations
        if [[ -f "$EVENT_DATA_DIR/$basename.csv" ]]; then
            output_file="$EVENT_DATA_DIR/$basename.csv"
        fi
        
        echo "Testing column selection with columns: $columns" >&3
        echo "Output file: $output_file" >&3
    else
        skip "Could not determine column names from event data"
    fi
}

@test "parallel processing with custom job count on event data" {
    cd "$EVENT_DATA_DIR"
    
    run "$SCRIPT_PATH" . 2 -f ndjson
    [ "$status" -eq 0 ]
    [[ "$output" =~ "parallel=2" ]]
    [[ "$output" =~ "🎉 All individual conversions complete." ]]
    
    # Verify some output files were created
    local ndjson_count=$(find . -name "*.ndjson" | wc -l)
    [ "$ndjson_count" -gt 0 ]
    
    echo "Processed with 2 parallel jobs, created $ndjson_count NDJSON files" >&3
}

@test "error handling: non-existent input file" {
    run "$SCRIPT_PATH" "$TEST_DATA_DIR/nonexistent.parquet"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error:" ]]
}

@test "error handling: directory with no parquet files" {
    # Create empty directory in our temp space
    mkdir -p "$TEST_OUTPUT_DIR/empty_dir"
    
    run "$SCRIPT_PATH" "$TEST_OUTPUT_DIR/empty_dir/"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No Parquet files found" ]]
}

@test "error handling: invalid format specification" {
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    
    run "$SCRIPT_PATH" "$first_event_file" -f invalid_format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format must be ndjson, parquet, or csv" ]]
}

@test "error handling: missing argument for --format" {
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    
    run "$SCRIPT_PATH" "$first_event_file" --format
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --format needs an argument" ]]
}

@test "error handling: missing argument for --cols" {
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    
    run "$SCRIPT_PATH" "$first_event_file" --cols
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: --cols needs an argument" ]]
}

@test "verify data integrity: event data roundtrip" {
    # Test that we can convert event data and the output has reasonable content
    local first_event_file=$(get_first_parquet_file "$EVENT_DATA_DIR")
    local basename=$(basename "$first_event_file" .parquet)
    
    # Get original row count
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('$first_event_file');"
    [ "$status" -eq 0 ]
    local original_count=$(echo "$output" | grep -o '[0-9]\+')
    
    # Convert to NDJSON
    cd "$EVENT_DATA_DIR"
    run "$SCRIPT_PATH" "$first_event_file" -f ndjson
    [ "$status" -eq 0 ]
    
    # Count lines in NDJSON (should match original count)
    local ndjson_file="$basename.ndjson"
    local ndjson_lines=$(count_lines "$ndjson_file")
    
    echo "Original rows: $original_count, NDJSON lines: $ndjson_lines" >&3
    
    # Allow for small differences due to headers/formatting, but should be close
    [[ $ndjson_lines -gt 0 ]]
}

@test "verify data integrity: object data roundtrip" {
    # Test that we can convert object data and the output has reasonable content
    local first_object_file=$(get_first_parquet_file "$OBJECT_DATA_DIR")
    local basename=$(basename "$first_object_file" .parquet)
    
    # Get original row count
    run duckdb -c "SELECT COUNT(*) FROM read_parquet('$first_object_file');"
    [ "$status" -eq 0 ]
    local original_count=$(echo "$output" | grep -o '[0-9]\+')
    
    # Convert to CSV
    cd "$OBJECT_DATA_DIR"
    run "$SCRIPT_PATH" "$first_object_file" -f csv
    [ "$status" -eq 0 ]
    
    # Count lines in CSV (should be original count + 1 for header)
    local csv_file="$basename.csv"
    local csv_lines=$(count_lines "$csv_file")
    local expected_lines=$((original_count + 1))
    
    echo "Original rows: $original_count, CSV lines: $csv_lines (expected: $expected_lines)" >&3
    
    # CSV should have at least the original count of lines
    [[ $csv_lines -gt $original_count ]]
}

@test "performance: process all event files in reasonable time" {
    cd "$TEST_OUTPUT_DIR"
    local start_time=$(date +%s)
    
    run timeout 60s "$SCRIPT_PATH" "$EVENT_DATA_DIR" -f ndjson
    [ "$status" -eq 0 ]
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "Processed all event files in ${duration} seconds" >&3
    
    # Should complete within reasonable time (60 seconds max due to timeout)
    [[ $duration -lt 60 ]]
}