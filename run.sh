#!/bin/bash
set -e

# Set log level
export RUST_LOG=debug

# Build the project
echo "Building the project..."
cargo build --release

# Create test directories
TEST_DIR="./test_data"
MERGED_DIR="$TEST_DIR/merged"
SPLIT_DIR="$TEST_DIR/split"
mkdir -p "$TEST_DIR" "$MERGED_DIR" "$SPLIT_DIR"

# Function to print time
print_time() {
    local duration=$1
    local operation=$2
    echo -e "\\n=== $operation completed in $(printf '%.2f' $duration)s ===\\n"
}

# Function to generate test CSV files
generate_test_csv() {
    local filename=$1
    local rows=$2
    echo "name,age,city" > "$filename"

    for i in $(seq 1 "$rows"); do
        echo "User$i,$((20 + RANDOM % 50)),City$((RANDOM % 5))" >> "$filename"
    done
    echo "Generated test file: $filename"
}

# Generate test files
echo -e "\n=== Generating test files ==="
generate_test_csv "$TEST_DIR/file1.csv" 10000
generate_test_csv "$TEST_DIR/file2.csv" 15000

# Test 1: Merge two CSV files
echo -e "\n=== Testing CSV Merge ==="
start_time=$(date +%s.%N)
./target/release/split_merge_hub_demo merge "$MERGED_DIR/merged.csv" -s "name,age" "$TEST_DIR/file1.csv" "$TEST_DIR/file2.csv"
end_time=$(date +%s.%N)
print_time $(echo "$end_time - $start_time" | bc) "Merge"

# Test 2: Split a CSV file
echo -e "\n=== Testing CSV Split ==="
start_time=$(date +%s.%N)
./target/release/split_merge_hub_demo split "$MERGED_DIR/merged.csv" "$SPLIT_DIR" -r 1000 -s "age,name"
end_time=$(date +%s.%N)
print_time $(echo "$end_time - $start_time" | bc) "Split"

# Test 3: Parallel processing
echo -e "\n=== Testing Parallel Processing ==="
start_time=$(date +%s.%N)
./target/release/split_merge_hub_demo merge "$MERGED_DIR/parallel_merged.csv" -s "name" "$TEST_DIR/file1.csv" "$TEST_DIR/file2.csv" --workers 4
end_time=$(date +%s.%N)
print_time $(echo "$end_time - $start_time" | bc) "Parallel Merge"

echo -e "\n=== Test Summary ==="
echo "1. Merged files: $MERGED_DIR/merged.csv"
echo "2. Split files: $SPLIT_DIR/"
echo "3. Parallel merge: $MERGED_DIR/parallel_merged.csv"
echo -e "\nAll tests completed successfully!"
