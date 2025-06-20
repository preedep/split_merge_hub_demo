#!/bin/bash
set -e

# Configuration
INPUT_DIR="large_files"
OUTPUT_DIR="merge_files"
OUTPUT_FILE="${OUTPUT_DIR}/merged_accounts.csv"
SORT_BY="account_no"
INPUT_FILES=("${INPUT_DIR}/accounts_1.csv" "${INPUT_DIR}/accounts_2.csv")

# Set log level
export RUST_LOG=info

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

echo "=== Starting CSV Merge Process ==="

# Check if input files exist
for file in "${INPUT_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Error: Input file not found: $file"
        exit 1
    fi
    echo "✅ Found input file: $file"
done

# Build the project if not already built
if [ ! -f "./target/release/split_merge_hub_demo" ]; then
    echo "\n🔨 Building the project (this may take a minute)..."
    cargo build --release --bin split_merge_hub_demo
fi

echo "\n🔄 Starting merge process..."
echo "   Input files: ${INPUT_FILES[*]}"
echo "   Output file: ${OUTPUT_FILE}"
echo "   Sort by: ${SORT_BY}"

# Record start time
START_TIME=$(date +%s)

# Run the merge command
set +e
./target/release/split_merge_hub_demo merge \
    "$OUTPUT_FILE" \
    --sort-by "$SORT_BY" \
    "${INPUT_FILES[@]}"

# Check if merge was successful
if [ $? -ne 0 ]; then
    echo "\n❌ Error: Merge failed"
    exit 1
fi
set -e

# Calculate and display duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Get file size in human-readable format
if command -v du &> /dev/null; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
else
    # Fallback if du is not available
    FILE_SIZE="unknown size"
fi

echo "\n✅ Merge completed successfully!"
echo "   Output file: ${OUTPUT_FILE}"
echo "   File size: ${FILE_SIZE}"
echo "   Time taken: ${DURATION} seconds"
echo "\n=== Process completed ===\n"

