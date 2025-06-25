#!/bin/bash
set -euo pipefail

# Increase file descriptor limit if possible
ulimit -n 8192 2>/dev/null || true

# Configuration
INPUT_DIR="large_files"
OUTPUT_DIR="merge_files"
OUTPUT_FILE="${OUTPUT_DIR}/merged_accounts.csv"
SORT_BY="account_no"  # Default sort column
RAYON_NUM_THREADS="8"
MERGE_K="12"  # Default k-way merge factor; adjust as needed for your hardware
# Set log level (can be overridden by environment variable)
export RUST_LOG_STYLE="always"
export RUST_LOG="debug"
export MERGE_K
export MERGE_BUF_MB=512
export MERGE_PARALLEL_GROUPS=4

# Ensure input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "❌ Error: Input directory not found: $INPUT_DIR"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Create a temporary file for storing the list of input files
FILE_LIST=$(mktemp)

# Find all account files in the input directory
# This will match files like account1.csv, account2.csv, etc.
find "$INPUT_DIR" -name 'account*.csv' | sort > "$FILE_LIST"

# Read the files into an array
INPUT_FILES=()
while IFS= read -r file; do
    INPUT_FILES+=("$file")
done < "$FILE_LIST"

# Clean up the temporary file
rm -f "$FILE_LIST"

# Check if we found any input files
if [ ${#INPUT_FILES[@]} -eq 0 ]; then
    echo "❌ Error: No account files found in $INPUT_DIR/account*.csv"
    exit 1
fi

echo "=== Starting CSV Merge Process ==="
echo "Found ${#INPUT_FILES[@]} account files to process"

# Display input files with sizes
for file in "${INPUT_FILES[@]}"; do
    if [ -f "$file" ]; then
        if command -v du &> /dev/null; then
            size=$(du -h "$file" | cut -f1)
            echo "📄 $file (${size})"
        else
            echo "📄 $file"
        fi
    else
        echo "⚠️  Warning: File not found: $file"
    fi
done

# Build the project in release mode for better performance
echo -e "\n🔨 Building the project in release mode with optimizations..."
RUSTFLAGS="-C target-cpu=native" cargo build --release --bin split_merge_hub_demo

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "❌ Error: Build failed"
    exit 1
fi

# Check available memory and adjust chunk size if needed
TOTAL_MEM_MB=$(($(sysctl -n hw.memsize 2>/dev/null || echo 8589934592) / 1048576))  # Default to 8GB if can't detect

# Set default chunk size
CHUNK_SIZE_MB=256

# Adjust chunk size based on total RAM
if [ "$TOTAL_MEM_MB" -lt 8192 ]; then
    CHUNK_SIZE_MB=128
elif [ "$TOTAL_MEM_MB" -ge 32768 ]; then
    CHUNK_SIZE_MB=2048
elif [ "$TOTAL_MEM_MB" -ge 16384 ]; then
    CHUNK_SIZE_MB=1024
fi

echo "   Detected ${TOTAL_MEM_MB}MB of system memory"
export CHUNK_SIZE_MB

# Prepare the command with optimized settings
CMD=(
    "./target/release/split_merge_hub_demo"
    "merge"
    "--output" "$OUTPUT_FILE"
    "--chunk-size" "$CHUNK_SIZE_MB"
)

# Add sort option if specified
if [ -n "$SORT_BY" ]; then
    CMD+=("--sort-by" "$SORT_BY")
fi

# Add input files
for file in "${INPUT_FILES[@]}"; do
    CMD+=("$file")
done

# Calculate total input size
TOTAL_INPUT_SIZE=0
for file in "${INPUT_FILES[@]}"; do
    if [ -f "$file" ]; then
        SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
        TOTAL_INPUT_SIZE=$((TOTAL_INPUT_SIZE + SIZE))
    fi
done

# Convert to human-readable format
if [ $TOTAL_INPUT_SIZE -gt 0 ]; then
    if command -v numfmt &> /dev/null; then
        HUMAN_SIZE=$(numfmt --to=iec --suffix=B $TOTAL_INPUT_SIZE)
    else
        HUMAN_SIZE="${TOTAL_INPUT_SIZE} bytes"
    fi
    echo "   Total input size: $HUMAN_SIZE"
fi

echo -e "\n🔄 Starting merge process..."
echo "   Output file: ${OUTPUT_FILE}"
echo "   Sort by: ${SORT_BY}"

# Record start time
START_TIME=$(date +%s)

# Run the merge command
echo -e "Running: ${CMD[*]}\n"
"${CMD[@]}"

# Check if merge was successful
if [ $? -ne 0 ]; then
    echo -e "\n❌ Error: Merge failed"
    exit 1
fi

# Calculate and display duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Get file size in human-readable format
if command -v du &> /dev/null; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" 2>/dev/null | cut -f1 || echo "unknown")
    # Get line count if available
    if command -v wc &> /dev/null; then
        LINE_COUNT=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo "unknown")
        # Subtract 1 for header if file is not empty
        if [ "$LINE_COUNT" != "unknown" ] && [ "$LINE_COUNT" -gt 0 ]; then
            LINE_COUNT=$((LINE_COUNT - 1))
        fi
    fi
else
    FILE_SIZE="unknown"
    LINE_COUNT="unknown"
fi

# Display summary
echo -e "\n✅ Merge completed successfully!"
echo "   Output file: ${OUTPUT_FILE}"

if [ "$FILE_SIZE" != "unknown" ]; then
    echo "   File size: ${FILE_SIZE}"

    # Check if output file was created and has content
    if [ ! -s "$OUTPUT_FILE" ]; then
        echo "⚠️  Warning: Output file is empty"
    fi
fi

if [ "$LINE_COUNT" != "unknown" ]; then
    echo "   Total records: ${LINE_COUNT}"
fi

# Remove problematic arithmetic expansion with possible empty variables
# Fix DURATION/0 or unset errors
if [ -n "${DURATION:-}" ] && [ "$DURATION" -gt 0 ] && [ -n "${TOTAL_INPUT_SIZE:-}" ] && [ "$TOTAL_INPUT_SIZE" -gt 0 ]; then
    MB_PER_SEC=$((TOTAL_INPUT_SIZE / DURATION / 1048576))
    echo "   Processing speed: ~${MB_PER_SEC} MB/s"
fi

if [ -n "${DURATION:-}" ] && [ "$DURATION" -ge 0 ]; then
    MIN=$((DURATION / 60))
    SEC=$((DURATION % 60))
    echo "   Time taken: ${DURATION} seconds (${MIN}m${SEC}s)"
fi

echo -e "\n=== Process completed ==="