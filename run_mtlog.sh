#!/bin/bash
set -euo pipefail

# Increase file descriptor limit if possible
ulimit -n 8192 2>/dev/null || true

# Configuration
INPUT_DIR="large_files"
OUTPUT_DIR="merge_files"
OUTPUT_FILE="${OUTPUT_DIR}/merged_mtlog"
RAYON_NUM_THREADS="8"
MERGE_K="12"  # Default k-way merge factor; adjust as needed for your hardware
# Set log level (can be overridden by environment variable)
export RUST_LOG_STYLE="always"
export RUST_LOG="debug"
export MERGE_K
export MERGE_BUF_MB=512
export MERGE_PARALLEL_GROUPS=4

export CHUNK_RECORDS=500000
# Set sort columns for MT log (date, time)
MTLOG_SORT_COLS="0:date,1:time"

# Ensure input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo " Error: Input directory not found: $INPUT_DIR"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Create a temporary file for storing the list of input files
FILE_LIST=$(mktemp)

# Find all mt_ files in the input directory (filter only mt_*)
find "$INPUT_DIR" -type f -name 'mt_*' | sort > "$FILE_LIST"

# Read the files into an array
INPUT_FILES=()
while IFS= read -r file; do
    INPUT_FILES+=("$file")
done < "$FILE_LIST"

# Clean up the temporary file
rm -f "$FILE_LIST"

# Check if we found any input files
if [ ${#INPUT_FILES[@]} -eq 0 ]; then
    echo " Error: No mt_log files found in $INPUT_DIR/mt_log*"
    exit 1
fi

echo "=== Starting MT Log Merge Process ==="
echo "Found ${#INPUT_FILES[@]} mt_log files to process"

# Display input files with sizes
for file in "${INPUT_FILES[@]}"; do
    if [ -f "$file" ]; then
        if command -v du &> /dev/null; then
            size=$(du -h "$file" | cut -f1)
            echo " $file (${size})"
        else
            echo " $file"
        fi
    fi
done

echo ""
echo "Merging files into $OUTPUT_FILE (MT log mode) ..."

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

echo ""
echo "Merging files into $OUTPUT_FILE (MT log mode) ..."
START_TIME=$(date +%s)

# Merge using --mt-log parameter
cargo run --bin split_merge_hub_demo --release -- merge "${INPUT_FILES[@]}" -o "$OUTPUT_FILE" --mt-log --mtlog-sort-cols "$MTLOG_SORT_COLS"
MERGE_STATUS=$?

END_TIME=$(date +%s)
DURATION_SEC=$((END_TIME - START_TIME))
DURATION_MIN=$(awk "BEGIN {printf \"%.2f\", $DURATION_SEC/60}")

if [ $MERGE_STATUS -ne 0 ]; then
    echo " MT Log merge failed"
    exit $MERGE_STATUS
fi

echo " MT Log merge completed: $OUTPUT_FILE"

# Get file size in human-readable format
if command -v du &> /dev/null; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" 2>/dev/null | cut -f1 || echo "unknown")
fi

# Format number with comma
format_number() {
    if command -v numfmt &> /dev/null; then
        numfmt --grouping "$1"
    else
        printf "%'d\n" "$1"
    fi
}

# Count number of rows (lines) in output file (by new line)
LINE_COUNT=$(wc -l < "$OUTPUT_FILE")
LINE_COUNT_FMT=$(format_number "$LINE_COUNT")

# Format total input size (bytes) with comma
TOTAL_INPUT_SIZE_FMT=$(format_number "$TOTAL_INPUT_SIZE")

# Display summary
cat <<EOF
==== Merge Summary ====
   Output file: $OUTPUT_FILE
   File size:  $FILE_SIZE
   Total records:  $LINE_COUNT_FMT
   Total input size: $TOTAL_INPUT_SIZE_FMT bytes
   Duration: ${DURATION_SEC}s (${DURATION_MIN} min)
EOF

if [ ! -s "$OUTPUT_FILE" ]; then
    echo "⚠️  Warning: Output file is empty"
fi
