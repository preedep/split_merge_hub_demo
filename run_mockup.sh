#!/bin/bash

# Set environment variables
export RUST_LOG=info

# Function to display usage information
show_help() {
    echo "Mockup Runner for Split/Merge Hub"
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  split     Split a large file into smaller chunks"
    echo "  merge     Merge multiple files into one"
    echo "  demo      Run a complete demo (split then merge)"
    echo "  help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 split --input large_file.csv --output chunks --chunk-size 1000"
    echo "  $0 merge --output merged.csv --input chunks/*.csv --sort-by account_no"
    echo "  $0 demo"
}

# Function to run the split command
run_split() {
    local input=""
    local output=""
    local chunk_size=1000
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --input)
                input="$2"
                shift 2
                ;;
            --output)
                output="$2"
                shift 2
                ;;
            --chunk-size)
                chunk_size="$2"
                shift 2
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Validate arguments
    if [[ -z "$input" || -z "$output" ]]; then
        echo "Error: Both --input and --output are required for split command"
        show_help
        exit 1
    fi
    
    echo "🚀 Starting mock file split"
    echo "Input: $input"
    echo "Output directory: $output"
    echo "Records per chunk: $chunk_size"
    
    cargo run --bin mock_split_merge_hub split "$input" "$output" --chunk-size "$chunk_size"
}

# Function to run the merge command
run_merge() {
    local output=""
    local inputs=()
    local sort_by=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --output)
                output="$2"
                shift 2
                ;;
            --input)
                shift
                # Collect all remaining arguments as input files
                while [[ $# -gt 0 ]]; do
                    inputs+=("$1")
                    shift
                done
                ;;
            --sort-by)
                sort_by="$2"
                shift 2
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Validate arguments
    if [[ -z "$output" || ${#inputs[@]} -eq 0 ]]; then
        echo "Error: --output and at least one --input file are required for merge command"
        show_help
        exit 1
    fi
    
    echo "🔗 Starting mock file merge"
    echo "Output: $output"
    echo "Input files (${#inputs[@]}):"
    printf '  %s\n' "${inputs[@]}"
    
    if [[ -n "$sort_by" ]]; then
        echo "Sort by: $sort_by"
        cargo run --bin mock_split_merge_hub merge "$output" "${inputs[@]}" --sort-by "$sort_by"
    else
        cargo run --bin mock_split_merge_hub merge "$output" "${inputs[@]}"
    fi
}

# Function to generate a large CSV file with sample data
generate_large_file() {
    local file_path=$1
    local target_size_gb=$2
    local records_per_mb=5000  # Approximate records per MB
    local target_records=$((target_size_gb * 1024 * records_per_mb))
    
    echo "📝 Generating $target_size_gb GB file: $file_path"
    echo "This may take a while..."
    
    # Create directory if it doesn't exist
    mkdir -p "$(dirname "$file_path")"
    
    # Write header
    echo "account_no,first_name,last_name" > "$file_path"
    
    # Write records in batches
    local batch_size=10000
    local total_written=0
    local batch_count=0
    
    while [ $total_written -lt $target_records ]; do
        # Generate a batch of records
        for ((i=0; i<batch_size; i++)); do
            echo "$(generate_account_number),$(generate_first_name),$(generate_last_name)" >> "$file_path"
        done
        
        total_written=$((total_written + batch_size))
        batch_count=$((batch_count + 1))
        
        # Show progress every 10 batches
        if [ $((batch_count % 10)) -eq 0 ]; then
            local progress=$((total_written * 100 / target_records))
            echo -ne "\rProgress: $progress% ($(numfmt --to=si --format="%.1f" $(stat -f %z "$file_path"))B written)"
        fi
    done
    
    echo -e "\n✅ Generated $file_path ($(numfmt --to=si --format="%.1f" $(stat -f %z "$file_path"))B)"
}

# Function to generate a random account number
generate_account_number() {
    printf "%010d" $((RANDOM % 10000000000))
}

# Function to generate a random first name
generate_first_name() {
    local first_names=("John" "Jane" "Michael" "Emily" "David" "Sarah" "Robert" "Lisa" "William" "Mary")
    echo "${first_names[RANDOM % ${#first_names[@]}]}"
}

# Function to generate a random last name
generate_last_name() {
    local last_names=("Smith" "Johnson" "Williams" "Brown" "Jones" "Miller" "Davis" "Garcia" "Rodriguez" "Wilson")
    echo "${last_names[RANDOM % ${#last_names[@]}]}"
}

# Function to run a complete demo with large files
run_large_demo() {
    echo "🚀 Starting Large File Demo (2x5GB) 🚀"
    echo "===================================="
    
    # Create a demo directory with timestamp
    DEMO_DIR="large_demo_$(date +%s)"
    mkdir -p "$DEMO_DIR"
    
    # Generate two 5GB files
    FILE1="$DEMO_DIR/large_file_1.csv"
    FILE2="$DEMO_DIR/large_file_2.csv"
    
    # Generate first 5GB file
    generate_large_file "$FILE1" 5
    
    # Generate second 5GB file
    generate_large_file "$FILE2" 5
    
    echo -e "\n✅ Demo files created:"
    ls -lh "$DEMO_DIR"
    
    echo -e "\n🔍 Sample data from first file:"
    head -n 3 "$FILE1"
    echo "..."
    
    echo -e "\n📊 File information:"
    du -h "$DEMO_DIR"
    
    echo -e "\n📝 Next steps:"
    echo "1. To process these files, you can use the following commands:"
    echo "   ./run_mockup.sh split --input $FILE1 --output ${DEMO_DIR}/chunks1 --chunk-size 1000000"
    echo "   ./run_mockup.sh merge --output ${DEMO_DIR}/merged.csv --input ${DEMO_DIR}/chunks1/*.csv --sort-by account_no"
    echo -e "\n2. To clean up when done:"
    echo "   rm -rf $DEMO_DIR"
    
    echo -e "\n🏁 Large file demo completed!"
}

# Function to run a complete demo
run_demo() {
    echo "🚀 Starting Mockup Demo 🚀"
    echo "========================="
    
    # Create a demo directory
    DEMO_DIR="demo_$(date +%s)"
    mkdir -p "$DEMO_DIR"
    
    # Create a sample input file (just for demo, actual file won't be used)
    INPUT_FILE="$DEMO_DIR/sample_data.csv"
    echo "account_no,first_name,last_name" > "$INPUT_FILE"
    
    # Step 1: Split the file
    echo -e "\n🔪 Step 1: Splitting file..."
    OUTPUT_DIR="$DEMO_DIR/chunks"
    mkdir -p "$OUTPUT_DIR"
    
    echo "Splitting into chunks of 1000 records each..."
    cargo run --bin mock_split_merge_hub split "$INPUT_FILE" "$OUTPUT_DIR" --chunk-size 1000
    
    # List the generated chunks
    echo -e "\n📁 Generated chunks:"
    ls -lh "$OUTPUT_DIR"
    
    # Step 2: Merge the chunks
    echo -e "\n🔗 Step 2: Merging chunks..."
    MERGED_FILE="$DEMO_DIR/merged_output.csv"
    
    # Collect all chunk files
    CHUNKS=("$OUTPUT_DIR"/*.csv)
    
    echo "Merging ${#CHUNKS[@]} chunks..."
    cargo run --bin mock_split_merge_hub merge "$MERGED_FILE" "${CHUNKS[@]}" --sort-by "account_no"
    
    # Show the result
    echo -e "\n✅ Demo completed!"
    echo "Merged output: $MERGED_FILE"
    echo -e "\nFirst few lines of merged output:"
    head -n 5 "$MERGED_FILE"
    echo -e "..."
    echo -e "\nDemo files are in: $DEMO_DIR"
    
    echo -e "\n💡 Try the large file demo with: ./run_mockup.sh large-demo"
}

# Main script execution
case "$1" in
    split)
        shift
        run_split "$@"
        ;;
    merge)
        shift
        run_merge "$@"
        ;;
    demo)
        run_demo
        ;;
    large-demo)
        run_large_demo
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Error: Unknown command '$1'"
        show_help
        exit 1
        ;;
esac