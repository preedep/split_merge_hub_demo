# Build the release version for better performance
cargo build --release --bin generate_large_files

# Run the generator (creates two 5GB files in the 'large_files' directory)
./target/release/generate_large_files