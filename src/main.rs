use std::fs::{self, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::Path;
use clap::{Parser, Subcommand, ValueEnum};
use crossbeam_channel as channel;
use rayon::prelude::*;

const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks for processing

/// A tool for splitting and merging files with parallel processing
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Merge multiple files into one
    Merge {
        /// Output file path
        output: String,
        
        /// Input files to merge
        #[arg(required = true)]
        input_files: Vec<String>,
    },
    
    /// Split a file into multiple smaller files
    Split {
        /// Input file to split
        input: String,
        
        /// Output directory for split files
        output_dir: String,
        
        /// Maximum size of each split file in MB
        #[arg(short, long, default_value_t = 10)]
        size: usize,
        
        /// Number of parallel workers (0 = auto-detect)
        #[arg(short, long, default_value_t = 0)]
        workers: usize,
    },
}

/// Merges multiple files into a single output file using parallel processing.
///
/// # Arguments
/// * `input_files` - A vector of input file paths to merge
/// * `output_file` - The path where the merged content will be written
///
/// # Returns
/// * `Result<(), io::Error>` - Ok(()) on success, or an IO error
fn merge_files(input_files: &[String], output_file: &str) -> io::Result<()> {
    // Create output file first to ensure we can write
    let _ = File::create(output_file)?;
    
    // Process files in parallel and collect their contents
    let contents: io::Result<Vec<Vec<u8>>> = input_files
        .par_iter()
        .map(|file_path| {
            let mut file = File::open(file_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            Ok(buffer)
        })
        .collect();
    
    // Write contents to output file
    let mut output = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(output_file)?;
    
    for (i, content) in contents?.into_iter().enumerate() {
        output.write_all(&content)?;
        // Add newline between files if not present
        if i < input_files.len() - 1 && !content.ends_with(&[b'\n']) {
            output.write_all(b"\n")?;
        }
    }
    
    Ok(())
}

/// Splits a file into multiple smaller files using parallel processing.
///
/// # Arguments
/// * `input_file` - Path to the file to split
/// * `output_dir` - Directory where the split files will be saved
/// * `max_size_mb` - Maximum size of each split file in MB
///
/// # Returns
/// * `Result<Vec<String>, io::Error>` - Vector of generated file paths, or an IO error
fn split_file(input_file: &str, output_dir: &str, max_size_mb: usize) -> io::Result<Vec<String>> {
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    let file_path = Path::new(input_file);
    let file_stem = file_path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("split");
    let extension = file_path.extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    
    let file = File::open(input_file)?;
    let file_size = file.metadata()?.len() as usize;
    let max_chunk_size = max_size_mb * 1024 * 1024; // Convert MB to bytes
    
    // Calculate number of chunks needed
    let num_chunks = (file_size + max_chunk_size - 1) / max_chunk_size;
    
    // Create a channel for collecting results
    let (tx, rx) = channel::bounded(num_chunks);
    
    // Process each chunk in parallel
    (0..num_chunks).into_par_iter().for_each_with((tx, file_path.to_path_buf()), |(sender, _), i| {
        let output_path = format!(
            "{}/{}_{:03}.{}",
            output_dir,
            file_stem,
            i + 1,
            if !extension.is_empty() { extension } else { "bin" }
        );
        
        // Process each chunk in its own thread
        let result = process_chunk(
            input_file,
            &output_path,
            i * max_chunk_size,
            max_chunk_size
        );
        
        // Send the result back
        let _ = sender.send(match result {
            Ok(path) => Ok(path),
            Err(e) => Err(e.to_string())
        });
    });
    
    // Collect results
    let mut output_files = Vec::with_capacity(num_chunks);
    for result in rx {
        match result {
            Ok(path) => output_files.push(path),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
    
    Ok(output_files)
}

/// Processes a single chunk of the file
fn process_chunk(input_file: &str, output_path: &str, offset: usize, chunk_size: usize) -> io::Result<String> {
    let mut input = File::open(input_file)?;
    let mut output = File::create(output_path)?;
    
    // Seek to the start of the chunk
    input.seek(SeekFrom::Start(offset as u64))?;
    
    // Read and write in smaller chunks to control memory usage
    let mut buffer = vec![0; CHUNK_SIZE.min(chunk_size)];
    let mut remaining = chunk_size;
    
    while remaining > 0 {
        let read_size = buffer.len().min(remaining);
        let read = input.read(&mut buffer[..read_size])?;
        if read == 0 { break; } // End of file
        
        output.write_all(&buffer[..read])?;
        remaining = remaining.saturating_sub(read);
    }
    
    Ok(output_path.to_string())
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Merge { output, input_files } => {
            if input_files.is_empty() {
                eprintln!("Error: No input files specified");
                return Ok(());
            }
            
            println!("Merging {} files into {}...", input_files.len(), output);
            merge_files(&input_files, &output)?;
            println!("✓ Successfully merged {} files into {}", input_files.len(), output);
        },
        Commands::Split { input, output_dir, size, workers } => {
            // Set number of worker threads if specified
            if workers > 0 {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .build_global()
                    .expect("Failed to initialize thread pool");
            }
            
            println!("Splitting {} into {}MB chunks in directory {}...", input, size, output_dir);
            let output_files = split_file(&input, &output_dir, size)?;
            println!("✓ Successfully split {} into {} parts in directory {}", 
                    input, output_files.len(), output_dir);
        },
    }
    
    Ok(())
}
