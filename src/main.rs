use std::fs::{self, File};
use std::io::{self, Read, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::cmp::Ordering;
use std::time::Instant;
use clap::{Parser, Subcommand, ValueEnum};
use crossbeam_channel as channel;
use rayon::prelude::*;
use log::{info, error, debug};
use csv::{ReaderBuilder, WriterBuilder, StringRecord, Reader, Writer};
use serde::Deserialize;

const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks for processing
const DEFAULT_CSV_ROWS: usize = 10_000; // Default rows per split file

/// A tool for splitting and merging CSV files with parallel processing
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Merge multiple CSV files into one
    Merge {
        /// Output CSV file path
        output: String,
        
        /// Comma-separated list of columns to sort by
        #[arg(short = 's', long, default_value = "")]
        sort_by: String,
        
        /// Input CSV files to merge
        #[arg(required = true)]
        input_files: Vec<String>,
    },
    
    /// Split a CSV file into multiple smaller files
    Split {
        /// Input CSV file to split
        input: String,
        
        /// Output directory for split files
        output_dir: String,
        
        /// Maximum number of rows per split file
        #[arg(short = 'r', long, default_value_t = DEFAULT_CSV_ROWS)]
        rows: usize,
        
        /// Comma-separated list of columns to sort by
        #[arg(short = 's', long, default_value = "")]
        sort_by: String,
        
        /// Number of parallel workers (0 = auto-detect)
        #[arg(short = 'w', long, default_value_t = 0)]
        workers: usize,
    },
}

/// Reads records from a CSV file
fn read_csv_records(file_path: &str) -> io::Result<(Option<StringRecord>, Vec<StringRecord>)> {
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    let headers = Some(rdr.headers()?.clone());
    let records = rdr.records().collect::<Result<Vec<_>, _>>()?;
    Ok((headers, records))
}

/// Sorts records based on specified columns
fn sort_records(records: &mut [StringRecord], headers: &StringRecord, sort_columns: &[&str]) {
    if sort_columns.is_empty() {
        return;
    }
    
    let column_indices: Vec<usize> = sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect();
    
    records.sort_by(|a, b| {
        for &idx in &column_indices {
            match (a.get(idx), b.get(idx)) {
                (Some(a_val), Some(b_val)) => {
                    let ord = a_val.cmp(b_val);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                _ => continue,
            }
        }
        Ordering::Equal
    });
}

/// Writes records to a CSV file
fn write_csv_records(
    output_path: &Path,
    headers: Option<&StringRecord>,
    records: &[StringRecord],
) -> io::Result<()> {
    let output = File::create(output_path)?;
    let mut wtr = WriterBuilder::new().from_writer(output);
    
    if let Some(headers) = headers {
        wtr.write_record(headers)?;
    }
    
    for record in records {
        wtr.write_record(record)?;
    }
    
    wtr.flush()?;
    Ok(())
}

/// Merges multiple CSV files into a single output file with optional sorting
///
/// # Arguments
/// * `input_files` - A vector of input CSV file paths to merge
/// * `output_file` - The path where the merged CSV will be written
/// * `sort_columns` - List of column names to sort by (empty for no sorting)
///
/// # Returns
/// * `io::Result<()>` - Ok(()) on success, or an IO error
fn merge_csv_files(input_files: &[String], output_file: &str, sort_columns: &[&str]) -> io::Result<()> {
    info!("Merging CSV files into {}", output_file);

    let start_time = Instant::now();
    info!("Starting merge operation...");
    
    let mut all_records = Vec::new();
    let mut headers = None;
    
    // Read all records from input files
    for file_path in input_files {
        info!("Reading CSV file: {}", file_path);
        let (file_headers, mut records) = read_csv_records(file_path)?;
        
        // Get headers from first file
        if headers.is_none() {
            headers = file_headers;
        }
        
        all_records.append(&mut records);
    }
    
    // Sort records if sort columns are specified
    if let Some(ref headers) = headers {
        sort_records(&mut all_records, headers, sort_columns);
    }
    
    // Write sorted records to output file
    if let Ok(t) =  write_csv_records(Path::new(output_file), headers.as_ref(), &all_records) {
        let duration = start_time.elapsed();
        info!("Merge completed in {:.2?}", duration);
    } else {
        error!("Failed to write records to output file: {}", output_file);
        return Err(io::Error::new(io::ErrorKind::Other, "Failed to write output file"));
    }
    
    Ok(())
}

/// Splits a CSV file into multiple smaller files with optional sorting
///
/// # Arguments
/// * `input_file` - Path to the input CSV file
/// * `output_dir` - Directory where split files will be saved
/// * `rows_per_file` - Maximum number of rows per output file
/// * `sort_columns` - List of column names to sort by (empty for no sorting)
///
/// # Returns
/// * `io::Result<()>` - Ok(()) on success, or an IO error
fn split_csv_file(input_file: &str, output_dir: &str, rows_per_file: usize, sort_columns: &[&str]) -> io::Result<()> {
    info!("Splitting CSV file: {}", input_file);
    let start_time = Instant::now();
    info!("Starting split operation...");
    
    fs::create_dir_all(output_dir)?;
    
    // Read all records and headers
    let (headers_opt, mut all_records) = read_csv_records(input_file)?;
    let headers = headers_opt.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "No headers found in CSV file")
    })?;
    
    // Sort records if sort columns are specified
    if !sort_columns.is_empty() {
        info!("Sorting records by columns: {:?}", sort_columns);
        sort_records(&mut all_records, &headers, sort_columns);
    }
    
    // Generate output file name components
    let file_stem = Path::new(input_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("split");
    
    // Write chunks to separate files
    for (i, chunk) in all_records.chunks(rows_per_file).enumerate() {
        let output_path = format!("{}/{}_part_{:04}.csv", output_dir, file_stem, i + 1);
        info!("Writing chunk {} with {} records to {}", i + 1, chunk.len(), output_path);
        
        write_csv_records(Path::new(&output_path), Some(&headers), chunk)?;
    }
    
    let num_files = (all_records.len() + rows_per_file - 1) / rows_per_file;
    info!("Successfully split into {} files", num_files);
    let duration = start_time.elapsed();
    info!("Split completed in {:.2?}", duration);
    
    Ok(())
}

fn main() -> io::Result<()> {
    pretty_env_logger::init();
    info!("Starting CSV split/merge tool");

    let cli = Cli::parse();

    match cli.command {
        Commands::Merge { output, sort_by, input_files } => {
            if input_files.is_empty() {
                error!("No input files specified");
                return Ok(());
            }

            let sort_columns: Vec<&str> = sort_by.split(',').filter(|s| !s.is_empty()).collect();
            info!("Merging {} files sorted by {:?}", input_files.len(), sort_columns);
            
            merge_csv_files(&input_files, &output, &sort_columns)?;
            info!("Successfully merged files into {}", output);
        },
        Commands::Split { input, output_dir, rows, sort_by, workers } => {
            if workers > 0 {
                debug!("Setting up thread pool with {} workers", workers);
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .build_global()
                    .unwrap_or_else(|e| error!("Failed to initialize thread pool: {}", e));
            }

            let sort_columns: Vec<&str> = sort_by.split(',').filter(|s| !s.is_empty()).collect();
            info!("Splitting {} into chunks of {} rows sorted by {:?}", 
                  input, rows, sort_columns);
            
            split_csv_file(&input, &output_dir, rows, &sort_columns)?;
        },
    }
    
    Ok(())
}
