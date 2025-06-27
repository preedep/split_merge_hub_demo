use anyhow::{Context, Result};
use chrono::Local;
use clap::{Parser, Subcommand};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{debug, info};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::slice::ParallelSliceMut;
use split_merge_hub_demo::parallel_merge::*;


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
        /// Input CSV files to merge
        #[arg(required = true)]
        input_files: Vec<String>,

        /// Output file path
        #[arg(short, long)]
        output: String,

        /// Columns to sort by (comma-separated)
        #[arg(long, value_delimiter = ',')]
        sort_by: Vec<String>,

        /// Chunk size in MB for processing large files
        #[arg(long, default_value = "500")]
        chunk_size: usize,
    },

    /// Split a CSV file into smaller chunks
    Split {
        /// Input CSV file to split
        input_file: String,

        /// Output directory for split files
        #[arg(short, long, default_value = "split_files")]
        output_dir: String,

        /// Maximum number of rows per file
        #[arg(short, long, default_value = "10000")]
        rows_per_file: usize,

        /// Columns to sort by (comma-separated)
        #[arg(long, value_delimiter = ',')]
        sort_by: Vec<String>,
    },
}

fn main() -> Result<()> {
    // Initialize logger with timestamp
    pretty_env_logger::init();

    // ... rest of the code remains the same ...
    let cli = Cli::parse();

    match cli.command {
        Commands::Merge {
            input_files,
            output,
            sort_by,
            chunk_size,
        } => unsafe {
            // Set the chunk size as an environment variable
            std::env::set_var("CHUNK_SIZE_MB", chunk_size.to_string());
            let sort_columns: Vec<&str> = sort_by.iter().map(|s| s.as_str()).collect();
            merge_csv_files(&input_files, &output, &sort_columns)
        },
        Commands::Split {
            input_file,
            output_dir,
            rows_per_file,
            sort_by,
        } => {
            let sort_columns: Vec<&str> = sort_by.iter().map(|s| s.as_str()).collect();
            split_csv_file(&input_file, &output_dir, rows_per_file, &sort_columns)
        }
    }
}

/// Merges multiple CSV files into a single output file with optional sorting
fn merge_csv_files(input_files: &[String], output_file: &str, sort_columns: &[&str]) -> Result<()> {
    info!("Merging {} files into {}", input_files.len(), output_file);
    let start_time = Instant::now();

    // Convert input files to PathBuf
    let input_paths: Vec<PathBuf> = input_files.iter().map(PathBuf::from).collect();

    // If no sorting is needed, just concatenate the files
    if sort_columns.is_empty() {
        debug!("No sorting needed");
        debug!("Concatenating files: {:?}", input_paths);

        let first_file = File::open(&input_paths[0]).context("Failed to open first input file")?;
        let headers = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&first_file)
            .headers()?
            .clone();

        concatenate_files(&input_paths, output_file, &headers)?;
    } else {
        // Use parallel merge sort for large files with sorting
        debug!("Using parallel merge sort");
        parallel_merge_sort(&input_paths, Path::new(output_file), sort_columns)
            .context("Parallel merge sort failed")?;
    }

    let duration = start_time.elapsed();
    info!("Merge completed in {:.2?}", duration);

    Ok(())
}

/// Concatenates multiple CSV files without sorting
fn concatenate_files(files: &[PathBuf], output_file: &str, headers: &StringRecord) -> Result<()> {
    info!("Concatenating {} files", files.len());

    let output = File::create(output_file).context("Failed to create output file")?;
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_writer(io::BufWriter::new(output));

    // Write headers
    writer
        .write_record(headers.iter())
        .context("Failed to write headers")?;

    // Concatenate all files
    for file in files.iter() {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true) // Always skip header row automatically
            .from_path(file)
            .with_context(|| format!("Failed to open input file: {}", file.display()))?;

        for result in rdr.records() {
            let record = result.context("Failed to read record")?;
            writer
                .write_record(&record)
                .context("Failed to write record")?;
        }
    }

    writer.flush().context("Failed to flush writer")?;
    Ok(())
}

/// Splits a CSV file into multiple smaller files with optional sorting
fn split_csv_file(
    input_file: &str,
    output_dir: &str,
    rows_per_file: usize,
    sort_columns: &[&str],
) -> Result<()> {
    info!(
        "Splitting {} into chunks of {} rows",
        input_file, rows_per_file
    );

    // Create the output directory
    fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    // Create a temporary directory for sorting
    let temp_dir = tempfile::tempdir().context("Failed to create temp directory")?;

    // If sorting is needed, sort the file first
    let sorted_file = if !sort_columns.is_empty() {
        let sorted_path = temp_dir.path().join("sorted.csv");
        external_sort(
            Path::new(input_file),
            &sorted_path,
            sort_columns,
            temp_dir.path(),
        )?;
        sorted_path
    } else {
        PathBuf::from(input_file)
    };

    // Open the input file
    let file = File::open(&sorted_file).context("Failed to open input file")?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::BufReader::new(file));

    // Get headers
    let headers = rdr.headers()?.clone();

    // Process the file in chunks
    let mut chunk_num = 0;
    let mut current_chunk: Vec<StringRecord> = Vec::with_capacity(rows_per_file);

    for result in rdr.records() {
        let record = result.context("Failed to read record")?;
        current_chunk.push(record);

        if current_chunk.len() >= rows_per_file {
            write_chunk(
                output_dir,
                &headers,
                &current_chunk,
                Path::new(input_file)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("split"),
                chunk_num,
            )?;

            chunk_num += 1;
            current_chunk.clear();
        }
    }

    // Write the last chunk if it's not empty
    if !current_chunk.is_empty() {
        write_chunk(
            output_dir,
            &headers,
            &current_chunk,
            Path::new(input_file)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("split"),
            chunk_num,
        )?;
    }

    info!("Split into {} files in {}", chunk_num + 1, output_dir);
    Ok(())
}

/// Writes a chunk of records to a file
fn write_chunk(
    output_dir: &str,
    headers: &StringRecord,
    records: &[StringRecord],
    base_name: &str,
    chunk_num: usize,
) -> Result<()> {
    let output_path =
        Path::new(output_dir).join(format!("{}_part_{:04}.csv", base_name, chunk_num));

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(&output_path)
        .context("Failed to create output file")?;

    // Write headers
    wtr.write_record(headers.iter())
        .context("Failed to write headers")?;

    // Write records
    for record in records {
        wtr.write_record(record)?;
    }

    wtr.flush()?;
    Ok(())
}

/// Sorts a CSV file using external sort
fn external_sort(
    input_path: &Path,
    output_path: &Path,
    sort_columns: &[&str],
    _temp_dir: &Path,
) -> Result<()> {
    info!("Sorting {:?} by {:?}", input_path, sort_columns);

    // Read headers
    let file = File::open(input_path).context("Failed to open input file")?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::BufReader::new(file));

    let headers = rdr.headers()?.clone();

    // Get column indices for sorting
    let column_indices: Vec<usize> = sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect();

    if column_indices.is_empty() {
        return Err(anyhow::anyhow!("No valid sort columns found"));
    }

    // Read all records
    let mut records: Vec<StringRecord> = rdr.records().collect::<Result<_, _>>()?;

    // Sort records in parallel
    records.par_sort_by(|a, b| {
        for &idx in &column_indices {
            let a_val = a.get(idx).unwrap_or("");
            let b_val = b.get(idx).unwrap_or("");
            let cmp = a_val.cmp(b_val);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Write sorted records to output
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_path)
        .context("Failed to create output file")?;

    wtr.write_record(headers.iter())?;
    for record in records {
        wtr.write_record(&record)?;
    }

    wtr.flush()?;

    Ok(())
}
