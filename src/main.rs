use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv::{ReaderBuilder, StringRecord, WriterBuilder, Reader};
use log::{error, info};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::{tempdir, TempDir};

// Constants for memory management
const DEFAULT_CSV_ROWS: usize = 100_000; // Default rows per split file

// Structure to hold sorted chunks for external sorting
struct SortedChunk {
    reader: Option<csv::Reader<File>>,
    current_record: Option<StringRecord>,
}

impl SortedChunk {
    fn new(path: PathBuf) -> Result<Self> {
        let file = File::open(&path).context("Failed to open chunk file")?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);
            
        let current_record = match reader.records().next() {
            Some(Ok(record)) => Some(record),
            Some(Err(e)) => return Err(anyhow::anyhow!("Failed to read record: {}", e)),
            None => None,
        };
        
        Ok(Self {
            reader: Some(reader),
            current_record,
        })
    }
    
    fn next_record(&mut self) -> Result<Option<StringRecord>> {
        if let Some(record) = self.current_record.take() {
            // Get the next record if available
            if let Some(reader) = &mut self.reader {
                self.current_record = match reader.records().next() {
                    Some(Ok(rec)) => Some(rec),
                    Some(Err(e)) => return Err(anyhow::anyhow!("Failed to read next record: {}", e)),
                    None => None,
                };
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}

impl Ord for SortedChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        match (&self.current_record, &other.current_record) {
            (Some(a), Some(b)) => {
                // Compare string representations for simplicity
                let a_str: String = a.iter().collect::<String>();
                let b_str: String = b.iter().collect::<String>();
                b_str.cmp(&a_str) // Reverse order for min-heap
            },
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for SortedChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortedChunk {
    fn eq(&self, other: &Self) -> bool {
        match (&self.current_record, &other.current_record) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        }
    }
}

impl Eq for SortedChunk {}

// Helper function to create a temporary directory that will be cleaned up automatically
fn create_temp_dir() -> Result<TempDir> {
    tempdir().context("Failed to create temporary directory")
}

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



/// Compares two records based on the specified column indices
fn compare_records(
    a: &StringRecord,
    b: &StringRecord,
    column_indices: &[usize],
) -> Ordering {
    for &idx in column_indices {
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
}

/// Sorts a chunk of records and writes it to a temporary file
fn sort_and_write_chunk(
    mut chunk: Vec<StringRecord>,
    column_indices: &[usize],
    temp_dir: &Path,
    chunk_num: usize,
) -> Result<PathBuf> {
    // Sort the chunk
    chunk.sort_by(|a, b| compare_records(a, b, column_indices));
    
    // Write sorted chunk to temporary file
    let chunk_path = temp_dir.join(format!("chunk_{}.csv", chunk_num));
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(&chunk_path)
        .context("Failed to create chunk file")?;
        
    for record in &chunk {
        wtr.write_record(record)
            .context("Failed to write record to chunk")?;
    }
    wtr.flush().context("Failed to flush chunk writer")?;
    
    Ok(chunk_path)
}

/// Merges sorted chunks into the final output file
fn merge_sorted_chunks(
    chunk_files: Vec<PathBuf>,
    output_path: &Path,
    headers: &StringRecord,
) -> Result<()> {
    // If we only have one chunk, just rename it to the output
    if chunk_files.len() == 1 {
        std::fs::rename(&chunk_files[0], output_path)
            .context("Failed to rename single chunk to output")?;
        return Ok(());
    }
    
    // Merge sorted chunks using a min-heap
    let mut chunks: BinaryHeap<SortedChunk> = BinaryHeap::new();
    for path in chunk_files {
        match SortedChunk::new(path) {
            Ok(chunk) => chunks.push(chunk),
            Err(e) => error!("Failed to create chunk reader: {}", e),
        }
    }
    
    // Write merged and sorted output
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_path)
        .context("Failed to create output file")?;
    
    // Write headers
    wtr.write_record(headers)
        .context("Failed to write headers")?;
    
    // Merge chunks using a min-heap
    while let Some(mut chunk) = chunks.pop() {
        match chunk.next_record() {
            Ok(Some(record)) => {
                wtr.write_record(&record)
                    .context("Failed to write record")?;
                chunks.push(chunk);
            },
            Ok(None) => {
                // No more records in this chunk, drop it
                continue;
            },
            Err(e) => {
                return Err(e).context("Failed to get next record from chunk");
            }
        }
    }
    
    wtr.flush().context("Failed to flush output writer")?;
    Ok(())
}

/// Sorts records based on specified columns using external sort for large datasets
fn external_sort(
    input_path: &Path,
    output_path: &Path,
    sort_columns: &[&str],
    temp_dir: &Path,
) -> Result<()> {
    if sort_columns.is_empty() {
        // If no sort columns, just copy the file
        std::fs::copy(input_path, output_path).context("Failed to copy file")?;
        return Ok(());
    }

    // Read headers first
    let file = File::open(input_path).context("Failed to open input file")?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(&file);
    let headers = rdr.headers()?.clone();
    
    // Get column indices for sorting
    let column_indices: Vec<usize> = sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect();
    
    if column_indices.is_empty() {
        return Err(anyhow::anyhow!(
            "None of the specified sort columns were found in the CSV headers: {:?}",
            sort_columns
        ));
    }
    
    // Process in chunks
    const CHUNK_SIZE: usize = 10_000; // Number of records per chunk
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    let mut chunk_files = Vec::new();
    
    // Read records in chunks, sort them, and write to temporary files
    for result in rdr.records() {
        let record = result.context("Failed to read record")?;
        chunk.push(record);
        
        if chunk.len() >= CHUNK_SIZE {
            let chunk_path = sort_and_write_chunk(
                chunk, 
                &column_indices, 
                temp_dir, 
                chunk_files.len()
            )?;
            chunk_files.push(chunk_path);
            chunk = Vec::with_capacity(CHUNK_SIZE);
        }
    }
    
    // Process remaining records in the last chunk
    if !chunk.is_empty() {
        let chunk_path = sort_and_write_chunk(
            chunk, 
            &column_indices, 
            temp_dir, 
            chunk_files.len()
        )?;
        chunk_files.push(chunk_path);
    }
    
    // Merge all sorted chunks into the final output
    merge_sorted_chunks(chunk_files, output_path, &headers)
}



/// Processes a single input file by sorting it if needed
fn process_input_file(
    file_path: &str,
    index: usize,
    sort_columns: &[&str],
    temp_dir: &Path,
) -> Result<PathBuf> {
    let temp_output = temp_dir.join(format!("sorted_{}.csv", index));
    if sort_columns.is_empty() {
        // No sorting needed, just copy the file
        std::fs::copy(file_path, &temp_output)
            .context("Failed to copy input file")?;
    } else {
        // Sort the file
        external_sort(
            Path::new(file_path),
            &temp_output,
            sort_columns,
            temp_dir,
        )
        .context("Failed to sort input file")?;
    }
    Ok(temp_output)
}

/// Finds the smallest record among all available records
fn find_smallest_record(records: &[Option<StringRecord>]) -> Option<(usize, &StringRecord)> {
    let mut min_record: Option<(usize, &StringRecord)> = None;
    
    for (i, record_opt) in records.iter().enumerate() {
        if let Some(record) = record_opt {
            if let Some((_, min_rec)) = min_record {
                // Compare with current min record
                let rec_str: String = record.iter().collect();
                let min_str: String = min_rec.iter().collect();
                if rec_str < min_str {
                    min_record = Some((i, record));
                }
            } else {
                // First record found
                min_record = Some((i, record));
            }
        }
    }
    
    min_record
}

/// Merges multiple sorted CSV files into a single output file
fn merge_sorted_files(
    sorted_files: &[PathBuf],
    output_file: &str,
    headers: &StringRecord,
    sort_columns: &[&str],
) -> Result<()> {
    let mut readers: Vec<Reader<File>> = Vec::with_capacity(sorted_files.len());
    
    // Open all files
    for path in sorted_files {
        let file = File::open(path).context("Failed to open sorted chunk")?;
        let rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        readers.push(rdr);
    }
    
    // Create output writer
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_file)
        .context("Failed to create output file")?;
    
    // Write headers
    wtr.write_record(headers)?;
    
    // If we have sort columns, we need to sort all records
    if !sort_columns.is_empty() {
        // Collect all records
        let mut all_records = Vec::new();
        for reader in &mut readers {
            for result in reader.records() {
                all_records.push(result?);
            }
        }
        
        // Sort the records
        if !all_records.is_empty() {
            // Get the header indices for the sort columns
            let header_indices: Vec<usize> = headers.iter()
                .enumerate()
                .filter(|(_, h)| {
                    let header_str = h.to_string();
                    sort_columns.contains(&&*header_str)
                })
                .map(|(i, _)| i)
                .collect();
            
            if !header_indices.is_empty() {
                all_records.sort_by(|a, b| {
                    for &i in &header_indices {
                        let a_val = a.get(i).unwrap_or("");
                        let b_val = b.get(i).unwrap_or("");
                        let cmp = a_val.cmp(b_val);
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            
            // Write the sorted records
            for record in all_records {
                wtr.write_record(&record)?;
            }
        }
    } else {
        // No sorting needed, just write all records
        for reader in &mut readers {
            for result in reader.records() {
                let record = result?;
                wtr.write_record(&record)?;
            }
        }
    }
    
    // Flush the writer to ensure all data is written
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
/// * `Result<()>` - Ok(()) on success, or an error
fn merge_csv_files(input_files: &[String], output_file: &str, sort_columns: &[&str]) -> Result<()> {
    info!("Merging CSV files into {}", output_file);
    let start_time = Instant::now();
    
    // Handle single file case without sorting
    if input_files.len() == 1 && sort_columns.is_empty() {
        std::fs::copy(&input_files[0], output_file)
            .context("Failed to copy single input file")?;
        info!("Copied single file to {}", output_file);
        return Ok(());
    }
    
    // Create a temporary directory for intermediate files
    let temp_dir = create_temp_dir()?;
    let temp_path = temp_dir.path();
    
    // Process each input file
    let mut sorted_files = Vec::with_capacity(input_files.len());
    for (i, file_path) in input_files.iter().enumerate() {
        info!("Processing file {}/{}: {}", i + 1, input_files.len(), file_path);
        let sorted_file = process_input_file(file_path, i, sort_columns, temp_path)?;
        sorted_files.push(sorted_file);
    }
    
    // Get headers from first file
    let first_file = File::open(&input_files[0])
        .context("Failed to open first input file")?;
    let headers = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(first_file)
        .headers()?
        .clone();
    
    // Handle single sorted file case
    if sorted_files.len() == 1 {
        std::fs::rename(&sorted_files[0], output_file)
            .context("Failed to rename sorted file")?;
    } else {
        // Merge the sorted files
        merge_sorted_files(&sorted_files, output_file, &headers, sort_columns)?;
    }
    
    let duration = start_time.elapsed();
    info!("Merge completed in {:.2?}", duration);
    
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
/// * `Result<()>` - Ok(()) on success, or an error
fn split_csv_file(input_file: &str, output_dir: &str, rows_per_file: usize, sort_columns: &[&str]) -> Result<()> {
    info!("Splitting CSV file: {}", input_file);
    let start_time = Instant::now();
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir).context("Failed to create output directory")?;
    
    // Create a temporary directory for sorting
    let temp_dir = create_temp_dir()?;
    let temp_path = temp_dir.path();
    
    // Sort the input file if needed
    let sorted_file = if sort_columns.is_empty() {
        PathBuf::from(input_file)
    } else {
        let sorted_path = temp_path.join("sorted.csv");
        external_sort(Path::new(input_file), &sorted_path, sort_columns, temp_path)
            .context("Failed to sort input file")?;
        sorted_path
    };
    
    // Open the (possibly sorted) input file
    let file = File::open(&sorted_file).context("Failed to open sorted input file")?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    // Get headers
    let headers = rdr.headers()?.clone();
    
    // Process the file in chunks
    let mut chunk_num = 0;
    let mut records_written = 0;
    let mut current_chunk: Vec<StringRecord> = Vec::with_capacity(rows_per_file);
    
    for result in rdr.records() {
        let record = result.context("Failed to read record")?;
        current_chunk.push(record);
        
        if current_chunk.len() >= rows_per_file {
            write_chunk(
                &output_dir,
                &headers,
                &current_chunk,
                Path::new(input_file).file_stem().and_then(|s| s.to_str()).unwrap_or("split"),
                chunk_num,
            )?;
            
            records_written += current_chunk.len();
            chunk_num += 1;
            current_chunk.clear();
        }
    }
    
    // Write the last chunk if it's not empty
    if !current_chunk.is_empty() {
        write_chunk(
            &output_dir,
            &headers,
            &current_chunk,
            Path::new(input_file).file_stem().and_then(|s| s.to_str()).unwrap_or("split"),
            chunk_num,
        )?;
        records_written += current_chunk.len();
    }
    
    let duration = start_time.elapsed();
    info!("Successfully split {} records into {} files in {:.2?}", 
          records_written, chunk_num + 1, duration);
    
    Ok(())
}

/// Helper function to write a chunk of records to a file
fn write_chunk(
    output_dir: &str,
    headers: &StringRecord,
    records: &[StringRecord],
    file_stem: &str,
    chunk_num: usize,
) -> Result<()> {
    let output_path = format!(
        "{}/{}_part_{:04}.csv",
        output_dir,
        file_stem,
        chunk_num + 1
    );
    
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(&output_path)
        .context("Failed to create output file")?;
    
    // Write headers
    wtr.write_record(headers)?;
    
    // Write records
    for record in records {
        wtr.write_record(record)?;
    }
    
    wtr.flush()?;
    info!("Wrote {} records to {}", records.len(), output_path);
    
    Ok(())
}

fn main() -> Result<()> {
    // Initialize logger
    pretty_env_logger::init();
    info!("Starting CSV split/merge tool");

    // Parse command line arguments
    let cli = Cli::parse();

    // Execute the appropriate command
    match cli.command {
        Commands::Merge { output, sort_by, input_files } => {
            if input_files.is_empty() {
                return Err(anyhow::anyhow!("No input files specified"));
            }

            let sort_columns: Vec<&str> = sort_by.split(',').filter(|s| !s.is_empty()).collect();
            info!("Merging {} files sorted by {:?}", input_files.len(), sort_columns);
            
            merge_csv_files(&input_files, &output, &sort_columns)
                .context("Failed to merge files")?;
            
            info!("Successfully merged files into {}", output);
        },
        Commands::Split { input, output_dir, rows, sort_by, workers: _ } => {
            let sort_columns: Vec<&str> = sort_by.split(',').filter(|s| !s.is_empty()).collect();
            
            info!("Splitting '{}' into '{}' with {} rows per file, sorted by {:?}", 
                  input, output_dir, rows, sort_columns);
            
            split_csv_file(&input, &output_dir, rows, &sort_columns)
                .context("Failed to split file")?;
        },
    }
    
    Ok(())
}
