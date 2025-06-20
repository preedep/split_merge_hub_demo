use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{error, info};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use tempfile::TempDir;

// Type alias for CSV reader with BufReader
type CsvReader = csv::Reader<BufReader<File>>;

// Get chunk size from environment or use default
fn get_chunk_size() -> usize {
    env::var("CHUNK_SIZE_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100) // Default to 100MB if not specified
        * 1_000_000 // Convert MB to bytes
}

const MAX_OPEN_FILES: usize = 100; // Maximum number of files to keep open

/// A record with its source index for merging
#[derive(Debug)]
struct MergeRecord {
    record: StringRecord,
    source_index: usize,
}

impl MergeRecord {
    /// Creates a new MergeRecord with the given record and source index
    fn new(record: StringRecord, source_index: usize) -> Self {
        Self { record, source_index }
    }
}

impl Ord for MergeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare records lexicographically
        for (a, b) in self.record.iter().zip(other.record.iter()) {
            let cmp = a.cmp(b);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        self.record.len().cmp(&other.record.len())
    }
}

impl PartialOrd for MergeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeRecord {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record
    }
}

impl Eq for MergeRecord {}

/// Performs parallel merge sort on multiple sorted CSV files
pub fn parallel_merge_sort(
    input_paths: &[impl AsRef<Path>],
    output_path: impl AsRef<Path>,
    sort_columns: &[&str],
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }
    info!("Starting parallel merge sort for {} files", input_paths.len());
    
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }

    // Create a temporary directory for chunk files
    let temp_dir = TempDir::new()
        .context("Failed to create temporary directory")?;
    info!("Using temporary directory: {:?}", temp_dir.path());
    
    let chunk_files: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
    let chunk_size = get_chunk_size();
    
    info!("Processing {} input files with chunk size: {} MB", 
        input_paths.len(), 
        chunk_size / 1_000_000
    );

    // Create a chunk counter for tracking progress
    let chunk_counter = Arc::new(AtomicUsize::new(0));
    
    // Process each input file in parallel
    let mut all_chunks = Vec::new();
    for input_path in input_paths {
        let path = input_path.as_ref().to_path_buf();
        info!("Processing input file: {:?}", path);
        let chunks = process_file(&path, &temp_dir, sort_columns, chunk_counter.as_ref())?;
        all_chunks.extend(chunks);
    }
    
    info!("Processed all files. Created {} chunks", all_chunks.len());

    // If we only have one chunk, just rename it to the output file
    if all_chunks.len() == 1 {
        info!("Single chunk found, renaming to output file");
        std::fs::rename(&all_chunks[0], output_path.as_ref())
            .with_context(|| format!("Failed to rename chunk to {:?}", output_path.as_ref()))?;
        return Ok(());
    }

    info!("Merging {} chunks into final output", all_chunks.len());
    
    // Merge all chunks
    let result = merge_chunks(&all_chunks, output_path.as_ref(), sort_columns);
    
    if result.is_ok() {
        info!("Successfully wrote output to {:?}", output_path.as_ref());
    }
    
    result
}

/// Processes input files into sorted chunks in parallel
fn process_chunks_in_parallel(
    input_files: &[PathBuf],
    temp_dir: &TempDir,
    sort_columns: &[&str],
) -> Result<Vec<PathBuf>> {
    let chunk_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    // Process each input file in parallel
    let chunk_files: Vec<PathBuf> = input_files
        .par_iter()
        .flat_map(|file_path| {
            match process_file(file_path, temp_dir, sort_columns, &chunk_counter) {
                Ok(files) => files,
                Err(e) => {
                    error!("Error processing file {:?}: {}", file_path, e);
                    vec![]
                }
            }
        })
        .collect();
    
    Ok(chunk_files)
}
/// Processes a single input file into sorted chunks
fn process_file(
    file_path: &Path,
    temp_dir: &TempDir,
    sort_columns: &[&str],
    chunk_counter: &std::sync::atomic::AtomicUsize,
) -> Result<Vec<PathBuf>> {
    info!("Processing file: {:?}", file_path);
    
    // Read headers first to get column indices
    let headers = {
        let file = std::fs::File::open(file_path)
            .with_context(|| format!("Failed to open file: {}", file_path.display()))?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(file));
        rdr.headers()?.clone()
    };
    
    // Get sort column indices
    let sort_indices: Vec<usize> = sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect();
    
    if sort_indices.is_empty() {
        anyhow::bail!("None of the specified sort columns were found in the file");
    }
    
    // Process the file in chunks
    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Failed to open file: {}", file_path.display()))?;
    let file_size = file.metadata()?.len() as usize;
    info!("File size: {} MB", file_size / 1_000_000);
    
    // Process the file chunks
    process_file_chunks(file_path, temp_dir, &headers, &sort_indices, chunk_counter)
}

/// Processes a single input file into sorted chunks
fn process_file_chunks(
    file_path: &Path,
    temp_dir: &TempDir,
    headers: &StringRecord,
    sort_indices: &[usize],
    chunk_counter: &std::sync::atomic::AtomicUsize,
) -> Result<Vec<PathBuf>> {
    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Failed to open file: {}", file_path.display()))?;
    
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::BufReader::new(file));
    
    let mut records = Vec::new();
    let mut current_size = 0;
    let chunk_size = get_chunk_size();
    let mut local_chunks = Vec::new();
    let mut record_count = 0;
    
    // Read records in chunks based on size
    for result in rdr.records() {
        let record = result?;
        let record_size = record.as_slice().len();
        record_count += 1;
        
        // If adding this record would exceed our chunk size, process the current chunk
        if !records.is_empty() && (current_size + record_size) > chunk_size {
            let chunk_path = sort_and_write_chunk(
                &mut records,
                temp_dir,
                headers,
                sort_indices,
                chunk_counter,
            )?;
            local_chunks.push(chunk_path);
            current_size = 0;
            records = Vec::new();
        }
        
        records.push(record);
        current_size += record_size;
    }
    
    // Process any remaining records in the last chunk
    if !records.is_empty() {
        let chunk_path = sort_and_write_chunk(
            &mut records,
            temp_dir,
            headers,
            sort_indices,
            chunk_counter,
        )?;
        local_chunks.push(chunk_path);
    }
    
    info!("Processed {} records into {} chunks", record_count, local_chunks.len());
    
    Ok(local_chunks)
}

/// Sorts a chunk of records and writes it to a temporary file
fn sort_and_write_chunk(
    records: &mut Vec<StringRecord>,
    temp_dir: &TempDir,
    headers: &StringRecord,
    sort_indices: &[usize],
    chunk_counter: &std::sync::atomic::AtomicUsize,
) -> Result<PathBuf> {
    // Sort the chunk using the specified columns
    records.par_sort_by(|a, b| compare_records(a, b, sort_indices));
    
    // Create a temporary file for this chunk
    let chunk_num = chunk_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let chunk_path = temp_dir.path().join(format!("chunk_{}.csv", chunk_num));
    
    // Write the sorted chunk to a file
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(&chunk_path)?;
    
    for record in records.drain(..) {
        writer.write_record(&record)?;
    }
    
    writer.flush()?;
    records.clear();
    
    Ok(chunk_path)
}

/// Merges multiple sorted chunks into a single output file
fn merge_chunks(
    chunk_files: &[PathBuf],
    output_path: &Path,
    sort_columns: &[&str],
) -> Result<()> {
    if chunk_files.is_empty() {
        return Ok(());
    }
    
    info!("Merging {} chunks into {:?}", chunk_files.len(), output_path);
    
    // If there's only one chunk, just rename it
    if chunk_files.len() == 1 {
        std::fs::rename(&chunk_files[0], output_path)?;
        return Ok(());
    }
    
    // Read headers from first chunk
    let headers = {
        let file = File::open(&chunk_files[0])
            .with_context(|| format!("Failed to open file: {:?}", &chunk_files[0]))?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(file));
        reader.headers()?.clone()
    };
    
    // Get sort column indices
    let sort_indices: Vec<usize> = sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect();
    
    if sort_indices.is_empty() {
        return Err(anyhow::anyhow!("No valid sort columns found in merge"));
    }
    
    // Open all chunk files
    let mut readers: Vec<Option<CsvReader>> = Vec::with_capacity(chunk_files.len());
    let mut heap = BinaryHeap::with_capacity(chunk_files.len());
    
    // Open first file if it has data
    {
        let file = File::open(&chunk_files[0])
            .with_context(|| format!("Failed to open file: {:?}", &chunk_files[0]))?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(BufReader::new(file));
            
        let mut record = StringRecord::new();
        if reader.read_record(&mut record)? {
            heap.push(MergeRecord {
                record,
                source_index: 0,
            });
            readers.push(Some(reader));
        } else {
            readers.push(None);
        }
    }
    
    // Open remaining chunk files
    for (i, chunk_path) in chunk_files.iter().enumerate().skip(1) {
        match File::open(chunk_path) {
            Ok(file) => {
                let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(BufReader::new(file));
                    
                let mut record = StringRecord::new();
                if reader.read_record(&mut record).with_context(|| 
                    format!("Failed to read record from file: {:?}", chunk_path)
                )? {
                    heap.push(MergeRecord {
                        record,
                        source_index: i,
                    });
                    readers.push(Some(reader));
                } else {
                    readers.push(None);
                }
            },
            Err(e) => {
                error!("Failed to open chunk file {:?}: {}", chunk_path, e);
                readers.push(None);
            }
        }
    }
    

    
    // Create output writer
    let mut writer = {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {:?}", output_path))?;
        let writer = WriterBuilder::new()
            .has_headers(true)
            .from_writer(BufWriter::new(file));
            
        writer
    };
    
    // Write headers
    writer.write_record(headers.iter())
        .with_context(|| "Failed to write headers to output file")?;
    
    // Perform k-way merge
    while let Some(MergeRecord { record, source_index }) = heap.pop() {
        // Write the smallest record to output
        writer.write_record(&record)?;
        
        // Get the next record from the same source
        if let Some(reader_opt) = readers.get_mut(source_index) {
            if let Some(reader) = reader_opt {
                let mut record = StringRecord::new();
                if reader.read_record(&mut record)? {
                    heap.push(MergeRecord {
                        record,
                        source_index,
                    });
                } else {
                    *reader_opt = None; // No more records in this chunk
                }
            }
        }
    }
    
    writer.flush()?;
    Ok(())
}

// The MergeRecord implementation is now at the top of the file

/// Reads headers from a CSV file
fn read_headers(file_path: &Path) -> Result<StringRecord> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    
    rdr.headers()
        .map(|h| h.clone())
        .map_err(Into::into)
}

/// Gets the indices of the columns to sort by
fn get_sort_column_indices(headers: &StringRecord, sort_columns: &[&str]) -> Vec<usize> {
    sort_columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == *col))
        .collect()
}

/// Compare two StringRecords based on the specified column indices
fn compare_records(a: &StringRecord, b: &StringRecord, sort_columns: &[usize]) -> Ordering {
    for &col in sort_columns {
        let a_field = a.get(col).unwrap_or("");
        let b_field = b.get(col).unwrap_or("");
        let cmp = a_field.cmp(b_field);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}
