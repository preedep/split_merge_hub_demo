// --- Imports ---
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{debug, error, info, warn};
use rayon::prelude::*;
use tempfile::TempDir;

// --- MergeRecord struct for heap ---
#[derive(Debug)]
struct MergeRecord {
    record: StringRecord,
    source_index: usize,
}

impl Ord for MergeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.record.as_slice().cmp(other.record.as_slice()).reverse()
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

// --- Header validation ---
fn validate_headers(input_paths: &[PathBuf]) -> Result<StringRecord> {
    let mut headers: Option<StringRecord> = None;
    for path in input_paths {
        let file = File::open(path)
            .with_context(|| format!("Failed to open file: {}", path.display()))?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(file));
        let current_headers = rdr.headers()?.clone();
        match &headers {
            None => headers = Some(current_headers),
            Some(expected_headers) if expected_headers != &current_headers => {
                return Err(anyhow::anyhow!(
                    "Header mismatch in file {}:\nExpected: {:?}\nFound:    {:?}",
                    path.display(),
                    expected_headers.iter().collect::<Vec<_>>(),
                    current_headers.iter().collect::<Vec<_>>()
                ));
            }
            _ => {}
        }
    }
    headers.ok_or_else(|| anyhow::anyhow!("No input files provided"))
}

// --- Sort column index resolution ---
fn get_sort_column_indices(headers: &StringRecord, sort_columns: &[&str]) -> Vec<usize> {
    let mut indices = Vec::new();
    let header_vec: Vec<&str> = headers.iter().collect();
    info!("Available columns in CSV: {:?}", header_vec);
    info!("Requested sort columns: {:?}", sort_columns);
    for col in sort_columns.iter() {
        let col_trimmed = col.trim();
        match headers.iter().position(|h| h.trim().eq_ignore_ascii_case(col_trimmed)) {
            Some(idx) => {
                info!("Sorting by column: '{}' (index {})", col_trimmed, idx);
                indices.push(idx);
            }
            None => {
                warn!("Warning: Sort column '{}' not found in headers", col_trimmed);
                let similar: Vec<&str> = headers
                    .iter()
                    .filter(|h| h.trim().to_lowercase().contains(&col_trimmed.to_lowercase()))
                    .collect();
                if !similar.is_empty() {
                    warn!("  Did you mean one of these? {:?}", similar);
                }
            }
        }
    }
    if indices.is_empty() {
        warn!("No valid sort columns found. Available columns: {:?}", header_vec);
    }
    indices
}

// --- Get first record of a file (for header detection) ---
fn get_first_record(path: &Path) -> Result<StringRecord> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path.display()))?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(BufReader::new(file));
    match rdr.records().next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(e)) => Err(anyhow::anyhow!("Failed to read record: {}", e)),
        None => Err(anyhow::anyhow!("File is empty: {}", path.display())),
    }
}

// --- Main k-way merge for chunk files ---
pub fn merge_chunks(chunk_files: &[PathBuf], output_path: &Path, sort_columns: &[&str]) -> Result<()> {
    if chunk_files.is_empty() {
        return Ok(());
    }
    info!("Merging {} chunks into {:?}", chunk_files.len(), output_path);
    // Read headers from first chunk
    let (headers, has_headers) = {
        let first_record = get_first_record(&chunk_files[0])?;
        let looks_like_headers = first_record.iter().all(|field| {
            field.chars().all(|c| c.is_alphabetic() || c == '_' || c == ' ')
        });
        if looks_like_headers {
            (first_record, true)
        } else {
            let default_headers: Vec<String> = (0..first_record.len())
                .map(|i| i.to_string())
                .collect();
            (StringRecord::from(default_headers), false)
        }
    };
    // Validate all chunks have matching headers if headers exist
    if has_headers {
        for (i, chunk_path) in chunk_files.iter().enumerate().skip(1) {
            let record = get_first_record(chunk_path)?;
            if record != headers {
                return Err(anyhow::anyhow!(
                    "Header mismatch in chunk {}:\nExpected: {:?}\nFound:    {:?}",
                    i + 1,
                    headers.iter().collect::<Vec<_>>(),
                    record.iter().collect::<Vec<_>>()
                ));
            }
        }
    }
    let sort_indices = if has_headers {
        get_sort_column_indices(&headers, sort_columns)
    } else {
        sort_columns
            .iter()
            .filter_map(|col| col.parse::<usize>().ok())
            .collect::<Vec<_>>()
    };
    let sort_indices = if sort_indices.is_empty() {
        warn!("No valid sort columns found, defaulting to first column");
        vec![0]
    } else {
        sort_indices
    };
    let mut readers: Vec<Option<csv::Reader<BufReader<File>>>> = Vec::with_capacity(chunk_files.len());
    let mut heap = BinaryHeap::with_capacity(chunk_files.len());
    for (i, chunk_path) in chunk_files.iter().enumerate() {
        match File::open(chunk_path) {
            Ok(file) => {
                let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(BufReader::new(file));
                let mut record = StringRecord::new();
                // Always skip the first record if it is a header row and this chunk has headers
                if has_headers {
                    if !reader.read_record(&mut record)? {
                        readers.push(None);
                        continue;
                    }
                }
                if reader.read_record(&mut record)? {
                    heap.push(MergeRecord { record, source_index: i });
                    readers.push(Some(reader));
                } else {
                    readers.push(None);
                }
            }
            Err(e) => {
                error!("Failed to open chunk file {:?}: {}", chunk_path, e);
                readers.push(None);
            }
        }
    }
    let mut writer = {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {:?}", output_path))?;
        WriterBuilder::new()
            .has_headers(true)
            .from_writer(BufWriter::new(file))
    };
    writer
        .write_record(headers.iter())
        .with_context(|| "Failed to write headers to output file")?;
    while let Some(MergeRecord { record, source_index }) = heap.pop() {
        writer.write_record(&record)?;
        if let Some(reader_opt) = readers.get_mut(source_index) {
            if let Some(reader) = reader_opt {
                let mut record = StringRecord::new();
                if reader.read_record(&mut record)? {
                    heap.push(MergeRecord { record, source_index });
                } else {
                    *reader_opt = None;
                }
            }
        }
    }
    writer.flush()?;
    Ok(())
}

// --- Parallel merge tree for chunk files ---
fn merge_two_chunks(
    chunk_a: &PathBuf,
    chunk_b: &PathBuf,
    output_path: &Path,
    sort_columns: &[&str],
) -> Result<()> {
    merge_chunks(&vec![chunk_a.clone(), chunk_b.clone()], output_path, sort_columns)
}

pub fn parallel_merge_chunks(
    mut chunk_files: Vec<PathBuf>,
    output_path: &Path,
    sort_columns: &[&str],
) -> Result<()> {
    let mut round = 0;
    let parent_dir = output_path.parent().unwrap_or_else(|| Path::new("."));
    debug!("Starting parallel merge with {} chunks", chunk_files.len());
    
    while chunk_files.len() > 1 {
        let pairs: Vec<_> = chunk_files.chunks(2).collect();
        let results: Vec<anyhow::Result<PathBuf>> = pairs
            .par_iter()
            .enumerate()
            .map(|(idx, pair)| {
                if pair.len() == 2 {
                    let out = parent_dir.join(format!("merge_round{}_{}.csv", round, idx));
                    merge_two_chunks(&pair[0], &pair[1], &out, sort_columns)?;
                    Ok(out)
                } else {
                    Ok(pair[0].clone())
                }
            })
            .collect();
        let mut merged_files = Vec::new();
        for res in results {
            merged_files.push(res?);
        }
        chunk_files = merged_files;
        round += 1;
    }
    std::fs::rename(&chunk_files[0], output_path)?;
    Ok(())
}

// --- Split a single input file into sorted chunks in parallel ---
fn split_file_to_chunks(
    file_path: &Path,
    temp_dir: &TempDir,
    sort_columns: &[&str],
    chunk_size_mb: usize,
    headers: &StringRecord,
) -> Result<Vec<PathBuf>> {
    debug!("Splitting file: {:?} into chunks of {} MB", file_path, chunk_size_mb);
    
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(BufReader::new(File::open(file_path)?));
    let mut records = Vec::new();
    let mut chunk_paths = Vec::new();
    let mut chunk_idx = 0;
    let mut current_size = 0;
    let chunk_size = chunk_size_mb * 1024 * 1024;
    for result in rdr.records() {
        let record = result?;
        current_size += record.as_slice().len();
        records.push(record);
        if current_size >= chunk_size {
            let mut chunk = records.split_off(0);
            let chunk_path = temp_dir.path().join(format!("chunk_{}_{}.csv", file_path.file_stem().unwrap().to_string_lossy(), chunk_idx));
            chunk_idx += 1;
            write_sorted_chunk(&chunk, &chunk_path, headers, sort_columns)?;
            chunk_paths.push(chunk_path);
            current_size = 0;
        }
    }
    if !records.is_empty() {
        let chunk_path = temp_dir.path().join(format!("chunk_{}_{}.csv", file_path.file_stem().unwrap().to_string_lossy(), chunk_idx));
        debug!("Writing last chunk: {:?} with {} records", chunk_path, records.len());
        write_sorted_chunk(&records, &chunk_path, headers, sort_columns)?;
        chunk_paths.push(chunk_path);
    }
    debug!("Created {} chunks for file {:?}", chunk_paths.len(), file_path);
    Ok(chunk_paths)
}

fn write_sorted_chunk(records: &[StringRecord], chunk_path: &Path, headers: &StringRecord, sort_columns: &[&str]) -> Result<()> {
    let mut sorted_records = records.to_vec();
    let sort_indices = get_sort_column_indices(headers, sort_columns);
    sorted_records.sort_by(|a, b| compare_records(a, b, &sort_indices));
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(chunk_path)?;
    wtr.write_record(headers)?;
    for rec in sorted_records {
        wtr.write_record(&rec)?;
    }
    wtr.flush()?;
    Ok(())
}

fn compare_records(a: &StringRecord, b: &StringRecord, sort_indices: &[usize]) -> std::cmp::Ordering {
    for &idx in sort_indices {
        let ord = a.get(idx).cmp(&b.get(idx));
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

// --- Main entry point for CLI/integration ---
pub fn parallel_merge_sort(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[&str],
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }
    let headers = validate_headers(input_paths)?;
    info!("Validated headers across all input files: {:?}", headers.iter().collect::<Vec<_>>());
    info!("Starting parallel merge sort for {} files", input_paths.len());
    let temp_dir = TempDir::new()?;
    let chunk_size_mb = std::env::var("CHUNK_SIZE_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    debug!("Using chunk size: {} MB", chunk_size_mb);
    // Split all input files to sorted chunks in parallel
    let all_chunks: Vec<PathBuf> = input_paths
        .par_iter()
        .map(|path| split_file_to_chunks(path, &temp_dir, sort_columns, chunk_size_mb, &headers))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    info!("Created {} sorted chunks", all_chunks.len());
    parallel_merge_chunks(all_chunks, output_path.as_ref(), sort_columns)
}