use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{error, info, warn};

// This struct must derive Ord, Eq, PartialOrd, PartialEq for BinaryHeap
#[derive(Debug)]
struct MergeRecord {
    record: StringRecord,
    source_index: usize,
}

impl Ord for MergeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
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

fn get_sort_column_indices(headers: &StringRecord, sort_columns: &[&str]) -> Vec<usize> {
    let mut indices = Vec::new();
    let header_vec: Vec<&str> = headers.iter().collect();

    info!("Available columns in CSV: {:?}", header_vec);
    info!("Requested sort columns: {:?}", sort_columns);

    for (i, col) in sort_columns.iter().enumerate() {
        let col_trimmed = col.trim();
        match headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case(col_trimmed))
        {
            Some(idx) => {
                info!("Sorting by column: '{}' (index {})", col_trimmed, idx);
                indices.push(idx);
            }
            None => {
                warn!(
                    "Warning: Sort column '{}' not found in headers",
                    col_trimmed
                );
                // Try to suggest similar column names
                let similar: Vec<&str> = headers
                    .iter()
                    .filter(|h| {
                        h.trim()
                            .to_lowercase()
                            .contains(&col_trimmed.to_lowercase())
                    })
                    .collect();
                if !similar.is_empty() {
                    warn!("  Did you mean one of these? {:?}", similar);
                }
            }
        }
    }

    if indices.is_empty() {
        warn!(
            "No valid sort columns found. Available columns: {:?}",
            header_vec
        );
    }

    indices
}

pub fn merge_chunks(chunk_files: &[PathBuf], output_path: &Path, sort_columns: &[&str]) -> Result<()> {
    if chunk_files.is_empty() {
        return Ok(());
    }
    info!("Merging {} chunks into {:?}", chunk_files.len(), output_path);

    // Read headers from first chunk
    let (headers, has_headers) = {
        let first_record = get_first_record(&chunk_files[0])?;
        let looks_like_headers = first_record.iter().all(|field| field.chars().all(|c| c.is_alphabetic() || c == '_' || c == ' '));
        if looks_like_headers {
            (first_record, true)
        } else {
            let default_headers: Vec<String> = (0..first_record.len()).map(|i| i.to_string()).collect();
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
        sort_columns.iter().filter_map(|col| col.parse::<usize>().ok()).collect::<Vec<_>>()
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
                let mut reader = ReaderBuilder::new().has_headers(false).from_reader(BufReader::new(file));
                let mut record = StringRecord::new();
                // Always skip the first record if it is a header row and this chunk has headers
                if has_headers {
                    // Read and discard the first record (header)
                    if !reader.read_record(&mut record)? {
                        readers.push(None);
                        continue;
                    }
                }
                // Read the first data record
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
        let file = File::create(output_path).with_context(|| format!("Failed to create output file: {:?}", output_path))?;
        WriterBuilder::new().has_headers(true).from_writer(BufWriter::new(file))
    };

    writer.write_record(headers.iter()).with_context(|| "Failed to write headers to output file")?;

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

// Add this helper to validate headers for parallel_merge_sort
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

// Export a main entry point for merging, as expected by main.rs
pub fn parallel_merge_sort(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[&str],
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }
    // Validate all input files have matching headers
    let headers = validate_headers(input_paths)?;
    info!("Validated headers across all input files: {:?}", headers.iter().collect::<Vec<_>>());
    info!("Starting parallel merge sort for {} files", input_paths.len());
    // You may want to use a temp dir and chunking here, but for now just call merge_chunks
    merge_chunks(input_paths, output_path.as_ref(), sort_columns)
}