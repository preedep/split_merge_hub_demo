// --- Imports ---
use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{debug, error, info, warn};
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;
use std::collections::BinaryHeap;

// --- MergeRecord struct for heap ---
#[derive(Debug)]
struct MergeRecord {
    record: StringRecord,
    source_index: usize,
}

impl Ord for MergeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.record
            .as_slice()
            .cmp(other.record.as_slice())
            .reverse()
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

// --- Compare records helper ---
fn compare_records(
    a: &StringRecord,
    b: &StringRecord,
    sort_indices: &[usize],
) -> std::cmp::Ordering {
    for &idx in sort_indices {
        let ord = a.get(idx).cmp(&b.get(idx));
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

// --- Header validation ---
fn validate_headers(input_paths: &[PathBuf]) -> Result<StringRecord> {
    let mut headers: Option<StringRecord> = None;
    for path in input_paths {
        let file =
            File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;
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

// --- Get first record of a file (for header detection) ---
fn get_first_record(path: &Path) -> Result<StringRecord> {
    let file =
        File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(BufReader::new(file));
    match rdr.records().next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(e)) => Err(anyhow::anyhow!("Failed to read record: {}", e)),
        None => Err(anyhow::anyhow!("File is empty: {}", path.display())),
    }
}

// --- Parallel split, sort, write chunks ---
pub fn parallel_split_file_to_chunks(
    file_path: &Path,
    temp_dir: &TempDir,
    sort_columns: &[&str],
    chunk_size_mb: usize,
    headers: &StringRecord,
) -> Result<Vec<PathBuf>> {
    use std::io::{BufRead, Seek, SeekFrom};
    use std::thread;
    use std::time::Duration;
    // Start total timer
    let total_start = Instant::now();
    let file_size = std::fs::metadata(file_path)?.len();
    let chunk_size = chunk_size_mb as u64 * 1024 * 1024;
    let mut chunk_offsets = vec![0u64];
    let mut f = File::open(file_path)?;
    let mut reader = BufReader::new(&f);
    let mut pos = 0u64;
    let mut buf = String::new();
    let prescan_start = Instant::now();
    // Find chunk boundaries by byte offset (approximate, align to line)
    while pos < file_size {
        let target = (pos + chunk_size).min(file_size);
        let mut bytes = 0;
        while pos + bytes < target {
            let read = reader.read_line(&mut buf)?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
            buf.clear();
        }
        pos += bytes;
        chunk_offsets.push(pos);
    }
    if *chunk_offsets.last().unwrap() < file_size {
        chunk_offsets.push(file_size);
    }
    let prescan_elapsed = prescan_start.elapsed();
    info!(
        "[split] Pre-scan complete. File: {:?}, Size: {} bytes, Chunks: {}, ChunkSize: {} MB, Pre-scan Time: {:.2?}",
        file_path,
        file_size,
        chunk_offsets.len() - 1,
        chunk_size_mb,
        prescan_elapsed
    );
    info!(
        "[split] Processing {} chunks in parallel...",
        (chunk_offsets.len() - 1)
    );
    let chunk_timer = Instant::now();
    let chunk_paths: Result<Vec<PathBuf>> = chunk_offsets.windows(2).enumerate().par_bridge().map(|(i, window)| -> Result<PathBuf> {
        let chunk_start_time = Instant::now();
        let start = window[0];
        let end = window[1];
        let mut f = File::open(file_path)?;
        f.seek(SeekFrom::Start(start))?;
        let mut reader = BufReader::new(f);
        let mut records = Vec::new();
        let mut line = String::new();
        let mut pos = start;
        // Skip header if not first chunk
        if i > 0 {
            reader.read_line(&mut line)?;
            line.clear();
        }
        while pos < end {
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            if !line.trim().is_empty() {
                let mut csv_reader = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(line.as_bytes());
                if let Some(Ok(record)) = csv_reader.records().next() {
                    records.push(record);
                }
            }
            pos += read as u64;
            line.clear();
        }
        let sort_indices = get_sort_column_indices(headers, sort_columns);
        let sort_start = Instant::now();
        records.sort_by(|a, b| compare_records(a, b, &sort_indices));
        let sort_elapsed = sort_start.elapsed();
        let chunk_path = temp_dir.path().join(format!("chunk_parallel_{}.csv", i));
        let write_start = Instant::now();
        let mut wtr = WriterBuilder::new().has_headers(true).from_path(&chunk_path)?;
        wtr.write_record(headers)?;
        for rec in &records {
            wtr.write_record(rec)?;
        }
        wtr.flush()?;
        let write_elapsed = write_start.elapsed();
        let chunk_elapsed = chunk_start_time.elapsed();
        info!(
            "[split] Chunk {}/{} | Bytes: {}-{} | Records: {} | Path: {:?} | Read+Parse+Sort: {:.2?} | Write: {:.2?} | Total: {:.2?}",
            i + 1,
            chunk_offsets.len() - 1,
            start,
            end,
            records.len(),
            chunk_path,
            sort_elapsed,
            write_elapsed,
            chunk_elapsed
        );
        Ok(chunk_path)
    }).collect();
    let chunk_total_elapsed = chunk_timer.elapsed();
    let chunk_count = match &chunk_paths {
        Ok(paths) => paths.len(),
        Err(_) => 0
    };
    info!(
        "[split] ALL DONE. File: {:?}, Size: {} bytes, Chunks: {}, Time: {:.2?}",
        file_path,
        file_size,
        chunk_count,
        chunk_total_elapsed
    );
    debug!("[split] Total elapsed (including pre-scan): {:.2?}", total_start.elapsed());
    chunk_paths
}

// --- Merge/Sort helpers ---
/// Merges multiple CSV chunk files into a single sorted CSV file.
///
/// # Arguments
///
/// * `chunk_files` - A slice of `PathBuf` representing the paths to the chunk files that need to be merged.
///
/// * `output_path` - A `Path` reference denoting the path to the output CSV file where the merged data will be written.
///
/// * `sort_columns` - A slice of `&str` that specifies the columns (either by name or index) to use for sorting the records.
///
/// # Returns
///
/// * `Result<()>` - Returns `Ok(())` if the operation completes successfully. Returns an error if any stage of the merging or writing fails.
///
/// # Behavior
///
/// * The function checks whether the provided `chunk_files` array is empty and exits early with `Ok(())` if it is.
/// * Validates that all chunk files have matching headers (if headers exist).
/// * Creates a sorted output file by iteratively reading records from the input chunk files and merging them using the provided `sort_columns`.
/// * Uses a binary heap to perform an efficient merge of sorted data from the various input chunk files.
///
/// # Special Cases
///
/// * If the chunk files contain headers, the function checks whether the first record in each chunk file matches
///   the headers of the first chunk. If they do not match, an error is returned.
/// * If no headers are detected, the function provides default headers (using column indices as names).
/// * If no valid `sort_columns` are provided, the function defaults to sorting by the first column.
///
/// # Environment Variables
///
/// * `MERGE_BUF_MB` - Optionally specifies the buffer size in MB to use when reading and writing files. Defaults to 32 MB if not set.
///
/// # Errors
///
/// * Returns an error if any of the input chunk files fail to open.
/// * Returns an error if writing to the output file fails.
/// * Returns an error if there is a mismatch between headers in the chunk files.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// let chunk_files = vec![
///     PathBuf::from("chunk1.csv"),
///     PathBuf::from("chunk2.csv"),
///     PathBuf::from("chunk3.csv"),
/// ];
/// let output_path = PathBuf::from("merged_output.csv");
/// let sort_columns = &["name", "age"];
///
/// if let Err(e) = merge_chunks(&chunk_files, &output_path, sort_columns) {
///     eprintln!("Error during merging: {}", e);
/// }
/// ```
///
/// # Performance
///
/// * Uses buffered readers and writers to minimize I/O overhead.
/// * Sorts using a binary heap for optimal merge efficiency.
///
/// # Logging
///
/// * Logs useful information such as the number of chunks being merged, warnings about sorting columns, and any errors encountered.
///
/// # Debugging
///
/// * The function logs the time taken to perform the merging operation when debug mode is enabled.
///
pub fn merge_chunks(
    chunk_files: &[PathBuf],
    output_path: &Path,
    sort_columns: &[&str],
) -> Result<()> {
    let t0 = Instant::now();
    if chunk_files.is_empty() {
        return Ok(());
    }
    info!(
        "Merging {} chunks into {:?}",
        chunk_files.len().to_formatted_string(&Locale::en),
        output_path
    );
    // Read headers from first chunk
    debug!("Reading headers from first chunk");
    let (headers, has_headers) = {
        let first_record = get_first_record(&chunk_files[0])?;
        let looks_like_headers = first_record.iter().all(|field| {
            field
                .chars()
                .all(|c| c.is_alphabetic() || c == '_' || c == ' ')
        });
        if looks_like_headers {
            (first_record, true)
        } else {
            let default_headers: Vec<String> =
                (0..first_record.len()).map(|i| i.to_string()).collect();
            (StringRecord::from(default_headers), false)
        }
    };
    // Validate all chunks have matching headers if headers exist
    debug!("Validating headers");
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
    let buffer_size_mb = std::env::var("MERGE_BUF_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32); // default 32MB
    let buffer_size = buffer_size_mb * 1024 * 1024;
    let mut readers: Vec<Option<csv::Reader<BufReader<File>>>> =
        Vec::with_capacity(chunk_files.len());
    let mut heap = BinaryHeap::with_capacity(chunk_files.len());
    for (i, chunk_path) in chunk_files.iter().enumerate() {
        debug!("Opening chunk {}: {:?}", i, chunk_path);
        match File::open(chunk_path) {
            Ok(file) => {
                let chunk_file_size = std::fs::metadata(chunk_path).map(|m| m.len()).unwrap_or(0);
                debug!("Chunk {} size: {} bytes", i, chunk_file_size);
                let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(BufReader::with_capacity(buffer_size, file));
                let mut record = StringRecord::new();
                // Always skip the first record if it is a header row and this chunk has headers
                if has_headers {
                    debug!("Chunk {}: Skipping header record", i);
                    if !reader.read_record(&mut record)? {
                        readers.push(None);
                        continue;
                    }
                    // เฉพาะ chunk แรก (i==0) ให้ push header ลง heap เพื่อให้ header อยู่บรรทัดแรกเท่านั้น
                    if i == 0 {
                        debug!("Chunk {}: Pushing header to heap: {:?}", i, record);
                        heap.push(MergeRecord {
                            record: record.clone(),
                            source_index: i,
                        });
                    }
                }
                if reader.read_record(&mut record)? {
                    if record.len() != headers.len() {
                        error!(
                            "CSV format error at chunk {} (file: {:?}), record (line {}): expected {} fields, found {} fields. Record: {:?}",
                            i,
                            chunk_path,
                            if has_headers { 2 } else { 1 }, // แถวแรกหลัง header หรือแถวแรกสุด
                            headers.len(),
                            record.len(),
                            record
                        );
                    } else {
                        debug!(
                            "Chunk {} (file: {:?}), record (line {}): OK, fields: {}. Record: {:?}",
                            i,
                            chunk_path,
                            if has_headers { 2 } else { 1 },
                            record.len(),
                            record
                        );
                    }
                    heap.push(MergeRecord {
                        record,
                        source_index: i,
                    });
                    readers.push(Some(reader));
                } else {
                    readers.push(Some(reader));
                }
            }
            Err(e) => {
                error!("Failed to open chunk file {:?}: {}", chunk_path, e);
                readers.push(None);
            }
        }
    }
    debug!("All chunk files opened and initial records pushed to heap. Heap size: {}", heap.len());
    let mut writer = {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {:?}", output_path))?;
        WriterBuilder::new()
            .has_headers(true)
            .from_writer(BufWriter::with_capacity(buffer_size, file))
    };
    writer
        .write_record(headers.iter())
        .with_context(|| "Failed to write headers to output file")?;
    debug!("Wrote headers to output: {:?}", output_path);
    let mut merged_record_count = 0u64;
    let mut skipped_record_count = 0u64;
    while let Some(MergeRecord {
        record,
        source_index,
    }) = heap.pop()
    {
        debug!(
            "Merging record from chunk {}: fields: {}, Record: {:?}",
            source_index,
            record.len(),
            record
        );
        if record.len() != headers.len() {
            error!(
                "CSV format error while merging from chunk {}: expected {} fields, found {} fields. Record: {:?}",
                source_index,
                headers.len(),
                record.len(),
                record
            );
            skipped_record_count += 1;
            continue;
        }
        writer.write_record(record.iter())?;
        merged_record_count += 1;
    }
    info!(
        "Merge complete: output {:?}, total merged records: {}, skipped: {}",
        output_path, merged_record_count, skipped_record_count
    );
    writer.flush()?;
    debug!("merge_chunks finished in: {:?}", t0.elapsed());
    Ok(())
}

/// Merges multiple sorted chunk files into a single sorted output file in parallel using a k-way merge approach.
///
/// # Arguments
///
/// * `chunk_files` - A vector of `PathBuf` objects representing the paths to the sorted chunk files to merge.
/// * `output_path` - A reference to a `Path` where the resulting merged file will be written.
/// * `sort_columns` - A slice of strings that specifies the columns which determine the order of sorting in the merged file.
/// * `k` - The number of files to include in each merge group during the k-way merge process.
///
/// # Environment Variables
///
/// * `MERGE_PARALLEL_GROUPS` - Optional environment variable that determines the number of parallel groups for merging. If not set, defaults to `4`.
///
/// # Process
///
/// 1. If the number of chunk files exceeds `k * MERGE_PARALLEL_GROUPS`, the files are divided into multiple parallel groups for merging.
/// 2. Each group is merged in parallel into a temporary file.
/// 3. The temporary files created from the first pass are merged again in rounds until only one file remains.
/// 4. Temporary files are deleted after each round of merging to save disk space.
/// 5. Finally, the last remaining file is renamed to the specified `output_path`.
///
/// # Parallelism
///
/// The merging process is performed in parallel within groups using the `par_iter` from the `rayon` crate. Each merge operation within a group is performed concurrently to speed up the process.
///
/// # Errors
///
/// This function returns a `Result<()>`:
/// * Returns `Ok(())` if the merging process completes successfully.
/// * Returns an error if any file operation (e.g., reading, writing, deleting a file) fails, or if merging fails.
///
/// # Logging
///
/// This function emits debug logs to provide insights into the merge process:
/// * Logs the start and end of the merging process, including elapsed time.
/// * Logs the number of chunk files and `k` value at the start of the merge.
/// * Logs file deletions and any errors encountered while deleting temporary files.
///
/// # Debugging
///
/// * The function logs the time taken to perform the merging operation when debug mode is enabled.
///
pub fn parallel_merge_chunks(
    mut chunk_files: Vec<PathBuf>,
    output_path: &Path,
    sort_columns: &[&str],
    k: usize, // new parameter for k-way merge
) -> Result<()> {
    let t0 = Instant::now();
    let mut round = 0;
    let parent_dir = output_path.parent().unwrap_or_else(|| Path::new("."));
    debug!(
        "Starting parallel merge with {} chunks (k-way: {})",
        chunk_files.len().to_formatted_string(&Locale::en),
        k
    );
    let parallel_groups = std::env::var("MERGE_PARALLEL_GROUPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4);
    while chunk_files.len() > 1 {
        // If this is the last round and chunk_files > k*parallel_groups, do parallel group merge for more CPU usage
        let groups: Vec<_> = if chunk_files.len() > k * parallel_groups {
            // Split into parallel_groups groups, each with ~chunk_files.len()/parallel_groups files
            let group_size = (chunk_files.len() + parallel_groups - 1) / parallel_groups;
            chunk_files.chunks(group_size).map(|g| g.to_vec()).collect()
        } else {
            chunk_files.chunks(k).map(|g| g.to_vec()).collect()
        };
        let results: Vec<anyhow::Result<PathBuf>> = groups
            .par_iter()
            .enumerate()
            .map(|(idx, group)| {
                if group.len() > 1 {
                    let out = parent_dir.join(format!("merge_round{}_{}.csv", round, idx));
                    merge_chunks(&group, &out, sort_columns)?;
                    Ok(out)
                } else {
                    Ok(group[0].clone())
                }
            })
            .collect();
        let mut merged_files = Vec::new();
        let mut to_delete = Vec::new();
        for (i, res) in results.into_iter().enumerate() {
            let merged = res?;
            let group = &groups[i];
            if group.len() > 1 {
                for f in group {
                    to_delete.push(f.clone());
                }
            }
            merged_files.push(merged);
        }
        // Delete temp files immediately after round
        for f in &to_delete {
            if let Err(e) = std::fs::remove_file(f) {
                warn!("Failed to delete temp file {:?}: {}", f, e);
            } else {
                debug!("Deleted temp file: {:?}", f);
            }
        }
        chunk_files = merged_files;
        round += 1;
    }
    std::fs::rename(&chunk_files[0], output_path)?;
    debug!("parallel_merge_chunks finished in: {:?}", t0.elapsed());
    Ok(())
}

/// Performs a parallel merge sort on multiple input files, producing a single sorted output file.
///
/// # Arguments
/// * `input_paths` - A slice of `PathBuf` representing the paths to the input files that need to be sorted.
/// * `output_path` - The path to the output file where the sorted data will be written.
/// * `sort_columns` - A slice of strings representing the column names by which the data should be sorted.
///
/// # Environment Variables
/// * `CHUNK_SIZE_MB` - Optional. Specifies the size (in MB) of chunks the input files will be split into during the split phase. Defaults to `256` if not provided or invalid.
/// * `MERGE_K` - Optional. Specifies the number of files to merge at a time in the k-way merge phase. Defaults to `2` if not provided, invalid, or less than `2`.
///
/// # Process
/// 1. Validate that all input files have consistent headers.
/// 2. Split the input files into smaller chunks in parallel, based on the chunk size.
/// 3. Perform a k-way merge of the chunks in parallel, producing the final sorted output file.
///
/// # Returns
/// * `Ok(())` if the operation completes successfully.
/// * `Err(anyhow::Error)` if any step encounters an error (e.g., no input files, invalid headers, file system failures).
///
/// # Errors
/// * Returns an error if no input files are provided.
/// * Returns an error if the headers across input files are inconsistent.
/// * Any I/O or parsing failures during the process will also result in an error.
///
/// # Notes
/// * The headers for all input files must match for the sorting to proceed.
/// * The number of files in the k-way merge phase (`MERGE_K`) is determined dynamically based on the environment variable or defaults to `2`.
/// * Temporary intermediate files are created during the split and merge phases and are automatically cleaned up after completion or failure.
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use anyhow::Result;
///
/// fn main() -> Result<()> {
///     let input_files = vec![PathBuf::from("file1.csv"), PathBuf::from("file2.csv")];
///     let output_file = "sorted_output.csv";
///     let sort_columns = vec!["column1", "column2"];
///
///     parallel_merge_sort(&input_files, output_file, &sort_columns)?;
///
///     Ok(())
/// }
/// ```
///
/// # Internal Parallelism
/// - The function uses a Rayon thread pool with the number of threads matching the system's logical CPU count.
/// - Parallelism ensures that writing and sorting large chunks is efficient without blocking the main thread.
///
/// # Logging
/// - The function emits debug logs to provide insights into the merge process:
///   - Logs the start and end of the merging process, including elapsed time.
///   - Logs the number of chunk files and `k` value at the start of the merge.
///   - Logs file deletions and any errors encountered while deleting temporary files.
///
/// # Debugging
/// - The function logs the time taken to perform the merging operation when debug mode is enabled.
///
pub fn parallel_merge_sort(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[&str],
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }
    let headers = validate_headers(input_paths)?;
    info!(
        "Validated headers across all input files: {:?}",
        headers.iter().collect::<Vec<_>>()
    );
    info!(
        "Starting parallel merge sort for {} files",
        input_paths.len().to_formatted_string(&Locale::en)
    );
    let temp_dir = TempDir::new()?;
    let total_start = Instant::now();
    let split_start = Instant::now();
    let chunk_size_mb = std::env::var("CHUNK_SIZE_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256);
    let all_chunks: Vec<PathBuf> = input_paths
        .par_iter()
        .map(|path| parallel_split_file_to_chunks(path, &temp_dir, sort_columns, chunk_size_mb, &headers))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    info!("Split phase finished in: {:?}", split_start.elapsed());

    info!("Starting merge phase...");
    let merge_start = Instant::now();
    // Default k=2 for backward compatibility, or get from env
    let k = match std::env::var("MERGE_K") {
        Ok(val) => match val.parse::<usize>() {
            Ok(parsed) if parsed >= 2 => parsed,
            _ => {
                warn!("MERGE_K is set but invalid ({}), using default k=2", val);
                2
            }
        },
        Err(_) => 2,
    };
    info!("Using k-way merge: k={}", k);
    parallel_merge_chunks(all_chunks, output_path.as_ref(), sort_columns, k)?;
    info!("Merge phase finished in: {:?}", merge_start.elapsed());

    info!("Total merge+sort finished in: {:?}", total_start.elapsed());
    Ok(())
}
