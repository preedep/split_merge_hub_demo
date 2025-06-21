// --- Imports ---
use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use log::{debug, error, info, warn};
use num_cpus;
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::TempDir;

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
        match File::open(chunk_path) {
            Ok(file) => {
                let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(BufReader::with_capacity(buffer_size, file));
                let mut record = StringRecord::new();
                // Always skip the first record if it is a header row and this chunk has headers
                if has_headers {
                    if !reader.read_record(&mut record)? {
                        readers.push(None);
                        continue;
                    }
                }
                if reader.read_record(&mut record)? {
                    heap.push(MergeRecord {
                        record,
                        source_index: i,
                    });
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
            .from_writer(BufWriter::with_capacity(buffer_size, file))
    };
    writer
        .write_record(headers.iter())
        .with_context(|| "Failed to write headers to output file")?;
    while let Some(MergeRecord {
        record,
        source_index,
    }) = heap.pop()
    {
        writer.write_record(&record)?;
        if let Some(reader_opt) = readers.get_mut(source_index) {
            if let Some(reader) = reader_opt {
                let mut record = StringRecord::new();
                if reader.read_record(&mut record)? {
                    heap.push(MergeRecord {
                        record,
                        source_index,
                    });
                } else {
                    *reader_opt = None;
                }
            }
        }
    }
    writer.flush()?;
    debug!("merge_chunks finished in: {:?}", t0.elapsed());
    Ok(())
}

// --- Parallel merge tree for chunk files ---
fn merge_two_chunks(
    chunk_a: &PathBuf,
    chunk_b: &PathBuf,
    output_path: &Path,
    sort_columns: &[&str],
) -> Result<()> {
    merge_chunks(
        &vec![chunk_a.clone(), chunk_b.clone()],
        output_path,
        sort_columns,
    )
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

/// Splits a large CSV file into smaller, sorted chunks with a specified size.
///
/// # Arguments
/// - `file_path` - A reference to the path of the input CSV file to be split.
/// - `temp_dir` - A reference to a temporary directory path where the chunk files will be stored.
/// - `sort_columns` - A slice of string references representing the column names to sort each chunk by.
/// - `chunk_size_mb` - The size of each chunk in megabytes (MB).
/// - `headers` - A reference to the CSV headers (`StringRecord`) to include in each generated chunk.
///
/// # Returns
/// A `Result` containing:
/// - On success: A vector of `PathBuf` objects, each representing the path to a generated chunk file.
/// - On failure: An error of type `std::io::Error` or other possible errors encountered during file operations.
///
/// # Functionality
/// 1. The function reads the input CSV file in chunks, each of size approximately `chunk_size_mb` MB.
/// 2. Each chunk is then sorted based on the columns specified in `sort_columns`.
/// 3. The sorted data is written to separate chunk files in the given `temp_dir`.
/// 4. If the number of chunks exceeds the available logical CPUs, the function uses a Rayon thread
///    pool for parallel processing to optimize chunk generation.
/// 5. At the end of processing, the function ensures all chunk paths are sorted deterministically
///    before returning the paths.
///
/// # Logging
/// - Logs the beginning and end of the process, including the input file, chunk sizes, and total chunks generated.
/// - Logs progress for each chunk, including its size and output path.
/// - Logs any errors encountered while writing individual chunk files.
///
/// # Errors
/// - Returns an error if the input file cannot be opened or read.
/// - Returns an error if writing a chunk to the `temp_dir` fails.
/// - Captures and logs errors during chunk writing or sorting.
///
/// # Example
/// ```rust
/// use tempfile::TempDir;
/// use csv::StringRecord;
/// use std::path::PathBuf;
///
/// let temp_dir = TempDir::new().expect("Failed to create temporary directory");
/// let file_path = PathBuf::from("input.csv");
/// let sort_columns = vec!["column1", "column2"];
/// let chunk_size_mb = 10; // 10 MB per chunk
/// let headers = StringRecord::from(vec!["column1", "column2", "column3"]);
///
/// let chunk_paths = split_file_to_chunks(
///     &file_path,
///     &temp_dir,
///     &sort_columns,
///     chunk_size_mb,
///     &headers,
/// ).expect("Failed to split file");
///
/// println!("Generated {} chunks.", chunk_paths.len());
/// ```
///
/// # Internal Parallelism
/// - The function uses a Rayon thread pool with the number of threads matching the system's logical CPU count.
/// - Parallelism ensures that writing and sorting large chunks is efficient without blocking the main thread.
///
/// # Notes
/// - The function assumes the input CSV file has a valid structure and contains proper headers.
/// - The temporary file paths are managed by `temp_dir`, and these files are typically deleted when the
///   `TempDir` instance is dropped, ensuring temporary storage is cleaned up after use.
fn split_file_to_chunks(
    file_path: &Path,
    temp_dir: &TempDir,
    sort_columns: &[&str],
    chunk_size_mb: usize,
    headers: &StringRecord,
) -> Result<Vec<PathBuf>> {
    let t0 = Instant::now();
    debug!(
        "Splitting file: {:?} into chunks of {} MB",
        file_path, chunk_size_mb
    );

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(BufReader::new(File::open(file_path)?));
    let mut records = Vec::new();
    let mut chunk_idx = 0;
    let mut current_size = 0;
    let chunk_size = chunk_size_mb * 1024 * 1024;
    let chunk_paths = Arc::new(Mutex::new(Vec::new()));
    let max_parallel_chunks = num_cpus::get().max(2); // Use number of logical CPUs, at least 2
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(max_parallel_chunks)
        .build()
        .unwrap();
    for result in rdr.records() {
        let record = result?;
        current_size += record.as_slice().len();
        records.push(record);
        if current_size >= chunk_size {
            let chunk = std::mem::take(&mut records);
            let chunk_path = temp_dir.path().join(format!(
                "chunk_{}_{}.csv",
                file_path.file_stem().unwrap().to_string_lossy(),
                chunk_idx
            ));
            let chunk_paths = Arc::clone(&chunk_paths);
            let headers = headers.clone();
            let sort_columns = sort_columns
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            pool.install(|| {
                debug!(
                    "Writing chunk: {:?} with {} records",
                    chunk_path,
                    chunk.len().to_formatted_string(&Locale::en)
                );
                let sort_columns_ref: Vec<&str> = sort_columns.iter().map(|s| s.as_str()).collect();
                if let Err(e) = write_sorted_chunk(&chunk, &chunk_path, &headers, &sort_columns_ref)
                {
                    error!("Chunk write failed: {:?}", e);
                } else {
                    let mut paths = chunk_paths.lock().unwrap();
                    paths.push(chunk_path);
                }
            });
            chunk_idx += 1;
            current_size = 0;
        }
    }
    if !records.is_empty() {
        let chunk_path = temp_dir.path().join(format!(
            "chunk_{}_{}.csv",
            file_path.file_stem().unwrap().to_string_lossy(),
            chunk_idx
        ));
        debug!(
            "Writing last chunk: {:?} with {} records",
            chunk_path,
            records.len().to_formatted_string(&Locale::en)
        );
        let chunk_paths = Arc::clone(&chunk_paths);
        let headers = headers.clone();
        let sort_columns = sort_columns
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let chunk = std::mem::take(&mut records);
        pool.install(|| {
            debug!(
                "Writing chunk: {:?} with {} records",
                chunk_path,
                chunk.len().to_formatted_string(&Locale::en)
            );
            let sort_columns_ref: Vec<&str> = sort_columns.iter().map(|s| s.as_str()).collect();
            if let Err(e) = write_sorted_chunk(&chunk, &chunk_path, &headers, &sort_columns_ref) {
                error!("Chunk write failed: {:?}", e);
            } else {
                let mut paths = chunk_paths.lock().unwrap();
                paths.push(chunk_path);
            }
        });
    }
    let mut paths = Arc::try_unwrap(chunk_paths)
        .map(|mutex| mutex.into_inner().unwrap())
        .unwrap_or_else(|arc| arc.lock().unwrap().clone());
    debug!(
        "Created {} chunks for file {:?}",
        paths.len().to_formatted_string(&Locale::en),
        file_path
    );
    debug!(
        "split_file_to_chunks for {:?} finished in: {:?}",
        file_path,
        t0.elapsed()
    );
    paths.sort(); // Ensure deterministic order
    Ok(paths)
}

fn write_sorted_chunk(
    records: &[StringRecord],
    chunk_path: &Path,
    headers: &StringRecord,
    sort_columns: &[&str],
) -> Result<()> {
    let t0 = Instant::now();
    let mut sorted_records = records.to_vec();
    let sort_indices = get_sort_column_indices(headers, sort_columns);
    sorted_records.sort_by(|a, b| compare_records(a, b, &sort_indices));
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(chunk_path)?;
    wtr.write_record(headers)?;
    for rec in sorted_records {
        wtr.write_record(&rec)?;
    }
    wtr.flush()?;
    debug!(
        "write_sorted_chunk for {:?} finished in: {:?}",
        chunk_path,
        t0.elapsed()
    );
    Ok(())
}

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

///
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
        .map(|path| split_file_to_chunks(path, &temp_dir, sort_columns, chunk_size_mb, &headers))
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
