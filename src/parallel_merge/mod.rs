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
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

// --- MergeRecord struct for heap ---
#[derive(Debug)]
struct MergeRecord {
    record: StringRecord,
    source_index: usize,
    sort_indices: Arc<Vec<usize>>,
}

impl Ord for MergeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // ใช้ compare_records ตาม sort_indices
        compare_records(&self.record, &other.record, &self.sort_indices).reverse()
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
        let a_val = a.get(idx).unwrap_or("");
        let b_val = b.get(idx).unwrap_or("");
        // Try numeric comparison first
        let ord = match (a_val.parse::<i64>(), b_val.parse::<i64>()) {
            (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
            _ => a_val.cmp(b_val),
        };
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

/// Splits a large CSV file into multiple smaller chunks, processes them in parallel, and sorts the records
/// within each chunk based on specified columns. The processed chunks are temporarily stored as individual files.
///
/// # Parameters
/// - `file_path`: Path to the input CSV file that needs to be split and processed.
/// - `temp_dir`: A temporary directory where the chunk files will be written.
/// - `sort_columns`: A slice of column names that should be used to sort the records within each chunk.
/// - `chunk_size_mb`: The desired size (in megabytes) of each chunk.
/// - `headers`: The headers row of the input CSV file, represented as a `StringRecord`.
///
/// # Returns
/// A `Result` containing a vector of paths (`Vec<PathBuf>`) to the generated chunk files if successful,
/// or an error if the operation fails at any step.
///
/// # Behavior
/// - Reads and validates the input CSV file.
/// - Splits the file into chunks based on the desired size, ensuring that field lengths are taken into account.
/// - Sorts the records within each chunk based on the specified columns.
/// - Writes the sorted records of each chunk into separate CSV files in the provided temporary directory.
/// - Processes the chunks in parallel for efficiency.
///
/// # Logging
/// - Comprehensive log messages provide detailed insights, including:
///   - Pre-scan details (file size, record count, chunk count, etc.).
///   - Processing status for each chunk (e.g., sorting time, writing time, total processing time).
///   - Examples of first/last few rows in each chunk for debugging.
/// - Logs errors related to inconsistent record lengths or parsing issues in the input CSV.
///
/// # Heuristics
/// - The chunk size is determined based on both the specified size in MB and an estimate of ~16 bytes per field.
/// - Ensures that each chunk contains at least one record to prevent empty chunks.
///
/// # Threading
/// - Uses parallel processing (`rayon::into_par_iter()`) to distribute chunks across threads for faster processing.
///
/// # Errors
/// Returns an error in cases such as:
/// - File access issues (e.g., file not found, permission errors).
/// - CSV parsing or format inconsistencies.
/// - Issues with writing chunk files to the temporary directory.
///
/// # Example
/// ```rust
/// use csv::StringRecord;
/// use std::path::Path;
/// use tempfile::TempDir;
///
/// // Assume `headers` and other variables are initialized
/// let file_path = Path::new("large_file.csv");
/// let temp_dir = TempDir::new().unwrap();
/// let sort_columns = &["column1", "column2"];
/// let chunk_size_mb = 10; // 10 MB chunks
/// let headers = StringRecord::from(vec!["column1", "column2", "column3"]);
///
/// let result = parallel_split_file_to_chunks(
///     &file_path,
///     &temp_dir,
///     sort_columns,
///     chunk_size_mb,
///     &headers,
/// );
///
/// match result {
///     Ok(paths) => println!("Chunks created: {:?}", paths),
///     Err(e) => eprintln!("Error splitting file: {:?}", e),
/// }
/// ```
///
/// # Dependencies
/// - `csv`: For reading and writing CSV files.
/// - `rayon`: For parallel processing.
/// - `tempfile`: For managing temporary directories.
/// - `log`: For logging information, warnings, and errors.
///
/// # Notes
/// - The function assumes that the input CSV file contains headers.
/// - The temporary files will remain in the `temp_dir` until manually cleaned up.
/// - Sorting relies on the specified `sort_columns`, and all sort column names must exist in `headers`.
///
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
    let prescan_start = Instant::now();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    let all_records: Vec<StringRecord> = rdr
        .records()
        .filter_map(|r| match r {
            Ok(rec) if rec.len() == headers.len() => Some(rec),
            Ok(rec) => {
                error!(
                    "CSV format error: expected {} fields, found {} fields. Record: {:?}",
                    headers.len(),
                    rec.len(),
                    rec
                );
                None
            }
            Err(e) => {
                error!("CSV parse error: {}", e);
                None
            }
        })
        .collect();
    let prescan_elapsed = prescan_start.elapsed();
    let chunk_size = chunk_size_mb * 1024 * 1024 / (headers.len() * 16).max(1); // heuristic: ~16 bytes per field
    let chunk_size = chunk_size.max(1);
    let chunk_count = (all_records.len() + chunk_size - 1) / chunk_size;
    info!(
        "[split] Pre-scan complete. File: {:?}, Size: {} bytes, Records: {}, Chunks: {}, ChunkSize: {} (records), Pre-scan Time: {:.2?}",
        file_path, fmtnum(file_size), fmtnum(all_records.len()), fmtnum(chunk_count), fmtnum(chunk_size), prescan_elapsed
    );
    info!(
        "[split] Processing {} chunks in parallel...",
        fmtnum(chunk_count)
    );
    let chunk_timer = Instant::now();
    let chunk_paths: Result<Vec<PathBuf>> = (0..chunk_count).into_par_iter().map(|i| -> Result<PathBuf> {
        let chunk_start_time = Instant::now();
        let start = i * chunk_size;
        let end = ((i + 1) * chunk_size).min(all_records.len());
        let mut records = all_records[start..end].to_vec();
        let sort_indices = get_sort_column_indices(headers, sort_columns);
        info!("[SPLIT] Using sort columns: {:?} (indices: {:?})", sort_columns, sort_indices);
        let sort_start = Instant::now();
        records.sort_by(|a, b| compare_records(a, b, &sort_indices));
        let sort_elapsed = sort_start.elapsed();
        if !records.is_empty() {
            debug!("[SPLIT] Chunk {} first 3 rows: {:?}", i, &records.iter().take(3).collect::<Vec<_>>());
            debug!("[SPLIT] Chunk {} last 3 rows: {:?}", i, &records.iter().rev().take(3).collect::<Vec<_>>());
        }
        let file_stem = file_path.file_stem().and_then(|s| s.to_str()).unwrap_or("input");
        let chunk_path = temp_dir.path().join(format!("chunk_parallel_{}_{}.csv", file_stem, i));
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
            "[split] Chunk {}/{} | Records: {} | Path: {:?} | Sort: {:.2?} | Write: {:.2?} | Total: {:.2?}",
            i + 1, fmtnum(chunk_count), fmtnum(records.len()), chunk_path, sort_elapsed, write_elapsed, chunk_elapsed
        );
        Ok(chunk_path)
    }).collect();
    let chunk_total_elapsed = chunk_timer.elapsed();
    let chunk_count = match &chunk_paths {
        Ok(paths) => paths.len(),
        Err(_) => 0,
    };
    info!(
        "[split] ALL DONE. File: {:?}, Size: {} bytes, Chunks: {}, Time: {:.2?}",
        file_path,
        fmtnum(file_size),
        fmtnum(chunk_count),
        chunk_total_elapsed
    );
    chunk_paths
}

/// This function performs a parallel merge sort on large files, splitting them into manageable chunks, sorting them based on specified columns, 
/// and merging them into a single sorted output file.
///
/// # Arguments
///
/// * `input_paths` - A slice of [`PathBuf`] representing the paths of input files to be sorted.
/// * `output_path` - A path to the file where the final sorted output will be written.
/// * `sort_columns` - A slice of string slices representing the columns to sort by.
///
/// # Returns
///
/// * `Result<()>` - Returns `Ok(())` if the sorting operation completes successfully, or an error if an issue occurs during the process.
///
/// # Functionality
///
/// 1. **Input Validation:**
///     - Checks if input files are provided; returns an error if the list is empty.
///     - Validates the headers across all input files to ensure consistency.
///
/// 2. **Parallel Chunk Splitting:**
///     - Splits each input file deterministically into smaller chunks based on the specified column(s) and a configurable chunk size (default is 256 MB).
///     - Stores the intermediate chunks in a temporary directory.
///
/// 3. **Parallel Sorting Within Chunks:**
///     - Sorts each chunk independently based on the specified sort column(s).
///     - Ensures deterministic sorting by sorting chunk paths to fix merging order.
///
/// 4. **K-way Merge Phase:**
///     - Performs a k-way merge on the sorted chunks to produce the final sorted output.
///     - The value of `k` can be configured through the `MERGE_K` environment variable (default is 2).
///
/// 5. **Result Output:**
///     - Writes the sorted data into the specified `output_path`.
///     - Logs timing information for each phase of the operation.
///
/// # Environment Variables
///
/// * `CHUNK_SIZE_MB` - Defines the size of each chunk in megabytes during the split phase (default: 256 MB).
/// * `MERGE_K` - Defines the number of chunks to merge at a time during the merge phase (default: 2; minimum: 2).
///
/// # Errors
///
/// This function may return errors in the following cases:
/// * If no input files are provided.
/// * If input file headers are inconsistent across files.
/// * If issues occur during file operations such as reading, writing, or temporary directory creation.
/// * If there are exceptions in the splitting, sorting, or merging phases.
///
/// # Logging
///
/// During its execution, the function logs useful information such as:
/// * Validated headers across input files.
/// * Timing information for different phases (split, merge, total duration).
/// * The `k` value used for k-way merging.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// let input_files = vec![PathBuf::from("file1.csv"), PathBuf::from("file2.csv")];
/// let output_file = PathBuf::from("sorted_output.csv");
/// let sort_columns = vec!["column_name"];
///
/// parallel_merge_sort(&input_files, output_file, &sort_columns).expect("Sorting failed");
/// ```
#[allow(dead_code)]
pub fn parallel_merge_sort(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[&str],
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!("No input files provided"));
    }
    let mut input_paths_sorted = input_paths.to_vec();
    input_paths_sorted.sort_by_key(|p| p.to_string_lossy().to_string());
    let headers = validate_headers(&input_paths_sorted)?;
    info!(
        "Validated headers across all input files: {:?}",
        headers.iter().collect::<Vec<_>>()
    );
    info!(
        "Starting parallel merge sort for {} files",
        fmtnum(input_paths_sorted.len())
    );
    let temp_dir = TempDir::new()?;
    let total_start = Instant::now();
    let split_start = Instant::now();
    let chunk_size_mb = std::env::var("CHUNK_SIZE_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256);
    // Split each input file deterministically and collect chunks in same order
    let chunk_lists: Vec<_> = input_paths_sorted
        .iter()
        .map(|path| {
            parallel_split_file_to_chunks(path, &temp_dir, sort_columns, chunk_size_mb, &headers)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut all_chunks: Vec<PathBuf> = chunk_lists.into_iter().flatten().collect();
    // Sort chunk paths for deterministic merge order
    all_chunks.sort_by_key(|p| p.to_string_lossy().to_string());
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
    info!("Using k-way merge: k={}", fmtnum(k));
    parallel_merge_chunks(all_chunks, output_path.as_ref(), sort_columns, k)?;
    info!("Merge phase finished in: {:?}", merge_start.elapsed());

    info!("Total merge+sort finished in: {:?}", total_start.elapsed());
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MTLogSortType {
    Date,
    Time,
    Num,
    Str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MTLogSortColumn {
    pub index: usize,
    pub col_type: MTLogSortType,
}

/// Performs a parallel merge sort on multiple MT log files.
///
/// This function reads multiple input files, splits the data into chunks, 
/// sorts the chunks in parallel, and then performs a k-way merge to create
/// a single sorted output file. It is designed for large-scale log processing 
/// and can efficiently handle millions of records.
///
/// # Parameters
/// - `input_paths`: A slice of `PathBuf` representing the paths to the input files to be merged.
/// - `output_path`: The path to the output file where the merged and sorted logs will be written.
/// - `sort_columns`: A slice of `MTLogSortColumn` which defines the sort key(s) and order for the records.
///
/// # Environment Variables
/// - `CHUNK_RECORDS` (optional): Specifies the number of records per chunk for parallel processing.
///   Defaults to 1,000,000 if unset.
///
/// # Process
/// 1. **Chunk Splitting and Sorting:**
///     - Reads input files line by line.
///     - Groups records into chunks based on `CHUNK_RECORDS`.
///     - Each chunk is sorted based on the `sort_columns` using parallel processing.
///     - Temporary files are created for each sorted chunk.
///
/// 2. **K-Way Merge:**
///     - Merges all sorted chunks into one sorted output file using a binary heap for efficiency.
///     - Continuously writes merged records to the output file.
///
/// 3. **Logging:**
///     - Logs progress during chunk creation, sorting, and merging.
///     - Provides elapsed time and record count statistics for each step.
///
/// # Returns
/// - `Ok(())` if the process completes successfully.
/// - `Err(anyhow::Error)` if any error occurs during the process, such as I/O failures.
///
/// # Errors
/// Possible errors include:
/// - Missing or invalid input files in `input_paths`.
/// - I/O errors when reading from input files, writing to temporary chunk files, or writing the output file.
/// - Sorting logic errors due to malformed input or incorrect column specifications.
///
/// # Example Usage
/// ```rust
/// use std::path::PathBuf;
/// use my_crate::{parallel_merge_sort_mtlog, MTLogSortColumn};
///
/// let input_paths = vec![PathBuf::from("log1.txt"), PathBuf::from("log2.txt")];
/// let output_path = PathBuf::from("merged_log.txt");
/// let sort_columns = vec![MTLogSortColumn::Timestamp, MTLogSortColumn::Severity];
///
/// parallel_merge_sort_mtlog(&input_paths, output_path, &sort_columns).unwrap();
/// ```
///
/// # Performance
/// - The function uses multithreading and parallel sorting to handle large input efficiently.
/// - Sorting is performed using `par_sort_unstable`, and the k-way merge is optimized with a binary heap.
///
/// # Notes
/// - Ensure the input files are compatible with the expected format and sorting criteria.
/// - Temporary files created during the process are automatically removed after merging completes.
///
/// # Dependencies
/// - `rayon`: Used for parallel sorting.
/// - `tempfile`: Used for creating and managing temporary chunk files.
/// - `log`: Used for logging the progress of the operation.
pub fn parallel_merge_sort_mtlog(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[MTLogSortColumn],
) -> Result<()> {
    use std::io::{BufRead, BufReader, BufWriter, Write};
    use std::fs::File;
    use std::collections::BinaryHeap;
    use std::cmp::Ordering;
    use std::time::Instant;
    
    let total_timer = Instant::now();
    if input_paths.is_empty() {
        log::warn!("[mtlog] No input files provided for MT log merge");
        return Err(anyhow::anyhow!("No input files provided"));
    }
    log::info!("[mtlog] Starting parallel chunked merge of {} files into {:?}", input_paths.len().to_formatted_string(&Locale::en), output_path.as_ref());

    // 1. Split to sorted chunks in parallel (by number of records)
    let chunk_records = std::env::var("CHUNK_RECORDS").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1_000_000);
    log::info!("[mtlog] Chunk size: {} records", chunk_records.to_formatted_string(&Locale::en));
    let mut chunk_files = Vec::new();
    let mut all_lines = Vec::new();
    let mut cur_records = 0;
    let chunk_timer = Instant::now();
    let mut total_records: usize = 0;
    for path in input_paths {
        log::info!("[mtlog] Reading input file: {}", path.display());
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            cur_records += 1;
            total_records += 1;
            all_lines.push(line);
            if cur_records >= chunk_records {
                let mut chunk = std::mem::take(&mut all_lines);
                cur_records = 0;
                let sort_timer = Instant::now();
                chunk.par_sort_unstable_by(|a, b| compare_mtlog_by_columns(a, b, sort_columns));
                log::info!("[mtlog] Sorted chunk of {} records in {:.2?}", chunk.len().to_formatted_string(&Locale::en), sort_timer.elapsed());
                let mut tmp = tempfile::NamedTempFile::new()?;
                for l in &chunk { writeln!(tmp, "{}", l)?; }
                chunk_files.push(tmp.into_temp_path());
                log::info!("[mtlog] Wrote sorted chunk file #{} ({} records)", chunk_files.len().to_formatted_string(&Locale::en), chunk.len().to_formatted_string(&Locale::en));
            }
        }
    }
    if !all_lines.is_empty() {
        let sort_timer = Instant::now();
        all_lines.par_sort_unstable_by(|a, b| compare_mtlog_by_columns(a, b, sort_columns));
        log::info!("[mtlog] Sorted final chunk of {} records in {:.2?}", all_lines.len().to_formatted_string(&Locale::en), sort_timer.elapsed());
        let mut tmp = tempfile::NamedTempFile::new()?;
        for l in &all_lines { writeln!(tmp, "{}", l)?; }
        chunk_files.push(tmp.into_temp_path());
        log::info!("[mtlog] Wrote sorted chunk file #{} ({} records)", chunk_files.len().to_formatted_string(&Locale::en), all_lines.len().to_formatted_string(&Locale::en));
    }
    log::info!("[mtlog] {} sorted chunk files created in {:.2?}", chunk_files.len().to_formatted_string(&Locale::en), chunk_timer.elapsed());
    log::info!("[mtlog] Total input records: {}", total_records.to_formatted_string(&Locale::en));

    // 2. K-way merge sorted chunks
    let merge_timer = Instant::now();
    let mut writer = BufWriter::new(File::create(output_path.as_ref())?);
    let mut readers: Vec<_> = chunk_files.iter().map(|p| BufReader::new(File::open(p).unwrap())).collect();
    let mut heap = BinaryHeap::new();
    let mut merged_count = 0usize;
    // Prime heap
    for (idx, rdr) in readers.iter_mut().enumerate() {
        let mut buf = String::new();
        if rdr.read_line(&mut buf)? > 0 {
            let line = buf.trim_end_matches('\n').to_string();
            heap.push(MTLogHeapItem { line, idx, sort_columns });
        }
    }
    while let Some(MTLogHeapItem { line, idx, .. }) = heap.pop() {
        writeln!(writer, "{}", line)?;
        merged_count += 1;
        if merged_count % 500_000 == 0 {
            log::info!("[mtlog] Merged {} records so far...", merged_count.to_formatted_string(&Locale::en));
        }
        let rdr = &mut readers[idx];
        let mut buf = String::new();
        if rdr.read_line(&mut buf)? > 0 {
            let next_line = buf.trim_end_matches('\n').to_string();
            heap.push(MTLogHeapItem { line: next_line, idx, sort_columns });
        }
    }
    writer.flush()?;
    log::info!("[mtlog] K-way merge complete: {} records merged in {:.2?}", merged_count.to_formatted_string(&Locale::en), merge_timer.elapsed());
    log::info!("[mtlog] Merge complete: chunked+sorted into {:?} in {:.2?}", output_path.as_ref(), total_timer.elapsed());
    Ok(())
}

// Heap item for k-way merge of sorted MT log chunk files
#[derive(Eq)]
struct MTLogHeapItem<'a> {
    line: String,
    idx: usize,
    sort_columns: &'a [MTLogSortColumn],
}

impl<'a> Ord for MTLogHeapItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reverse for min-heap
        compare_mtlog_by_columns(&other.line, &self.line, self.sort_columns)
    }
}
impl<'a> PartialOrd for MTLogHeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> PartialEq for MTLogHeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.line == other.line
    }
}

/// Compares two MTLog entries represented as strings (`a` and `b`) based on the specified sort columns.
///
/// This function iterates through the provided list of `sort_columns` to determine the ordering comparison
/// between the two MTLog entries. Each `MTLogSortColumn` in the list specifies both the column index 
/// and the type of sorting to apply (e.g., Date, Time, Num, Str). The order of the `sort_columns` array 
/// determines the precedence for comparison.
///
/// # Arguments
///
/// * `a` - A string slice representing the first MTLog entry to compare.
/// * `b` - A string slice representing the second MTLog entry to compare.
/// * `sort_columns` - A slice of `MTLogSortColumn` structs specifying which columns and sorting 
///   types (e.g., Date, Time, Num, Str) to use for comparison.
///
/// # Returns
///
/// A `std::cmp::Ordering` value:
/// * `Ordering::Less` if `a` is smaller than `b` based on the sort columns.
/// * `Ordering::Greater` if `a` is greater than `b` based on the sort columns.
/// * `Ordering::Equal` if both `a` and `b` are considered equal based on the sort columns.
///
/// The function performs the following for each column in `sort_columns`:
/// 1. Extracts the values for both `a` and `b` using the `get_mtlog_field` function based on the column index.
/// 2. Compares the extracted values using the rules defined by the column's type:
///    - `MTLogSortType::Date` or `MTLogSortType::Time`: Lexicographical comparison of the strings.
///    - `MTLogSortType::Num`: Parsing to `u64` for comparison, with a fallback to `0` if parsing fails.
///    - `MTLogSortType::Str`: Lexicographical string comparison.
/// 3. If the comparison result for the current column is not equal, return the comparison result immediately.
/// 4. If all columns are equal, return `Ordering::Equal`.
///
/// # Panics
///
/// This function assumes that the `get_mtlog_field` function will provide valid field values
/// for the specified column indexes and may panic if an invalid index is used or if there's a 
/// logic issue in `get_mtlog_field`.
///
/// # Examples
///
/// ```rust
/// // Example usage of compare_mtlog_by_columns
/// let mtlog1 = "2023-09-15 12:00:00,INFO,User logged in";
/// let mtlog2 = "2023-09-15 12:05:00,INFO,User logged out";
/// let sort_columns = vec![
///     MTLogSortColumn { index: 0, col_type: MTLogSortType::Date },
///     MTLogSortColumn { index: 1, col_type: MTLogSortType::Time },
/// ];
///
/// let result = compare_mtlog_by_columns(mtlog1, mtlog2, &sort_columns);
/// assert_eq!(result, std::cmp::Ordering::Less);
/// ```
fn compare_mtlog_by_columns(a: &str, b: &str, sort_columns: &[MTLogSortColumn]) -> std::cmp::Ordering {
    for col in sort_columns {
        let (v1, v2) = (get_mtlog_field(a, col.index), get_mtlog_field(b, col.index));
        let ord = match col.col_type {
            MTLogSortType::Date | MTLogSortType::Time => v1.cmp(&v2),
            MTLogSortType::Num => v1.parse::<u64>().unwrap_or(0).cmp(&v2.parse::<u64>().unwrap_or(0)),
            MTLogSortType::Str => v1.cmp(&v2),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// ดึง field จาก fixed-width string ตาม column index
fn get_mtlog_field(line: &str, col: usize) -> String {
    // กำหนดตำแหน่งและความยาว field ตาม MTLogRecord (ตัวอย่าง: [start, len])
    // ต้องตรงกับโครงสร้างจริง!
    const OFFSETS: &[(usize, usize)] = &[
        (0,8),    // 0: milog_rec_sys_date
        (8,6),    // 1: milog_rec_sys_time
        (14,7),   // 2: milog_rec_taskno
        (21,4),   // 3: milog_channel_code
        (25,1),   // 4: milog_rec_rectype
        (26,8),   // 5: milog_ts_ext_tran_code
        (34,1),   // 6: milog_tran_type
        (35,1),   // 7: milog_record_status
        // ... เพิ่ม field อื่น ๆ ตามต้องการ ...
    ];
    if col >= OFFSETS.len() {
        return String::new();
    }
    let (start, len) = OFFSETS[col];
    line.get(start..start+len).unwrap_or("").trim().to_string()
}

/// K-way parallel merge of sorted chunk files into a single sorted output CSV.
/// - `chunk_paths`: paths to sorted chunk files (with header)
/// - `output_path`: path to final merged output file
/// - `sort_columns`: columns to sort by
/// - `k`: k-way merge factor
pub fn parallel_merge_chunks(
    chunk_paths: Vec<PathBuf>,
    output_path: &Path,
    sort_columns: &[&str],
    k: usize,
) -> Result<()> {
    use std::collections::VecDeque;
    use std::fs::File;
    use std::io::BufReader;

    if chunk_paths.is_empty() {
        return Err(anyhow::anyhow!("No chunk files to merge"));
    }
    info!(
        "[merge] Starting k-way merge: {} chunks -> {:?}",
        fmtnum(chunk_paths.len()),
        output_path
    );
    let merge_start = Instant::now();

    // Read headers from the first chunk
    let first_chunk = &chunk_paths[0];
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(first_chunk)?;
    let headers = rdr.headers()?.clone();
    drop(rdr);

    // Prepare output writer
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)?;
    // Write header only if this is the final output
    if output_path.extension().and_then(|s| s.to_str()) == Some("csv") {
        wtr.write_record(headers.iter())?;
    }

    // Determine sort indices
    let sort_indices = Arc::new(get_sort_column_indices(&headers, sort_columns));
    if sort_indices.is_empty() {
        warn!("No sort indices found, output will not be sorted");
    }

    // For large merges, do multi-pass k-way merge if chunk count > k
    let mut current_chunks = chunk_paths;
    let mut pass = 0;
    let mut _temp_dirs = Vec::new(); // <-- keep temp dirs alive
    while current_chunks.len() > 1 {
        pass += 1;
        let mut next_chunks = Vec::new();
        let temp_dir = tempfile::tempdir()?;
        _temp_dirs.push(temp_dir); // <-- keep temp_dir alive
        let temp_dir_ref = _temp_dirs.last().unwrap();
        let groups = current_chunks.chunks(k).enumerate();
        info!(
            "[merge] Merge pass {}: {} groups of up to {} files",
            pass,
            (current_chunks.len() + k - 1) / k,
            fmtnum(k)
        );

        for (group_idx, group) in groups {
            let out_path = temp_dir_ref
                .path()
                .join(format!("merge_pass{}_group{}.csv", pass, group_idx));
            info!(
                "[merge]   Group {}: merging {} files -> {:?}",
                group_idx,
                fmtnum(group.len()),
                out_path
            );
            merge_k_files(group, &out_path, &headers, &sort_indices)?;
            next_chunks.push(out_path);
        }
        current_chunks = next_chunks;
    }
    // Final merge (or only pass if <= k)
    let final_chunks = if current_chunks.is_empty() {
        vec![]
    } else {
        current_chunks
    };
    if !final_chunks.is_empty() {
        info!(
            "[merge] Final merge: {} files -> {:?}",
            fmtnum(final_chunks.len()),
            output_path
        );
        merge_k_files(&final_chunks, output_path, &headers, &sort_indices)?;
    }
    wtr.flush()?;
    info!(
        "[merge] Merge complete: {:?} in {:.2?}",
        output_path,
        merge_start.elapsed()
    );
    Ok(())
}

/// Merges multiple CSV files into a single sorted output file.
///
/// This function takes multiple input CSV file paths, reads their contents,
/// and merges them into a single CSV file while maintaining the specified
/// sorting order based on the given sort indices.
///
/// # Parameters
///
/// - `files`: A slice of `PathBuf` representing the paths to the input CSV files.
///   Each file is assumed to have a header row that will be skipped during merging.
/// - `output_path`: A reference to a `Path` where the merged output file should be created.
///   If the `output_path` has a `.csv` extension, headers will be written into the output.
/// - `headers`: A reference to a `StringRecord` representing the header row to be written
///   into the output file if applicable.
/// - `sort_indices`: An `Arc`-wrapped `Vec<usize>` specifying indices of the fields in each
///   record to use for sorting the data across files.
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error of type `Result` if the merging fails due to
/// any IO issues or CSV parsing errors.
///
/// # Behavior
///
/// 1. Reads each input file and skips its header row.
/// 2. Initializes a binary heap (`BinaryHeap`) to keep track of the order of records across files.
/// 3. Pushes the first record of each input file into the heap.
/// 4. Writes records into the output file in sorted order, using the `sort_indices` as a
///    reference for comparison.
/// 5. Continues to fetch and sort records from the input files until all records are processed.
/// 6. Writes the specified headers to the output file only if the file has a `.csv` extension.
///
/// # Errors
///
/// Returns an error if:
/// - Any input file cannot be opened.
/// - Headers cannot be written to the output file.
/// - Records cannot be read from the input files or written to the output file.
/// - The output file path cannot be initialized or flushed.
///
/// # Example Usage
///
/// ```rust
/// use csv::StringRecord;
/// use std::path::{Path, PathBuf};
/// use std::sync::Arc;
///
/// let input_files = vec![PathBuf::from("file1.csv"), PathBuf::from("file2.csv")];
/// let output_path = Path::new("merged_output.csv");
/// let headers = StringRecord::from(vec!["Column1", "Column2", "Column3"]);
/// let sort_indices = Arc::new(vec![0, 1]); // Sort by the first and second columns.
///
/// match merge_k_files(&input_files, output_path, &headers, &sort_indices) {
///     Ok(()) => println!("Files merged successfully!"),
///     Err(e) => eprintln!("Error merging files: {}", e),
/// }
/// ```
///
/// # Notes
///
/// - The sorting is performed in-memory using a priority queue (`BinaryHeap`) for efficiency.
/// - Ensure that input files are sorted correctly based on the `sort_indices` before calling this function.
/// - The function expects all files to have consistent formats (same columns and order).
/// - Large input files may cause high memory usage as the heap stores a record from each input file.
fn merge_k_files(
    files: &[PathBuf],
    output_path: &Path,
    headers: &StringRecord,
    sort_indices: &Arc<Vec<usize>>,
) -> Result<()> {
    use csv::StringRecord;
    use std::collections::BinaryHeap;
    use std::fs::File;
    use std::io::BufReader;

    if files.is_empty() {
        return Ok(());
    }
    let mut readers: Vec<_> = files
        .iter()
        .map(|path| {
            let file = File::open(path)?;
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(BufReader::new(file));
            // Always skip header row for every chunk
            let _ = rdr.headers();
            Ok(rdr)
        })
        .collect::<Result<Vec<_>>>()?;

    // Prepare output writer
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)?;
    // Write header only if this is the final output
    if output_path.extension().and_then(|s| s.to_str()) == Some("csv") {
        wtr.write_record(headers.iter())?;
    }

    // Initialize heap with first record from each reader
    let mut heap = BinaryHeap::new();
    for (i, rdr) in readers.iter_mut().enumerate() {
        if let Some(Ok(rec)) = rdr.records().next() {
            heap.push(MergeRecord {
                record: rec,
                source_index: i,
                sort_indices: Arc::clone(sort_indices),
            });
        }
    }
    let mut record_counts = vec![0usize; readers.len()];
    while let Some(MergeRecord {
        record,
        source_index,
        ..
    }) = heap.pop()
    {
        wtr.write_record(&record)?;
        record_counts[source_index] += 1;
        if let Some(Ok(next_rec)) = readers[source_index].records().next() {
            heap.push(MergeRecord {
                record: next_rec,
                source_index,
                sort_indices: Arc::clone(sort_indices),
            });
        }
    }
    wtr.flush()?;
    Ok(())
}

// --- Example: format_number helper ---
fn fmtnum<N: ToFormattedString>(n: N) -> String {
    n.to_formatted_string(&Locale::en)
}
