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
    let prescan_start = Instant::now();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    let all_records: Vec<StringRecord> = rdr
        .records()
        .filter_map(|r| match r {
            Ok(rec) if rec.len() == headers.len() => Some(rec),
            Ok(rec) => {
                error!("CSV format error: expected {} fields, found {} fields. Record: {:?}", headers.len(), rec.len(), rec);
                None
            },
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
    info!("[split] Processing {} chunks in parallel...", fmtnum(chunk_count));
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
        Err(_) => 0
    };
    info!(
        "[split] ALL DONE. File: {:?}, Size: {} bytes, Chunks: {}, Time: {:.2?}",
        file_path, fmtnum(file_size), fmtnum(chunk_count), chunk_total_elapsed
    );
    chunk_paths
}

// --- Merge/Sort helpers ---
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
        .map(|path| parallel_split_file_to_chunks(path, &temp_dir, sort_columns, chunk_size_mb, &headers))
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
    info!("[merge] Starting k-way merge: {} chunks -> {:?}", fmtnum(chunk_paths.len()), output_path);
    let merge_start = Instant::now();

    // Read headers from the first chunk
    let first_chunk = &chunk_paths[0];
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(first_chunk)?;
    let headers = rdr.headers()?.clone();
    drop(rdr);

    // Prepare output writer
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output_path)?;
    wtr.write_record(headers.iter())?;

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
        info!("[merge] Merge pass {}: {} groups of up to {} files", pass, (current_chunks.len() + k - 1) / k, fmtnum(k));

        for (group_idx, group) in groups {
            let out_path = temp_dir_ref.path().join(format!("merge_pass{}_group{}.csv", pass, group_idx));
            info!("[merge]   Group {}: merging {} files -> {:?}", group_idx, fmtnum(group.len()), out_path);
            merge_k_files(group, &out_path, &headers, &sort_indices)?;
            next_chunks.push(out_path);
        }
        current_chunks = next_chunks;
    }
    // Final merge (or only pass if <= k)
    let final_chunks = if current_chunks.is_empty() { vec![] } else { current_chunks };
    if !final_chunks.is_empty() {
        info!("[merge] Final merge: {} files -> {:?}", fmtnum(final_chunks.len()), output_path);
        merge_k_files(&final_chunks, output_path, &headers, &sort_indices)?;
    }
    wtr.flush()?;
    info!("[merge] Merge complete: {:?} in {:.2?}", output_path, merge_start.elapsed());
    Ok(())
}

/// Helper: merge up to k sorted CSV files into a single sorted output file.
fn merge_k_files(
    files: &[PathBuf],
    output_path: &Path,
    headers: &StringRecord,
    sort_indices: &Arc<Vec<usize>>,
) -> Result<()> {
    use std::fs::File;
    use std::io::BufReader;
    use csv::StringRecord;
    use std::collections::BinaryHeap;

    if files.is_empty() {
        return Ok(());
    }
    let mut readers: Vec<_> = files
        .iter()
        .map(|path| {
            let file = File::open(path)?;
            let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(BufReader::new(file));
            // Always skip header row for every chunk
            let _ = rdr.headers();
            Ok(rdr)
        })
        .collect::<Result<Vec<_>>>()?;

    // Prepare output writer
    let mut wtr = csv::WriterBuilder::new().has_headers(false).from_path(output_path)?;
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
    while let Some(MergeRecord { record, source_index, .. }) = heap.pop() {
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
