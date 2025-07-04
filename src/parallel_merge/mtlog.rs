// --- MTLog Parallel Merge Implementation ---

use anyhow::Result;
use log::{debug, error, info, warn};
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

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

fn get_merge_parallel_groups() -> usize {
    std::env::var("MERGE_PARALLEL_GROUPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v >= 1)
        .unwrap_or(1)
}

fn get_merge_buf_size() -> usize {
    std::env::var("MERGE_BUF_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(8 * 1024 * 1024)
}

fn get_log_interval() -> usize {
    std::env::var("MERGE_LOG_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(500_000)
}

fn get_mtlog_field(line: &str, col: usize) -> String {
    // Adjust offsets as needed for your MTLogRecord
    const OFFSETS: &[(usize, usize)] = &[
        (0,8), (8,6), (14,7), (21,4), (25,1), (26,8), (34,1), (35,1)
    ];
    if col >= OFFSETS.len() {
        return String::new();
    }
    let (start, len) = OFFSETS[col];
    line.get(start..start+len).unwrap_or("").trim().to_string()
}

fn compare_mtlog_by_columns(a: &str, b: &str, sort_columns: &[MTLogSortColumn]) -> Ordering {
    for col in sort_columns {
        let (v1, v2) = (get_mtlog_field(a, col.index), get_mtlog_field(b, col.index));
        let ord = match col.col_type {
            MTLogSortType::Date | MTLogSortType::Time => v1.cmp(&v2),
            MTLogSortType::Num => v1.parse::<u64>().unwrap_or(0).cmp(&v2.parse::<u64>().unwrap_or(0)),
            MTLogSortType::Str => v1.cmp(&v2),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[derive(Eq)]
struct MTLogHeapItem<'a> {
    line: String,
    idx: usize,
    sort_columns: &'a [MTLogSortColumn],
}

impl<'a> Ord for MTLogHeapItem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_mtlog_by_columns(&other.line, &self.line, self.sort_columns)
    }
}
impl<'a> PartialOrd for MTLogHeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> PartialEq for MTLogHeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.line == other.line
    }
}

pub fn merge_k_files_mtlog(
    files: &[PathBuf],
    output_path: &Path,
    sort_columns: &[MTLogSortColumn],
) -> Result<()> {
    let merge_timer = Instant::now();
    info!("[mtlog] [MERGE] Starting k-way merge of {} files into {:?}", files.len().to_formatted_string(&Locale::en), output_path);
    for (i, f) in files.iter().enumerate() {
        if !f.exists() {
            warn!("[mtlog] [MERGE] Input file #{}: {} (WARNING: does not exist)", i + 1, f.display());
        } else if std::fs::metadata(f)?.len() == 0 {
            warn!("[mtlog] [MERGE] Input file #{}: {} (WARNING: empty file)", i + 1, f.display());
        } else {
            debug!("[mtlog] [MERGE] Input file #{}: {}", i + 1, f.display());
        }
    }
    let mut writer = BufWriter::with_capacity(get_merge_buf_size(), File::create(output_path)?);
    let mut readers: Vec<_> = files
        .iter()
        .map(|f| BufReader::with_capacity(get_merge_buf_size(), File::open(f).expect("Failed to open chunk file")))
        .collect();
    let mut heap = std::collections::BinaryHeap::new();
    for (idx, rdr) in readers.iter_mut().enumerate() {
        let mut buf = String::new();
        if rdr.read_line(&mut buf)? > 0 {
            let line = buf.trim_end_matches('\n').to_string();
            heap.push(MTLogHeapItem { line, idx, sort_columns });
        }
    }
    let mut merged_count = 0usize;
    let mut last_log_group = 0usize;
    let log_interval = get_log_interval();
    while let Some(MTLogHeapItem { line, idx, .. }) = heap.pop() {
        writeln!(writer, "{}", line)?;
        merged_count += 1;
        let current_group = merged_count / log_interval;
        if current_group > last_log_group {
            let elapsed = merge_timer.elapsed();
            info!("[mtlog] [MERGE] Merged {} records so far... elapsed: {:.2?}", merged_count.to_formatted_string(&Locale::en), elapsed);
            last_log_group = current_group;
        }
        let rdr = &mut readers[idx];
        let mut buf = String::new();
        if rdr.read_line(&mut buf)? > 0 {
            let next_line = buf.trim_end_matches('\n').to_string();
            heap.push(MTLogHeapItem { line: next_line, idx, sort_columns });
        }
    }
    writer.flush()?;
    let elapsed = merge_timer.elapsed();
    let output_size = std::fs::metadata(output_path)?.len();
    info!("[mtlog] [MERGE] Merge finished: {} records -> {:?} ({} bytes) in {:.2?}", merged_count.to_formatted_string(&Locale::en), output_path, output_size.to_formatted_string(&Locale::en), elapsed);
    // --- Validation: count lines in output ---
    let file = File::open(output_path)?;
    let reader = BufReader::new(file);
    let mut line_count = 0usize;
    let mut prev_line: Option<String> = None;
    let mut sorted = true;
    for line in reader.lines() {
        let line = line?;
        if let Some(prev) = &prev_line {
            if compare_mtlog_by_columns(prev, &line, sort_columns) == Ordering::Greater {
                error!("[mtlog][validate] Output is NOT sorted at line {}!", line_count + 1);
                sorted = false;
                break;
            }
        }
        prev_line = Some(line.clone());
        line_count += 1;
    }
    info!("[mtlog][validate] Output line count: {}", line_count.to_formatted_string(&Locale::en));
    if sorted {
        info!("[mtlog][validate] Output is sorted correctly.");
    }
    info!("[mtlog][SUMMARY] Merge summary: records={}, file_size={} bytes, elapsed={:.2?}, sorted={}",
        merged_count.to_formatted_string(&Locale::en),
        output_size.to_formatted_string(&Locale::en),
        elapsed,
        sorted
    );
    Ok(())
}

pub fn parallel_merge_sort_mtlog(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    sort_columns: &[MTLogSortColumn],
) -> Result<()> {
    let total_timer = Instant::now();
    if input_paths.is_empty() {
        warn!("[mtlog] No input files provided for MT log merge");
        return Err(anyhow::anyhow!("No input files provided"));
    }
    info!("[mtlog] [CHUNK] Starting parallel chunked merge of {} files into {:?}", input_paths.len().to_formatted_string(&Locale::en), output_path.as_ref());
    let chunk_records = std::env::var("CHUNK_RECORDS").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1_000_000);
    info!("[mtlog] [CHUNK] Chunk size: {} records", chunk_records.to_formatted_string(&Locale::en));
    let mut chunk_files: Vec<PathBuf> = Vec::new();
    let mut all_lines = Vec::new();
    let mut cur_records = 0;
    let chunk_timer = Instant::now();
    let mut total_records: usize = 0;
    let buf_size = get_merge_buf_size();
    for (file_idx, path) in input_paths.iter().enumerate() {
        info!("[mtlog] [CHUNK] Reading input file #{}: {}", file_idx + 1, path.display());
        if !path.exists() {
            warn!("[mtlog] [CHUNK] Input file {} does not exist!", path.display());
            continue;
        }
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(buf_size, file);
        for line in reader.lines() {
            let line = line?;
            cur_records += 1;
            total_records += 1;
            all_lines.push(line);
            if cur_records >= chunk_records {
                info!("[mtlog] [CHUNK] Sorting chunk of {} records...", all_lines.len().to_formatted_string(&Locale::en));
                let mut chunk = std::mem::take(&mut all_lines);
                cur_records = 0;
                let sort_timer = Instant::now();
                chunk.par_sort_unstable_by(|a, b| compare_mtlog_by_columns(a, b, sort_columns));
                info!("[mtlog] [CHUNK] Sorted chunk of {} records in {:.2?}", chunk.len().to_formatted_string(&Locale::en), sort_timer.elapsed());
                let tmp = tempfile::NamedTempFile::new()?;
                {
                    let mut writer = BufWriter::with_capacity(buf_size, tmp.as_file());
                    for l in &chunk { writeln!(writer, "{}", l)?; }
                    writer.flush()?;
                }
                let chunk_path = tmp.path().to_path_buf();
                tmp.persist(&chunk_path)?;
                info!("[mtlog] [CHUNK] Wrote sorted chunk file #{} ({} records): {}", chunk_files.len() + 1, chunk.len().to_formatted_string(&Locale::en), chunk_path.display());
                chunk_files.push(chunk_path);
            }
        }
    }
    if !all_lines.is_empty() {
        info!("[mtlog] [CHUNK] Sorting final chunk of {} records...", all_lines.len().to_formatted_string(&Locale::en));
        let sort_timer = Instant::now();
        all_lines.par_sort_unstable_by(|a, b| compare_mtlog_by_columns(a, b, sort_columns));
        info!("[mtlog] [CHUNK] Sorted final chunk of {} records in {:.2?}", all_lines.len().to_formatted_string(&Locale::en), sort_timer.elapsed());
        let tmp = tempfile::NamedTempFile::new()?;
        {
            let mut writer = BufWriter::with_capacity(buf_size, tmp.as_file());
            for l in &all_lines { writeln!(writer, "{}", l)?; }
            writer.flush()?;
        }
        let chunk_path = tmp.path().to_path_buf();
        tmp.persist(&chunk_path)?;
        info!("[mtlog] [CHUNK] Wrote sorted chunk file #{} ({} records): {}", chunk_files.len() + 1, all_lines.len().to_formatted_string(&Locale::en), chunk_path.display());
        chunk_files.push(chunk_path);
    }
    info!("[mtlog] [CHUNK] {} sorted chunk files created in {:.2?}", chunk_files.len().to_formatted_string(&Locale::en), chunk_timer.elapsed());
    info!("[mtlog] [CHUNK] Total input records: {}", total_records.to_formatted_string(&Locale::en));
    let parallel_groups = get_merge_parallel_groups();
    if parallel_groups <= 1 || chunk_files.len() <= 2 {
        merge_k_files_mtlog(&chunk_files, output_path.as_ref(), sort_columns)?;
    } else {
        let group_size = (chunk_files.len() + parallel_groups - 1) / parallel_groups;
        let group_chunks: Vec<Vec<PathBuf>> = chunk_files
            .chunks(group_size)
            .map(|c| c.to_vec())
            .collect();
        let temp_dir = tempfile::tempdir()?;
        info!("[mtlog] [GROUP] Starting {} parallel group merges (group size: {})", group_chunks.len(), group_size);
        let group_outputs: Vec<PathBuf> = group_chunks
            .par_iter()
            .enumerate()
            .map(|(i, group)| {
                let group_path = temp_dir.path().join(format!("group_merge_{}.mtlog", i));
                info!("[mtlog] [GROUP] Merging group #{}/{} ({} files) into {}", i + 1, group_chunks.len(), group.len(), group_path.display());
                let group_timer = Instant::now();
                let result = merge_k_files_mtlog(group, &group_path, sort_columns);
                info!("[mtlog] [GROUP] Finished group #{}/{} in {:.2?}", i + 1, group_chunks.len(), group_timer.elapsed());
                result?;
                Ok(group_path)
            })
            .collect::<Result<Vec<_>>>()?;
        info!("[mtlog] [GROUP] All group merges complete. Merging group outputs into final output...");
        merge_k_files_mtlog(&group_outputs, output_path.as_ref(), sort_columns)?;
    }
    let total_elapsed = total_timer.elapsed();
    info!("[mtlog] [SUMMARY] Parallel merge complete: output={:?}, elapsed={:.2?}", output_path.as_ref(), total_elapsed);
    Ok(())
}
