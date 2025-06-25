use anyhow::{Context, Result};
use humansize::{format_size, DECIMAL};
use rand::Rng;
use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <rows_per_file>", args[0]);
        std::process::exit(1);
    }
    let rows_per_file: usize = args[1].parse().expect("Please provide a valid number for rows_per_file");

    println!(
        "🚀 Starting to generate 2 large CSV files ({} rows each)",
        rows_per_file
    );

    // Create output directory
    let output_dir = "large_files";
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    // Generate first file
    let file1 = format!("{}/accounts_1.csv", output_dir);
    generate_large_csv(&file1, rows_per_file, 1)
        .with_context(|| format!("Failed to generate {}", file1))?;

    // Generate second file
    let file2 = format!("{}/accounts_2.csv", output_dir);
    generate_large_csv(&file2, rows_per_file, rows_per_file + 1)
        .with_context(|| format!("Failed to generate {}", file2))?;

    println!(
        "\n✅ Successfully generated 2 files in '{}' directory",
        output_dir
    );
    println!("   - {}", file1);
    println!("   - {}", file2);

    Ok(())
}

fn generate_large_csv(file_path: &str, rows: usize, start_account_no: usize) -> Result<()> {
    println!("\n📝 Generating: {} ({} rows)", file_path, rows);

    let start_time = Instant::now();
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, file); // 16MB buffer

    // Write header
    writer.write_all(b"account_no,first_name,last_name\n")?;

    let batch_size = 200_000;
    // เปลี่ยนจาก (0..rows) เป็น (1..=rows) เพื่อให้ครบทุก row
    let indices: Vec<usize> = (1..=rows).collect();
    let mut records_written = 0;

    for (batch_i, batch_indices) in indices.chunks(batch_size).enumerate() {
        // Generate CSV lines in parallel as a Vec<String> แล้ว join แบบไม่มี newline เกิน
        let batch_lines: Vec<String> = batch_indices
            .par_iter()
            .map(|&i| {
                let account_no = start_account_no + i - 1;
                format!("{},{},{}", account_no, format!("FirstName{}", account_no), format!("LastName{}", account_no))
            })
            .collect();
        let batch_csv = batch_lines.join("\n");
        writer.write_all(batch_csv.as_bytes())?;
        // เพิ่ม newline เฉพาะถ้าไม่ใช่ batch สุดท้าย
        if (batch_i + 1) * batch_size < rows {
            writer.write_all(b"\n")?;
        }
        records_written += batch_indices.len();
        let elapsed = start_time.elapsed().as_secs();
        let speed = records_written as f64 / (elapsed.max(1) as f64);
        println!(
            "   - {} records written ({} rec/sec)",
            records_written, speed as usize
        );
    }
    writer.flush()?;

    let duration = start_time.elapsed();
    let file_size = std::fs::metadata(file_path)?.len();
    println!(
        "   Done in {:.2}s | {} rows | {}",
        duration.as_secs_f64(),
        rows,
        format_size(file_size, DECIMAL)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_generate_large_csv_creates_file() -> Result<()> {
        let test_file = "test_small.csv";
        generate_large_csv(test_file, 100, 1)?;
        let metadata = fs::metadata(test_file)?;
        assert!(metadata.len() > 0);
        fs::remove_file(test_file)?;
        Ok(())
    }
}
