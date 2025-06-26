use anyhow::{Context, Result};
use chrono::{Duration, NaiveDateTime, Utc};
use humansize::{format_size, DECIMAL};
use rand::seq::SliceRandom;
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
    let rows_per_file: usize = args[1]
        .parse()
        .expect("Please provide a valid number for rows_per_file");

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

fn generate_large_csv(file_path: &str, rows: usize, _start_account_no: usize) -> Result<()> {
    println!("\n📝 Generating: {} ({} rows)", file_path, rows);

    let start_time = Instant::now();
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, file); // 16MB buffer

    // Write header
    writer.write_all(b"transaction_date,account_no,first_name,last_name,address\n")?;

    let batch_size = 200_000;
    // สุ่ม account_no ไม่เรียงและไม่ซ้ำ
    let mut account_nos: Vec<usize> = (1..=rows * 10).collect();
    let mut rng = rand::thread_rng();
    account_nos.shuffle(&mut rng);
    let picked: Vec<usize> = account_nos.into_iter().take(rows).collect();

    let mut records_written = 0;
    for batch_indices in picked.chunks(batch_size) {
        // Generate CSV lines in parallel as a Vec<String> แล้ว join แบบไม่มี newline เกิน
        let batch_lines: Vec<String> = batch_indices
            .par_iter()
            .map(|&account_no| {
                // Random transaction_date: between 2020-01-01 00:00:00 and now
                let start = NaiveDateTime::parse_from_str("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
                let now = Utc::now().naive_utc();
                let duration = now - start;
                let mut rng = rand::thread_rng();
                let random_secs = rng.gen_range(0..duration.num_seconds());
                let transaction_date = start + Duration::seconds(random_secs);
                // Random address
                let address = format!("Address{}", rng.gen_range(100000..999999));
                format!(
                    "{},{},{},{},{}",
                    transaction_date.format("%Y-%m-%d %H:%M:%S"), account_no,  format!("FirstName{}", account_no),  format!("LastName{}", account_no),  address
                )
            })
            .collect();
        let batch_str = batch_lines.join("\n");
        writer.write_all(batch_str.as_bytes())?;
        writer.write_all(b"\n")?;
        records_written += batch_lines.len();
    }
    writer.flush()?;
    let elapsed = start_time.elapsed();
    println!("   -> Done: {} rows in {:.2?}", records_written, elapsed);
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
