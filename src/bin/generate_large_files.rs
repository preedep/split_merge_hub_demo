use anyhow::{Context, Result};
use csv::Writer;
use humansize::{format_size, DECIMAL};
use rand::Rng;
use std::time::Instant;

const TARGET_SIZE_GB: u64 = 5;
const RECORDS_PER_BATCH: usize = 10_000;

fn main() -> Result<()> {
    println!(
        "🚀 Starting to generate 2 large CSV files ({}GB each)",
        TARGET_SIZE_GB
    );

    // Create output directory
    let output_dir = "large_files";
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    // Generate first file
    let file1 = format!("{}/accounts_1.csv", output_dir);
    generate_large_csv(&file1, TARGET_SIZE_GB)
        .with_context(|| format!("Failed to generate {}", file1))?;

    // Generate second file
    let file2 = format!("{}/accounts_2.csv", output_dir);
    generate_large_csv(&file2, TARGET_SIZE_GB)
        .with_context(|| format!("Failed to generate {}", file2))?;

    println!(
        "\n✅ Successfully generated 2 files in '{}' directory",
        output_dir
    );
    println!("   - {}", file1);
    println!("   - {}", file2);

    Ok(())
}

fn generate_large_csv(file_path: &str, target_size_gb: u64) -> Result<()> {
    println!("\n📝 Generating: {}", file_path);

    let start_time = Instant::now();
    let target_bytes = target_size_gb * 1024 * 1024 * 1024;
    let mut writer = Writer::from_path(file_path).context("Failed to create CSV writer")?;

    // Write header
    writer.write_record(&["account_no", "first_name", "last_name"])?;

    let mut bytes_written = 0;
    let mut records_written = 0;
    let mut last_update = Instant::now();

    // Generate records in batches
    while bytes_written < target_bytes as usize {
        let mut batch_bytes = 0;

        // Write a batch of records
        for _ in 0..RECORDS_PER_BATCH {
            if bytes_written >= target_bytes as usize {
                break;
            }

            let record = generate_record();
            let record_bytes = record.len() + 2; // +2 for CRLF

            writer.write_record(&record)?;

            bytes_written += record_bytes;
            batch_bytes += record_bytes;
            records_written += 1;
        }

        // Flush periodically
        writer.flush()?;

        // Show progress every second
        let now = Instant::now();
        if now.duration_since(last_update).as_secs() >= 1 {
            let progress = (bytes_written as f64 / target_bytes as f64 * 100.0).min(100.0);
            let speed = (batch_bytes as f64 / 1024.0 / 1024.0)
                / now.duration_since(last_update).as_secs_f64();
            println!(
                "  - Progress: {:.1}%  Speed: {:.1} MB/s  Written: {}",
                progress,
                speed,
                format_size(bytes_written, DECIMAL)
            );
            last_update = now;
        }
    }

    // Final flush
    writer.flush()?;

    let elapsed = start_time.elapsed();
    let mb_per_sec = (bytes_written as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    println!("  - Completed in {:.2?}", elapsed);
    println!("  - Records written: {}", records_written);
    println!("  - Average speed: {:.1} MB/s", mb_per_sec);

    Ok(())
}

fn generate_record() -> Vec<String> {
    let mut rng = rand::thread_rng();

    // Generate account number (10 digits)
    let account_no: u64 = rng.gen_range(1_000_000_000..=9_999_999_999);

    // Generate first name
    let first_names = [
        "James",
        "Mary",
        "John",
        "Patricia",
        "Robert",
        "Jennifer",
        "Michael",
        "Linda",
        "William",
        "Elizabeth",
        "David",
        "Barbara",
        "Richard",
        "Susan",
        "Joseph",
        "Jessica",
        "Thomas",
        "Sarah",
        "Charles",
        "Karen",
    ];
    let first_name = first_names[rng.gen_range(0..first_names.len())];

    // Generate last name
    let last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
        "Thomas",
        "Taylor",
        "Moore",
        "Jackson",
        "Martin",
    ];
    let last_name = last_names[rng.gen_range(0..last_names.len())];

    vec![
        account_no.to_string(),
        first_name.to_string(),
        last_name.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_generate_record() {
        let record = generate_record();
        assert_eq!(record.len(), 3);
        assert_eq!(record[0].len(), 10); // account_no should be 10 digits
        assert!(!record[1].is_empty()); // first_name should not be empty
        assert!(!record[2].is_empty()); // last_name should not be empty
    }

    #[test]
    fn test_generate_small_csv() -> Result<()> {
        let test_file = "test_small.csv";

        // Generate a small file (about 1MB)
        generate_large_csv(test_file, 1)?;

        // Verify the file exists and has content
        let metadata = fs::metadata(test_file)?;
        assert!(metadata.len() > 0);

        // Clean up
        fs::remove_file(test_file)?;
        Ok(())
    }
}
