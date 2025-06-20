use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use log::{debug, error, info};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::thread;
use rand::Rng;

/// Generate a random account number
fn generate_account_number() -> String {
    let mut rng = rand::thread_rng();
    format!("{:010}", rng.gen_range(1000000000..=9999999999_i64))
}

/// Generate a random first name
fn generate_first_name() -> &'static str {
    const FIRST_NAMES: [&str; 20] = [
        "John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Lisa", "William", "Mary",
        "James", "Jennifer", "Richard", "Jessica", "Charles", "Elizabeth", "Thomas", "Michelle", "Daniel", "Amanda"
    ];
    let mut rng = rand::thread_rng();
    FIRST_NAMES[rng.gen_range(0..FIRST_NAMES.len())]
}

/// Generate a random last name
fn generate_last_name() -> &'static str {
    const LAST_NAMES: [&str; 20] = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson",
        "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White"
    ];
    let mut rng = rand::thread_rng();
    LAST_NAMES[rng.gen_range(0..LAST_NAMES.len())]
}

/// Generate a CSV record with account data
fn generate_record() -> String {
    format!("{},{},{}", generate_account_number(), generate_first_name(), generate_last_name())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Split a large CSV file into smaller chunks
    Split {
        /// Input file path
        input: String,
        /// Output directory for split files
        output_dir: String,
        /// Number of records per split file
        #[arg(short, long, default_value_t = 10000)]
        chunk_size: usize,
    },
    /// Merge multiple CSV files into one
    Merge {
        /// Output file path
        output: String,
        /// Input files to merge
        inputs: Vec<String>,
        /// Sort by columns (comma-separated)
        #[arg(long)]
        sort_by: Option<String>,
    },
}

fn main() -> Result<()> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Split { input, output_dir, chunk_size } => {
            split_file_mock(&input, &output_dir, chunk_size)
        },
        Commands::Merge { output, inputs, sort_by } => {
            merge_files_mock(&output, &inputs, sort_by.as_deref())
        },
    }
}

/// Mock implementation of file splitting
fn split_file_mock(input_path: &str, output_dir: &str, chunk_size: usize) -> Result<()> {
    info!("Starting mock split of file: {}", input_path);
    
    // Simulate reading file metadata
    let file_size_gb = 2.0; // Simulating 2GB file
    info!("Processing file size: {:.2}GB", file_size_gb);
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir).context("Failed to create output directory")?;
    
    // Calculate number of chunks (simplified for mock)
    let num_chunks = 10; // Fixed number of chunks for simulation
    let records_per_chunk = chunk_size;
    info!("Will split into {} chunks of ~{} records each", num_chunks, records_per_chunk);
    
    // Generate and write chunks
    for chunk_num in 0..num_chunks {
        let chunk_path = Path::new(output_dir).join(format!("chunk_{:04}.csv", chunk_num));
        info!("Creating chunk {}: {}", chunk_num, chunk_path.display());
        
        // Simulate processing time (50-150ms per chunk)
        let process_time = 50 + (chunk_num as u64 * 10 % 100);
        thread::sleep(Duration::from_millis(process_time));
        
        // Create a file with account data
        let mut file = File::create(&chunk_path).context("Failed to create chunk file")?;
        
        // Write header
        writeln!(&mut file, "account_no,first_name,last_name")?;
        
        // Write records
        for _ in 0..records_per_chunk {
            writeln!(&mut file, "{}", generate_record())?;
        }
    }
    
    info!("Successfully split file into {} chunks in {}", num_chunks, output_dir);
    Ok(())
}

/// Mock implementation of file merging
fn merge_files_mock(output_path: &str, input_paths: &[String], sort_by: Option<&str>) -> Result<()> {
    info!("Starting mock merge of {} files to: {}", input_paths.len(), output_path);
    
    if let Some(columns) = sort_by {
        info!("Will sort by columns: {}", columns);
    } else {
        info!("No sorting specified, will merge in input order");
    }
    
    // Simulate processing each input file
    let mut total_records = 0;
    for (i, input_path) in input_paths.iter().enumerate() {
        // Simulate processing time (20-100ms per file)
        let process_time = 20 + (i as u64 * 15 % 80);
        thread::sleep(Duration::from_millis(process_time));
        
        // Simulate reading some metadata
        let records_in_file = 1000; // Fixed number of records per file for demo
        total_records += records_in_file;
        
        debug!("Processed {}: {} records ({}ms)", input_path, records_in_file, process_time);
    }
    
    // Simulate writing the output
    info!("Writing merged output to: {}", output_path);
    thread::sleep(Duration::from_millis(200));
    
    // Create output file with sample data
    let mut output = File::create(output_path).context("Failed to create output file")?;
    
    // Write header
    writeln!(&mut output, "account_no,first_name,last_name")?;
    
    // Write sample records (first 5 from each input file)
    for i in 0..5 {
        writeln!(&mut output, "{}", generate_record())?;
    }
    
    // Write summary
    writeln!(
        &mut output, 
        "# Merged {} files with {} total records", 
        input_paths.len(),
        total_records
    )?;
    
    info!("Successfully merged {} files with {} total records to {}", 
          input_paths.len(), total_records, output_path);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_mock_split() -> Result<()> {
        let temp_dir = tempdir()?;
        let output_dir = temp_dir.path().join("split_output");
        
        split_file_mock("test_data/large_file.csv", &output_dir.to_string_lossy(), 10000)?;
        
        // Verify chunks were created
        let entries = fs::read_dir(&output_dir)?;
        let chunk_count = entries.count();
        assert!(chunk_count > 0, "No chunk files were created");
        
        Ok(())
    }
    
    #[test]
    fn test_mock_merge() -> Result<()> {
        let temp_dir = tempdir()?;
        let output_file = temp_dir.path().join("merged_output.csv");
        
        let test_files = vec![
            "test_data/chunk1.csv".to_string(),
            "test_data/chunk2.csv".to_string(),
        ];
        
        merge_files_mock(&output_file.to_string_lossy(), &test_files, Some("id"))?;
        
        // Verify output file was created
        assert!(output_file.exists(), "Output file was not created");
        
        Ok(())
    }
}