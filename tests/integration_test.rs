use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn test_merge_csv_files() {
    // Create test input files
    let input1 = "test_input1.csv";
    let input2 = "test_input2.csv";
    let output = "test_output.csv";
    
    // Clean up from previous test runs
    let _ = fs::remove_file(input1);
    let _ = fs::remove_file(input2);
    let _ = fs::remove_file(output);
    
    // Create test input files
    fs::write(input1, "id,name\n1,Alice\n3,Charlie").unwrap();
    fs::write(input2, "id,name\n2,Bob\n4,David").unwrap();
    
    // Run the merge command
    let status = Command::new("cargo")
        .args(["run", "--", "merge", output, input1, input2])
        .status()
        .expect("Failed to execute command");
    
    assert!(status.success(), "Merge command failed");
    
    // Verify the output - order doesn't matter for unsorted merge
    let output_content = fs::read_to_string(output).expect("Failed to read output file");
    let mut lines: Vec<&str> = output_content.lines().collect();
    
    // Check header
    assert_eq!(lines[0], "id,name", "Header mismatch");
    
    // Check all expected records are present (order doesn't matter)
    let records: Vec<&str> = lines[1..].to_vec();
    assert_eq!(records.len(), 4, "Expected 4 records");
    assert!(records.contains(&"1,Alice"), "Missing record: 1,Alice");
    assert!(records.contains(&"2,Bob"), "Missing record: 2,Bob");
    assert!(records.contains(&"3,Charlie"), "Missing record: 3,Charlie");
    assert!(records.contains(&"4,David"), "Missing record: 4,David");
    
    // Clean up
    fs::remove_file(input1).unwrap();
    fs::remove_file(input2).unwrap();
    fs::remove_file(output).unwrap();
}

#[test]
fn test_merge_with_sorting() {
    // Create test input files
    let input1 = "test_sort1.csv";
    let input2 = "test_sort2.csv";
    let output = "test_sorted_output.csv";
    
    // Clean up from previous test runs
    let _ = fs::remove_file(input1);
    let _ = fs::remove_file(input2);
    let _ = fs::remove_file(output);
    
    // Create test input files with out-of-order IDs
    fs::write(input1, "id,name\n3,Charlie\n1,Alice").unwrap();
    fs::write(input2, "id,name\n4,David\n2,Bob").unwrap();
    
    // Run the merge command with sorting
    let status = Command::new("cargo")
        .args(["run", "--", "merge", "--sort-by", "id", output, input1, input2])
        .status()
        .expect("Failed to execute command");
    
    assert!(status.success(), "Merge with sort command failed");
    
    // Read and verify the output file
    let output_content = fs::read_to_string(output).expect("Failed to read output file");
    let mut lines: Vec<&str> = output_content.lines().collect();
    
    // Debug output
    println!("Output content: {:?}", output_content);
    println!("Number of lines: {}", lines.len());
    println!("Lines: {:?}", lines);
    
    // Check header
    assert_eq!(lines[0], "id,name", "Header mismatch");
    
    // Remove header and verify records
    lines.remove(0);
    
    // Debug output for records
    println!("Records found: {:?}", lines);
    
    // Verify we have the expected number of records
    assert_eq!(lines.len(), 4, "Expected 4 records, found {}", lines.len());
    
    // Verify all expected records are present and in order
    let expected_records = vec!["1,Alice", "2,Bob", "3,Charlie", "4,David"];
    assert_eq!(lines, expected_records, "Records are not in the expected order");
    
    // Clean up
    fs::remove_file(input1).unwrap();
    fs::remove_file(input2).unwrap();
    fs::remove_file(output).unwrap();
}

#[test]
fn test_split_file() {
    // Create test input file with 4 records (5 lines including header)
    let input = "test_split_input.csv";
    let input_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,David";
    fs::write(input, input_content).expect("Failed to create test input file");
    
    // Define output directory
    let output_dir = "test_split_output";
    let _ = fs::remove_dir_all(output_dir); // Clean up from previous runs
    fs::create_dir_all(output_dir).expect("Failed to create output directory");
    
    // Run the split command
    let output = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("split")
        .arg(input)
        .arg(output_dir)
        .arg("--rows")
        .arg("2")
        .output()
        .expect("Failed to execute command");
    
    // Verify command succeeded
    assert!(output.status.success(), "Split command failed");
    
    // Check output files
    let entries: Vec<_> = fs::read_dir(output_dir)
        .expect("Failed to read output directory")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .collect();
    
    assert!(!entries.is_empty(), "No output CSV files found in directory");
    
    // Collect all records from all output files
    let mut all_records = Vec::new();
    for entry in &entries {
        let content = fs::read_to_string(entry.path())
            .expect("Failed to read output file");
        let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
        
        // First line should be header
        if !lines.is_empty() {
            assert_eq!(lines[0], "id,name", "Header mismatch in output file");
            // Skip header for record counting
            lines.remove(0);
        }
        all_records.extend(lines);
    }
    
    // Verify we have all records (4 data records)
    assert_eq!(all_records.len(), 4, "Expected 4 data records");
    
    // Verify all expected records are present (order doesn't matter for this test)
    let expected_records: Vec<String> = vec!["1,Alice", "2,Bob", "3,Charlie", "4,David"]
        .into_iter()
        .map(String::from)
        .collect();
    
    for record in &expected_records {
        assert!(all_records.contains(record), "Missing record: {}", record);
    }
    
    // Clean up
    fs::remove_file(input).unwrap();
    fs::remove_dir_all(output_dir).unwrap();
}
