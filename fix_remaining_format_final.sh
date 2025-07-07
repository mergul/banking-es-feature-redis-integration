#!/bin/bash

# Replace all remaining format! macro calls with safe alternatives
# Simple single parameter replacements
sed -i 's/format!("\([^"]*\) {}", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string())/g' tests/integration_tests.rs

# Duration formatting with {:?}
sed -i 's/format!("{:?}", \([^)]*\))/format!("{:?}", \\1)/g' tests/integration_tests.rs

# Multi-parameter format strings
sed -i 's/format!("\([^"]*\) {}: {}\", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + ": " + \\&\\3.to_string())/g' tests/integration_tests.rs

# Complex patterns with percentages
sed -i 's/format!("\([^"]*\) {} ({:.2}%)\", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " (" + \\&\\3.to_string() + "%)")/g' tests/integration_tests.rs

# Duration formatting with {:.2}
sed -i 's/format!("\([^"]*\) {:.2}s\n", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + "s\\n")/g' tests/integration_tests.rs

# Events per second formatting
sed -i 's/format!("\([^"]*\) {:.2}\n", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + "\\n")/g' tests/integration_tests.rs

# Success rate formatting
sed -i 's/format!("\([^"]*\) {:.2}%\n", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + "%\\n")/g' tests/integration_tests.rs

# Cache hit rate formatting
sed -i 's/format!("\([^"]*\) {:.2}%\n", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + "%\\n")/g' tests/integration_tests.rs

# Account balance formatting
sed -i 's/format!("\([^"]*\) {}: Balance = {}\n", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + ": Balance = " + \\&\\3.to_string() + "\\n")/g' tests/integration_tests.rs

# Worker completion formatting
sed -i 's/format!("\([^"]*\) {} completed after {} operations\n", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " completed after " + \\&\\3.to_string() + " operations\\n")/g' tests/integration_tests.rs

# Progress formatting
sed -i 's/format!("\([^"]*\) {} ops, {:.2} EPS, {:.1}% success\n", \([^,]*\), \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " ops, " + \\&\\3.to_string() + " EPS, " + \\&\\4.to_string() + "% success\\n")/g' tests/integration_tests.rs

# Performance validation formatting
sed -i 's/format!("\([^"]*\) {} (Achieved: {:.2}) - {}\n", \([^,]*\), \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " (Achieved: " + \\&\\3.to_string() + ") - " + \\&\\4.to_string() + "\\n")/g' tests/integration_tests.rs

# Latency target formatting
sed -i 's/format!("\([^"]*\) {:?} (Achieved: {:?}) - {}\n", \([^,]*\), \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&format!("{:?}", \\2) + " (Achieved: " + \\&format!("{:?}", \\3) + ") - " + \\&\\4.to_string() + "\\n")/g' tests/integration_tests.rs

# Starting performance test formatting
sed -i 's/format!("\([^"]*\) {} workers\n", \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " workers\\n")/g' tests/integration_tests.rs

# Worker operation failed formatting
sed -i 's/format!("\([^"]*\) {} operation failed: {:?}\n", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " operation failed: " + \\&format!("{:?}", \\3) + "\\n")/g' tests/integration_tests.rs

# Worker completed operations formatting
sed -i 's/format!("\([^"]*\) {} completed {} operations\n", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " completed " + \\&\\3.to_string() + " operations\\n")/g' tests/integration_tests.rs

# Operation breakdown formatting
sed -i 's/format!("\([^"]*\) {} ({:.2}%)\n", \([^,]*\), \([^)]*\))/("\\1".to_string() + \\&\\2.to_string() + " (" + \\&\\3.to_string() + "%)\\n")/g' tests/integration_tests.rs

# Average latency formatting
sed -i 's/format!("\([^"]*\) {:?}\n", \([^)]*\))/("\\1".to_string() + \\&format!("{:?}", \\2) + "\\n")/g' tests/integration_tests.rs 