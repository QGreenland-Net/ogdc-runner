#!/bin/sh

# Simple parallel processing example
# Each command will be executed in parallel across partitions

# Step 1: Process files - add header and line numbers
cat "$INPUT_FILE" | awk 'BEGIN {print "--- Processed File ---"} {print NR": "$0}' > "$OUTPUT_FILE"

# Step 2: Add metadata timestamp
cat "$INPUT_FILE" > "$OUTPUT_FILE" && echo "Processed at: $(date)" >> "$OUTPUT_FILE"

# Step 3: Count lines and append summary
cat "$INPUT_FILE" > "$OUTPUT_FILE" && echo "Total lines: $(wc -l < "$INPUT_FILE")" >> "$OUTPUT_FILE"
