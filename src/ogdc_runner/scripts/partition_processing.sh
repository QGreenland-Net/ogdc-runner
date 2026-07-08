#!/bin/sh
# shellcheck disable=SC1073,SC1054,SC1083,SC1009,SC1056,SC1072
# This is a template file with {user_command} placeholder that will be replaced by Python code
# Shellcheck cannot parse the placeholder syntax, so we disable those specific checks
set -eu

# Get parameters
RECIPE_ID="{{inputs.parameters.recipe-id}}"
PARTITION_ID="{{inputs.parameters.partition-id}}"
CMD_INDEX="{{inputs.parameters.cmd-index}}"
FIRST_COMMAND_INPUT_MODE="{first_command_input_mode}"

# Determine input directory based on command index
if [ "$CMD_INDEX" -eq 0 ]; then
    export INPUT_DIR="/mnt/workflow/$RECIPE_ID/inputs"
else
    PREV_CMD_INDEX=$((CMD_INDEX - 1))
    export INPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$PREV_CMD_INDEX-partition-$PARTITION_ID"
fi

export OUTPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$CMD_INDEX-partition-$PARTITION_ID"
mkdir -p "$OUTPUT_DIR"

# For first command, use partition manifest (original URLs/files)
# For subsequent commands, discover actual files from previous output
if [ "$CMD_INDEX" -eq 0 ]; then
    # Read and parse partition manifest (JSON array of original input files)
    PARTITION_FILES='{{inputs.parameters.partition-manifest}}'
    echo "Processing partition with files: $PARTITION_FILES"
    echo "Input directory: $INPUT_DIR"
    echo "Output directory: $OUTPUT_DIR"

    # Process each file in the partition
    printf '%s' "$PARTITION_FILES" | python3 -c 'import json, sys
for file_path in json.load(sys.stdin):
    print(file_path)
' > /tmp/partition-files.txt

    while IFS= read -r file; do
        [ -z "$file" ] && continue

        filename=$(basename "$file")

        echo "Processing file: $file"
        if [ "$FIRST_COMMAND_INPUT_MODE" = "mounted-pvc" ]; then
            export INPUT_FILE="$file"
        else
            export INPUT_FILE="$INPUT_DIR/$filename"
        fi
        export OUTPUT_FILE="$OUTPUT_DIR/$filename"

        # Execute user command
        {user_command}
    done < /tmp/partition-files.txt
else
    # For subsequent commands, process all files from previous output directory
    echo "Discovering files from previous command output: $INPUT_DIR"
    echo "Output directory: $OUTPUT_DIR"

    # Find all files in the input directory (non-recursively)
    if [ ! -d "$INPUT_DIR" ]; then
        echo "ERROR: Input directory does not exist: $INPUT_DIR"
        exit 1
    fi

    # Process each file found in the input directory
    for INPUT_FILE in "$INPUT_DIR"/*; do
        [ -e "$INPUT_FILE" ] || continue  # Skip if no files exist
        [ -f "$INPUT_FILE" ] || continue  # Skip directories

        filename=$(basename "$INPUT_FILE")
        echo "Processing file: $filename"

        export INPUT_FILE
        export OUTPUT_FILE="$OUTPUT_DIR/$filename"

        # Execute user command
        {user_command}
    done
fi
