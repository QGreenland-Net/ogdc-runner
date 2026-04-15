#!/bin/sh
# shellcheck disable=SC1073,SC1054,SC1083,SC1009,SC1056,SC1072
# PVC partition processing: like partition_processing.sh but at CMD_INDEX=0
# INPUT_FILE is set to the full PVC-mounted path from the manifest (no download).
# Template file — {user_command} is replaced by Python at build time.
set -eu

RECIPE_ID="{{inputs.parameters.recipe-id}}"
PARTITION_ID="{{inputs.parameters.partition-id}}"
CMD_INDEX="{{inputs.parameters.cmd-index}}"

export OUTPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$CMD_INDEX-partition-$PARTITION_ID"
mkdir -p "$OUTPUT_DIR"

if [ "$CMD_INDEX" -eq 0 ]; then
    FILES=$(echo '{{inputs.parameters.partition-manifest}}' | tr -d '[]"' | tr ',' '\n')
    for file in $FILES; do
        [ -z "$file" ] && continue
        file=$(echo "$file" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        export INPUT_FILE="$file"
        export OUTPUT_FILE="$OUTPUT_DIR/$(basename "$file")"
        {user_command}
    done
else
    PREV_CMD_INDEX=$((CMD_INDEX - 1))
    export INPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$PREV_CMD_INDEX-partition-$PARTITION_ID"
    [ -d "$INPUT_DIR" ] || { echo "ERROR: $INPUT_DIR does not exist"; exit 1; }
    for INPUT_FILE in "$INPUT_DIR"/*; do
        [ -f "$INPUT_FILE" ] || continue
        export INPUT_FILE
        export OUTPUT_FILE="$OUTPUT_DIR/$(basename "$INPUT_FILE")"
        {user_command}
    done
fi
