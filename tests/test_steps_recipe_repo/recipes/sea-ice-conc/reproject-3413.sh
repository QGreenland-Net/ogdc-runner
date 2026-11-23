#!/bin/sh
set -eu
gdalwarp -t_srs EPSG:3413 "$INPUT_FILE" "$OUTPUT_DIR/$(basename "$INPUT_FILE")"
