#!/bin/sh
set -eu
gdal_calc.py -A "$INPUT_FILE" --outfile="$OUTPUT_DIR/$(basename "$INPUT_FILE")" --calc="A*100"
