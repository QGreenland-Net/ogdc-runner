#!/bin/sh
# Reusable QGreenland raster packaging step: internal tiling, compression, overviews.
# Map mode: runs once per input file. OGDC provides INPUT_FILE and OUTPUT_DIR;
# template parameters arrive as OGDC_PARAM_* environment variables.
set -eu

: "${INPUT_FILE:?INPUT_FILE is required}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"
COMPRESS="${OGDC_PARAM_COMPRESS:-DEFLATE}"
PREDICTOR="${OGDC_PARAM_PREDICTOR:-2}"
RESAMPLING="${OGDC_PARAM_RESAMPLING:-average}"
LEVELS="${OGDC_PARAM_OVERVIEW_LEVELS:-2 4 8 16}"

name="$(basename "$INPUT_FILE")"
out="$OUTPUT_DIR/${name%.*}.tif"

gdal_translate -q -of GTiff -co TILED=YES -co "COMPRESS=$COMPRESS" \
  -co "PREDICTOR=$PREDICTOR" "$INPUT_FILE" "$out"
# shellcheck disable=SC2086  # LEVELS is a space-separated list by contract
gdaladdo -q -r "$RESAMPLING" "$out" $LEVELS
