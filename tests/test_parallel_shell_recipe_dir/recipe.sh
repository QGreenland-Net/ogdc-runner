#!/bin/sh

# Parallel GeoPackage processing - Reproject and Convert
# Each partition processes its assigned files independently

ogr2ogr -f GPKG -t_srs EPSG:4326 "$OUTPUT_DIR"/"$(basename "$INPUT_FILE" .gpkg)_4326.gpkg" "$INPUT_FILE"

ogr2ogr -f GeoJSON "$OUTPUT_DIR"/"$(basename "$INPUT_FILE" .gpkg).geojson" "$INPUT_FILE"
