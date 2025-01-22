# shellcheck disable=SC2148
ogr2ogr -segmentize 1 -t_srs "EPSG:3413" /output_dir/arctic_circle.gpkg /input_dir/arctic_circle.geojson
