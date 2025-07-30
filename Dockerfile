# Docker image for running gdal/ogr and fetch (wget) commands for ogdc recipes.
FROM ghcr.io/osgeo/gdal:alpine-normal-latest
# We use and wget to fetch data from remote sources.
RUN apk update && apk add wget rsync
