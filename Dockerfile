# Docker image for running gdal/ogr and fetch (wget) commands for ogdc recipes.
# WARNING: this image *MUST NOT* be based on busybox/alpine linux due to a known
# networking issue in non-local environments.  For context, see:
# https://github.com/QGreenland-Net/ogdc-helm/issues/31
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest
# We use and wget to fetch data from remote sources.
RUN apt update && apt install -y wget rsync git pip python3-venv

WORKDIR /ogdc_runner/
COPY . /ogdc_runner
RUN python -m venv .venv && . ./.venv/bin/activate && pip install --editable .

EXPOSE 8000

CMD . ./.venv/bin/activate && fastapi dev --port 8000 --host 0.0.0.0 src/ogdc_runner/service.py
