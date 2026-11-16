# Migration Information

This document tracks migrations

### from MinIO

- Rename `INTERNAL_S3_ENDPOINT_URL` to `S3_ENDPOINT_URL` (the old name still
  works as a fallback).
- Set `S3_BUCKET`, `S3_REGION` (e.g. `default` for Ceph RGW), and, if needed,
  `S3_ADDRESSING_STYLE`.
- Set `OGDC_STORAGE_LINK_SIGNING_KEY` to a random secret of at least 32
  characters. Without a valid key no output links are created.
- `PUBLIC_S3_ENDPOINT_URL` now points at the service's `/storage` route rather
  than at the object store, and must be routed to the service.
- Links issued before the upgrade (S3 presigned URLs) are not valid with the new
  route; request a new link via `ogdc-runner get-output`.
