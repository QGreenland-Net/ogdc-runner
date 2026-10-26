# OGDC Service API

The `ogdc-runner` service API sits between users of the CLI and
[Argo Workflows](https://argo-workflows.readthedocs.io/en/latest/). It is
responsible for translating user recipes into workflows that are submitted to
and executed by Argo.

The service interface is defined using [FastAPI](https://fastapi.tiangolo.com/).
See the
[FastAPI docs on First Steps](https://fastapi.tiangolo.com/tutorial/first-steps/)
to learn how to get started with using FastAPI.

the OGDC service is defined as a subpackage of the `ogdc-runner`. See:
{mod}`ogdc_runner.service`.

## FastAPI application

The FastAPI application and API routes are defined in
{mod}`ogdc_runner.service.main`.

## Database

The OGDC service API uses a postgresql database to serve as persistent storage
of user information and (eventually) recipe status information.

See {mod}`ogdc_runner.service.db` for details.

## Authentication

The OGDC API uses OpenID Connect (OIDC), via the DataONE keycloak service, for
authentication. Instead of local username/password logins, clients authenticate
via a standard redirect flow to the DataONE keycloak. Keycloak issues access and
refresh tokens, which are then used to authorize subsequent actions (like
submitting a recipe).

See {mod}ogdc_runner.service.auth and the
[dataone.auth](https://github.com/DataONEorg/dataone-auth) package for
implementation details.

### Routes needing auth

When writing new API routes, consider if they require user authentication or
specific permissions. We enforce authentication and scope validation using
FastAPI dependencies, which are assigned directly to the APIRouter in
{mod}`ogdc_runner.service.auth_routes`. The dependency can also be added
explicitly to invidividual function signatures, but is not required.

The `dataone.auth` auth client is set up in {mod}`ogdc_runner.service.auth`, and
is used to communicate with the keycloak instance.

### Adding new users

New users can be given access via the keycloak administrator user interface.

## Required envvars

- `OGDC_DB_USERNAME`: database username that the service will use to interact
  with the backend PostgreSQL database.
- `OGDC_DB_PASSWORD`: database password that the service will use to interact
  with the backend PostgreSQL database.
- `OGDC_DB_HOST`: hostname of the backend PostgreSQL database (typically
  `${RELEASE_NAME}-db-cnpg-rw` when deployed via the ogdc-helm chart).
- `OGDC_DB_NAME` (optional): database name, defaults to `ogdc`.
- `OGDC_SECRET_KEY` (optional): value of the secret key used to encode/decode
  cookies used for authentication.
- `ACCESS_MODE` (optional): one of `authenticated`, `read_only`, or `open`;
  defaults to `authenticated`.
- `OIDC_CLIENT_SECRETS_FILE`: mounted OIDC client JSON path. Required in
  authenticated mode; changes require a service restart.
- `OGDC_WORKFLOW_PVC_NAME` (optional): name of the PersistentVolumeClaim mounted
  into workflow pods, defaults to `cephfs-qgnet-ogdc-workflow-pvc`. The
  ogdc-helm chart sets this to `cephfs-${RELEASE_NAME}-workflow-pvc`.
- `ARGO_NAMESPACE` (optional): kubernetes namespace in which argo workflows are
  submitted, defaults to `qgnet`. The ogdc-helm chart sets this to the release
  namespace.

## S3 object storage and `/storage` links

Argo artifacts and `temporary` recipe outputs live in an S3-compatible object
store. The store is configured through the following environment variables (the
ogdc-helm chart sets them):

| Variable                        | Meaning                                                                   | Default                                                   |
| ------------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------- |
| `S3_ENDPOINT_URL`               | S3 API endpoint                                                           | falls back to legacy `INTERNAL_S3_ENDPOINT_URL`; required |
| `S3_BUCKET`                     | Bucket for argo artifacts and outputs                                     | `argo-workflows`                                          |
| `S3_REGION`                     | S3 region                                                                 | `us-east-1`                                               |
| `S3_ADDRESSING_STYLE`           | `path`, `virtual`, or `auto`                                              | `path`                                                    |
| `S3_SECRET_KEY_ID`              | Static access key ID                                                      | required                                                  |
| `S3_SECRET_KEY_KEY`             | Static secret access key                                                  | required                                                  |
| `PUBLIC_S3_ENDPOINT_URL`        | Public base of output links, e.g. `https://ogdc.test.dataone.org/storage` | required to create links                                  |
| `OGDC_STORAGE_LINK_SIGNING_KEY` | Secret used to HMAC-sign output links                                     | required (>= 32 chars, not a placeholder) for links       |

### Why output links are served by the service

Output links must stay under `https://<public_host>/storage/...`. An S3
presigned URL cannot be used for this: the SigV4 signature covers the `Host`
header and the canonical path, so rewriting the host of a presigned URL (as was
done for MinIO, which tolerated it) makes real S3 implementations such as Ceph
RGW reject the request.

Instead, `get_temporary_output_data_url` returns

```text
{PUBLIC_S3_ENDPOINT_URL}/{s3_key}?expires=<unix>&signature=<hex>
```

where `signature` is
`HMAC-SHA256(OGDC_STORAGE_LINK_SIGNING_KEY, "{s3_key}\n{expires}")` and the link
is valid for 2 hours. The service route `GET /storage/{s3_key}` (see
{mod}`ogdc_runner.service.storage_routes`) verifies the signature in constant
time and the expiry, then streams the object from S3 using the service's own
credentials. The route is unauthenticated: possession of the link is the
capability. It answers `403` for a bad or malformed link (including an `expires`
longer than 12 digits) or an unsafe key (`..` or a leading `/`), `410` for an
expired link, `404` for a missing object, `502` for other S3 errors (including
an unreachable endpoint), and `503` if the signing key or S3 is not configured.
The signing key must be at least 32 characters and not the `your-value-here`
placeholder; otherwise it is treated as not configured. Links are never created
without a valid signing key.

The route lives at `/storage` in the application, outside of the authenticated
routes. Route `/storage` to the service in the ingress without rewriting the
path. Note that the signature appears in the query string, so access logs of
proxies in front of the service should not be shared; links expire after 2
hours.

### Migrating from MinIO

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
