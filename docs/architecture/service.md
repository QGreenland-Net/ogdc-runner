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
- `OGDC_WORKFLOW_PVC_NAME` (optional): name of the PersistentVolumeClaim mounted
  into workflow pods, defaults to `cephfs-qgnet-ogdc-workflow-pvc`. The
  ogdc-helm chart sets this to `cephfs-${RELEASE_NAME}-workflow-pvc`.
- `ARGO_NAMESPACE` (optional): kubernetes namespace in which argo workflows are
  submitted, defaults to `qgnet`. The ogdc-helm chart sets this to the release
  namespace.
