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

The OGDC API uses a standard OAuth2 approach. Users authenticate with a
username/password, and the service responds with a token that can then be used
to authenticate further actions (like submitting a recipe). The approach is
roughly based on the
[FastAPI Security Tutorial](https://fastapi.tiangolo.com/tutorial/security/).

See {mod}`ogdc_runner.service.auth` for details.

### Routes needing auth

When writing new API routes, consider if the route should require user
authentication. If so, use the `auth.AuthenticatedUserDependency` as a type
annotation in the function args:

```
from ogdc_runner.service import auth

@app.get("/user")
async def get_current_user(
    current_user: auth.AuthenticatedUserDependency,
) -> dict[str, str]:
    """Return the current authenticated user.

    Useful for testing that authentication is working as expected.
    """
    return {"current_user": current_user.name}
```

Using the `auth.AuthenticatedUserDependency` as a type annotation in the
function args will cause the associated route (in this case, `user`) to require
authentication via a token obtained via the
{class}`ogdc_runner.service.auth.token` route (which requires
username/password).

### Adding new users

TODO.

This is currently done manually, and will be addressed in
[#133](https://github.com/QGreenland-Net/ogdc-runner/issues/133).

## Required envvars

- `OGDC_DB_USERNAME`: database username that the service will use to interact
  with the backend PostgreSQL database.
- `OGDC_DB_PASSWORD`: database password that the service will use to interact
  with the backend PostgreSQL database.
- `OGDC_JWT_SECRET_KEY`: value of the secret key used to encode/decode the JWT
  tokens used for authentication.
- `OGDC_ADMIN_PASSWORD`: the value desired for the service's admin user account.
