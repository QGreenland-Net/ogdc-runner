# Contributing

See the [Scientific Python Developer Guide][spc-dev-intro] for a detailed
description of best practices for developing scientific packages.

[spc-dev-intro]: https://learn.scientific-python.org/development/

```{note}
Add yourself as an author in `pyproject.toml`
```

## Setting up a local development environment

First, ensure you have [ogdc-helm](https://github.com/QGreenland-Net/ogdc-helm)
setup for local development with `rancher-desktop`. The argo server ports are
expected to be forwarded for access via localhost.

Now you can set up a python development environment for `ogdc-runner` by
running:

```bash
python -m venv .venv
source ./.venv/bin/activate
pip install -v --editable ".[dev]"
```

### Using a local docker image for workflow execution

The `ogdc-runner` supports using a local `ogdc-runner` image for development
purposes (e.g., you want to change and test something about the image without
needing to release it to the GHCR).

First, build a local image:

```{note}
The docker image must be built in the `rancher-desktop` context so
that it is available to the Argo deployment on the developer's local machine.
Check that you have the correct context selected with `docker context ls`.
```

```{warning}
The `ogdc-runner` docker image (and any others intended to be run on the k8s
cluster as part of the OGDC) **MUST NOT** be based on busybox/alpine Linux due
to a known networking issue in non-local environments. For context, see:
<https://github.com/QGreenland-Net/ogdc-helm/issues/31>
```

```
docker build . -t ogdc-runner
```

Next, set the `ENVIRONMENT` envvar to `local`. This will tell `ogdc-runner` to
use the locally built image instead of the one hosted on the GHCR:

```
export ENVIRONMENT=local
```

## Using `ogdc-runner` with a remote cluster

If using `ogdc-runner` with a remote cluster (e.g,. `dev-qgnet` at the ADC)

Set the `ARGO_WORKFLOWS_URL` to point to the cluster. E.g.,:

```
export ARGO_WORKFLOWS_SERVICE_URL=https://api.test.dataone.org/ogdc/
```

### Dev clusters

If using the `latest` `ogdc-runner` image on a remote dev cluster, set the
`ENVIRONMENT` envvar to `dev`:

```
export ENVIRONMENT=dev
```

This will cause the `ghcr.io/qgreenland-net/ogdc-runner:latest` image to always
be re-pulled, ensuring that the latest `main` branch image is being used.

## Testing, linting, rendering docs with Nox

The fastest way to start is to use Nox. If you don't have Nox, you can use
`pipx run nox` to run it without installing, or `pipx install nox`. If you don't
have pipx, then you can install with `pip install pipx`. If you use macOS, use
`brew install pipx nox`. To use:

```console
nox
```

This will test using every installed version of Python on your system, skipping
ones that are not installed.

### Running specific tasks with Nox

```console
nox -s {job-name}
```

To view available jobs:

```console
nox -l
```

Nox handles everything for you, including setting up an temporary virtual
environment for each run.

### Reusing Nox virtual environments

**By default, Nox deletes and recreates virtual environments for every run.**
Because this is slow, you may want to skip that step with `-R` flag:

```console
nox -R  # MUCH faster!
```

Please read more in the
[official docs](https://nox.thea.codes/en/stable/usage.html#re-using-virtualenvs)

## Automated pre-commit checks

`pre-commit` can check that code passes required checks before committing:

```bash
pip install pre-commit  # or brew install pre-commit on macOS
pre-commit install  # install Git pre-commit hook from .pre-commit-config.yml
```

You can also/alternatively run `pre-commit run` (will run for changed files
only) or `pre-commit run --all-files` to check even without installing the hook.

## Testing

Use pytest to run the unit checks:

```bash
pytest
```

### Coverage

Use pytest-cov to generate coverage reports:

```bash
pytest --cov=ogdc-runner
```

## Building docs

You can build the docs using:

```bash
nox -s docs
```

You can see a preview with:

```bash
nox -s docs -- --serve
```

## Continuous Integration

This project uses [GitHub Actions](https://docs.github.com/en/actions) to
automatically test, build, and publish the `ogdc-runner`.

See the `ogdc-runner` repository's
[.github/workflows/](https://github.com/QGreenland-Net/ogdc-runner/tree/main/.github/workflows)
directory to see configured actions.

In short, GHA are setup to:

- Run tests/package builds on PRs and merges with `main`
- Publish the latest Docker image with merges to `main`
- Publish version tagged Docker image and publish PyPi package on version tags
  (e.g., `v0.1.0`). Upon successflu publication of the Docker image and Python
  package, a GitHub release for the version tag is automatically created.

## Releasing

This project uses [semantic versioning](https://semver.org/).

> Given a version number MAJOR.MINOR.PATCH, increment the:
>
> 1. MAJOR version when you make incompatible API changes
> 2. MINOR version when you add functionality in a backward compatible manner
> 3. PATCH version when you make backward compatible bug fixes

Decide what the version will be for your release, and ensure that the CHANGELOG
contains an entry for the `## NEXT_VERSION`.

**Bump the Version**

Use bump-my-version to automatically update the version number in all configured
files (e.g., pyproject.toml, CHANGELOG.md).

> Choose the appropriate part to bump:
>
> PATCH releasebump-my-version bump patch MINOR releasebump-my-version bump
> minor MAJOR releasebump-my-version bump major

Once `main` is ready for a release (feature branches are merged and the
CHANGELOG is up-to-date), tag the latest commit with the version to be released
(e.g., `v0.1.0`) and push it to GitHub:

```bash
git tag v0.1.0
git push origin v0.1.0
```

```{note}
The git tag is used during the package build to set the version number. This is
accomplished via the use of `hatch-vcs`. When a build is run,
`src/ogdc_runner/_version.py` is generated automatically with the version
number.
```

Pushing a tag will then trigger GitHub actions to:

- Build `ogdc-runner` python package and push to PyPi
- Build `ogdc-runner` Docker image tagged with the version and push to GitHub
  Container Registry.
- Create a GitHub Release for the tag version
