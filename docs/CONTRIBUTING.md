# Contributing

See the [Scientific Python Developer Guide][spc-dev-intro] for a detailed
description of best practices for developing scientific packages.

[spc-dev-intro]: https://learn.scientific-python.org/development/

```{note}
Add yourself as an author in `pyproject.toml`
```

## Familiarize yourself with the project

- Be sure to look over the [Architecture](./architecture/index.md) docs to get
  an understanding of the `ogdc-runner` before contributing any code.

- The `ogdc-runner` is one component of the QGreenland-Net Open Geospatial Data
  Cloud (OGDC). See the [QGreenland-Net](https://qgreenland-net.github.io/)
  webpage for more information about how this project fits into that larger
  effort. In particular, check out the
  [QGreenland-Net Contributing docs](https://qgreenland-net.github.io/contributing/)!

## Setting up a local development environment

First, ensure you have [ogdc-helm](https://github.com/QGreenland-Net/ogdc-helm)
setup for local development with `rancher-desktop` using `skaffold`.

Now you can set up a python development environment for `ogdc-runner` by
running:

```bash
python -m venv .venv
source ./.venv/bin/activate
pip install -v --editable ".[dev]"
```

### Required environment variables

Set the `ENVIRONMENT` envvar to `local`. This will tell `ogdc-runner` to operate
in "local" development mode:

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

To run all tests, simply run `nox`:

```console
nox
```

This will run the typechecker, unit, and integration tests (requiring
`ogdc-helm` to be deployed locally).

### Running specific tasks with Nox

```console
nox -s {job-name}
```

For example, to run only the tests run in CI, which are fast and do not require
a locally deployed OGDC stack:

```
nox -s test_ci
```

To view available jobs:

```console
nox -l
```

Nox handles everything for you, including setting up an temporary virtual
environment for each run.

### Testing

Use `nox` to run all tests (with pytest):

```bash
nox
```

### Building docs

You can build the docs using:

```bash
nox -s docs
```

You can see a preview with:

```bash
nox -s docs -- --serve
```

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

Choose the appropriate part to bump:

- PATCH release: `bump-my-version bump patch`
- MINOR release: `bump-my-version bump minor`
- MAJOR release: `bump-my-version bump major`

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
