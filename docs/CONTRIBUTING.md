# Contributing

See the [Scientific Python Developer Guide][spc-dev-intro] for a detailed
description of best practices for developing scientific packages.

[spc-dev-intro]: https://learn.scientific-python.org/development/

## Setting up a development environment manually

You can set up a development environment by running:

```bash
python -m venv .venv
source ./.venv/bin/activate
pip install -v --editable .[dev]
```

## Running the CLI in dev

To use the CLI to run simple ogdc recipes with argo:

- Build the `ogdc-runner` docker image (TODO: add GHA for building and pushing
  to ghcr!):

```
docker build . -t ogdc-runner
```

> [!NOTE] The docker image must be built in the `rancher-desktop` context so
> that it is available to the Argo deployment on the developer's local machine.
> Check that you have the correct context selected with `docker context ls`.

Now use the CLI to submit a simple ogdc recipe:

```
$ ogdc-runner submit-and-wait ~/code/ogdc-recipes/recipes/seal-tags/
Successfully submitted recipe with workflow name seal-tags-6gxfw
Workflow status: Running
Workflow status: Running
Workflow status: Running
Workflow status: Succeeded
```

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

### Re-using Nox virtual environments

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
