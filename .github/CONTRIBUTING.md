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

## Testing, linting, rendering docs with `nox`

The fastest way to start is to use nox. If you don't have nox, you can use
`pipx run nox` to run it without installing, or `pipx install nox`. If you don't
have pipx, then you can install with `pip install pipx`. If you use macOS, use
`brew install pipx nox`. To use:

```
nox
```

This will lint and test using every installed version of Python on your system,
skipping ones that are not installed. You can also run specific jobs:

```console
$ nox -s lint  # Lint only
$ nox -s tests  # Python tests
$ nox -s docs -- --serve  # Build and serve the docs
$ nox -s build  # Make an SDist and wheel
```

Nox handles everything for you, including setting up an temporary virtual
environment for each run.

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
