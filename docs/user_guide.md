# User Guide

```{warning}
The `ogdc-runner` and other associated components of the OGDC are under active
development and currently intended for internal users (QGreenland-Net team
members). We are working hard to finalize APIs and expose functionality for a
wider audience. In the meantime, frequent breaking changes are expected.
```

## Installing

Use `pip` to install the `ogdc-runner`:

```bash
pip install ogdc-runner
```

## Using the CLI

Use the `--help` flag for the most up-to-date usage information:

```
$ ogdc runner --help
Usage: ogdc-runner [OPTIONS] COMMAND [ARGS]...

  A tool for submitting data transformation recipes to OGDC for execution.

Options:
  --help  Show this message and exit.

Commands:
  check-workflow-status  Check an argo workflow's status.
  submit                 Submit a recipe to OGDC for execution.
  validate-all-recipes   Validate all OGDC recipes in a git...
  validate-recipe        Validate an OGDC recipe directory.
```

### Submitting a recipe

To submit an OGDC recipe, use the `submit` subcommand.

```
ogdc-runner submit --wait github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags
```

`submit` takes the path to an OGDC-recipe directory. This must be an an
`fsspec`-compatible string representing a valid OGDC-recipe directory accessible
by the OGDC service. The most common input is a public GitHub repository string
like the one given above.

## OGDC Recipes

An OGDC recipe is a directory containing a `meta.yaml` file and other associated
recipe-specific configuration files that define a data transformation pipeline.

The QGreenland-Net team maintains the
[ogdc-recipes](https://github.com/QGreenland-Net/ogdc-recipes/) repository,
which contains operational examples of data transformation recipes that can be
used as examples.

Recipes support parallel execution for processing multiple input files
concurrently. This is configured via the `parallel` section in the workflow
configuration. See [Parallel Execution](./recipes.md#parallel-execution) for
details.

To learn more about recipes, see [OGDC Recipes](./recipes.md).
