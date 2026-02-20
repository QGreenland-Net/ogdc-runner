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

```{note}
If the installation fails with a message about `psycopg2` failing to build, you
may need to install the postgresql dev kit/client on your machine first.

E.g., on Ubuntu: `apt install libpq-dev`.

On MacOS: `brew install postgresql`
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
  create-user            Create a new OGDC user.
  submit                 Submit a recipe to OGDC for execution.
  validate-all-recipes   Validate all OGDC recipes in a git...
  validate-recipe        Validate an OGDC recipe directory.
```

### Submitting a recipe

In order to submit recipes to the OGDC service backend, the `OGDC_API_USERNAME`
and `OGDC_API_PASSWORD` must be set:

```
export OGDC_API_USERNAME=my-username
export OGDC_API_PASSWORD=my-password
```

Then, to submit an OGDC recipe, use the `submit` subcommand.

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

To learn more about recipes, see [OGDC Recipes](./recipes.md).

### Input Types

Recipes can fetch data from different types of sources.

#### URL Inputs

Provide a direct URL to your data:

```yaml
input:
  params:
    - type: "url"
      value: "https://example.com/data.zip"
```

#### DataONE Inputs

If your data is in a DataONE repository, you can fetch it using the dataset
identifier:

```yaml
input:
  params:
    - type: "dataone"
      value: "resource_map_doi:10.18739/A29G5GD39"
```

Need specific files from the dataset? Use wildcard patterns:

```yaml
input:
  params:
    - type: "dataone"
      value: "resource_map_doi:10.18739/A29G5GD39"
      filename: "*.nc" # Fetch only NetCDF files
```

Wildcard patterns work like you'd expect:

- `*` matches anything (e.g., `data_*.nc` gets `data_1.nc`, `data_2.nc`, etc.)
- `?` matches one character (e.g., `data_?.nc` gets `data_1.nc` but not
  `data_10.nc`)

Check out the
[seal-tags](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/seal-tags)
and
[greenland-ice-sheet](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/greenland-ice-sheet)
recipes to see DataONE inputs in practice.
