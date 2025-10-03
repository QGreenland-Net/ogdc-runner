# User Guide

## Installing

To install the `ogdc-runner`:

- Install: `pip install ogdc-runner`

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
```

### Submitting a recipe

To submit an OGDC recipe, use the `submit` subcommand.

```
ogdc-runner submit --wait github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags
```

`submit` takes the path to an OGDC-recipe directory (this can be a local path or
an `ffspec`-compatible directory string).

## OGDC Recipes

An OGDC recipe is a directory containing a `meta.yaml` file and other associated
recipe-specific configuration files.

TODO: link to ogdc-recipes repo

The `meta.yaml` provides key metadata that drive the OGDC recipe's execution.
The contents of `meta.yaml` should conform to the
{class}`ogdc_runner.models.recipe_config.RecipeConfig` Pydantic model.

An example recipe `meta.yml` is shown below:

```{literalinclude} ../tests/test_recipe_dir/meta.yml
:language: yaml
```

TODOs:

- Document how inputs work
- Document out outputs (to PVC) work. Note that additional output types are
  planned (e.g., publish to DataONE)
-

### Shell Recipe

`shell` is a recipe type that involves executing a series of `sh` commands in
sequence, much like a shell script. This recipe type is best suited for
relatively simple transformations on small/medium sized data.

In addition to `meta.yaml`, `shell` recipes expect a `recipe.sh` file that
defines the series of commands to be run against the input data.

It is expected that most of the commands included in the `recipe.sh` be `gdal`
or `ogr2ogr` commands to perform e.g., reprojection or subsetting.

An example of a `recipe.sh` file is shown below:

```{literalinclude} ../tests/test_recipe_dir/recipe.sh
:language: sh
```

```{warning}
Although `recipe.sh` file should contain valid `sh` commands such as `ogr2ogr`, it is not expected to be executable as a shell script on its own (without `ogdc-runner`). This is because there are some specific expectations that must be followed, as outlined below!
```

- It is expected that each command in the `recipe.sh` place data in
  `/output_dir/`
- The input data for each step is always assumed to be in `/input_dir/`. The
  previous step's `/output_dir/` becomes the next step's `/input_dir/`. The
  first step's `/input_dir/` contains the data specified in the `meta.yaml`'s
  `input`.
- Multi-line constructs are not allowed. It is assumed that each line not
  prefixed by `#` is a command that will be executed via `sh -c {line}`.
- Each command is executed in isolation. Do not expect envvars (e.g.,
  `export ENVVAR=foo`) to persist between lines.

### Visualization Recipe

TODO
