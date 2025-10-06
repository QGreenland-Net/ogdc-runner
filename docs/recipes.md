# OGDC Recipes

An OGDC recipe is a directory containing a `meta.yaml` file and other associated
recipe-specific configuration files that define a data transformation pipeline.

The QGreenland-Net team maintains the
[ogdc-recipes](https://github.com/QGreenland-Net/ogdc-recipes/) repository,
which contains operational examples of data transformation recipes that can be
used as examples.

## Recipe Configuration

All of the configuration options for recipes are modeled by
[Pydantic](https://docs.pydantic.dev/latest/). See the
{mod}`ogdc_runner.models.recipe_config` documentation for complete information
on configuration options.

### `meta.yaml`

The `meta.yaml` provides key metadata that drive the OGDC recipe's execution and
is defined by the {class}`ogdc_runner.models.recipe_config.RecipeConfig`
Pydantic model.

An example recipe `meta.yml` is shown below:

```{literalinclude} ../tests/test_recipe_dir/meta.yml
:language: yaml
```

TODOs:

- Document how inputs work
- Document out outputs (to PVC) work. Note that additional output types are
  planned (e.g., publish to DataONE)

## Recipe types

There are multiple types of OGDC recipe. Which an author should use depends on
the data processing use-case.

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

For an example of a `shell` recipe, we recommend taking a look at the
[ogdc-recipes seal-tags recipe](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/seal-tags)
example.

### Visualization Recipe

The `visualization` recipe type takes a geospatial data file as input and
produces 3D web-tiles of the data for visualization in a web-map.

```{warning}
This section of the documentation is incomplete!
**TODO**: more detail / link to viz workflow documentation.
```

For an example of a `visualization` recipe, we recommend taking a look at the
[ogdc-recipes viz-workflow recipe](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/viz-workflow)
example.
