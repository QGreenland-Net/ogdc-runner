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
is defined by the {class}`ogdc_runner.models.recipe_config.RecipeMeta` Pydantic
model.

An example recipe `meta.yml` is shown below:

```{literalinclude} ../tests/test_recipe_dir/meta.yml
:language: yaml
```

Key configuration options are:

#### `name`

Each recipe must have a `name`, which is a string of characters providing a
human-readable name for the given recipe.

Example: `"Water measurements from seal tag data"`

#### `workflow`

Section containing configuration on what type of workflow this recipe uses, and
any workflow-specific configuration options. See {ref}`workflow-types` below for
more information about different workflow types.

See the {class}`ogdc_runner.models.recipe_config.Workflow` class for details.

#### `input`

The input data source. See the
{class}`ogdc_runner.models.recipe_config.RecipeInput` class for details.

#### `output`

{class}`ogdc_runner.models.recipe_config.RecipeOutput` is the base class
representing configuration for OGDC recipe outputs. Child classes define the
output-type specific configuration required to publish final outputs of a
recipe.

##### PVC Output

If no configuration is supplied, this is the default. Recipe outputs will be
stored on the `qgnet-ogdc-workflow-pvc` PVC in kubernetes under a directory
named after the `recipe_id`.

See {class}`ogdc_runner.models.recipe_config.PvcRecipeOutput` for details.

##### Temporary output

When the output type is set to `temporary`, recipe outputs will be stored
temporarily (for 7 days). After successful workflow completion, users can
retrieve this output as a .zip file via the `ogdc-runner get-output` command.

See {class}`ogdc_runner.models.recipe_config.TemporaryRecipeOutput` for details.

##### DataONE output

```{warning}
Although `dataone_id` is a documented output type, it is currently
**unused**.
```

See {class}`ogdc_runner.models.recipe_config.DataOneRecipeOutput` for details.

<!-- prettier-ignore-start -->
(workflow-types)=
## Workflow types
<!-- prettier-ignore-end -->

There are multiple types of OGDC workflow. Which an author should use depends on
the data processing use-case.

### Shell Workflow

`shell` is a workflow type that involves executing a series of `sh` commands in
sequence, much like a shell script. This workflow type is best suited for
relatively simple transformations on small/medium sized data.

See {class}`ogdc_runner.models.recipe_config.ShellWorkflow` for details on
configuration options.

In addition to `meta.yaml`, `shell` workflows expect a `recipe.sh` file that
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

For an example of a recipe using the `shell` workflow, we recommend taking a
look at the
[ogdc-recipes seal-tags recipe](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/seal-tags)
example.

### Visualization Workflow

The `visualization` workflow takes a geospatial data file as input and produces
3D web-tiles of the data for visualization in a web-map.

See {class}`ogdc_runner.models.recipe_config.VizWorkflow` for details on
configuration options.

```{warning}
This section of the documentation is incomplete!
**TODO**: more detail / link to viz workflow documentation.
```

For an example of a recipe using the `visualization` workflow, we recommend
taking a look at the
[ogdc-recipes viz-workflow recipe](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/viz-workflow)
example.
