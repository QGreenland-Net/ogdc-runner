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
any workflow-specific configuration options. See
[Workflow types](#workflow-types) below for more information about different
workflow types.

See the {class}`ogdc_runner.models.recipe_config.Workflow` class for details.

#### `input`

The input data source. See the
{class}`ogdc_runner.models.recipe_config.RecipeInput` class for details.

#### `output`

```{warning}
Although `dataone_id` is a documented output type, it is currently **unused**. As of this  writing, outputs are stored on the `qgnet-ogdc-workflow-pvc`, under a directory named after the `recipe_id`. This is an evolving part of the API, and we expect new output types to be supported soon.
```

## Workflow types

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

## Parallel Execution

Currently only `shell` workflows support parallel execution for processing
multiple input files concurrently. Parallel execution distributes work across
multiple Argo workflow tasks, enabling efficient processing of large datasets.

### Configuration

Parallel execution is configured via the `parallel` field within the workflow
configuration. See {class}`ogdc_runner.models.recipe_config.ParallelConfig` for
complete configuration options.

```yaml
workflow:
  type: "shell"
  parallel:
    enabled: true
    partition_strategy: "files"
    partition_size: 2
```

#### `enabled`

Boolean flag to enable parallel execution. When `false` (default), workflow
executes sequentially.

#### `partition_strategy`

Currently supports `"files"` strategy, which groups input files into partitions
for parallel processing.

#### `partition_size`

Number of files per partition. The orchestrator divides input files into chunks
of this size, creating one parallel task per partition. For example, with 5
input files and `partition_size: 2`, three partitions are created: two with 2
files and one with 1 file.

```{note}
Partitions may have different numbers of files. If the total number of input
files doesn't divide evenly by `partition_size`, the last partition will contain
the remainder. For instance, 7 files with `partition_size: 3` creates partitions
of [3, 3, 1] files.
```

### Execution Model

Parallel execution uses Argo's DAG (Directed Acyclic Graph) to create
independent tasks that can run concurrently. The maximum parallelism is
controlled at the workflow level, allowing Argo to automatically schedule tasks
as cluster resources become available.

Each parallel task:

- Receives a partition of input files via workflow parameters
- Executes the same command independently for **each file** in its partition
- Writes outputs to isolated directories (one per partition)
- Runs in a separate container with its own resource allocation

```{important}
**File-level execution**: Each command in the recipe is executed once per file
in the partition. The runner sets environment variables (`$INPUT_FILE` and
`$OUTPUT_FILE`) for each file, and your command processes them one at a time
within the partition. You don't need to handle the partition splitting - the
orchestrator does this automatically.
```
