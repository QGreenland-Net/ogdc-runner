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

#### PVC Mount Inputs

If your dataset is pre-staged on a Kubernetes PersistentVolumeClaim (PVC), you
can reference it directly. The PVC is mounted read-only into all workflow
containers at `/mnt/data/{claim_name}/`:

```yaml
input:
  params:
    - type: pvc_mount
      claim_name: arctic-dem-pvc
      path: /tiles/v3/
      glob: "*.tif"
```

- **`claim_name`** (required): Name of the PVC in the cluster namespace.
- **`path`** (required): Subpath within the PVC containing the input files.
  Parent directory references (`..`) are rejected.
- **`glob`** (optional, default `"*"`): Recursive glob pattern for file
  selection. For example, `*.gpkg` matches GeoPackages directly under `path` and
  in any nested directories.

The `claim_name` must be pre-provisioned by a cluster operator and included in
the deployment's configured input PVC allowlist. The default OGDC workflow PVC
claim configured by `OGDC_WORKFLOW_PVC_NAME` is always allowed and can also be
referenced as a `pvc_mount` input. Recipes that reference any other PVC claim
outside the allowlist are rejected before workflow submission.

The runner does not create PersistentVolumes or PersistentVolumeClaims for
`pvc_mount` inputs. It only adds Argo workflow volume references to existing PVC
claim names.

PVC inputs are exclusive: a recipe may use one or more `pvc_mount` inputs, or it
may use URL/DataONE inputs, but it cannot combine PVC inputs with URL/DataONE
inputs in the same recipe.

Recipe scripts read files from the mounted path. For the example above, files
are accessible at `/mnt/data/arctic-dem-pvc/tiles/v3/**/*.tif`.

PVC inputs work in both sequential and parallel shell and visualization
workflows. Sequential shell workflows recursively link matching PVC files into
`/input_dir` so existing shell recipes can keep using `/input_dir/...` paths.
Sequential visualization workflows recursively enumerate matching PVC files at
runtime and call `workflow.stage(path)` for each path in the input manifest.
Parallel workflows recursively enumerate files matching the glob pattern at
runtime, write retained partition manifests to the workflow PVC, and distribute
those partitions automatically. For visualization workflows, the retained PVC
manifests feed staging when `enable_stager` is true. When `enable_stager` is
false, the same manifests can feed rasterization, 3D tile generation, or web
tile generation directly, depending on the enabled viz workflow stages.

See {class}`ogdc_runner.models.recipe_config.PvcMountInput` for details.

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

The `visualization` workflow takes geospatial data files as input and produces
cloud optimized tiled outputs for analysis, visualization and archival. This
workflow is designed for large geospatial datasets that require specialized
processing to be displayed efficiently in web-based map applications.

See {class}`ogdc_runner.models.recipe_config.VizWorkflow` for details on
configuration options.

#### Overview

The visualization workflow orchestrates a multi-stage pipeline that transforms
geospatial data through several processing steps:

1. **Staging**: Large vector files are sliced into smaller, tiled pieces that
   correspond to tiles in a defined Tile Matrix Set (TMS). This step also
   handles data standardization, deduplication, and property management.

2. **Rasterization**: Vector tiles are converted to raster formats (GeoTIFFs and
   PNGs), with statistics calculated as specified in the configuration.

3. **3D Tile Creation**: Vector data is converted to Cesium 3D tiles format,
   enabling efficient web-based visualization of large polygon datasets.

The workflow produces four output formats:

- **GeoPackages** (vector): High-resolution, lossless archival format
- **GeoTIFFs** (raster): Multi-resolution raster data with calculated statistics
- **PNG Web Tiles**: Pre-rendered raster tiles with palettes for quick
  visualization
- **Cesium 3D Tiles**: Vector tiles for interactive 3D visualization with
  attribute data pop-ups

#### Configuration

In addition to `meta.yaml`, `visualization` workflows require a `config.json`
file that defines:

- Input data source and format
- Tile Matrix Set (TMS) for tiling strategy
- Statistical calculations to perform
- Color palettes for visualization
- Output specifications

The following configuration options are specific to the `visualization` workflow
in `meta.yaml`:

##### `config_file`

The path to the JSON configuration file (default: `"config.json"`).

Example: `"my_custom_config.json"`

##### `batch_size`

The number of tiles to process in parallel (default: `250`). Increasing this
value can improve performance on high-performance computing systems with
sufficient resources.

Example: `500`

#### Core Packages

The visualization workflow is powered by several specialized Python packages:

- **viz-workflow**: The main orchestrator that coordinates configuration
  management and workflow processing.

- **viz-staging**: Prepares vector data by slicing large files into TMS-aligned
  tiles, re-projecting data, handling deduplication, and managing file paths.

- **viz-raster**: Converts vector tiles to raster formats (GeoTIFFs for archival
  and PNGs for web display), with configurable statistics calculation.

- **viz-3dtiles**: Wraps the py3dtiles library to create Cesium 3D tilesets,
  building hierarchical JSON structures and reading shapefiles.

#### Input Requirements

The visualization workflow accepts vector geospatial files as input:

- Shapefiles (.shp)
- GeoPackages (.gpkg)
- GeoJSON (.geojson)

Raster input support (GeoTIFF) is planned for future releases.

#### Use Cases

The visualization workflow is best suited for:

- Large geospatial vector datasets requiring multi-resolution tiling
- Data that needs both web visualization and archival formats
- Datasets with complex attribute information to be explored interactively
- Applications requiring 3D visualization of polygon or point cloud data

For an example of a recipe using the `visualization` workflow, we recommend
taking a look at the
[ogdc-recipes viz-workflow recipe](https://github.com/QGreenland-Net/ogdc-recipes/tree/main/recipes/viz-workflow)
example.

Additional detailed documentation and examples are available in the
[Permafrost Discovery Gateway viz-info repository](https://github.com/PermafrostDiscoveryGateway/viz-info).

<!-- prettier-ignore-start -->
(parallel-execution)=
## Parallel Execution
<!-- prettier-ignore-end -->

`shell` and `visualization` workflows support parallel execution for processing
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

For partitioned workflows, the full file lists for each partition are written
to retained JSON manifests on the workflow PVC:

```text
/mnt/workflow/{recipe_id}/partition-manifests/{stage}/partition-{partition_id}.json
```

Argo fan-out parameters only carry compact partition IDs and manifest path
references. This keeps large file lists out of the workflow spec and Argo
controller state while preserving the manifests on PVC for provenance and
debugging.

Each parallel task:

- Receives a partition ID and reads its file list from a manifest on the
  workflow PVC
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

### PVC Parallel Execution

When using `pvc_mount` inputs with parallel execution, the runner cannot
enumerate input files at submit time because it does not mount the PVC. Instead,
partitioning happens **inside the cluster at workflow runtime**:

1. The runner builds a workflow containing a `list-pvc-files` container step and
   submits it to Argo. The workflow references pre-existing PVC claims and does
   not include `volumeClaimTemplates` or create storage resources.
2. At runtime, `list-pvc-files` runs on a pod that mounts the input PVC,
   recursively enumerates files matching the `glob` pattern, and groups them
   into partitions of `partition_size`.
3. The listing step writes full partition manifests under
   `/mnt/workflow/{recipe_id}/partition-manifests/pvc-inputs/` and outputs a
   compact JSON list like `[{"partition_id": 0}, ...]`.
4. Argo reads that compact JSON via `with_param` and spawns one parallel task
   per partition. Each task reads its full file list from the retained manifest
   on the workflow PVC.

```yaml
input:
  params:
    - type: pvc_mount
      claim_name: arctic-dem-pvc
      path: /tiles/v3/
      glob: "*.tif"
workflow:
  type: shell
  parallel:
    enabled: true
    partition_strategy: files
    partition_size: 4
```

At `CMD_INDEX=0`, `$INPUT_FILE` is set to the full PVC path (e.g.,
`/mnt/data/arctic-dem-pvc/tiles/v3/tile_001.tif`). For subsequent commands,
`$INPUT_FILE` reads from the previous command's output directory as normal.

```{note}
For URL/DataONE inputs, the runner knows every input at submit time and creates
the initial partition manifests before fan-out starts. For PVC inputs, the
runner has no access to the PVC filesystem, so the listing step inside the
workflow handles discovery and writes the manifests at runtime.
```
