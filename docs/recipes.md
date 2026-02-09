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
