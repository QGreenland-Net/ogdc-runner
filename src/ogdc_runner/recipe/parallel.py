from __future__ import annotations

from hera.workflows import (
    DAG,
    Artifact,
    HTTPArtifact,
    Parameter,
    Script,
    Workflow,
    models,
)
from loguru import logger

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    submit_workflow,
)

# Import common utilities
from ogdc_runner.common import (
    apply_custom_container_config,
    data_already_published,
)
from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


def make_download_viz_config_template(
    config_url: str = "https://gist.githubusercontent.com/rushirajnenuji/1b41924b8cb81ae8a9795823b9a89ea2/raw/3f0f78840dd345a69e1a863b972eedec6c74c2a6/viz-config.json",
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Script:
    """Creates a template that downloads visualization configuration from a URL.

    Args:
        config_url: URL to the visualization configuration file
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Script template for downloading viz config
    """
    template = Script(
        name="download-viz-config",
        outputs=[
            Artifact(
                name="viz-config-json", path="/mnt/workflow/config/viz-config.json"
            ),
        ],
        image="ghcr.io/rushirajnenuji/vizstaging" if not custom_image else custom_image,
        image_tag=custom_tag if custom_tag else None,
        command=["python"],
        source=f"""
import requests
from pathlib import Path

url = "{config_url}"
output_path = Path("{{{{outputs.artifacts.viz-config-json.path}}}}")
output_path.parent.mkdir(parents=True, exist_ok=True)

response = requests.get(url)
response.raise_for_status()

with open(output_path, "w") as f:
    f.write(response.text)
""",
        volume_mounts=[
            models.VolumeMount(
                name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow"
            )
        ],
    )
    return template


def make_batch_template(
    num_features: int = 200,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Script:
    """Creates a template that batches input data into chunks.

    Args:
        num_features: Number of features to include in each batch
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Script template for batching
    """
    template = Script(
        name="batching",
        inputs=[
            Parameter(name="num_features", value=str(num_features)),
            HTTPArtifact(
                name="batch-input",
                path="/mnt/workflow/input/ice_basins.gpkg",
                url="https://demo.arcticdata.io/tiles/3dtt/Ice_Basins_1000.gpkg",
            ),
        ],
        outputs=[
            Artifact(name="batch-output", path="/mnt/workflow/output/batch"),
        ],
        image="ghcr.io/rushirajnenuji/vizstaging" if not custom_image else custom_image,
        image_tag=custom_tag if custom_tag else None,
        command=["python"],
        source="""
import json
import sys
from pathlib import Path
import geopandas as gpd

# Redirect print statements to stderr instead of stdout
# This way they won't interfere with the JSON output
def print_log(message):
    print(message, file=sys.stderr)

gdf = gpd.read_file("{{inputs.artifacts.batch-input.path}}")
results = []
for idx, start in enumerate(range(0, len(gdf), {{inputs.parameters.num_features}})):
    output_fp = Path("{{outputs.artifacts.batch-output.path}}/" + f"chunk-{idx}.gpkg")
    print_log(f"Writing chunk {idx} to {output_fp}")
    output_fp.parent.mkdir(parents=True, exist_ok=True)
    gdf[start:start+{{inputs.parameters.num_features}}].to_file(
        filename=output_fp,
        driver="GPKG",
    )
    results.append(str(output_fp))

# Output only the JSON to stdout
print(json.dumps(results))
""",
        volume_mounts=[
            models.VolumeMount(
                name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow"
            )
        ],
    )
    return template


def make_tiling_template(
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Script:
    """Creates a template for tiling geospatial data.

    Args:
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Script template for tiling
    """
    template = Script(
        name="tiling",
        inputs=[
            Parameter(name="chunk-filepath"),
        ],
        image="ghcr.io/rushirajnenuji/vizstaging" if not custom_image else custom_image,
        image_tag=custom_tag if custom_tag else None,
        command=["python"],
        source="""
import json
from pathlib import Path
from pdgstaging import TileStager
import sys

# Log to stderr
def print_log(message):
    print(message, file=sys.stderr)

# Read the viz-config.json from the PVC
workflow_config = json.loads(Path("/mnt/workflow/config/viz-config.json").read_text())
stager = TileStager(workflow_config, check_footprints=False)
print_log('Staging chunk file')
print_log('{{inputs.parameters.chunk-filepath}}')
stager.stage("{{inputs.parameters.chunk-filepath}}")
print_log("Staging done")
""",
        volume_mounts=[
            models.VolumeMount(
                name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow"
            )
        ],
    )
    return template


def make_and_submit_parallel_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    custom_namespace: str | None = None,
    update_global: bool = False,
    enable_rasterize: bool = False,
    num_features: int = 200,
    input_url: str = "https://demo.arcticdata.io/tiles/3dtt/Ice_Basins_1000.gpkg",
    config_url: str = "https://gist.githubusercontent.com/rushirajnenuji/1b41924b8cb81ae8a9795823b9a89ea2/raw/3f0f78840dd345a69e1a863b972eedec6c74c2a6/viz-config.json",
) -> str:
    """Create and submit an Argo workflow for parallel processing of geospatial data.

    This workflow follows the pattern from the 'ogdc-recipe-ice-basins-pdg' Argo workflow,
    using Hera's Python API instead of YAML.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
        custom_namespace: Optional custom namespace for the workflow
        update_global: If True, update the global image config; if False, only apply to this workflow
        enable_rasterize: Whether to enable the rasterize step
        num_features: Number of features to process in each batch
        input_url: URL to the input data file
        config_url: URL to the visualization configuration file

    Returns:
        The name of the submitted workflow
    """
    with Workflow(
        generate_name=f"{recipe_config.id}-pdg-",
        entrypoint="main",
        namespace="qgnet",
        service_account_name="argo-workflow",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[
            {
                "name": "qgnet-ogdc-workflow-pvc",
                "persistentVolumeClaim": {"claimName": "qgnet-ogdc-workflow-pvc"},
            }
        ],
        annotations={
            "workflows.argoproj.io/description": "Parallel workflow for PDG visualization tiles"
        },
        labels={"workflows.argoproj.io/archive-strategy": "false"},
    ) as w:
        # Apply custom configuration if provided
        apply_custom_container_config(
            workflow=w,
            custom_image=custom_image,
            custom_tag=custom_tag,
            custom_namespace=custom_namespace,
            update_global=update_global,
        )

        # Create the templates
        download_viz_config_template = make_download_viz_config_template(
            config_url=config_url,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        batch_template = make_batch_template(
            num_features=num_features,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        tiling_template = make_tiling_template(
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        # Set up the DAG
        with DAG(name="main") as dag:
            # Step 1: Download viz-config.json
            download_task = download_viz_config_template()

            # Step 2: Batching step
            batch_task = batch_template()

            # Step 3: Tiling step, depends on both the download and batch tasks
            stage_task = tiling_template(
                arguments={
                    "chunk-filepath": "{{item}}",
                },
                with_param=batch_task.get_result_as("result"),
            )

            # Define task dependencies
            download_task >> batch_task  # Download config before batching
            batch_task >> stage_task  # Batch before tiling

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)
    return workflow_name


def submit_parallel_ogdc_recipe(
    *,
    recipe_dir: str,
    wait: bool,
    overwrite: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    custom_namespace: str | None = None,
    update_global: bool = False,
    enable_rasterize: bool = False,
    num_features: int = 50,
    input_url: str | None = None,
    config_url: str | None = None,
) -> str:
    """Submit an OGDC recipe for parallel processing via Argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete
        overwrite: Whether to overwrite existing published data
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
        custom_namespace: Optional custom namespace for the workflow
        update_global: If True, update the global image config; if False, only apply to this workflow
        enable_rasterize: Whether to enable the rasterize step
        num_features: Number of features to process in each batch
        input_url: Optional URL to the input data file (defaults to Ice_Basins_1000.gpkg)
        config_url: Optional URL to the visualization configuration file

    Returns:
        The name of the submitted workflow
    """
    # Get the recipe configuration
    recipe_config = get_recipe_config(recipe_dir)

    # Check if the user-submitted workflow has already been published
    if data_already_published(
        recipe_config=recipe_config,
        overwrite=overwrite,
        custom_image=custom_image,
        custom_tag=custom_tag,
    ):
        err_msg = f"Data for recipe {recipe_config.id} have already been published."
        raise OgdcDataAlreadyPublished(err_msg)

    # Set default URLs if not provided
    if input_url is None:
        input_url = "https://demo.arcticdata.io/tiles/3dtt/Ice_Basins_1000.gpkg"

    if config_url is None:
        config_url = "https://gist.githubusercontent.com/rushirajnenuji/1b41924b8cb81ae8a9795823b9a89ea2/raw/3f0f78840dd345a69e1a863b972eedec6c74c2a6/viz-config.json"

    # Submit the workflow
    workflow_name = make_and_submit_parallel_workflow(
        recipe_config=recipe_config,
        wait=wait,
        custom_image=custom_image,
        custom_tag=custom_tag,
        custom_namespace=custom_namespace,
        update_global=update_global,
        enable_rasterize=enable_rasterize,
        num_features=num_features,
        input_url=input_url,
        config_url=config_url,
    )

    logger.info(f"Submitted parallel workflow {workflow_name}")
    return workflow_name
