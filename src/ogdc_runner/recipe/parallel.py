from __future__ import annotations

from hera.workflows import (
    DAG,
    Artifact,
    Container,
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


def make_batch_template(
    input_url: str,
    num_features: int = 200,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Script:
    """Creates a template that batches input data into chunks.

    Args:
        input_url: URL to the input data file
        num_features: Number of features to include in each batch
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Script template for batching
    """
    template = Script(
        name="batch-template",
        inputs=[
            Parameter(name="num_features", value=str(num_features)),
            Artifact(
                name="batch-input",
                http=models.HTTPArtifact(url=input_url),
                path="/tmp/pdg_processing/input/ice_basins.gpkg",
            ),
        ],
        outputs=[
            Artifact(name="batch-output", path="/tmp/pdg_processing/output/batch"),
        ],
        image="ghcr.io/mfisher87/pdgstaging" if not custom_image else custom_image,
        image_tag=custom_tag if custom_tag else None,
        source="""
import json
import sys
from pathlib import Path

import geopandas as gpd

gdf = gpd.read_file("{{inputs.artifacts.batch-input.path}}")
results = []
for idx, start in enumerate(range(0, len(gdf), {{inputs.parameters.num_features}})):
    output_fp = Path("{{outputs.artifacts.batch-output.path}}/" + f"chunk-{idx}.gpkg")
    output_fp.parent.mkdir(parents=True, exist_ok=True)
    gdf[start:start+{{inputs.parameters.num_features}}].to_file(
        filename=output_fp,
        driver="GPKG",
    )
    results.append(str(output_fp))

json.dump(results, sys.stdout)
        """,
        command=["python"],
    )

    return template


def make_stage_template(
    config_url: str,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Script:
    """Creates a template that stages data for visualization.

    Args:
        config_url: URL to the visualization configuration file
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Script template for staging
    """
    template = Script(
        name="stage-template",
        inputs=[
            Parameter(name="chunk-filepath"),
            Artifact(
                name="viz-config-json",
                http=models.HTTPArtifact(url=config_url),
                path="/tmp/config.json",
            ),
            Artifact(name="batch-output", path="/tmp/pdg_processing/output/batch"),
        ],
        outputs=[
            Artifact(name="staging-output", path="/tmp/pdg_processing/output/staged"),
        ],
        image="ghcr.io/mfisher87/pdgstaging" if not custom_image else custom_image,
        image_tag=custom_tag if custom_tag else None,
        source="""
import json
from pathlib import Path

from pdgstaging import TileStager

workflow_config = json.loads(Path("{{inputs.artifacts.viz-config-json.path}}").read_text())
stager = TileStager(workflow_config, check_footprints=False)
stager.stage("{{inputs.parameters.chunk-filepath}}")
print("Staging done")
        """,
        command=["python"],
    )

    return template


def make_rasterize_template(
    config_url: str,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Container:
    """Creates a template that rasterizes staged data.

    Args:
        config_url: URL to the visualization configuration file
        custom_image: Optional custom container image
        custom_tag: Optional custom container image tag

    Returns:
        A Container template for rasterization
    """
    template = Container(
        name="rasterize-template",
        inputs=[
            Artifact(
                name="viz-config-json",
                http=models.HTTPArtifact(url=config_url),
                path="/tmp/config.json",
            ),
            Artifact(name="staging-output", path="/tmp/pdg_processing/output/staged"),
        ],
        outputs=[
            Artifact(name="raster-output", path="/tmp/pdg_processing/output/raster"),
        ],
        image="ghcr.io/permafrostdiscoverygateway/viz-workflow:0.2.3"
        if not custom_image
        else custom_image,
        image_tag=custom_tag if custom_tag else None,
        command=["python"],
        args=["-m", "pdgraster", "-c", "{{inputs.artifacts.viz-config-json.path}}"],
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
    config_url: str = "https://gist.githubusercontent.com/mfisher87/f13f87949809a4eef0485f3eb05b9534/raw/47cbf12b30b40d0a51da51dd729985ff5a8459e8/qgnet_config.json",
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
        workflows_service=ARGO_WORKFLOW_SERVICE,
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

        # Create the batch template
        batch_template = make_batch_template(
            input_url=input_url,
            num_features=num_features,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        # Create the stage template
        stage_template = make_stage_template(
            config_url=config_url,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        # Create the rasterize template if enabled
        if enable_rasterize:
            rasterize_template = make_rasterize_template(
                config_url=config_url,
                custom_image=custom_image,
                custom_tag=custom_tag,
            )

        # Set up the DAG
        with DAG(name="main"):
            # Batch task
            batch_task = batch_template(name="batch")

            # Stage task with parameter from batch
            stage_task = stage_template(
                name="stage",
                arguments={
                    "parameters": {"chunk-filepath": "{{item}}"},
                    "artifacts": {
                        "batch-output": batch_task.get_artifact("batch-output")
                    },
                },
                with_param="{{tasks.batch.outputs.result}}",
            ).after(batch_task)

            # Rasterize task if enabled
            if enable_rasterize:
                rasterize_template(
                    name="rasterize",
                    arguments={
                        "artifacts": {
                            "staging-output": stage_task.get_artifact("staging-output")
                        }
                    },
                ).after(stage_task)

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
        config_url = "https://gist.githubusercontent.com/mfisher87/f13f87949809a4eef0485f3eb05b9534/raw/47cbf12b30b40d0a51da51dd729985ff5a8459e8/qgnet_config.json"

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
