from __future__ import annotations

from hera.workflows import (
    DAG,
    Artifact,
    HTTPArtifact,
    Parameter,
    Workflow,
    script,
)
from hera.workflows.models import (
    PersistentVolumeClaimVolumeSource,
    Volume,
    VolumeMount,
)
from loguru import logger
from pydantic import AnyUrl

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    apply_custom_container_config,
    submit_workflow,
)
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


@script(
    name="download-viz-config",
    outputs=[
        Artifact(name="viz-config-json", path="/mnt/workflow/config/viz-config.json"),
    ],
    image="python:3.10-slim",
    command=["python"],
    volume_mounts=[
        VolumeMount(name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow")
    ],
)
def download_viz_config(config_url) -> None:  # type: ignore[no-untyped-def]
    """Downloads visualization configuration from a URL."""

    from pathlib import Path

    import requests  # type: ignore[import-untyped]

    output_path = Path("{{outputs.artifacts.viz-config-json.path}}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(config_url)
    response.raise_for_status()

    with Path.open(output_path, "w") as f:
        f.write(response.text)


@script(
    name="batching",
    inputs=[
        Parameter(name="input_url"),
        HTTPArtifact(
            name="batch-input",
            path="/mnt/workflow/input/ice_basins.gpkg",
            url="{{inputs.parameters.input_url}}",
        ),
    ],
    outputs=[
        Artifact(
            name="batch-output",
            path="/mnt/workflow/output/batch",
        ),
    ],
    image="ghcr.io/rushirajnenuji/viz-staging:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow")
    ],
)
def batch_process(input_url, num_features) -> None:  # type: ignore[no-untyped-def] # noqa: ARG001
    """Processes data in batches."""
    import json
    import sys
    from pathlib import Path

    import geopandas as gpd  # type: ignore[import-not-found]

    # Redirect print statements to stderr instead of stdout
    # This way they won't interfere with the JSON output
    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    gdf = gpd.read_file("{{inputs.artifacts.batch-input.path}}")
    results = []
    for idx, start in enumerate(range(0, len(gdf), num_features)):
        output_fp = Path(
            "{{outputs.artifacts.batch-output.path}}/" + f"chunk-{idx}.gpkg"
        )
        print_log(f"Writing chunk {idx} to {output_fp}")
        output_fp.parent.mkdir(parents=True, exist_ok=True)
        gdf[start : start + num_features].to_file(
            filename=output_fp,
            driver="GPKG",
        )
        results.append(str(output_fp))

    # Output only the JSON to stdout
    print(json.dumps(results))


@script(
    name="tiling",
    inputs=[
        Parameter(name="chunk-filepath"),
    ],
    image="ghcr.io/rushirajnenuji/viz-staging:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name="qgnet-ogdc-workflow-pvc", mount_path="/mnt/workflow")
    ],
)
def tiling_process() -> None:
    """Creates tiles from a geospatial data chunk."""
    import json
    import sys
    from pathlib import Path

    from pdgstaging import TileStager  # type: ignore[import-not-found]

    # Log to stderr
    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    # Read the viz-config.json from the PVC
    workflow_config = json.loads(
        Path("/mnt/workflow/config/viz-config.json").read_text()
    )
    stager = TileStager(workflow_config, check_footprints=False)
    print_log("Staging chunk file")
    print_log("{{inputs.parameters.chunk-filepath}}")
    stager.stage("{{inputs.parameters.chunk-filepath}}")
    print_log("Staging done")


def make_and_submit_viz_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    update_global: bool = False,
    enable_tiling: bool = False,
    enable_rasterize: bool = False,
    enable_3dtiles: bool = False,
    num_features: int = 250,
    input_url: AnyUrl | str | None = None,
    config_url: AnyUrl | str | None = None,
) -> str:
    """Create and submit an Argo workflow for parallel processing of geospatial data.

    This workflow follows the pattern from the 'ogdc-recipe-ice-basins-pdg' Argo workflow,
    using Hera's Python API instead of YAML.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
        update_global: If True, update the global image config; if False, only apply to this workflow
        enable_rasterize: Whether to enable the rasterize step
        num_features: Number of features to process in each batch
        input_url: URL to the input data file
        config_url: URL to the visualization configuration file

    Returns:
        The name of the submitted workflow
    """
    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="main",
        namespace="qgnet",
        service_account_name="argo-workflow",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[
            Volume(
                name="qgnet-ogdc-workflow-pvc",
                persistent_volume_claim=PersistentVolumeClaimVolumeSource(
                    claim_name="qgnet-ogdc-workflow-pvc"
                ),
            )
        ],
        annotations={
            "workflows.argoproj.io/description": "Visualization workflow for OGDC",
        },
        labels={"workflows.argoproj.io/archive-strategy": "false"},
    ) as w:
        # Apply custom configuration if provided
        apply_custom_container_config(
            custom_image=custom_image,
            custom_tag=custom_tag,
            update_global=update_global,
        )

        # Set up the DAG
        with DAG(name="main"):
            # Step 1: Download viz-config.json
            download_task = download_viz_config(arguments={"config_url": config_url})

            # Step 2: Batching step
            batch_task = batch_process(
                arguments={"input_url": input_url, "num_features": num_features}
            )

            # Step 3: Tiling step, depends on batch task results
            if enable_tiling:
                stage_task = tiling_process(
                    arguments={
                        "chunk-filepath": "{{item}}",
                    },
                    with_param=batch_task.get_result_as("result"),
                )

            # Step 4: Rasterization step (if needed)
            if enable_rasterize:
                pass  # Placeholder for rasterization logic

            # Step 5: 3D Tiles step (if needed)
            if enable_3dtiles:
                pass  # Placeholder for 3D Tiles logic

            if enable_tiling:
                [  # type: ignore[operator]
                    download_task,
                    batch_task,
                ] >> stage_task
                # tiling depends on both download and batch tasks

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)
    return workflow_name


def submit_viz_workflow_recipe(
    *,
    recipe_dir: str,
    wait: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    update_global: bool = False,
    num_features: int = 50,
) -> str:
    """Submit an OGDC recipe for parallel processing via Argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete
        overwrite: Whether to overwrite existing published data
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
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

    params = recipe_config.input.params
    if params and len(params) >= 2:
        input_url = params[0]
        config_url = params[1]

    # Submit the workflow
    workflow_name = make_and_submit_viz_workflow(
        recipe_config=recipe_config,
        wait=wait,
        custom_image=custom_image,
        custom_tag=custom_tag,
        update_global=update_global,
        num_features=num_features,
        input_url=input_url,
        config_url=config_url,
    )

    logger.info(f"Completed workflow: {workflow_name}")
    return workflow_name
