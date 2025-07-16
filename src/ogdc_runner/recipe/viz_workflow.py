from __future__ import annotations

import json
from pathlib import Path

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    HTTPArtifact,
    Parameter,
    Task,
    Workflow,
    script,
)
from hera.workflows.models import (
    VolumeMount,
)
from loguru import logger
from pydantic import AnyUrl

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    OGDC_WORKFLOW_PVC,
    submit_workflow,
)
from ogdc_runner.constants import VIZ_RECIPE_BATCH_SIZE
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


@script(
    name="batching",
    inputs=[
        Parameter(name="input_url"),
        Parameter(name="recipe_id"),
        HTTPArtifact(
            name="batch-input",
            path="/mnt/workflow/{{inputs.parameters.recipe_id}}/input/ice_basins.gpkg",
            url="{{inputs.parameters.input_url}}",
        ),
    ],
    outputs=[
        Artifact(
            name="batch-output",
            path="/mnt/workflow/{{inputs.parameters.recipe_id}}/batch",
        ),
    ],
    image="ghcr.io/rushirajnenuji/viz-staging:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
    ],
)
def batch_process(num_features) -> None:  # type: ignore[no-untyped-def]
    """Processes data in batches."""
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
        Parameter(name="recipe_id"),
    ],
    image="ghcr.io/rushirajnenuji/viz-staging:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
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
        Path("/mnt/workflow/{{inputs.parameters.recipe_id}}/config.json").read_text()
    )

    tiler = TileStager(workflow_config, check_footprints=False)
    print_log("Staging chunk file")
    print_log("{{inputs.parameters.chunk-filepath}}")
    tiler.stage("{{inputs.parameters.chunk-filepath}}")
    print_log("Staging done")


def read_config_file_content(recipe_config: RecipeConfig) -> str:
    """Read the config.json file content from the recipe directory.

    Args:
        recipe_config: The recipe configuration containing the directory path

    Returns:
        The content of the config.json file as a string, or empty JSON if file doesn't exist
    """
    config_file_path = Path(recipe_config.recipe_directory) / "config.json"
    if config_file_path.exists():
        return config_file_path.read_text()
    # Fallback to empty config if file doesn't exist
    return "{}"


def make_and_submit_viz_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
    input_url: AnyUrl | str | None = None,
) -> str:
    """Create and submit an Argo workflow for parallel processing of geospatial data.

    This workflow follows the pattern from the 'ogdc-recipe-ice-basins-pdg' Argo workflow,
    using Hera's Python API instead of YAML.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete
        input_url: URL to the input data file

    Returns:
        The name of the submitted workflow
    """
    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="main",
        namespace="qgnet",
        service_account_name="argo-workflow",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[OGDC_WORKFLOW_PVC],
        annotations={
            "workflows.argoproj.io/description": "Visualization workflow for OGDC",
        },
        labels={"workflows.argoproj.io/archive-strategy": "false"},
    ) as w:
        # Create templates outside the DAG context
        # Read the config.json file content from the recipe directory
        config_content = read_config_file_content(recipe_config)

        stage_config_file_template = Container(
            name="stage-viz-config",
            image="alpine:latest",
            command=["sh", "-c"],
            args=[
                f"""mkdir -p /mnt/workflow/{recipe_config.id}/input && \\
mkdir -p /mnt/workflow/{recipe_config.id}/batch && \\
mkdir -p /mnt/workflow/{recipe_config.id}/output/staged && \\
mkdir -p /mnt/workflow/{recipe_config.id}/output/geotiff && \\
mkdir -p /mnt/workflow/{recipe_config.id}/output/3dtiles && \\
cat > /mnt/workflow/{recipe_config.id}/config.json << 'EOF'
{config_content}
EOF"""
            ],
            volume_mounts=[
                VolumeMount(
                    name=OGDC_WORKFLOW_PVC.name,
                    mount_path="/mnt/workflow/",
                )
            ],
        )

        # Set up the DAG
        with DAG(name="main"):
            # Step 1: Download viz-config.json
            # download_task = download_viz_config(arguments={"config_url": config_url})

            stage_config_task = Task(
                name="stage-viz-config",
                template=stage_config_file_template,
            )

            # Step 2: Batching step
            batch_task = batch_process(
                arguments={
                    "input_url": input_url,
                    "recipe_id": recipe_config.id,
                    "num_features": VIZ_RECIPE_BATCH_SIZE,
                },
            )

            stage_task = tiling_process(
                arguments={
                    "chunk-filepath": "{{item}}",
                    "recipe_id": recipe_config.id,
                },
                with_param=batch_task.get_result_as("result"),
            )

            [  # type: ignore[operator]
                stage_config_task,
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
) -> str:
    """Submit an OGDC recipe for parallel processing via Argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete

    Returns:
        The name of the submitted workflow
    """
    # Get the recipe configuration
    recipe_config = get_recipe_config(recipe_dir)

    if recipe_config.image is not None:
        # Update Argo custom image
        pass

    input_param = recipe_config.input.params[0]
    if input_param.type == "url":
        input_url = input_param.value

    # Submit the workflow
    workflow_name = make_and_submit_viz_workflow(
        recipe_config=recipe_config,
        wait=wait,
        input_url=input_url,
    )

    logger.info(f"Completed workflow: {workflow_name}")
    return workflow_name
