from __future__ import annotations

import json

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    HTTPArtifact,
    Parameter,
    Task,
    script,
)
from hera.workflows.models import (
    VolumeMount,
)

from ogdc_runner.argo import (
    OGDC_WORKFLOW_PVC,
    OgdcWorkflow,
    submit_workflow,
)
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.recipe_config import PvcRecipeOutput, RecipeConfig

# ruff: noqa: PLC0415


@script(
    name="batching",
    inputs=[
        Parameter(name="input_url"),
        Parameter(name="recipe_id"),
        HTTPArtifact(
            name="batch-input",
            path="/mnt/workflow/{{inputs.parameters.recipe_id}}/input/input.gpkg",
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

    from pdgstaging import (  # type: ignore[import-not-found]
        TileStager,
    )  # Log to stderr

    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    # Read the viz-config.json from the PVC
    # This configuration controls how VizWorkflow processes the visualization data.
    # For available configuration options, see:
    # https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
    workflow_config = json.loads(
        Path("/mnt/workflow/{{inputs.parameters.recipe_id}}/config.json").read_text()
    )

    tiler = TileStager(workflow_config, check_footprints=False)
    print_log("Staging chunk file")
    print_log("{{inputs.parameters.chunk-filepath}}")
    tiler.stage("{{inputs.parameters.chunk-filepath}}")
    print_log("Staging done")


def make_and_submit_viz_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
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
    if recipe_config.workflow.type != "visualization":
        err_msg = f"Expected recipe configuration with workflow type `visualization`. Got: {recipe_config.workflow.type}"
        raise OgdcInvalidRecipeConfig(err_msg)

    if not isinstance(recipe_config.output, PvcRecipeOutput):
        # The viz workflow writes outputs to PVC (eventually this will write to
        # a tile store). Temporary outputs and publishing to a dataset are not
        # supported.
        err_msg = (
            f"Expected PVC output type for viz workflow. Got: {recipe_config.output}"
        )
        raise OgdcInvalidRecipeConfig(err_msg)

    input_param = recipe_config.input.params[0]
    if input_param.type == "url":
        input_url = input_param.value
    else:
        raise NotImplementedError(
            f"Input type '{input_param.type}' is not supported for visualization workflows. "
            f"Only 'url' input type is currently supported."
        )

    with OgdcWorkflow(
        name="visualization",
        recipe_config=recipe_config,
        archive_workflow=True,
        entrypoint="main",
        volumes=[OGDC_WORKFLOW_PVC],
        annotations={
            "workflows.argoproj.io/description": "Visualization workflow for OGDC",
        },
        labels={
            "workflows.argoproj.io/archive-strategy": "false",
        },
    ) as w:
        # Create templates outside the DAG context
        # Read the config.json file content from the recipe directory
        config_content = recipe_config.workflow.get_config_file_json()

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
            # Step1: Stage the viz-config.json file
            # This is the configuration file for the visualization workflow
            # It also creates necessary directories required for the workflow
            # to run successfully
            stage_config_task = Task(
                name="stage-viz-config",
                template=stage_config_file_template,
            )

            # Step 2: Batching step
            batch_task = batch_process(
                arguments={
                    "input_url": input_url,
                    "recipe_id": recipe_config.id,
                    "num_features": recipe_config.workflow.batch_size,
                },
            )

            # Step 3: Tiling step
            # This step processes each chunk of vector data in parallel
            # using the tiling_process script defined above
            stage_task = tiling_process(
                arguments={
                    "chunk-filepath": "{{item}}",
                    "recipe_id": recipe_config.id,
                },
                with_param=batch_task.get_result_as("result"),
            )

            # Define the workflow structure
            # tiling depends on both download and batch tasks
            [  # type: ignore[operator]
                stage_config_task,
                batch_task,
            ] >> stage_task

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)
    return workflow_name
