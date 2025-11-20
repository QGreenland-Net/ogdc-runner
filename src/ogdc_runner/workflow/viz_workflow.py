from __future__ import annotations

import json

from hera.workflows import (
    DAG,
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

from ogdc_runner.argo import (
    ARGO_MANAGER,
    ARGO_WORKFLOW_SERVICE,
    OGDC_WORKFLOW_PVC,
    submit_workflow,
)
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.recipe_config import RecipeConfig

# ruff: noqa: PLC0415


@script(
    name="staging",
    inputs=[
        Parameter(name="input_url"),
        Parameter(name="recipe_id"),
        HTTPArtifact(
            name="staging-input",
            path="/mnt/workflow/{{inputs.parameters.recipe_id}}/input/input.gpkg",
            url="{{inputs.parameters.input_url}}",
        ),
    ],
    image="ghcr.io/permafrostdiscoverygateway/pdgworkflow:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
    ],
)
def staging_process() -> None:
    """Stages the input file directly for visualization."""
    import sys
    from pathlib import Path

    from pdgworkflow import (  # type: ignore[import-not-found]
        WorkflowManager,
    )

    # Log to stderr
    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    # Read the viz-config.json from the PVC
    # This configuration controls how VizWorkflow processes the visualization data.
    # For available configuration options, see:
    # https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
    workflow_config = json.loads(
        Path("/mnt/workflow/{{inputs.parameters.recipe_id}}/config.json").read_text()
    )

    workflow_manager = WorkflowManager(workflow_config)
    print_log("Staging input file")
    print_log("{{inputs.artifacts.staging-input.path}}")
    workflow_manager.stage("{{inputs.artifacts.staging-input.path}}")
    print_log("Staging done")

    # Get list of staged files and return as JSON to stdout
    staged_files = workflow_manager.list_staged_files()
    print_log(f"Staged {len(staged_files)} files")

    # Print to stdout (NOT stderr) so Hera can capture it
    print(json.dumps(staged_files))


@script(
    name="rasterization",
    inputs=[
        Parameter(name="recipe_id"),
        Parameter(name="staged_files"),  # Receive the list of staged files
    ],
    image="ghcr.io/permafrostdiscoverygateway/pdgworkflow:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
    ],
)
def rasterization_process() -> None:
    """
    Creates geotiff and summary web tiles (png) from a geospatial vector data tile.
    """
    import json
    import sys
    from pathlib import Path

    from pdgworkflow import (  # type: ignore[import-not-found]
        WorkflowManager,
    )

    # Log to stderr
    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    # Read the viz-config.json from the PVC
    # This configuration controls how VizWorkflow processes the visualization data.
    # For available configuration options, see:
    # https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
    workflow_config = json.loads(
        Path("/mnt/workflow/{{inputs.parameters.recipe_id}}/config.json").read_text()
    )

    # Parse the staged files list
    staged_files = json.loads("{{inputs.parameters.staged_files}}")
    print_log(f"Received {len(staged_files)} staged files: {staged_files}")

    workflow_manager = WorkflowManager(workflow_config)
    print_log("Rasterizing files..")
    workflow_manager.rasterize_all()
    print_log("Rasterizing done")


@script(
    name="b3dm-tiling",
    inputs=[
        Parameter(name="recipe_id"),
        Parameter(name="staged_files"),  # Receive the list of staged files
    ],
    image="ghcr.io/permafrostdiscoverygateway/pdgworkflow:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
    ],
)
def b3dm_tiling_process() -> None:
    """Creates B3DM tiles (3D Tiles) from a geospatial vector data tile."""
    import json
    import sys
    from pathlib import Path

    from pdgworkflow import (  # type: ignore[import-not-found]
        WorkflowManager,
    )

    def print_log(message: str) -> None:
        print(message, file=sys.stderr)

    # Read the viz-config.json from the PVC
    # This configuration controls how VizWorkflow processes the visualization data.
    # For available configuration options, see:
    # https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
    workflow_config = json.loads(
        Path("/mnt/workflow/{{inputs.parameters.recipe_id}}/config.json").read_text()
    )

    # Parse the staged files list
    staged_files = json.loads("{{inputs.parameters.staged_files}}")
    print_log(f"Received {len(staged_files)} staged files: {staged_files}")

    workflow_manager = WorkflowManager(workflow_config)
    print_log("Generating b3dm 3D tiles..")
    workflow_manager.run_3d_tiling()
    print_log("b3dm 3D tiles generation completed.")


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

    input_param = recipe_config.input.params[0]
    if input_param.type == "url":
        input_url = input_param.value
    else:
        raise NotImplementedError(
            f"Input type '{input_param.type}' is not supported for visualization workflows. "
            f"Only 'url' input type is currently supported."
        )

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="main",
        namespace=ARGO_MANAGER.config.namespace,
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
        config_content = recipe_config.workflow.get_config_file_json()

        stage_config_file_template = Container(
            name="stage-viz-config",
            command=["sh", "-c"],
            args=[
                f"""mkdir -p /mnt/workflow/{recipe_config.id}/input && \\
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
            # Step 1: Stage the viz-config.json file
            # This is the configuration file for the visualization workflow
            # It also creates necessary directories required for the workflow
            # to run successfully
            stage_config_task = Task(
                name="stage-viz-config",
                template=stage_config_file_template,
            )

            # Step 2: Staging/Tiling step
            # Directly stage the input file for visualization
            stage_task = staging_process(
                arguments={
                    "input_url": input_url,
                    "recipe_id": recipe_config.id,
                },
            )

            # Step 3: Rasterization step
            # This step creates geotiff and summary web tiles (png)
            # from the staged geospatial vector data
            rasterization_task = rasterization_process(
                arguments={
                    "recipe_id": recipe_config.id,
                    "staged_files": stage_task.get_result_as("result"),
                }
            )

            # Step 4: B3DM Tiling step
            # This step generates 3D tiles from the staged vector data
            b3dm_tiling_task = b3dm_tiling_process(
                arguments={
                    "recipe_id": recipe_config.id,
                    "staged_files": stage_task.get_result_as("result"),
                }
            )

            # Define the workflow structure
            # Staging depends on config staging
            # Rasterization and b3dm_tiling execute in parallel after staging
            (
                stage_config_task
                >> stage_task
                >> [
                    rasterization_task,
                    b3dm_tiling_task,
                ]
            )

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)
    return workflow_name
