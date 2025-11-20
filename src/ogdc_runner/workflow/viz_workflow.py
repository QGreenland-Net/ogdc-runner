from __future__ import annotations

import json
from pathlib import Path

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
from ogdc_runner.models.recipe_config import RecipeConfig, VizWorkflow

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
    image="ghcr.io/rushirajnenuji/viz-staging:latest",
    command=["python"],
    volume_mounts=[
        VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
    ],
)
def staging_process() -> None:
    """Stages the input file directly for visualization."""
    import sys
    from pathlib import Path

    from pdgstaging import (  # type: ignore[import-not-found]
        TileStager,
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

    tiler = TileStager(workflow_config, check_footprints=False)
    print_log("Staging input file")
    print_log("{{inputs.artifacts.staging-input.path}}")
    tiler.stage("{{inputs.artifacts.staging-input.path}}")
    print_log("Staging done")


def read_config_file_content(
    recipe_directory: Path, workflow_config: VizWorkflow
) -> str:
    """Read the viz workflow config json file content from the recipe directory.

    This configuration file is used by the pdgworkflow for visualization workflows.
    When an empty config ({}) is returned, WorkflowManager will use its default behavior.

    For documentation on available configuration options, see:
    - ConfigManager documentation: https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
    - Example config: https://github.com/QGreenland-Net/ogdc-recipes/blob/main/recipes/viz-workflow/config.json

    Args:
        recipe_config: The recipe configuration containing the directory path

    Returns:
        The content of the config.json file as a string, or empty JSON if file doesn't exist.
        An empty config ({}) will cause ConfigManager to use default behavior.
    """
    config_file_path = recipe_directory / workflow_config.config_file
    # TODO: this should utilize fsspec like `get_recipe_config`! We expect to be
    # able to access config that's stored on a Git Repo. If that's used here,
    # the defaults will be returned. See:
    # https://github.com/QGreenland-Net/ogdc-runner/issues/101
    if config_file_path.exists():
        return config_file_path.read_text()
    # Fallback to empty config if file doesn't exist - ConfigManager will use defaults
    return "{}"


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
        config_content = read_config_file_content(
            Path(recipe_config.recipe_directory),
            recipe_config.workflow,
        )

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
            # Step1: Stage the viz-config.json file
            # This is the configuration file for the visualization workflow
            # It also creates necessary directories required for the workflow
            # to run successfully
            stage_config_task = Task(
                name="stage-viz-config",
                template=stage_config_file_template,
            )

            # Step 2: Staging step
            # Directly stage the input file for visualization
            stage_task = staging_process(
                arguments={
                    "input_url": input_url,
                    "recipe_id": recipe_config.id,
                },
            )

            # Define the workflow structure
            # staging depends on config staging
            stage_config_task >> stage_task

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)
    return workflow_name
