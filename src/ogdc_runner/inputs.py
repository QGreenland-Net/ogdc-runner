"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
)
from hera.workflows.models import VolumeMount

from ogdc_runner.argo import OGDC_WORKFLOW_PVC
from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import RecipeConfig


def make_fetch_input_template(
    recipe_config: RecipeConfig,
    use_pvc: bool = False,
) -> Container:
    """Creates a container template that fetches multiple inputs from URLs or file paths.

    Supports:
    - HTTP/HTTPS URLs
    - File paths (including PVC paths)

    Args:
        recipe_config: Recipe configuration containing input parameters
        use_pvc: Whether to use PVC for input/output

    Returns:
        Container template to fetch inputs
    """
    output_dir = (
        f"/mnt/workflow/{recipe_config.id}/output_dir" if use_pvc else "/output_dir"
    )

    fetch_commands = []
    for param in recipe_config.input.params:
        if param.type == "url":
            fetch_commands.append(
                f"wget --content-disposition -P {output_dir}/ {param.value}"
            )
        elif param.type == "pvc_mount":
            err_msg = "PVC mounts are not yet supported"
            raise NotImplementedError(err_msg)
        else:
            raise OgdcWorkflowExecutionError(
                f"Unsupported input type: {param.type} for parameter {param.value}"
            )

    combined_command = " && ".join(fetch_commands) or "echo 'No input files to fetch'"

    return Container(
        name=f"{recipe_config.id}-fetch-template-",
        command=["sh", "-c"],
        args=[f"mkdir -p {output_dir}/ && {combined_command}"],
        outputs=[Artifact(name="output-dir", path="/output_dir/")]
        if not use_pvc
        else None,
        volume_mounts=[VolumeMount(name=OGDC_WORKFLOW_PVC, mount_path="/mnt/workflow/")]
        if use_pvc
        else None,
    )
