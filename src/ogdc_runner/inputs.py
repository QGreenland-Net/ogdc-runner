"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

from typing import Any

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
        use_pvc: If True, store inputs on PVC; if False, use Argo artifacts

    Returns:
        Container template configured for input fetching

    Raises:
        OgdcWorkflowExecutionError: If unsupported input type is encountered
        NotImplementedError: If PVC mount input type is used (not yet supported)
    """
    output_dir = _get_output_directory(recipe_config.id, use_pvc)
    fetch_commands = _build_fetch_commands(recipe_config.input.params, output_dir)

    return Container(
        name=f"{recipe_config.id}-fetch-template-",
        command=["sh", "-c"],
        args=[f"mkdir -p {output_dir}/ && {fetch_commands}"],
        outputs=[Artifact(name="output-dir", path="/output_dir/")]
        if not use_pvc
        else None,
        volume_mounts=[
            VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow/")
        ]
        if use_pvc
        else None,
    )


def _get_output_directory(recipe_id: str, use_pvc: bool) -> str:
    """Determine the output directory path based on storage type.

    Args:
        recipe_id: Unique recipe identifier
        use_pvc: Whether using PVC storage

    Returns:
        Output directory path
    """
    if use_pvc:
        return f"/mnt/workflow/{recipe_id}/inputs"
    return "/output_dir"


def _build_fetch_commands(params: list[Any], output_dir: str) -> str:
    """Build shell commands to fetch all input parameters.

    Args:
        params: List of input parameters
        output_dir: Directory to store fetched files

    Returns:
        Combined shell command string

    Raises:
        OgdcWorkflowExecutionError: If unsupported input type encountered
        NotImplementedError: If PVC mount type used
    """
    commands = []

    for param in params:
        if param.type == "url":
            commands.append(_build_url_fetch_command(param.value, output_dir))
        elif param.type == "pvc_mount":
            error_msg = "PVC mount inputs are not yet supported"
            raise NotImplementedError(error_msg)
        else:
            raise OgdcWorkflowExecutionError(
                f"Unsupported input type: {param.type} for parameter {param.value}"
            )

    return " && ".join(commands) if commands else "echo 'No input files to fetch'"


def _build_url_fetch_command(url: str, output_dir: str) -> str:
    """Build wget command to fetch a URL.

    Args:
        url: URL to fetch
        output_dir: Directory to save the file

    Returns:
        Shell command string
    """
    return f"wget --content-disposition -P {output_dir}/ {url}"
