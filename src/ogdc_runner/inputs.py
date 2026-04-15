"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

from typing import Any

from hera.workflows import (
    Artifact,
    Container,
)
from hera.workflows.models import VolumeMount

from ogdc_runner.argo import OGDC_WORKFLOW_PVC, get_input_pvc_volume_mounts
from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import (
    DataOneInput,
    PvcMountInput,
    RecipeConfig,
    UrlInput,
)


def make_fetch_input_template(
    recipe_config: RecipeConfig,
    use_pvc: bool = False,
) -> Container:
    """Creates a container template that fetches multiple inputs from URLs or file paths.

    Supports:
    - HTTP/HTTPS URLs
    - File paths (including PVC paths)
    - DataONE datasets

    Args:
        recipe_config: Recipe configuration containing input parameters
        use_pvc: If True, store inputs on PVC; if False, use Argo artifacts

    Returns:
        Container template configured for input fetching

    Raises:
        OgdcWorkflowExecutionError: If unsupported input type is encountered
    """
    output_dir = _get_output_directory(recipe_config.id, use_pvc)
    fetch_commands = _build_fetch_commands(recipe_config.input.params, output_dir)

    volume_mounts: list[VolumeMount] = []
    if use_pvc:
        volume_mounts.append(
            VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow/")
        )
    # Input PVCs must be readable from the fetch step regardless of `use_pvc`:
    # - Sequential path: the fetch step is a no-op that only prints a message,
    #   but keeping mounts consistent avoids surprises.
    # - Parallel/PVC path: the fetch step needs read access too.
    volume_mounts.extend(get_input_pvc_volume_mounts(recipe_config))

    return Container(
        name=f"{recipe_config.id}-fetch-template-",
        command=["sh", "-c"],
        args=[f"mkdir -p {output_dir}/ && {fetch_commands}"],
        outputs=[Artifact(name="output-dir", path="/output_dir/")]
        if not use_pvc
        else None,
        volume_mounts=volume_mounts or None,
    )


def _get_output_directory(recipe_id: str, use_input_as_output: bool) -> str:
    """Determine the output directory path based on whether inputs are stored for reuse.

    Args:
        recipe_id: Unique recipe identifier
        use_input_as_output: If True, return `"/mnt/workflow/{recipe_id}/inputs"`. Otherwise `/output_dir`.

    Returns:
        Output directory as a string.
    """
    if use_input_as_output:
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
    """
    commands = []

    for param in params:
        if isinstance(param, UrlInput):
            commands.append(_build_url_fetch_command(str(param.value), output_dir))
        elif isinstance(param, DataOneInput):
            # DataONE input - download all resolved objects
            if param.resolved_objects:
                for obj in param.resolved_objects:
                    url = obj["url"]
                    commands.append(f"wget --content-disposition -P /output_dir/ {url}")
            else:
                raise OgdcWorkflowExecutionError(
                    f"DataONE input has no resolved objects: {param}"
                )
        elif isinstance(param, PvcMountInput):
            # PVC inputs are mounted directly into every workflow container —
            # there is nothing to download. Emit a marker echo so the fetch step
            # has a real command to run and its output artifact is non-empty.
            commands.append(
                f"echo 'pvc input mounted at {param.full_path}'"
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
