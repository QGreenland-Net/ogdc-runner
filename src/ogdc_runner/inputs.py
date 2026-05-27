"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

import json
import shlex
from importlib.resources import files
from typing import Any

from hera.workflows import (
    Artifact,
    Container,
    Parameter,
)
from hera.workflows.models import ValueFrom, VolumeMount

from ogdc_runner.argo import OGDC_WORKFLOW_PVC, get_input_pvc_volume_mounts
from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import (
    DataOneInput,
    PvcMountInput,
    RecipeConfig,
    UrlInput,
)
from ogdc_runner.partition_manifests import FILES_MANIFEST_PATH_PARAM


def _dedupe_volume_mounts(mounts: list[VolumeMount]) -> list[VolumeMount]:
    deduped: list[VolumeMount] = []
    seen: set[tuple[str | None, str | None]] = set()
    for mount in mounts:
        key = (mount.name, mount.mount_path)
        if key in seen:
            continue
        deduped.append(mount)
        seen.add(key)
    return deduped


def make_pvc_listing_template(
    pvc_inputs: list[PvcMountInput],
    partition_size: int,
    input_pvc_mounts: list[VolumeMount],
    *,
    name: str = "list-pvc-files",
    image: str | None = None,
) -> Container:
    """Create a container that enumerates PVC input files at runtime.

    The container writes full manifests to the workflow PVC and only exposes
    compact manifest references as Argo output parameters.
    """
    script_template = (
        files("ogdc_runner.scripts").joinpath("list_pvc_inputs.sh").read_text()
    )
    pvc_inputs_json = json.dumps(
        [
            {"path": pvc_input.full_path, "glob": pvc_input.glob}
            for pvc_input in pvc_inputs
        ]
    )
    listing_cmd = script_template.replace(
        "{pvc_inputs_json}",
        f"PVC_INPUTS_JSON={shlex.quote(pvc_inputs_json)}",
    ).replace(
        "{partition_size}",
        shlex.quote(str(partition_size)),
    )

    container = Container(
        name=name,
        command=["sh", "-c"],
        args=[listing_cmd],
        inputs=[Parameter(name="recipe-id")],
        outputs=[
            Parameter(
                name="partitions",
                value_from=ValueFrom(path="/tmp/partitions.json"),
            ),
            Parameter(
                name=FILES_MANIFEST_PATH_PARAM,
                value_from=ValueFrom(path="/tmp/files_manifest_path.txt"),
            ),
        ],
        volume_mounts=_dedupe_volume_mounts(
            [
                VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow"),
                *input_pvc_mounts,
            ]
        ),
    )
    if image is not None:
        container.image = image
    return container


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
    # Always mount input PVCs so the fetch step can access them.
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

    pvc_inputs: list[PvcMountInput] = []
    has_fetched_inputs = False
    for param in params:
        if isinstance(param, UrlInput):
            has_fetched_inputs = True
            commands.append(_build_url_fetch_command(str(param.value), output_dir))
        elif isinstance(param, DataOneInput):
            has_fetched_inputs = True
            # DataONE input - download all resolved objects
            if param.resolved_objects:
                for obj in param.resolved_objects:
                    url = obj["url"]
                    commands.append(
                        f"wget --content-disposition -P {output_dir}/ {url}"
                    )
            else:
                raise OgdcWorkflowExecutionError(
                    f"DataONE input has no resolved objects: {param}"
                )
        elif isinstance(param, PvcMountInput):
            pvc_inputs.append(param)

    if pvc_inputs:
        if has_fetched_inputs:
            msg = "pvc_mount inputs cannot be combined with URL or DataONE inputs"
            raise OgdcWorkflowExecutionError(msg)
        commands.append(_build_pvc_stage_command(pvc_inputs, output_dir))

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


def _build_pvc_stage_command(
    pvc_inputs: list[PvcMountInput],
    output_dir: str,
) -> str:
    script_template = (
        files("ogdc_runner.scripts").joinpath("stage_pvc_inputs.sh").read_text()
    )
    pvc_inputs_json = json.dumps(
        [
            {"path": pvc_input.full_path, "glob": pvc_input.glob}
            for pvc_input in pvc_inputs
        ]
    )
    return script_template.replace(
        "{pvc_inputs_json}",
        f"PVC_INPUTS_JSON={shlex.quote(pvc_inputs_json)}",
    ).replace(
        "{output_dir}",
        shlex.quote(output_dir),
    )
