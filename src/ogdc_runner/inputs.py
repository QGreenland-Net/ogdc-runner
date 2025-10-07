"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
)

from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import RecipeConfig


def make_fetch_input_template(
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that fetches multiple inputs from URLs or file paths.

    Supports:
    - HTTP/HTTPS URLs
    - File paths (including PVC paths)
    """
    # Create commands to fetch each input
    fetch_commands = []

    for param in recipe_config.input.params:
        # Check if the parameter is a URL
        if param.type == "url":
            # It's a URL, use wget
            fetch_commands.append(
                f"wget --content-disposition -P /output_dir/ {param.value}"
            )
        elif param.type == "file_system":
            filename = str(param.value).split("/")[-1]
            fetch_commands.append(f"cp {param.value} /output_dir/{filename}")
        elif param.type == "pvc_mount":
            # TODO: support PVC paths as input.
            # Because it is a PVC, we expect it to be mounted to the first
            # step's container, so no move should be necessary.
            err_msg = "PVC mounts are not yet supported"
            raise NotImplementedError(err_msg)
        else:
            raise OgdcWorkflowExecutionError(
                f"Unsupported input type: {param.type} for parameter {param.value}"
            )

    # Join all commands with && for sequential execution
    combined_command = " && ".join(fetch_commands)
    if not combined_command:
        combined_command = "echo 'No input files to fetch'"

    template = Container(
        name=f"{recipe_config.id}-fetch-template-",
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {combined_command}",
        ],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template
