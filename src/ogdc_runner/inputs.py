"""Code for accessing input data of OGDC recipes"""

from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
)

from ogdc_runner.models.recipe_config import DataOneInput, RecipeConfig, UrlInput


def make_fetch_input_template(
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that fetches multiple inputs from URLs or file paths.

    Supports:
    - HTTP/HTTPS URLs
    - File paths (including PVC paths)
    - DataONE datasets
    """
    # Create commands to fetch each input
    fetch_commands = []

    for param in recipe_config.input.params:
        # Check if the parameter is a URL
        if isinstance(param, UrlInput):
            # It's a URL, use wget
            fetch_commands.append(
                f"wget --content-disposition -P /output_dir/ {param.value}"
            )
        elif isinstance(param, DataOneInput):
            url = param._resolved_url or param.value
            fetch_commands.append(f"wget --content-disposition -P /output_dir/ {url}")

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
