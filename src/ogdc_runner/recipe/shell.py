from __future__ import annotations

import fsspec
from hera.workflows import (
    Artifact,
    Container,
    Steps,
    Workflow,
)
from loguru import logger

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    submit_workflow,
)

# Import common utilities
from ogdc_runner.common import (
    make_fetch_input_template,
)
from ogdc_runner.constants import SHELL_RECIPE_FILENAME
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.publish import make_publish_template


def make_cmd_template(
    name: str,
    command: str,
) -> Container:
    """Creates a command template with an optional custom image."""
    template = Container(
        name=name,
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {command}",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template


def make_and_submit_shell_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
) -> str:
    """Create and submit an argo workflow based on a shell recipe.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete

    Returns the name of the workflow as a str.
    """
    # Parse commands from the shell recipe file
    commands = parse_commands_from_recipe_file(
        recipe_config.recipe_directory,
        SHELL_RECIPE_FILENAME,
    )

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        # Create command templates
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = make_cmd_template(
                name=f"run-cmd-{idx}",
                command=command,
            )
            cmd_templates.append(cmd_template)

        # Use the multi-input fetch template
        fetch_template = make_fetch_input_template(
            recipe_config=recipe_config,
        )

        # Create publication template
        publish_template = make_publish_template(
            recipe_id=recipe_config.id,
        )

        # Create the workflow steps
        with Steps(name="steps"):
            step = fetch_template()
            for idx, cmd_template in enumerate(cmd_templates):
                step = cmd_template(
                    name=f"step-{idx}",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
                )
            # Publish final data
            publish_template(
                name="publish-data",
                arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
            )

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)

    return workflow_name


def parse_commands_from_recipe_file(recipe_dir: str, filename: str) -> list[str]:
    """Read commands from a recipe file.

    Args:
        recipe_dir: The directory containing the recipe file
        filename: The name of the recipe file to parse

    Returns:
        A list of commands from the recipe file, with comments removed
    """
    recipe_path = f"{recipe_dir}/{filename}"
    logger.info(f"Reading recipe from {recipe_path}")

    with fsspec.open(recipe_path, "rt") as f:
        lines = f.read().split("\n")
    commands = [line for line in lines if line and not line.startswith("#")]

    return commands
