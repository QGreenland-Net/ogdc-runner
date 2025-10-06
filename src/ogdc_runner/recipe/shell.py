from __future__ import annotations

import fsspec
from hera.workflows import (
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
    data_already_published,
    make_cmd_template,
    make_fetch_input_template,
    make_publish_template,
)
from ogdc_runner.constants import SHELL_RECIPE_FILENAME
from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.recipe.viz_workflow import submit_viz_workflow_recipe


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


def submit_ogdc_recipe(
    *,
    recipe_dir: str,
    wait: bool,
    overwrite: bool,
) -> str:
    """Submit an OGDC recipe for processing via argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete
        overwrite: Whether to overwrite existing published data

    Returns the name of the OGDC shell recipe submitted to Argo.
    """
    # Get the recipe configuration
    recipe_config = get_recipe_config(recipe_dir)

    # Check if the user-submitted workflow has already been published
    if data_already_published(
        recipe_config=recipe_config,
        overwrite=overwrite,
    ):
        err_msg = f"Data for recipe {recipe_config.id} have already been published."
        raise OgdcDataAlreadyPublished(err_msg)

    # Check if the recipe is a visualization workflow
    if recipe_config.id == "viz-workflow":
        return submit_viz_workflow_recipe(
            recipe_dir=recipe_dir,
            wait=wait,
        )

    # We currently expect all recipes to be "shell"
    shell_recipe_workflow_name = make_and_submit_shell_workflow(
        recipe_config=recipe_config,
        wait=wait,
    )

    return shell_recipe_workflow_name


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
