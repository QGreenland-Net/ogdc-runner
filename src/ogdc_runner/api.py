"""API for interacting with the OGDC."""

from __future__ import annotations

from pathlib import Path

from ogdc_runner.exceptions import OgdcDataAlreadyPublished, OgdcInvalidRecipeConfig
from ogdc_runner.models.steps import StepsRecipeConfig
from ogdc_runner.publish import data_already_published
from ogdc_runner.recipe import load_recipe_config
from ogdc_runner.workflow.shell import make_and_submit_shell_workflow
from ogdc_runner.workflow.viz_workflow import make_and_submit_viz_workflow


def submit_ogdc_recipe(
    *,
    recipe_dir: Path,
    wait: bool,
    overwrite: bool,
) -> str:
    """Submit an OGDC recipe for processing via argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete
        overwrite: Whether to overwrite existing published data

    Returns the name of the OGDC shell recipe submitted to Argo.

    Raises `OgdcInvalidRecipeConfig` for invalid recipes, and for steps recipes,
    which cannot be executed yet.
    """
    # Get the recipe configuration
    recipe_config = load_recipe_config(recipe_dir)

    # Steps recipes are validated but have no executor yet. Never let them
    # fall through to the shell workflow builder. Slice 3 removes this gate.
    if isinstance(recipe_config, StepsRecipeConfig):
        msg = "steps recipes are not executable yet"
        raise OgdcInvalidRecipeConfig(msg)

    # Check if the user-submitted workflow has already been published
    if data_already_published(
        recipe_config=recipe_config,
        overwrite=overwrite,
    ):
        err_msg = f"Data for recipe {recipe_config.id} have already been published."
        raise OgdcDataAlreadyPublished(err_msg)

    # Check if the recipe is a visualization workflow
    if recipe_config.workflow.type == "visualization":
        return make_and_submit_viz_workflow(
            recipe_config=recipe_config,
            wait=wait,
        )

    # We currently expect all recipes to be "shell"
    shell_recipe_workflow_name = make_and_submit_shell_workflow(
        recipe_config=recipe_config,
        wait=wait,
    )

    return shell_recipe_workflow_name
