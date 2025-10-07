"""API for interacting with the OGDC."""

from __future__ import annotations

from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.publish import data_already_published
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.recipe.shell import make_and_submit_shell_workflow
from ogdc_runner.recipe.viz_workflow import submit_viz_workflow_recipe


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
