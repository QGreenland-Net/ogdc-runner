"""API for interacting with the OGDC."""

from __future__ import annotations

from pathlib import Path

from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.publish import data_already_published
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.workflow.shell import make_and_submit_shell_workflow
from ogdc_runner.workflow.viz_workflow import make_and_submit_viz_workflow


def submit_ogdc_recipe(
    *,
    recipe_dir: Path,
    overwrite: bool,
    identifier: str | None = None,
) -> str:
    """Submit an OGDC recipe for processing via argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        overwrite: Whether to overwrite existing published data
        identifier: Optional order identifier.

    Returns the name of the OGDC recipe submitted to Argo.
    """
    # Get the recipe configuration
    recipe_config = get_recipe_config(recipe_dir)

    # Check if the user-submitted workflow has already been published
    if data_already_published(
        recipe_config=recipe_config,
        overwrite=overwrite,
    ):
        # TODO: update status to indicate that the recipe is already published..
        err_msg = f"Data for recipe {recipe_config.id} have already been published."
        raise OgdcDataAlreadyPublished(err_msg)

    # Check if the recipe is a visualization workflow
    if recipe_config.workflow.type == "visualization":
        return make_and_submit_viz_workflow(
            recipe_config=recipe_config,
            identifier=identifier,
        )

    # We currently expect all other recipes to be "shell"
    return make_and_submit_shell_workflow(
        recipe_config=recipe_config,
        identifier=identifier,
    )
