from __future__ import annotations

from pathlib import Path

import pytest

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import wait_for_workflow_completion
from ogdc_runner.exceptions import OgdcDataAlreadyPublished, OgdcInvalidRecipeDir


def test_submit_ogdc_recipe_with_invalid_dir(tmp_path, test_order_id):
    """Test submitting a recipe from a non-existent directory raises an error."""
    with pytest.raises(OgdcInvalidRecipeDir):
        submit_ogdc_recipe(
            recipe_dir=Path(tmp_path / "nonexistent"),
            overwrite=True,
            order_id=test_order_id,
        )


def test_submit_ogdc_recipe(test_shell_workflow_recipe_directory, test_order_id):
    """Test that an ogdc recipe can be submitted and executed successfully."""

    # Note: `overwrite` is set here to ensure that outputs from a previous test
    # run are overwritten. This is not ideal. Tests that create data should
    # cleanup after themselves.
    wf_name = submit_ogdc_recipe(
        recipe_dir=test_shell_workflow_recipe_directory,
        overwrite=True,
        order_id=test_order_id,
    )
    wait_for_workflow_completion(wf_name)


@pytest.mark.order(after="test_submit_ogdc_recipe")
def test_submit_ogdc_recipe_fails_already_published(
    test_shell_workflow_recipe_directory,
    test_order_id,
):
    """Test that the ogdc recipe has been published and an exception is raised
    on re-submission (without overwrite option)."""
    with pytest.raises(OgdcDataAlreadyPublished):
        submit_ogdc_recipe(
            recipe_dir=test_shell_workflow_recipe_directory,
            overwrite=False,
            order_id=test_order_id,
        )
