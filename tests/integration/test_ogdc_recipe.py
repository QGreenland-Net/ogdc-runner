from __future__ import annotations

import pytest

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.recipe.simple import submit_ogdc_recipe


def test_submit_ogdc_recipe(test_recipe_directory):
    """Test that an ogdc recipe can be submitted and executed successfully."""

    # Note: `overwrite` is set here to ensure that outptus from a previous test
    # run are overwritten. This is not ideal. Tests that create data should
    # cleanup after themselves.
    workflow_name = submit_ogdc_recipe(
        recipe_dir=test_recipe_directory,
        overwrite=True,
        wait=True,
    )

    # Cleanup test workflow.
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)


@pytest.mark.order(after="test_submit_ogdc_recipe")
def test_submit_ogdc_recipe_fails_already_published(test_recipe_directory):
    """Test that the ogdc recipe has been published and an exception is raised
    on re-submission (without overwrite option)."""
    with pytest.raises(OgdcDataAlreadyPublished):
        submit_ogdc_recipe(
            recipe_dir=test_recipe_directory,
            overwrite=False,
            wait=True,
        )
