from __future__ import annotations

import pytest

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.exceptions import OgdcDataAlreadyPublished


def test_submit_ogdc_viz_recipe(test_viz_workflow_recipe_directory):
    """Test that an ogdc visualization recipe can be submitted and executed successfully."""
    workflow_name = submit_ogdc_recipe(
        recipe_dir=test_viz_workflow_recipe_directory,
        overwrite=True,
        wait=True,
    )

    # Cleanup test workflow.
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)


@pytest.mark.order(after="test_submit_ogdc_viz_recipe")
def test_submit_ogdc_viz_recipe_fails_already_published(
    test_viz_workflow_recipe_directory,
):
    """Test that the ogdc viz recipe has been published and an exception is raised
    on re-submission (without overwrite option)."""
    with pytest.raises(OgdcDataAlreadyPublished):
        submit_ogdc_recipe(
            recipe_dir=test_viz_workflow_recipe_directory,
            overwrite=False,
            wait=True,
        )
