from __future__ import annotations

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE


def test_submit_ogdc_viz_recipe_fast(test_viz_workflow_recipe_directory):
    """Test that an ogdc visualization recipe can be submitted successfully (fast, no wait)."""
    workflow_name = submit_ogdc_recipe(
        recipe_dir=test_viz_workflow_recipe_directory,
        overwrite=True,
        wait=False,
    )

    # Cleanup test workflow (don't wait for it to complete).
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)
