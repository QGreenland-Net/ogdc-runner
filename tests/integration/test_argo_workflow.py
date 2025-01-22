"""

For more information about testing workflows, see:
https://hera.readthedocs.io/en/stable/walk-through/hera-tour/#workflow-testing
"""

from __future__ import annotations

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.recipe.simple import make_simple_workflow


def test_simple_argo_workflow(test_recipe_directory):
    workflow = make_simple_workflow(test_recipe_directory)
    submitted_wf = workflow.create(wait=True)
    assert submitted_wf.status  # type: ignore[union-attr]
    assert submitted_wf.status.phase == "Succeeded"  # type: ignore[union-attr]
    # Cleanup the test workflow
    if workflow.name:
        ARGO_WORKFLOW_SERVICE.delete_workflow(workflow.name)
