"""

For more information about testing workflows, see:
https://hera.readthedocs.io/en/stable/walk-through/hera-tour/#workflow-testing
"""

from __future__ import annotations

from pathlib import Path

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.recipe.simple import make_simple_workflow

SIMPLE_RECIPE_TEST_PATH = str(Path(__file__).parent / "test_recipe")


def test_simple_argo_workflow():
    workflow = make_simple_workflow(SIMPLE_RECIPE_TEST_PATH)
    submitted_wf = workflow.create(wait=True)
    assert submitted_wf.status  # type: ignore[union-attr]
    assert submitted_wf.status.phase == "Succeeded"  # type: ignore[union-attr]
    # Cleanup the test workflow
    if workflow.name:
        ARGO_WORKFLOW_SERVICE.delete_workflow(workflow.name)
