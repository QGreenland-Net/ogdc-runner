from __future__ import annotations

import importlib
import json
import os
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
from hera.workflows import Workflow, models

from ogdc_runner.exceptions import OgdcDataAlreadyPublished

DEFAULT_VIZ_WORKFLOW_TEST_IMAGE = (
    "ghcr.io/permafrostdiscoverygateway/viz-workflow:1.1.0-dev-2"
)


@pytest.fixture(scope="module")
def viz_recipe_directory(tmp_path_factory) -> Path:
    """Use a unique recipe id so the test does not need a cleanup workflow."""
    source_dir = Path(__file__).parents[1] / "test_viz_workflow_recipe_dir"
    recipe_dir = tmp_path_factory.mktemp("viz-workflow-recipe")
    shutil.copytree(source_dir, recipe_dir, dirs_exist_ok=True)

    meta_path = recipe_dir / "meta.yml"
    meta_content = meta_path.read_text()
    meta_path.write_text(
        meta_content.replace(
            'name: "ADC Visualization workflow"',
            f'name: "ADC Visualization workflow {uuid.uuid4().hex[:8]}"',
        )
    )

    config_path = recipe_dir / "config.json"
    config = json.loads(config_path.read_text())
    config["ext_input"] = ".gpkg"
    config["z_range"] = [0, 5]
    config["enable_3dtiles"] = False
    config_path.write_text(json.dumps(config, indent=2))

    return recipe_dir


@pytest.fixture
def submit_ogdc_viz_recipe(monkeypatch):
    """Submit viz recipes with the same image configured for the local viz stack."""
    viz_workflow_image = os.environ.get(
        "VIZ_WORKFLOW_IMAGE",
        DEFAULT_VIZ_WORKFLOW_TEST_IMAGE,
    )
    monkeypatch.setenv("VIZ_WORKFLOW_IMAGE", viz_workflow_image)
    monkeypatch.setenv("VIZ_WORKFLOW_SETUP_IMAGE", viz_workflow_image)

    from ogdc_runner import api
    from ogdc_runner import argo
    from ogdc_runner.workflow import viz_workflow

    @contextmanager
    def workflow_with_test_ttl(*args, **kwargs):
        with Workflow(*args, **kwargs) as w:
            assert w.labels
            w.labels["ogdc/persist-workflow-in-archive"] = "false"
            w.ttl_strategy = models.TTLStrategy(seconds_after_success=600)
            yield w

    monkeypatch.setattr(argo, "Workflow", workflow_with_test_ttl)

    importlib.reload(viz_workflow)
    importlib.reload(api)

    return api.submit_ogdc_recipe


def test_submit_ogdc_viz_recipe(
    viz_recipe_directory,
    submit_ogdc_viz_recipe,
):
    """Test that an ogdc visualization recipe can be submitted and executed successfully."""
    submit_ogdc_viz_recipe(
        recipe_dir=viz_recipe_directory,
        overwrite=False,
        wait=True,
    )


@pytest.mark.order(after="test_submit_ogdc_viz_recipe")
def test_submit_ogdc_viz_recipe_fails_already_published(
    viz_recipe_directory,
    submit_ogdc_viz_recipe,
):
    """Test that the ogdc viz recipe has been published and an exception is raised
    on re-submission (without overwrite option)."""
    with pytest.raises(OgdcDataAlreadyPublished):
        submit_ogdc_viz_recipe(
            recipe_dir=viz_recipe_directory,
            overwrite=False,
            wait=True,
        )
