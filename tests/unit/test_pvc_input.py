from __future__ import annotations

import pytest
from pydantic import ValidationError

from ogdc_runner.models.recipe_config import PvcMountInput, RecipeInput


def test_mount_paths_are_built_correctly():
    """mount_path and full_path should join cleanly with no double-slashes."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/v3/", glob="*.tif")
    assert pvc.mount_path == "/mnt/data/arctic-dem-pvc"
    assert pvc.full_path == "/mnt/data/arctic-dem-pvc/tiles/v3"


def test_pvc_mount_parses_from_meta_yaml_dict():
    """A meta.yml entry with type=pvc_mount should parse into a PvcMountInput."""
    recipe_input = RecipeInput.model_validate(
        {
            "params": [
                {
                    "type": "pvc_mount",
                    "claim_name": "test-pvc",
                    "path": "/data/",
                }
            ]
        }
    )
    param = recipe_input.params[0]
    assert isinstance(param, PvcMountInput)


def test_missing_required_fields_raises():
    with pytest.raises(ValidationError):
        PvcMountInput(claim_name="foo")  # type: ignore[call-arg]
