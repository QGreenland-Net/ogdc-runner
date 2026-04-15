from __future__ import annotations

from ogdc_runner.argo import make_input_pvc_volume, make_input_pvc_volume_mount
from ogdc_runner.inputs import _build_fetch_commands
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
    assert isinstance(recipe_input.params[0], PvcMountInput)


def test_input_pvc_volume_and_mount_are_read_only():
    """Input PVCs must be mounted read-only so recipes can't mutate source data."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/")

    volume = make_input_pvc_volume(pvc.claim_name)
    mount = make_input_pvc_volume_mount(pvc)

    assert volume.persistent_volume_claim.read_only is True
    assert mount.read_only is True
    assert mount.mount_path == "/mnt/data/arctic-dem-pvc"
    assert mount.name == volume.name


def test_pvc_input_does_not_trigger_a_download():
    """Fetch step must not wget a PVC input — the data is already mounted."""
    pvc = PvcMountInput(claim_name="foo", path="/bar/")
    cmd = _build_fetch_commands([pvc], output_dir="/output_dir")

    assert "wget" not in cmd
    assert pvc.full_path in cmd
