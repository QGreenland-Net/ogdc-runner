from __future__ import annotations

from ogdc_runner.argo import make_input_pvc_volume, make_input_pvc_volume_mount
from ogdc_runner.inputs import _build_fetch_commands
from ogdc_runner.models.recipe_config import PvcMountInput, RecipeInput
from ogdc_runner.partitioning import _extract_file_paths
from ogdc_runner.workflow.shell import _make_pvc_listing_template


def test_mount_paths_are_built_correctly():
    """mount_path and full_path join cleanly with no double-slashes."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/v3/", glob="*.tif")
    assert pvc.mount_path == "/mnt/data/arctic-dem-pvc"
    assert pvc.full_path == "/mnt/data/arctic-dem-pvc/tiles/v3"


def test_pvc_mount_parses_from_dict():
    """type=pvc_mount in a raw dict discriminates to PvcMountInput."""
    recipe_input = RecipeInput.model_validate(
        {"params": [{"type": "pvc_mount", "claim_name": "test-pvc", "path": "/data/"}]}
    )
    assert isinstance(recipe_input.params[0], PvcMountInput)


def test_input_pvc_volume_and_mount_are_read_only():
    """Input PVCs must be mounted read-only so recipes can't mutate source data."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/")
    volume = make_input_pvc_volume(pvc.claim_name)
    mount = make_input_pvc_volume_mount(pvc)

    assert volume.persistent_volume_claim is not None
    assert volume.persistent_volume_claim.read_only is True
    assert mount.read_only is True
    assert mount.mount_path == "/mnt/data/arctic-dem-pvc"
    assert mount.name == volume.name


def test_pvc_fetch_does_not_download():
    """Fetch step must not wget a PVC input — the data is already mounted."""
    pvc = PvcMountInput(claim_name="foo", path="/bar/")
    cmd = _build_fetch_commands([pvc], output_dir="/output_dir")
    assert "wget" not in cmd
    assert pvc.full_path in cmd


def test_extract_file_paths_empty_for_pvc():
    """_extract_file_paths returns [] for PVC inputs; listing happens at runtime."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/")
    assert _extract_file_paths([pvc]) == []


def test_pvc_listing_template():
    """Listing template has the right name, script, mounts, and partitions output."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/v3/", glob="*.tif")
    pvc_mount = make_input_pvc_volume_mount(pvc)
    template = _make_pvc_listing_template(
        pvc_input=pvc, partition_size=4, input_pvc_mounts=[pvc_mount]
    )

    assert template.name == "list-pvc-files"

    script = template.args[0]  # type: ignore[index]
    assert pvc.full_path in script
    assert "*.tif" in script
    assert "4" in script

    assert template.volume_mounts is not None
    assert any(m.name == "input-pvc-arctic-dem-pvc" for m in template.volume_mounts)

    raw_outputs = template.outputs
    assert raw_outputs is not None
    assert not isinstance(raw_outputs, (str, bytes))
    assert any(o.name == "partitions" for o in raw_outputs)  # type: ignore[union-attr]

