from __future__ import annotations

import json
import os
import shlex
import subprocess
from importlib.resources import files
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from ogdc_runner.argo import make_input_pvc_volume, make_input_pvc_volume_mount
from ogdc_runner.inputs import _build_fetch_commands, make_pvc_listing_template
from ogdc_runner.models.recipe_config import PvcMountInput, RecipeInput, UrlInput
from ogdc_runner.partitioning import _extract_file_paths
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.workflow.shell import (
    _build_partition_processing_script,
    make_and_submit_shell_workflow,
)


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


def test_pvc_mount_rejects_mixed_url_inputs():
    """PVC inputs are an exclusive input mode."""
    with pytest.raises(ValidationError, match="cannot be combined"):
        RecipeInput(
            params=[
                UrlInput(value="https://example.com/input.gpkg", type="url"),
                PvcMountInput(claim_name="test-pvc", path="/data/"),
            ]
        )


def test_claim_name_rejects_invalid_k8s_names():
    """claim_name must be a valid K8s DNS label."""
    with pytest.raises(ValidationError, match="DNS label"):
        PvcMountInput(claim_name="UPPER_CASE", path="/data/")
    with pytest.raises(ValidationError, match="too long"):
        PvcMountInput(claim_name="a" * 64, path="/data/")
    with pytest.raises(ValidationError, match="DNS label"):
        PvcMountInput(claim_name="-leading-dash", path="/data/")


def test_long_claim_name_uses_truncated_volume_name():
    """Long PVC claim names remain valid while generated volume names stay short."""
    pvc = PvcMountInput(claim_name="a" * 63, path="/data/")
    volume = make_input_pvc_volume(pvc.claim_name)
    mount = make_input_pvc_volume_mount(pvc)

    assert volume.persistent_volume_claim is not None
    assert volume.persistent_volume_claim.claim_name == pvc.claim_name
    assert len(volume.name) <= 63
    assert mount.name == volume.name


def test_path_rejects_parent_directory_references():
    """PVC paths must stay inside the mounted claim path."""
    with pytest.raises(ValidationError, match="parent directory"):
        PvcMountInput(claim_name="test-pvc", path="../other-pvc")


def test_glob_rejects_unsafe_paths():
    """PVC glob patterns must stay under the configured input path."""
    with pytest.raises(ValidationError, match="relative"):
        PvcMountInput(claim_name="test-pvc", path="/data/", glob="/other/*.gpkg")
    with pytest.raises(ValidationError, match="parent directory"):
        PvcMountInput(claim_name="test-pvc", path="/data/", glob="../*.gpkg")


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
    assert "symlink_to" in cmd


def test_extract_file_paths_empty_for_pvc():
    """_extract_file_paths returns [] for PVC inputs; listing happens at runtime."""
    pvc = PvcMountInput(claim_name="arctic-dem-pvc", path="/tiles/")
    assert _extract_file_paths([pvc]) == []


def test_pvc_listing_template():
    """Listing template has the right name, script, mounts, and partitions output."""
    pvc = PvcMountInput(
        claim_name="arctic-dem-pvc",
        path="/tiles/v3 with spaces/",
        glob="it's*.tif",
    )
    pvc_mount = make_input_pvc_volume_mount(pvc)
    template = make_pvc_listing_template(
        pvc_inputs=[pvc], partition_size=4, input_pvc_mounts=[pvc_mount]
    )

    assert template.name == "list-pvc-files"

    script = template.args[0]  # type: ignore[index]
    assert pvc.full_path in script
    assert "'\"'\"'" in script
    assert "*.tif" in script
    assert "4" in script

    assert template.volume_mounts is not None
    assert any(m.name == "input-pvc-arctic-dem-pvc" for m in template.volume_mounts)

    raw_outputs = template.outputs
    assert raw_outputs is not None
    assert not isinstance(raw_outputs, (str, bytes))
    assert any(o.name == "partitions" for o in raw_outputs)  # type: ignore[union-attr]
    assert any(o.name == "files-manifest-path" for o in raw_outputs)  # type: ignore[union-attr]


def test_pvc_listing_template_includes_all_pvc_inputs():
    """Parallel PVC listing must not silently ignore additional PVC inputs."""
    first = PvcMountInput(claim_name="first-pvc", path="/a/", glob="*.tif")
    second = PvcMountInput(claim_name="second-pvc", path="/b/", glob="*.gpkg")

    template = make_pvc_listing_template(
        pvc_inputs=[first, second],
        partition_size=2,
        input_pvc_mounts=[
            make_input_pvc_volume_mount(first),
            make_input_pvc_volume_mount(second),
        ],
    )

    script = template.args[0]  # type: ignore[index]
    assert first.full_path in script
    assert second.full_path in script
    assert "*.tif" in script
    assert "*.gpkg" in script


def _render_script(name: str, replacements: dict[str, str]) -> str:
    script = files("ogdc_runner.scripts").joinpath(name).read_text()
    for placeholder, value in replacements.items():
        script = script.replace(placeholder, value)
    return script


def test_pvc_listing_script_recurses_into_subdirectories(tmp_path):
    """PVC listing should find files staged below nested dataset directories."""
    input_path = tmp_path / "AK_Water_Mosaic_gpkg"
    nested_path = input_path / "43_18"
    nested_path.mkdir(parents=True)
    root_match = input_path / "root.gpkg"
    nested_match = nested_path / "nested.gpkg"
    ignored_file = nested_path / "ignored.txt"
    root_match.touch()
    nested_match.touch()
    ignored_file.touch()

    partitions_path = tmp_path / "partitions.json"
    files_manifest_path_output = tmp_path / "files_manifest_path.txt"
    manifest_dir = tmp_path / "partition-manifests"
    script = _render_script(
        "list_pvc_inputs.sh",
        {
            "{pvc_inputs_json}": "PVC_INPUTS_JSON="
            + shlex.quote(json.dumps([{"path": str(input_path), "glob": "*.gpkg"}])),
            "{partition_size}": "10",
        },
    )

    subprocess.run(
        ["sh", "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PARTITIONS_PATH": str(partitions_path),
            "FILES_MANIFEST_PATH_OUTPUT": str(files_manifest_path_output),
            "PARTITION_MANIFEST_DIR": str(manifest_dir),
            "RECIPE_ID": "test-recipe",
        },
    )
    files_manifest_path = files_manifest_path_output.read_text()
    listed_files = json.loads(Path(files_manifest_path).read_text())
    partitions = json.loads(partitions_path.read_text())

    assert listed_files == sorted([str(nested_match), str(root_match)])
    assert partitions == [
        {
            "partition_id": 0,
        }
    ]
    assert json.loads((manifest_dir / "partition-0.json").read_text()) == listed_files


def test_pvc_stage_script_recurses_into_subdirectories(tmp_path):
    """Sequential PVC staging should symlink recursive glob matches."""
    input_path = tmp_path / "AK_Water_Mosaic_gpkg"
    nested_path = input_path / "43_18"
    output_path = tmp_path / "output"
    nested_path.mkdir(parents=True)
    nested_match = nested_path / "nested.gpkg"
    ignored_file = nested_path / "ignored.txt"
    nested_match.touch()
    ignored_file.touch()

    script = _render_script(
        "stage_pvc_inputs.sh",
        {
            "{pvc_inputs_json}": "PVC_INPUTS_JSON="
            + shlex.quote(json.dumps([{"path": str(input_path), "glob": "*.gpkg"}])),
            "{output_dir}": shlex.quote(str(output_path)),
        },
    )

    subprocess.run(["sh", "-c", script], check=True, capture_output=True, text=True)

    staged_file = output_path / nested_match.name
    assert staged_file.is_symlink()
    assert staged_file.resolve() == nested_match
    assert not (output_path / ignored_file.name).exists()


def test_partition_script_supports_mounted_pvc_mode():
    """PVC parallel mode should use mounted file paths directly at cmd index 0."""
    script = _build_partition_processing_script(
        'cp "$INPUT_FILE" "$OUTPUT_FILE"',
        first_command_input_mode="mounted-pvc",
    )

    assert 'FIRST_COMMAND_INPUT_MODE="mounted-pvc"' in script
    assert 'export INPUT_FILE="$file"' in script


def _render_shell_workflow(config: Any) -> dict[str, Any]:
    rendered_workflows: list[dict[str, Any]] = []

    def fake_submit(workflow: Any, wait: bool = False) -> str:
        del wait
        rendered_workflows.append(workflow.to_dict())
        return "test-workflow"

    with patch("ogdc_runner.workflow.shell.submit_workflow", fake_submit):
        assert make_and_submit_shell_workflow(config, wait=False) == "test-workflow"

    return rendered_workflows[0]


def _template(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    return next(
        template
        for template in workflow["spec"]["templates"]
        if template["name"] == name
    )


def _assert_references_existing_claims_only(workflow: dict[str, Any]) -> None:
    spec = workflow["spec"]
    assert "volumeClaimTemplates" not in spec
    for volume in spec["volumes"]:
        assert set(volume) <= {"name", "persistentVolumeClaim"}
        assert "claimName" in volume["persistentVolumeClaim"]


def test_sequential_shell_workflow_stages_pvc_inputs_as_symlinks(
    monkeypatch,
    test_shell_workflow_recipe_directory,
):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    config = get_recipe_config(
        test_shell_workflow_recipe_directory,
        check_urls=False,
    )
    config.input = RecipeInput(
        params=[
            PvcMountInput(
                claim_name="ogdc-test-pvc",
                path="/tiles/",
                glob="*.gpkg",
            )
        ]
    )

    workflow = _render_shell_workflow(config)
    fetch_template = _template(workflow, f"{config.id}-fetch-template-")
    run_template = _template(workflow, "run-cmd-0")

    _assert_references_existing_claims_only(workflow)
    assert "stage_pvc_inputs" not in fetch_template["container"]["args"][0]
    assert "symlink_to" in fetch_template["container"]["args"][0]
    assert "input-pvc-ogdc-test-pvc" in {
        mount["name"] for mount in run_template["container"]["volumeMounts"]
    }


def test_parallel_shell_workflow_renders_pvc_inputs(
    monkeypatch,
    test_shell_workflow_recipe_directory,
):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    config = get_recipe_config(
        test_shell_workflow_recipe_directory,
        check_urls=False,
    )
    config.workflow.parallel.enabled = True
    config.input = RecipeInput(
        params=[
            PvcMountInput(
                claim_name="ogdc-test-pvc",
                path="/tiles/",
                glob="*.gpkg",
            ),
        ]
    )

    workflow = _render_shell_workflow(config)
    tasks = _template(workflow, "main")["dag"]["tasks"]

    _assert_references_existing_claims_only(workflow)
    assert "fetch" not in [task["name"] for task in tasks]
    assert "list-pvc-files" in [task["name"] for task in tasks]
    cmd_task = next(task for task in tasks if task["name"] == "cmd-0")
    assert (
        cmd_task["arguments"]["parameters"][0]["value"]
        == f"/mnt/workflow/{config.id}/partition-manifests/pvc-inputs/"
        "partition-{{item.partition_id}}.json"
    )
