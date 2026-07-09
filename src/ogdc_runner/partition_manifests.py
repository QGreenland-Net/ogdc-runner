from __future__ import annotations

import json
from base64 import b64encode
from collections.abc import Sequence
from typing import Any

from hera.workflows import Container, Parameter
from hera.workflows.models import ValueFrom, VolumeMount

PARTITION_MANIFEST_PARAM = "partition-manifest"
PARTITION_MANIFEST_PATH_PARAM = "partition-manifest-path"
FILES_MANIFEST_PATH_PARAM = "files-manifest-path"
PARTITION_REFS_PARAM = "partitions"
PARTITION_MANIFESTS_DIR_NAME = "partition-manifests"


def workflow_manifest_dir(recipe_id: str, *parts: str) -> str:
    """Return the workflow PVC directory used for partition manifests."""
    suffix = "/".join(part.strip("/") for part in parts if part.strip("/"))
    base = f"/mnt/workflow/{recipe_id}/{PARTITION_MANIFESTS_DIR_NAME}"
    return f"{base}/{suffix}" if suffix else base


def partition_manifest_path(
    recipe_id: str, partition_id: int | str, *parts: str
) -> str:
    """Return a concrete partition manifest path on the workflow PVC."""
    return f"{workflow_manifest_dir(recipe_id, *parts)}/partition-{partition_id}.json"


def partition_manifest_path_arg(recipe_id: str, *parts: str) -> str:
    """Return a partition manifest path containing Argo's item partition ID."""
    return partition_manifest_path(recipe_id, "{{item.partition_id}}", *parts)


def manifest_inputs(manifest_param: str, manifest_path_param: str) -> list[Parameter]:
    """Return compatible inline/path manifest inputs for worker templates.

    Existing URL/DataONE flows can still pass inline JSON, while scalable PVC
    flows pass a small path parameter pointing at a manifest stored on the
    workflow PVC.
    """
    return [
        Parameter(name=manifest_param, default="[]"),
        Parameter(name=manifest_path_param, default=""),
    ]


def partition_manifest_inputs() -> list[Parameter]:
    return manifest_inputs(PARTITION_MANIFEST_PARAM, PARTITION_MANIFEST_PATH_PARAM)


def partition_manifest_records(partitions: Sequence[Any]) -> list[dict[str, Any]]:
    """Convert FilePartition-like objects to JSON-serializable manifest records."""
    return [
        {
            "partition_id": partition.partition_id,
            "files": list(partition.files),
        }
        for partition in partitions
    ]


def make_partition_manifest_writer_template(
    *,
    name: str,
    recipe_id: str,
    manifest_subdir: str,
    partitions: Sequence[dict[str, Any]],
    workflow_volume_name: str,
    image: str | None = None,
) -> Container:
    """Create a setup template that writes partition manifests to the PVC.

    The output parameter intentionally contains only partition IDs. Full file
    lists remain on the workflow PVC after workflow completion for provenance.
    """
    encoded_partitions = b64encode(json.dumps(list(partitions)).encode()).decode()
    manifest_dir = workflow_manifest_dir(recipe_id, manifest_subdir)
    script = f"""python3 - <<'PYEOF'
import base64
import json
from pathlib import Path

partitions = json.loads(base64.b64decode("{encoded_partitions}").decode())
manifest_dir = Path("{manifest_dir}")
partition_refs_path = Path("/tmp/partition_refs.json")

manifest_dir.mkdir(parents=True, exist_ok=True)
partition_refs_path.parent.mkdir(parents=True, exist_ok=True)

partition_refs = []
for partition in partitions:
    partition_id = partition["partition_id"]
    partition_path = manifest_dir / f"partition-{{partition_id}}.json"
    partition_path.write_text(json.dumps(partition["files"]))
    partition_refs.append({{"partition_id": partition_id}})

partition_refs_path.write_text(json.dumps(partition_refs))
print(f"Wrote {{len(partition_refs)}} retained partition manifests to {{manifest_dir}}")
PYEOF"""

    container = Container(
        name=name,
        command=["sh", "-c"],
        args=[script],
        outputs=[
            Parameter(
                name=PARTITION_REFS_PARAM,
                value_from=ValueFrom(path="/tmp/partition_refs.json"),
            )
        ],
        volume_mounts=[
            VolumeMount(name=workflow_volume_name, mount_path="/mnt/workflow"),
        ],
    )
    if image is not None:
        container.image = image
    return container
