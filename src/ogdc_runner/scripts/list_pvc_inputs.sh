#!/bin/sh
# shellcheck disable=SC1073,SC1054,SC1083,SC1056,SC1072
# This file is rendered as a shell template before execution.
set -eu

{pvc_inputs_json}
export PVC_INPUTS_JSON
export PARTITION_SIZE="{partition_size}"

python3 - <<'PYEOF'
import json
import os
import sys
from pathlib import Path

try:
    pvc_inputs = json.loads(os.environ["PVC_INPUTS_JSON"])
except json.JSONDecodeError:
    pvc_inputs = json.loads(os.environ["PVC_INPUTS_JSON"].replace("\\", "\\\\"))
partition_size = int(os.environ["PARTITION_SIZE"])
partitions_path = Path(os.environ.get("PARTITIONS_PATH", "/tmp/partitions.json"))
files_manifest_path_output = Path(
    os.environ.get("FILES_MANIFEST_PATH_OUTPUT", "/tmp/files_manifest_path.txt")
)
recipe_id = os.environ.get("RECIPE_ID", "{{inputs.parameters.recipe-id}}")
manifest_dir = Path(
    os.environ.get(
        "PARTITION_MANIFEST_DIR",
        f"/mnt/workflow/{recipe_id}/partition-manifests/pvc-inputs",
    )
)

files = set()
for pvc_input in pvc_inputs:
    input_path = Path(pvc_input["path"])
    pattern = pvc_input["glob"]
    if not input_path.is_dir():
        sys.stderr.write(f"PVC input path is not a directory: {input_path}\n")
        sys.exit(1)

    for child in input_path.rglob(pattern):
        if child.is_file():
            files.add(str(child))

files = sorted(files)
if not files:
    sys.stderr.write("No files found for PVC inputs\n")
    sys.exit(1)

partitions = [
    {
        "partition_id": i,
        "files": files[start : start + partition_size],
    }
    for i, start in enumerate(range(0, len(files), partition_size))
]
partition_refs = []

partitions_path.parent.mkdir(parents=True, exist_ok=True)
files_manifest_path_output.parent.mkdir(parents=True, exist_ok=True)
manifest_dir.mkdir(parents=True, exist_ok=True)

files_manifest_path = manifest_dir / "all-files.json"
with files_manifest_path.open("w") as f:
    json.dump(files, f)

for partition in partitions:
    partition_path = manifest_dir / f"partition-{partition['partition_id']}.json"
    with partition_path.open("w") as f:
        json.dump(partition["files"], f)
    partition_refs.append(
        {
            "partition_id": partition["partition_id"],
        }
    )

with partitions_path.open("w") as f:
    json.dump(partition_refs, f)

files_manifest_path_output.write_text(str(files_manifest_path))

sys.stderr.write(f"Generated {len(partitions)} partitions\n")
PYEOF
