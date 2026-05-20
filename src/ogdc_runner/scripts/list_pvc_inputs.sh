#!/bin/sh
set -eu

{pvc_inputs_json}
export PVC_INPUTS_JSON
export PARTITION_SIZE="{partition_size}"

python3 - <<'PYEOF'
import json
import os
import sys
from pathlib import Path

pvc_inputs = json.loads(os.environ["PVC_INPUTS_JSON"])
partition_size = int(os.environ["PARTITION_SIZE"])
partitions_path = Path(os.environ.get("PARTITIONS_PATH", "/tmp/partitions.json"))
files_path = Path(os.environ.get("FILES_PATH", "/tmp/files.json"))

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
    {"partition_id": i, "files": files[start : start + partition_size]}
    for i, start in enumerate(range(0, len(files), partition_size))
]

partitions_path.parent.mkdir(parents=True, exist_ok=True)
files_path.parent.mkdir(parents=True, exist_ok=True)

with partitions_path.open("w") as f:
    json.dump(partitions, f)

with files_path.open("w") as f:
    json.dump(files, f)

sys.stderr.write(f"Generated {len(partitions)} partitions\n")
PYEOF
