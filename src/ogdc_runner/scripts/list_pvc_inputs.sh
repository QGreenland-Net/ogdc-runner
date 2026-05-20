#!/bin/sh
set -eu

{pvc_inputs_json}
export PVC_INPUTS_JSON
export PARTITION_SIZE="{partition_size}"

python3 - <<'PYEOF'
import fnmatch
import json
import os
import sys
from pathlib import Path

pvc_inputs = json.loads(os.environ["PVC_INPUTS_JSON"])
partition_size = int(os.environ["PARTITION_SIZE"])

files = set()
for pvc_input in pvc_inputs:
    input_path = Path(pvc_input["path"])
    pattern = pvc_input["glob"]
    if not input_path.is_dir():
        sys.stderr.write(f"PVC input path is not a directory: {input_path}\n")
        sys.exit(1)

    for child in input_path.iterdir():
        if child.is_file() and fnmatch.fnmatch(child.name, pattern):
            files.add(str(child))

files = sorted(files)
if not files:
    sys.stderr.write("No files found for PVC inputs\n")
    sys.exit(1)

partitions = [
    {"partition_id": i, "files": files[start : start + partition_size]}
    for i, start in enumerate(range(0, len(files), partition_size))
]

with open("/tmp/partitions.json", "w") as f:
    json.dump(partitions, f)

with open("/tmp/files.json", "w") as f:
    json.dump(files, f)

sys.stderr.write(f"Generated {len(partitions)} partitions\n")
PYEOF
