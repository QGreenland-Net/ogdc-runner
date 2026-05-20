#!/bin/sh
set -eu

{pvc_inputs_json}
export PVC_INPUTS_JSON
export OUTPUT_DIR="{output_dir}"

python3 - <<'PYEOF'
import json
import os
import sys
from pathlib import Path

pvc_inputs = json.loads(os.environ["PVC_INPUTS_JSON"])
output_dir = Path(os.environ["OUTPUT_DIR"])
output_dir.mkdir(parents=True, exist_ok=True)

linked = 0
seen_names: dict[str, Path] = {}
for pvc_input in pvc_inputs:
    input_path = Path(pvc_input["path"])
    pattern = pvc_input["glob"]
    if not input_path.is_dir():
        sys.stderr.write(f"PVC input path is not a directory: {input_path}\n")
        sys.exit(1)

    matches = [
        child for child in sorted(input_path.rglob(pattern)) if child.is_file()
    ]
    if not matches:
        sys.stderr.write(
            f"No files found for PVC input path={input_path} glob={pattern}\n"
        )
        sys.exit(1)

    for source in matches:
        target = output_dir / source.name
        if source.name in seen_names and seen_names[source.name] != source:
            sys.stderr.write(
                "PVC input basename collision for "
                f"{source.name}: {seen_names[source.name]} and {source}\n"
            )
            sys.exit(1)

        seen_names[source.name] = source
        if target.exists() or target.is_symlink():
            if target.is_symlink() and target.resolve() == source:
                continue
            sys.stderr.write(
                f"Input basename collision at {target}; refusing to overwrite it\n"
            )
            sys.exit(1)
        target.symlink_to(source)
        linked += 1

sys.stderr.write(f"Linked {linked} PVC input files into {output_dir}\n")
PYEOF
