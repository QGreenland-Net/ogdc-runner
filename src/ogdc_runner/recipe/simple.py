from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader

from ogdc_runner.constants import SIMPLE_RECIPE_FILENAME
from ogdc_runner.recipe import get_recipe_config

environment = Environment(loader=PackageLoader("ogdc_runner"))
template = environment.get_template("simple_recipe.py.j2")

# TODO: get these from envvars
PVC_NAME = "qgnet-pvc-test-1"
MOUNT_DIR = Path("/data")


def _get_workdir(*, mount_dir: Path, id: str) -> Path:
    return mount_dir / id


def _get_commands(*, simple_recipe_path: Path, config: dict[str, Any]) -> list[str]:
    """Extract commands from a simple recipe file."""
    # read_lines is going to be more efficient I assume...
    lines = simple_recipe_path.read_text().split("\n")

    work_dir = _get_workdir(mount_dir=MOUNT_DIR, id=config["id"])

    # Omit comments and empty lines
    commands = [line for line in lines if line and not line.startswith("#")]

    interpolated_commands = []
    previous_subdir = work_dir / "fetch"
    interpolated_commands.append(f"mkdir {previous_subdir}")
    fetch_cmd = f"curl -o {previous_subdir} {config['input']['url']}"
    interpolated_commands.append(fetch_cmd)
    for idx, command in enumerate(commands):
        output_dir = work_dir / str(idx)
        interpolated_command = command.format(input_dir=previous_subdir, output_dir=output_dir)
        interpolated_commands.append(f"mkdir {output_dir}")
        interpolated_commands.append(interpolated_command)
        previous_subdir = output_dir
        
    return interpolated_commands


def render_simple_recipe(recipe_directory: Path) -> None:
    config = get_recipe_config(recipe_directory)
    commands = _get_commands(simple_recipe_path=recipe_directory / SIMPLE_RECIPE_FILENAME, config=config)

    print(
        template.render(
            commands=commands,
            recipe_id=config["id"],
            pvc_name=PVC_NAME,
            mount_dir=MOUNT_DIR,
        )
    )
