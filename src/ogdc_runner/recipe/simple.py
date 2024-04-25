from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, PackageLoader

from ogdc_runner.constants import SIMPLE_RECIPE_FILENAME
from ogdc_runner.recipe import get_recipe_config

environment = Environment(loader=PackageLoader("ogdc_runner"))
template = environment.get_template("simple_recipe.py.j2")

input_subkeys = {
    "url": "beam.io.SomeTransformThatCanReadAFileFromAUrl",
    "dataone_doi": "our_custom_transforms.DataOneDoiInput",
}


def _get_commands(simple_recipe_path: Path) -> list[str]:
    """Extract commands from a simple recipe file."""
    # read_lines is going to be more efficient I assume...
    lines = simple_recipe_path.read_text().split("\n")

    # Omit comments and empty lines
    commands = [line for line in lines if line and not line.startswith("#")]
    return commands


def _get_input_constructor_and_arg(config: dict) -> tuple[type, any]:
    acceptable_values = f"Acceptable values: {input_subkeys.keys()}"
    if num_keys := len(config["input"].keys()) > 1:
        raise RuntimeError(
            f"Expected 1 sub-key for the `input` key; got {num_keys}."
            f" {acceptable_values}"
        )

    key, val = list(config["input"].items())[0]

    try:
        clss = input_subkeys[key]
    except KeyError:
        raise RuntimeError(
            f"Received unexecpected sub-key for `input` key: {key}"
            f" {acceptable_values}"
        )

    return clss, val


def render_simple_recipe(recipe_directory: Path):
    commands = _get_commands(recipe_directory / SIMPLE_RECIPE_FILENAME)
    config = get_recipe_config(recipe_directory)

    input_constructor, input_constructor_arg = _get_input_constructor_and_arg(config)

    print(
        template.render(
            commands=commands,
            input_constructor=input_constructor,
            input_constructor_arg=input_constructor_arg,
        )
    )
