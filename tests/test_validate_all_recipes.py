from __future__ import annotations

import os

import pytest
from click.testing import CliRunner

from ogdc_runner.__main__ import cli


def test_validate_all_recipes_at_main():
    """Validate recipes at main (or OGDC_RECIPES_REF if set)."""
    ref = os.environ.get("OGDC_RECIPES_REF", "main")

    runner = CliRunner()
    result = runner.invoke(cli, ["validate-all-recipes", "--ref", ref])

    # Print output for visibility
    print(f"\nValidating recipes at ref: {ref}")
    print(f"\n{result.output}")

    if result.exit_code != 0:
        pytest.fail(
            f"validate-all-recipes failed for {ref} (exit_code={result.exit_code})\n{result.output}"
        )
