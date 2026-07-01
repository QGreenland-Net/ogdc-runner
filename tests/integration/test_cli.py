"""Integration tests that assert the CLI can be used to successfully submit recipes.

Requires running on a local cluster with access_mode: "open" set in the chart.
"""

from __future__ import annotations

from click.testing import CliRunner

from ogdc_runner.__main__ import cli


def test_submit(monkeypatch):
    """Test the `submit` subcommand.

    Submits the seal tags recipe and waits for completion.

    This is close to an end-to-end test, except the output of the recipe's
    execution is not checked.
    """
    # Ensure we target the local environment for these tests.
    monkeypatch.setenv("ENVIRONMENT", "local")

    # Submit seal tags recipe and wait until completion.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "submit",
            "--wait",
            "--overwrite",
            "github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags",
        ],
    )

    # An exit code of 0 indicates that the recipe was submitted and executed
    # successfully.
    assert result.exit_code == 0
