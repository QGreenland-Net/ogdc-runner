"""Unit tests for the CLI."""

from __future__ import annotations

from click.testing import CliRunner

from ogdc_runner.__main__ import cli


def test_help():
    """Tests that the CLI's `--help` works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
