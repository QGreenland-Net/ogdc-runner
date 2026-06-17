"""Unit tests for the CLI."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from ogdc_runner.__main__ import cli

TARGET_MODULE_FOR_MOCK = "ogdc_runner.__main__"


@pytest.fixture
def mock_cache_paths(tmp_path, monkeypatch):
    """Safely redirects token storage to a temporary folder during tests."""
    mock_dir = tmp_path / "ogdc"
    mock_file = mock_dir / "tokens.json"

    monkeypatch.setenv("TOKEN_CACHE_FILE", str(mock_file))
    return mock_file


def test_help():
    """Tests that the CLI's `--help` works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0


def test_set_token_with_explicit_flags(mock_cache_paths):
    """Tests setting tokens using the --access and --refresh flags."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["set-token", "--access", "dummy_access_123", "--refresh", "dummy_refresh_456"],
    )

    assert result.exit_code == 0
    assert "OGDC tokens updated successfully." in result.output

    # Verify the file was created and contains the correct data
    assert mock_cache_paths.exists()
    with mock_cache_paths.open("r") as f:
        saved_data = json.load(f)

    assert saved_data["access_token"] == "dummy_access_123"
    assert saved_data["refresh_token"] == "dummy_refresh_456"


def test_set_token_with_json_string(mock_cache_paths):
    """Tests setting tokens using a raw JSON payload."""
    runner = CliRunner()
    json_payload = (
        '{"message":"Success","token":{"access_token":"access.token.text",'
        '"refresh_token":"refresh.token.text"}}'
    )

    result = runner.invoke(cli, ["set-token", "--json-str", json_payload])

    assert result.exit_code == 0

    with mock_cache_paths.open("r") as f:
        saved_data = json.load(f)

    assert saved_data["access_token"] == "access.token.text"
    assert saved_data["refresh_token"] == "refresh.token.text"


def test_set_token_missing_all_arguments(mock_cache_paths):
    """Tests that the command fails cleanly if no tokens are provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["set-token"])

    # Since we raised click.UsageError, Click exits with code 2
    assert result.exit_code == 2
    assert "At least one of 'access', 'refresh'," in result.output

    # Ensure no file was mistakenly created
    assert not mock_cache_paths.exists()
