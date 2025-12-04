from __future__ import annotations

import os
import shutil
import subprocess

import pytest

# Per-recipe timeout in seconds
PER_RECIPE_TIMEOUT = 120


def _find_cli_executable() -> str:
    """Find the ogdc-runner CLI executable."""
    exe = shutil.which("ogdc-runner")
    if not exe:
        pytest.skip(
            "ogdc-runner CLI not found on PATH; install it or add it to PATH to run this test."
        )
    return exe


def _call_validate_all_with_cli(
    ref: str, timeout: int, exe_path: str, repo_url: str | None = None
) -> tuple[int, str]:
    """Call the validate-all-recipes CLI command."""
    cmd = [exe_path, "validate-all-recipes"]

    # Add repo URL if provided (otherwise uses default)
    if repo_url:
        cmd.append(repo_url)

    # Add ref option
    cmd.extend(["--ref", ref])

    try:
        proc = subprocess.run(
            cmd, check=False, capture_output=True, text=True, timeout=timeout
        )
        out = proc.stdout or ""
        if proc.stderr:
            out = out + ("\n---stderr---\n" + proc.stderr if out else proc.stderr)
        return proc.returncode, out
    except subprocess.TimeoutExpired:
        return 124, f"Timeout after {timeout}s"


def test_validate_all_recipes_at_main():
    """Validate recipes at main (or OGDC_RECIPES_REF if set)."""
    ref = os.environ.get("OGDC_RECIPES_REF", "main")
    exe = _find_cli_executable()

    # Generous timeout since this validates multiple recipes and clones the repo
    timeout = PER_RECIPE_TIMEOUT * 10

    rc, out = _call_validate_all_with_cli(ref, timeout=timeout, exe_path=exe)

    # Print output for visibility
    print(f"\nValidating recipes at ref: {ref}")
    print(f"\n{out}")

    if rc != 0:
        pytest.fail(f"validate-all-recipes failed for {ref} (rc={rc})\n{out}")
