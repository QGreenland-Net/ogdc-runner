from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

TEST_RECIPES_PATH = os.getenv("TEST_RECIPES_PATH", "tests")
# TODO: decide if this should be shorter
PER_RECIPE_TIMEOUT = int(os.getenv("PER_RECIPE_TIMEOUT", "600"))  # seconds


def _immediate_child_dirs(root: Path):
    """Return a list of immediate child directories of root."""
    return [p for p in root.iterdir() if p.is_dir()]


def discover_recipe_dirs_one_level_down(root: Path):
    """
    Discover recipe directories by looking for .meta.yml under each immediate child
    directory of `root`. This avoids discovering .meta.yml files directly under `root`.
    Returns a sorted list of unique parent directories that contain a .meta.yml.
    """
    parents: set[Path] = set()
    for child in _immediate_child_dirs(root):
        for p in child.rglob(".meta.yml"):
            parents.add(p.parent.resolve())
    return sorted(parents)


def _find_cli_executable() -> str:
    """
    Return the path to the 'ogdc-runner' CLI if available, otherwise raise.
    The test uses the CLI exclusively.
    """
    exe = shutil.which("ogdc-runner")
    if exe:
        return exe
    return None


def _call_validate_with_cli(
    recipe_dir: Path, timeout: int, exe_path: str
) -> tuple[int, str]:
    """
    Run the CLI command `ogdc-runner validate-recipe <dir>` and return (rc, combined output).
    Expects the ogdc-runner executable to be present at exe_path.
    """
    cmd = [exe_path, "validate-recipe", str(recipe_dir)]
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


@pytest.fixture(scope="session")
def recipes_root():
    """
    Yield the root path to search for recipes.
    Defaults to TEST_RECIPES_PATH. Skip the tests if the path doesn't exist.
    """
    p = Path(TEST_RECIPES_PATH).resolve()
    if not p.exists():
        pytest.skip(f"Test recipes root does not exist: {p}")
    return p


def test_validate_all_recipes_iterative(recipes_root):
    """
    Validate every recipe found under the configured test area (any directory with .meta.yml).
    Uses the ogdc-runner CLI exclusively. Collects all failures and fail once at the end with a combined report.
    """
    # Ensure CLI is available
    try:
        exe = _find_cli_executable()
    except FileNotFoundError:
        pytest.skip(
            "ogdc-runner CLI not found on PATH; install it or add it to PATH to run this test."
        )

    dirs = discover_recipe_dirs_one_level_down(Path(recipes_root))
    if not dirs:
        pytest.skip(
            f"No recipes (.meta.yml) discovered under the immediate children of {recipes_root}"
        )

    failures = []
    for d in dirs:
        rc, out = _call_validate_with_cli(d, timeout=PER_RECIPE_TIMEOUT, exe_path=exe)
        if rc != 0:
            failures.append((str(d), rc, out))

    if failures:
        msgs = []
        for p, rc, out in failures:
            msgs.append(f"Recipe {p} failed (rc={rc})\n{out}\n{'-' * 60}\n")
        pytest.fail("\n".join(msgs))
