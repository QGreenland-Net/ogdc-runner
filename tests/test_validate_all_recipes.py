from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

# Per-recipe timeout in seconds
PER_RECIPE_TIMEOUT = 600  # adjust if some recipes need more time


def _tests_dir() -> Path:
    """Return the directory containing this test file (the tests/ folder)."""
    return Path(__file__).parent.resolve()


def _immediate_child_dirs(root: Path):
    """Return a list of immediate child directories of root."""
    return [p for p in root.iterdir() if p.is_dir()]


def discover_recipe_dirs_one_level_down(root: Path) -> list:
    """
    Discover recipe directories by looking for .meta.yml OR meta.yml under each immediate child
    directory of `root`.
    """
    parents: set[Path] = set()
    for child in _immediate_child_dirs(root):
        for name in (".meta.yml", "meta.yml"):
            for p in child.rglob(name):
                parents.add(p.parent.resolve())
    return sorted(parents)


def _find_cli_executable() -> str:
    exe = shutil.which("ogdc-runner")
    if exe:
        return exe
    return None


def _call_validate_with_cli(
    recipe_dir: Path, timeout: int, exe_path: str
) -> tuple[int, str]:
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
    p = _tests_dir()
    if not p.exists():
        pytest.skip(f"Test recipes root does not exist: {p}")
    return p


def test_validate_all_recipes_iterative(recipes_root):
    """
    Validate every recipe found under the immediate subdirectories of the tests/ folder.
    Uses the ogdc-runner CLI exclusively. Collects all failures and fails once at the end.
    """
    try:
        exe = _find_cli_executable()
    except FileNotFoundError:
        pytest.skip(
            "ogdc-runner CLI not found on PATH; install it or add it to PATH to run this test."
        )

    dirs = discover_recipe_dirs_one_level_down(Path(recipes_root))
    # Print discovered dirs so pytest -s shows what will be validated
    print("Discovered recipe directories:")
    for p in dirs:
        print(" -", p)
    if not dirs:
        pytest.skip(
            f"No recipes (meta.yml or .meta.yml) discovered under immediate children of {recipes_root}"
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
