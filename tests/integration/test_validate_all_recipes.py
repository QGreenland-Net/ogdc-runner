from __future__ import annotations

import os

from ogdc_runner.recipe import validate_all_recipes_in_repo


def test_validate_all_recipes_at_main():
    """Validate recipes at main (or OGDC_RECIPES_REF if set)."""
    ref = os.environ.get("OGDC_RECIPES_REF", "main")
    repo_url = "https://github.com/qgreenland-net/ogdc-recipes.git"

    print(f"\nValidating recipes at ref: {ref}")
    validate_all_recipes_in_repo(repo_url, ref, check_urls=True)
