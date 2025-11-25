"""ogdc-runner

Defines Open Geospatial Data Cloud (OGDC) recipe API(s) and submits recipes to OGDC for
execution.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version(
        "ogdc-runner"
    )  # Use your package's name as registered on PyPI
except PackageNotFoundError:
    # This block handles cases where the package is imported but not yet installed (e.g., development mode)
    __version__ = "0.2.0"

__all__ = ["__version__"]
