from __future__ import annotations

import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path("../src/").resolve()))

project = "ogdc-runner"
copyright = f"{dt.date.today().year}, NSIDC and ADC"
author = "QGreenland-Net team"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinxcontrib.autodoc_pydantic",
    "sphinxcontrib.mermaid",
]

autodoc_pydantic_model_show_field_summary = False
autodoc_pydantic_model_show_json = False

autodoc_typehints = "description"

typehints_use_signature = True
typehints_use_signature_return = True

source_suffix = [".rst", ".md"]
exclude_patterns = [
    "_build",
    "**.ipynb_checkpoints",
    "Thumbs.db",
    ".DS_Store",
    ".env",
    ".venv",
]

html_theme = "furo"

myst_enable_extensions = [
    "colon_fence",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

autodoc_mock_imports = [
    "hera",
    "jinja",
    "fastapi",
]


# Ignore references that cannot be resolved by sphinx (mostly 3rd-party
# libraries)
nitpick_ignore = [
    ("py:class", "_io.BytesIO"),
    ("py:class", "_io.StringIO"),
    ("py:class", "fastapi.FastAPI"),
    ("py:class", "fastapi.Request"),
    ("py:class", "hera.workflows.Container"),
    ("py:class", "hera.workflows.Parameter"),
    ("py:class", "hera.workflows.Task"),
    ("py:class", "hera.workflows.Workflow"),
    ("py:class", "hera.workflows.WorkflowsService"),
    ("py:class", "hera.workflows.models.RetryStrategy"),
    ("py:class", "hera.workflows.models.Volume"),
    ("py:class", "hera.workflows.models.VolumeMount"),
    ("py:class", "sqlalchemy.engine.base.Engine"),
    # TODO: this one seems wrong...Why are we getting this warning:
    # <unknown>:1: WARNING: py:class reference target not found: list[dict[str [ref.class]
    # There is no other context except that warning
    ("py:class", "list[dict[str"),
]

always_document_param_types = True


templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# html_static_path = ["_static"]


def _run_apidoc_cmd(app: Any) -> None:
    this_dir = Path(app.srcdir)
    src_dir = (Path(app.srcdir) / "../src/ogdc_runner").resolve()
    api_output_dir = (this_dir / "api/").resolve()
    subprocess.run(
        [
            "sphinx-apidoc",
            "--separate",
            "-o",
            api_output_dir,
            "--no-toc",
            "--module-first",
            "--implicit-namespaces",
            "--force",
            src_dir,
        ],
        check=True,
    )


def setup(app: Any) -> None:
    """Setup build-initialization action to run `sphinx-apidoc` command

    This ensures the api docs are up-to-date each time the build is run.
    """
    app.connect("builder-inited", _run_apidoc_cmd)
