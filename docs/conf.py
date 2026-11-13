from __future__ import annotations

import datetime as dt
import logging
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
    ("py:class", "pydantic.Field"),
    # TODO: this one seems wrong...Why are we getting this warning:
    # <unknown>:1: WARNING: py:class reference target not found: list[dict[str [ref.class]
    # There is no other context except that warning
    ("py:class", "list[dict[str"),
]

autodoc_default_options = {
    # Exclude forward references from pydantic models
    # E.g.,: `WARNING: Cannot resolve forward reference in type annotations of
    # "ogdc_runner.models.recipe_config.Field" (module pydantic.fields): name
    # 'JsonValue' is not defined [sphinx_autodoc_typehints.forward_reference]`
    "exclude-members": "Field,computed_field",
}

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

    def _install_duplicate_warning_filter() -> None:
        """Hack: filter out warnings about duplicate references.

        Warnings are produced when we import objects in a module's `__init__.py`
        to expose via `__all__`. E.g.:

        ```
        <unknown>:1: WARNING: duplicate object description of
        ogdc_runner.models.ExecutionFunction.command, other instance in
        api/ogdc_runner.models, use :no-index: for one of them

        ```

        Because we have autodoc generate all the rst files, we can't add
        `:no-index:` manually.

        Ignoring this seems to be fine - the docs still render the way I expect
        them to.
        """

        class IgnoreDuplicateObjectDescription(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                return (
                    not record.getMessage()
                    .lower()
                    .startswith("duplicate object description of ")
                )

        sphinx_logger = logging.getLogger("sphinx")

        for handler in sphinx_logger.handlers:
            if handler.level == logging.WARNING:
                handler.filters.insert(0, IgnoreDuplicateObjectDescription())
                return

        err = "Could not find Sphinx's warning handler"
        raise RuntimeError(err)

    _install_duplicate_warning_filter()


def setup(app: Any) -> None:
    """Setup build-initialization action to run `sphinx-apidoc` command

    This ensures the api docs are up-to-date each time the build is run.
    """
    app.connect("builder-inited", _run_apidoc_cmd)
