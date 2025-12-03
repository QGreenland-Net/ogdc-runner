from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import nox

DIR = Path(__file__).parent.resolve()

nox.needs_version = ">=2024.3.2"
nox.options.sessions = ["typecheck", "tests"]
nox.options.default_venv_backend = "uv|virtualenv"
if os.environ.get("ENVIRONMENT") == "dev":
    # Use existing venvs where possible in dev
    nox.options.reuse_existing_virtualenvs = True
else:
    # All other envs should have the nox venvs recreated.
    nox.options.reuse_existing_virtualenvs = False


@nox.session
def typecheck(session: nox.Session) -> None:
    """Run typechecker."""
    session.install(".[test]")
    session.run("mypy", *session.posargs)


@nox.session(requires=["typecheck"])
def tests(session: nox.Session) -> None:
    """Run the unit and regular tests."""
    session.install(".[test]")
    session.run(
        "pytest",
        *session.posargs,
        # Set ARGO_WORKFLOWS_SERVICE_URL to point directly to exposed localhost
        # URL for local development.
        env={"ARGO_WORKFLOWS_SERVICE_URL": "http://localhost:2746"},
    )


@nox.session(reuse_venv=True)
def docs(session: nox.Session) -> None:
    """Build the docs. Pass "--serve" to serve. Pass "-b linkcheck" to check links."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true", help="Serve after building")
    parser.add_argument(
        "-b", dest="builder", default="html", help="Build target (default: html)"
    )
    args, posargs = parser.parse_known_args(session.posargs)

    if args.builder != "html" and args.serve:
        session.error("Must not specify non-HTML builder with --serve")

    extra_installs = ["sphinx-autobuild"] if args.serve else []

    session.install("-e.[docs]", *extra_installs)
    session.chdir("docs")

    if args.builder == "linkcheck":
        session.run(
            "sphinx-build", "-b", "linkcheck", ".", "_build/linkcheck", *posargs
        )
        return

    shared_args = (
        "--port",  # Use port 8080
        "8080",  # Use port 8080
        "-n",  # nitpicky mode
        "-T",  # full tracebacks
        f"-b={args.builder}",
        ".",
        f"_build/{args.builder}",
        *posargs,
    )

    if args.serve:
        session.run(
            "sphinx-autobuild",
            *shared_args,
        )
    else:
        session.run("sphinx-build", "--keep-going", *shared_args)


@nox.session
def build(session: nox.Session) -> None:
    """Build an SDist and wheel."""

    build_path = DIR.joinpath("build")
    if build_path.exists():
        shutil.rmtree(build_path)

    session.install("build")
    session.run("python", "-m", "build")
