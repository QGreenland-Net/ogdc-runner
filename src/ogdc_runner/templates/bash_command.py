from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
)


def make_cmd_template(
    name: str,
    command: str,
) -> Container:
    """Creates a template container for running the given command with an
    /input_dir/ and /output_dir/."""
    template = Container(
        name=name,
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {command}",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template
