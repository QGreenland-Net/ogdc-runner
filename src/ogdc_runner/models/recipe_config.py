from __future__ import annotations

import json
from functools import cached_property
from pathlib import Path
from typing import Literal

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)


class OgdcBaseModel(BaseModel):
    """Base pydantic model for the ogdc-runner."""

    # Disallow "extra" config that we do not expect. We want users to know if
    # they've made a mistake and added something that has no effect.
    model_config = ConfigDict(extra="forbid")


# Input parameter with type and value
class InputParam(OgdcBaseModel):
    value: AnyUrl | str
    type: Literal["url", "pvc_mount", "file_system"]


# Create a model for the recipe input
class RecipeInput(OgdcBaseModel):
    params: list[InputParam]

    @field_validator("params")
    def validate_params(cls, params: list[InputParam]) -> list[InputParam]:
        """Ensure there's at least one input parameter."""
        if not params:
            error_msg = "At least one input parameter is required"
            raise ValueError(error_msg)
        return params


class RecipeOutput(OgdcBaseModel):
    dataone_id: str = "TODO"


class Workflow(OgdcBaseModel):
    type: Literal["shell", "visualization"]


class ShellWorkflow(Workflow):
    type: Literal["shell"] = "shell"
    # the name of the `.sh` file containing the list of commands to run.
    sh_file: str = "recipe.sh"


class VizWorkflow(Workflow):
    type: Literal["visualization"] = "visualization"

    # the name of the viz workflow json configuration file. By default, this is
    # `None`, which means that the viz workflow will use its default
    # configuration.
    config_file: str | Path | None = None

    batch_size: int = 250

    @field_validator("config_file", mode="after")
    @classmethod
    def config_file_path(
        cls, value: str | Path | None, info: ValidationInfo
    ) -> Path | None:
        if (value is None) or isinstance(value, Path):
            return value

        if isinstance(info.context, dict) and (
            recipe_directory := info.context.get("recipe_directory")
        ):
            assert isinstance(recipe_directory, Path)
            config_filepath = recipe_directory / value
            if not config_filepath.exists():
                raise ValueError(
                    f"The file {value} is not present in the recipe directory"
                )

            config_text = config_filepath.read_text()

            try:
                json.loads(config_text)
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to read json from {value}") from e

            return config_filepath

        return Path(value)


class RecipeMeta(OgdcBaseModel):
    """Model for a recipe's metadata (`meta.yaml`)."""

    # Allow alphanumeric characters, `.`, ` ` (space), and `,`.
    # The name is used to create an ID for the recipe that must be k8s-compliant
    # (lower-case, alphanumeric characters, `.`, and `,`).
    name: str = Field(..., pattern=r"^[a-zA-Z0-9 .-]+$")

    # Workflow-specific configuration
    workflow: ShellWorkflow | VizWorkflow

    input: RecipeInput
    output: RecipeOutput = RecipeOutput()

    # Optional Docker image (supports both local and hosted images)
    # Examples: "my-local-image", "ghcr.io/owner/image:latest"
    image: str | None = Field(
        default=None, description="Docker image with optional tag"
    )


class RecipeConfig(RecipeMeta):
    """Model for a recipe's configuration.

    This includes the data in `meta.yaml`, plus some internal metadata/config
    that is generated dynamically at runtime (e.g., `recipe_directory`).
    """

    # Path to recipe directory on disk
    # This is where the rest of the config was set from.
    recipe_directory: Path

    @computed_field  # type: ignore[misc]
    @cached_property
    def id(self) -> str:
        k8s_name = self.name.lower().replace(" ", "-")

        return k8s_name

    @model_validator(mode="after")
    def inject_recipe_directory(self):  # type: ignore[no-untyped-def]
        self.workflow = self.workflow.model_validate(
            self.workflow,
            context={"recipe_directory": self.recipe_directory},
        )

        return self


class RecipeImage(OgdcBaseModel):
    """
    Image configuration for the recipe.

    Supports both local and hosted Docker images.
    """

    image: str = Field(..., description="Docker image name")
    tag: str = Field(default="latest", description="Docker image tag")

    @property
    def full_image_path(self) -> str:
        """Return the full image path including tag."""
        return f"{self.image}:{self.tag}"
