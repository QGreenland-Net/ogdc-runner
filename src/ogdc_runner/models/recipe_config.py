from __future__ import annotations

from typing import Literal

from pydantic import AnyUrl, BaseModel, Field, field_validator


# Input parameter with type and value
class InputParam(BaseModel):
    value: AnyUrl | str
    type: Literal["url", "pvc_mount", "file_system"]


# Create a model for the recipe input
class RecipeInput(BaseModel):
    params: list[InputParam]

    @field_validator("params")
    def validate_params(cls, params: list[InputParam]) -> list[InputParam]:
        """Ensure there's at least one input parameter."""
        if not params:
            error_msg = "At least one input parameter is required"
            raise ValueError(error_msg)
        return params


class RecipeOutput(BaseModel):
    dataone_id: str = "TODO"


# Create a model for the recipe configuration
class RecipeConfig(BaseModel):
    name: str

    # Allow lower-case alphanumeric characters, `.`, and `,`. These are the only
    # allowable characters in k8s object names. `id` to construct such names.
    id: str = Field(..., pattern=r"^[a-z0-9.-]+$")

    # Type of recipe, e.g., "shell", "visualization", etc.
    type: Literal["shell", "visualization"] = Field(default="shell")

    input: RecipeInput
    output: RecipeOutput = RecipeOutput()

    # Optional Docker image (supports both local and hosted images)
    # Examples: "my-local-image", "ghcr.io/owner/image:latest"
    image: str | None = Field(
        default=None, description="Docker image with optional tag"
    )

    # ffspec-compatible recipe directory string.
    # This is where the rest of the config was set from.
    recipe_directory: str


class RecipeImage(BaseModel):
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
