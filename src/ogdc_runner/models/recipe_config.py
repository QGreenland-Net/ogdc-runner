from __future__ import annotations

from pydantic import AnyUrl, BaseModel, Field, field_validator

# Input parameter can be either a URL or a file path (as string)
InputParam = AnyUrl | str


# Create a model for the recipe input
class RecipeInput(BaseModel):
    params: list[InputParam]

    @field_validator("params")
    def validate_params(cls, params):
        """Ensure there's at least one input parameter."""
        if not params:
            error_msg = "At least one input parameter is required"
            raise ValueError(error_msg)
        return params


class RecipeOutput(BaseModel):
    dataone_id: str


# Create a model for the recipe configuration
class RecipeConfig(BaseModel):
    name: str

    # Allow lower-case alphanumeric characters, `.`, and `,`. These are the only
    # allowable characters in k8s object names. `id` to construct such names.
    id: str = Field(..., pattern=r"^[a-z0-9.-]+$")

    input: RecipeInput
    output: RecipeOutput

    # ffspec-compatible recipe directory string.
    # This is where the rest of the config was set from.
    recipe_directory: str
