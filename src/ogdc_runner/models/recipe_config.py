from __future__ import annotations

from typing import Literal

from pydantic import AnyUrl, BaseModel, Field


# Create a model for the recipe input
class RecipeInput(BaseModel):
    url: AnyUrl


class RecipeOutput(BaseModel):
    dataone_id: str


class RecipeWorkflow(BaseModel):
    workflow_type: str


class CommandScriptWorkflow(RecipeWorkflow):
    workflow_type: Literal["command_script"] = "command_script"
    filename: str


class TemplateWorkflow(RecipeWorkflow):
    workflow_type: Literal["template"] = "template"
    name: str


# Create a model for the recipe configuration
class RecipeConfig(BaseModel):
    name: str

    # Allow lower-case alphanumeric characters, `.`, and `,`. These are the only
    # allowable characters in k8s object names. `id` to construct such names.
    id: str = Field(..., pattern=r"^[a-z0-9.-]+$")

    # Workflows that this recipe defines
    # By default, this is the "simple" recipe with a filename of "recipe.sh".
    workflows: list[CommandScriptWorkflow | TemplateWorkflow] = [
        CommandScriptWorkflow(filename="recipe.sh")
    ]

    input: RecipeInput
    output: RecipeOutput

    recipe_dir: str
