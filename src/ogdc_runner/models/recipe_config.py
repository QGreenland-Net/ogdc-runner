from pydantic import BaseModel, Field, AnyUrl

# Create a model for the recipe input
class RecipeInput(BaseModel):
    url: AnyUrl

class RecipeOutput(BaseModel):
    dataone_id: str

# Create a model for the recipe configuration
class RecipeConfig(BaseModel):
    name: str

    # Allow lower-case alphanumeric characters, `.`, and `,`. These are the only
    # allowable caracters in k8s object names. `id` to construct such names.
    id: str = Field(..., pattern=r'^[a-z0-9.-]+$')

    input: RecipeInput
    output: RecipeOutput
