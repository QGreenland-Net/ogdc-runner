from __future__ import annotations

import json
import logging
from functools import cache, cached_property
from pathlib import Path
from typing import Literal, Self

import requests
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

from ogdc_runner.dataone.resolver import resolve_dataone_input
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig

logger = logging.getLogger(__name__)


class OgdcBaseModel(BaseModel):
    """Base pydantic model for the ogdc-runner."""

    # Disallow "extra" config that we do not expect. We want users to know if
    # they've made a mistake and added something that has no effect.
    model_config = ConfigDict(extra="forbid")


class InputParam(OgdcBaseModel):
    """Input parameter for a recipe.

    When instantiated with `context={"check_urls": True}`, URL-type parameters
    will be validated to ensure they are accessible via HTTP HEAD request.
    """

    value: AnyUrl | str
    type: Literal["url", "pvc_mount", "file_system", "dataone"]

    # Optional fields for DataONE inputs
    member_node: str | None = None

    # Private fields populated during resolution (for dataone type)
    _resolved_url: str | None = None
    _entity_name: str | None = None
    _entity_description: str | None = None
    _format_id: str | None = None
    _dataset_pid: str | None = None

    @model_validator(mode="after")
    def validate_url_accessible(self, info: ValidationInfo) -> Self:
        """Validate that URL-type parameters are accessible."""
        if self.type != "url":
            return self

        context = info.context or {}
        if not context.get("check_urls", False):
            return self

        url = str(self.value)
        timeout = context.get("url_timeout", 30)

        try:
            response = requests.head(url, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ValueError(
                f"URL validation failed for {url}: HTTP {e.response.status_code}"
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise ValueError(
                f"URL validation failed for {url}: Connection failed"
            ) from e
        except requests.exceptions.Timeout as e:
            raise ValueError(
                f"URL validation failed for {url}: Timeout after {timeout}s"
            ) from e
        except Exception as e:
            raise ValueError(f"URL validation failed for {url}: {e}") from e

        return self


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


def _validate_filename_with_directory(
    filename: str,
    info: ValidationInfo,
) -> Path:
    """Validate that the given filename is a file that exists in the `info.context["recipe_directory"]`."""
    if not isinstance(info.context, dict) or "recipe_directory" not in info.context:
        err_str = "`recipe_directory` is required context."
        raise ValueError(err_str)

    recipe_directory = Path(info.context["recipe_directory"])

    config_filepath = recipe_directory / filename

    if not config_filepath.exists():
        raise FileNotFoundError(
            f"The file {filename} is not present in the recipe directory"
        )

    return config_filepath


class ShellWorkflow(Workflow):
    """Model representing the shell workflow configuration.

    Requires that the `recipe_directory` context be set when instantiating
    the class. E.g.,:

        workflow = ShellWorkflow.model_validate(
            {"sh_file": "recipe.sh"},
            context={"recipe_directory": Path("/path/to/recipe_directory")},
        ),

    """

    type: Literal["shell"] = "shell"
    # the name of the `.sh` file containing the list of commands to run.
    sh_file: str | Path = "recipe.sh"

    @model_validator(mode="after")
    def sh_file_path(
        self,
        info: ValidationInfo,
    ) -> Self:
        """Model-level validator that constructs a full path to `sh_file`."""
        if isinstance(self.sh_file, Path):
            return self

        filepath = _validate_filename_with_directory(self.sh_file, info)

        self.sh_file = filepath

        return self

    def get_commands_from_sh_file(self) -> list[str]:
        """Returns a list of commands run from the workflow `sh_file`."""
        if not isinstance(self.sh_file, Path):
            raise ValueError(
                "`sh_file` must be a fully qualified `Path`."
                f" Got: {self.sh_file} (type: {type(self.sh_file)})."
            )

        lines = self.sh_file.read_text().split("\n")
        commands = [line for line in lines if line and not line.startswith("#")]

        return commands


@cache
def _read_config_json(config_filepath: Path) -> str:
    """Validate that the config filepath has valid json."""
    config_text = config_filepath.read_text()

    try:
        json.loads(config_text)
    except json.JSONDecodeError as e:
        raise OgdcInvalidRecipeConfig(
            f"Failed to read json from {config_filepath}"
        ) from e

    return config_text


class VizWorkflow(Workflow):
    """Model representing the visualization workflow configuration.

    Requires that the `recipe_directory` context be set when instantiating
    the class. E.g.,:

        workflow = VizWorkflow.model_validate(
            {"config_file": "config.json"},
            context={"recipe_directory": Path("/path/to/recipe_directory")},
        ),

    """

    type: Literal["visualization"] = "visualization"

    # the name of the viz workflow json configuration file. By default, this is
    # `None`, which means that the viz workflow will use its default
    # configuration.
    config_file: str | Path | None = None

    batch_size: int = 250

    @model_validator(mode="after")
    def config_file_path(
        self,
        info: ValidationInfo,
    ) -> Self:
        """Model-level validator that constructs a full path to `sh_file`."""
        if self.config_file is None or isinstance(self.config_file, Path):
            return self

        config_filepath = _validate_filename_with_directory(self.config_file, info)

        # Verify that the file can be read and returns valid json.
        _read_config_json(config_filepath)

        self.config_file = config_filepath

        return self

    def get_config_file_json(self) -> str:
        """Get the viz workflow config as json.

        If passed a JSON file, read the file content and return. Otherwise, an empty
        configuration will be returned (`"{}"`).

        This configuration is used by the pdgworkflow for visualization workflows.
        When an empty config ({}) is returned, WorkflowManager will use its default behavior.

        For documentation on available configuration options, see:
        - ConfigManager documentation: https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/feature-wf-k8s/pdgworkflow/ConfigManager.py
        - Example config: https://github.com/QGreenland-Net/ogdc-recipes/blob/main/recipes/viz-workflow/config.json

        Returns:
            The content of the config.json file as a string, or empty JSON if file doesn't exist.
            An empty config ({}) will cause ConfigManager to use default behavior.
        """
        if isinstance(self.config_file, Path):
            return _read_config_json(self.config_file)

        return "{}"


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
    def resolve_dataone_inputs(self) -> RecipeConfig:
        """Resolve DataONE dataset identifiers to data object URLs."""

        for param in self.input.params:
            if param.type == "dataone":
                # Resolve the dataset to get data objects
                member_node = param.member_node or "https://arcticdata.io/metacat/d1/mn"

                try:
                    data_objects = resolve_dataone_input(
                        dataset_identifier=str(param.value),
                        member_node=member_node,
                    )

                    if not data_objects:
                        raise ValueError(
                            f"No data objects found in dataset {param.value}"
                        )

                    # For now, use the first data object
                    # TODO: Allow user to specify which object or handle multiple
                    obj = data_objects[0]

                    # Populate the resolved fields
                    param._resolved_url = obj["url"]
                    param._entity_name = obj["entity_name"]
                    param._entity_description = obj["entity_description"]
                    param._format_id = obj["format_id"]
                    param._dataset_pid = str(param.value)

                    msg = f"Resolved {param.value} -> {obj['identifier']}"
                    logger.info(msg)

                except Exception as e:
                    msg = f"Failed to resolve DataONE input {param.value}: {e}"
                    logger.error(msg)
                    raise ValueError(
                        f"Failed to resolve DataONE package {param.value}. "
                        f"Make sure the value is a dataset package identifier (e.g., urn:uuid:...). "
                        f"Error: {e}"
                    ) from e

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
