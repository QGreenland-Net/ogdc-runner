"""Experimental `recipe: {type: steps}` schema with reusable templates.

Structure only. Steps recipes are validated but not yet executable.
"""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

from pydantic import Field, computed_field, field_validator, model_validator

from ogdc_runner.models.base import OgdcBaseModel
from ogdc_runner.models.recipe_config import (
    DataOneRecipeOutput,
    PvcRecipeOutput,
    RecipeInput,
    TemporaryRecipeOutput,
    _read_config_json,
)

# Cap on steps per recipe.
MAX_STEPS = 20

# Names may end up in Kubernetes object names: lowercase DNS-label style.
_K8S_NAME = r"^[a-z0-9]([-a-z0-9]{0,40}[a-z0-9])?$"
# Template parameter names are UPPER_SNAKE.
_PARAM_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
# Shared template refs look like `templates/<path>.yml`.
_TEMPLATE_REF = re.compile(r"^templates/[A-Za-z0-9._/-]+\.ya?ml$")

Scalar = str | int | float | bool


def _check_relative_file(value: str) -> str:
    """Reject absolute paths and `..` so files stay inside their directory."""
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"must be a relative path without '..': {value!r}")
    return value


def _check_parameter_names(params: dict[str, ParamSpec]) -> dict[str, ParamSpec]:
    """Require UPPER_SNAKE parameter names."""
    bad = [k for k in params if not _PARAM_NAME.match(k)]
    if bad:
        raise ValueError(f"parameter names must be UPPER_SNAKE: {bad}")
    return params


class ParamSpec(OgdcBaseModel):
    """A template parameter with an optional default."""

    default: Scalar | None = None
    required: bool = False


class FilesOutput(OgdcBaseModel):
    """Output is a set of files matching `glob`.

    `source` is where they come from: the output directory (`dir`) or the
    template's return value (`return`).
    """

    type: Literal["files"] = "files"
    glob: str = "**/*"
    source: Literal["dir", "return"] = "dir"


class JsonOutput(OgdcBaseModel):
    """Output is a JSON value returned by the template, not files."""

    type: Literal["json"]
    source: Literal["return"] = "return"


class ShellTemplate(OgdcBaseModel):
    """A reusable step that runs a shell script (`sh_file`).

    `map` mode runs once per input file; `once` runs a single time.
    """

    name: str = Field(pattern=_K8S_NAME)
    type: Literal["shell"]
    mode: Literal["map", "once"] = "map"
    sh_file: str
    parameters: dict[str, ParamSpec] = Field(default_factory=dict)
    output: FilesOutput = FilesOutput()

    _sh_file = field_validator("sh_file")(_check_relative_file)
    _params = field_validator("parameters")(_check_parameter_names)


class CallSpec(OgdcBaseModel):
    """A Python call: a plain `function`, or a `factory` plus a `method` on it."""

    function: str | None = None
    factory: str | None = None
    method: str | None = None
    init_kwargs: dict[str, Any] = Field(default_factory=dict)
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _one_form(self) -> CallSpec:
        if (self.function is None) == (self.factory is None):
            msg = "exactly one of function or factory is required"
            raise ValueError(msg)
        if (self.factory is None) != (self.method is None):
            msg = "factory and method go together"
            raise ValueError(msg)
        return self


class VizTemplate(OgdcBaseModel):
    """A reusable visualization step, given by exactly one of `call` or `config_file`."""

    name: str = Field(pattern=_K8S_NAME)
    type: Literal["visualization"]
    mode: Literal["map", "once"] = "map"
    call: CallSpec | None = None
    config_file: str | None = None
    parameters: dict[str, ParamSpec] = Field(default_factory=dict)
    output: FilesOutput | JsonOutput = FilesOutput(source="return")

    _params = field_validator("parameters")(_check_parameter_names)

    @field_validator("config_file")
    @classmethod
    def _config_file(cls, v: str | None) -> str | None:
        return v if v is None else _check_relative_file(v)

    @model_validator(mode="after")
    def _exclusive(self) -> VizTemplate:
        if (self.call is None) == (self.config_file is None):
            msg = "visualization template needs exactly one of call or config_file"
            raise ValueError(msg)
        return self


# Pydantic picks the template class from the `type` field.
Template = Annotated[ShellTemplate | VizTemplate, Field(discriminator="type")]


class StepInput(OgdcBaseModel):
    """Where a step reads from: `recipe.input` or `<earlier-step>.output`."""

    from_: str = Field(alias="from")


class Step(OgdcBaseModel):
    """One step: run `template` on `input`, overriding its parameters via `with`."""

    name: str = Field(pattern=_K8S_NAME)
    template: str
    input: StepInput
    with_: dict[str, Scalar] = Field(default={}, alias="with")


class StepsRecipe(OgdcBaseModel):
    """The `recipe:` block: an ordered list of steps."""

    type: Literal["steps"]
    steps: list[Step] = Field(min_length=1, max_length=MAX_STEPS)


class StepsMeta(OgdcBaseModel):
    """Model for the `meta.yml` of a steps recipe (no legacy `workflow`)."""

    name: str = Field(..., pattern=r"^[a-zA-Z0-9 .-]+$")
    input: RecipeInput
    output: DataOneRecipeOutput | TemporaryRecipeOutput | PvcRecipeOutput = (
        PvcRecipeOutput()
    )
    recipe: StepsRecipe
    templates_from: list[str] = Field(default_factory=list)
    templates: list[Template] = Field(default_factory=list)

    @field_validator("templates_from")
    @classmethod
    def _templates_from(cls, refs: list[str]) -> list[str]:
        for ref in refs:
            if not _TEMPLATE_REF.match(ref) or ".." in ref.split("/"):
                raise ValueError(f"template ref must be templates/<file>.yml: {ref!r}")
        return refs

    def check_graph(self, shared: list[ShellTemplate | VizTemplate]) -> None:
        """Check step wiring against local + resolved shared templates.

        Pure: reads no files and resolves no inputs. Raises `ValueError`.
        """
        # Template names must be unique across local and shared templates.
        all_templates = [*self.templates, *shared]
        names = [t.name for t in all_templates]
        if len(names) != len(set(names)):
            msg = "duplicate template names across local and shared templates"
            raise ValueError(msg)
        by_name = {t.name: t for t in all_templates}

        # `seen` holds earlier step names; `consumed` those whose output is read.
        seen: list[str] = []
        consumed: set[str] = set()
        for step in self.recipe.steps:
            if step.name in seen:
                msg = f"duplicate step name: {step.name}"
                raise ValueError(msg)
            # A step may only read the recipe input or an earlier step's output.
            source = step.input.from_
            if source != "recipe.input":
                ref, _, attr = source.partition(".")
                if attr != "output" or ref not in seen:
                    msg = f"{step.name}: input must be recipe.input or <earlier-step>.output"
                    raise ValueError(msg)
                consumed.add(ref)
            template = by_name.get(step.template)
            if template is None:
                msg = f"{step.name}: unknown template {step.template!r}"
                raise ValueError(msg)
            # `with` must name declared parameters and cover the required ones.
            unknown = set(step.with_) - set(template.parameters)
            missing = {k for k, p in template.parameters.items() if p.required} - set(
                step.with_
            )
            if unknown or missing:
                msg = (
                    f"{step.name}: unknown parameters={sorted(unknown)}"
                    f" missing parameters={sorted(missing)}"
                )
                raise ValueError(msg)
            seen.append(step.name)

        # Every step except the last must feed a later step.
        unused = set(seen[:-1]) - consumed
        if unused:
            msg = f"steps whose output is never used: {sorted(unused)}"
            raise ValueError(msg)
        # The last step's output is the recipe result, so it must be files.
        if isinstance(by_name[self.recipe.steps[-1].template].output, JsonOutput):
            msg = "the final step must produce files"
            raise ValueError(msg)  # noqa: TRY004


class StepsRecipeConfig(StepsMeta):
    """A steps recipe plus the directory it was loaded from."""

    recipe_directory: Path

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def id(self) -> str:
        """Kubernetes-friendly slug of `name`, as for legacy recipes."""
        return self.name.lower().replace(" ", "-")

    @model_validator(mode="after")
    def _check_local_files(self) -> StepsRecipeConfig:
        """Local template files exist; the graph is final without shared templates."""
        for template in self.templates:
            filename = (
                template.sh_file
                if isinstance(template, ShellTemplate)
                else template.config_file
            )
            if filename is None:
                continue
            path = self.recipe_directory / filename
            if not path.is_file():
                msg = f"{template.name}: {filename} is not present in the recipe directory"
                raise ValueError(msg)
            if filename.endswith(".json"):
                _read_config_json(path)
        # Shared templates aren't loaded here, so the graph can only be
        # checked now when the recipe has none.
        if not self.templates_from:
            self.check_graph([])
        return self
