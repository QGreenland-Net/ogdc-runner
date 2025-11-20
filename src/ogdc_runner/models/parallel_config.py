"""Models for parallel execution orchestration.

Note: ParallelConfig is in recipe_config.py since it's recipe configuration.
This module contains execution-related models used by the parallel orchestrator.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import Field, field_validator

from ogdc_runner.models.base import OgdcBaseModel


class ExecutionFunction(OgdcBaseModel):
    """Represents a function or command to execute in parallel.

    You must specify exactly ONE of: command, script_module, or function.

    Attributes:
        name: Unique identifier for this execution function
        command: Shell command to execute (for shell workflows)
        script_module: Python script module name
        function: Python callable/function to execute (for Hera script-decorated functions)
    """

    name: str = Field(
        ...,
        description="Unique identifier for this execution function",
    )
    command: str | None = Field(
        default=None,
        description="Shell command to execute (for shell workflows)",
    )
    script_module: str | None = Field(
        default=None,
        description="Python script module name (for visualization workflows)",
    )
    function: Callable[..., Any] | None = Field(
        default=None,
        description="Python callable to execute (e.g., Hera @script decorated function)",
        exclude=True,  # Exclude from serialization since functions aren't JSON-serializable
    )

    @field_validator("function", mode="before")
    @classmethod
    def validate_function(cls, v: Any) -> Any:
        """Validate that function is callable if provided."""
        if v is not None and not callable(v):
            msg = "function must be a callable"
            raise ValueError(msg)
        return v

    def model_post_init(self, __context: Any) -> None:
        """Validate that exactly one execution type is specified."""
        execution_types = [self.command, self.script_module, self.function]
        specified_count = sum(1 for t in execution_types if t is not None)

        if specified_count == 0:
            msg = "Must specify exactly one of: command, script_module, or function"
            raise ValueError(msg)
        if specified_count > 1:
            msg = "Can only specify one of: command, script_module, or function"
            raise ValueError(msg)


class FilePartition(OgdcBaseModel):
    """Represents a partition of input files for parallel execution.

    Each partition represents a unit of work that will be executed in a separate
    container/task.

    Attributes:
        partition_id: Unique identifier for this partition
        files: List of file paths or URLs to process in this partition
        execution_function: Name of the ExecutionFunction to run on these files
        metadata: Additional metadata for this partition
    """

    partition_id: int = Field(
        ...,
        description="Unique identifier for this partition",
    )
    files: list[str] = Field(
        ...,
        description="List of file paths or URLs to process in this partition",
    )
    execution_function: str = Field(
        ...,
        description="Name of the ExecutionFunction to run on these files",
    )
    metadata: dict[str, str | int | float] = Field(
        default_factory=dict,
        description="Additional metadata for this partition",
    )
