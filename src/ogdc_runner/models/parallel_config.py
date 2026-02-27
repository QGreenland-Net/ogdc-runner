"""Models for parallel execution orchestration.

Note: ParallelConfig is in recipe_config.py since it's recipe configuration.
This module contains execution-related models used by the parallel orchestrator.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import Field

from ogdc_runner.models.base import OgdcBaseModel


class ExecutionFunction(OgdcBaseModel):
    """Represents a function or command to execute in parallel.

    Exactly one of command or function must be specified.

    Attributes:
        name: Unique identifier for this execution function
        command: Shell command to execute (for shell workflows)
        function: Python callable (for Hera @script decorated functions)
    """

    name: str = Field(
        description="Unique identifier for this execution function",
    )
    command: str | None = Field(
        default=None,
        description="Shell command to execute",
    )
    function: Callable[..., Any] | None = Field(
        default=None,
        description="Python callable (e.g., Hera @script decorated function)",
        exclude=True,
    )

    def model_post_init(self, __context: Any) -> None:
        """Validate that exactly one execution type is specified.

        Raises:
            ValueError: If zero or multiple execution types are specified
        """
        execution_types = [self.command, self.function]
        specified_count = sum(1 for t in execution_types if t is not None)

        if specified_count == 0:
            msg = "Must specify exactly one of: command or function"
            raise ValueError(msg)
        if specified_count > 1:
            msg = "Can only specify one of: command or function"
            raise ValueError(msg)


class FilePartition(OgdcBaseModel):
    """Represents a partition of input files for parallel execution.

    Each partition represents a unit of work executed in a separate container/task.

    Attributes:
        partition_id: Unique identifier for this partition
        files: File paths or URLs to process in this partition
        execution_function: Name of the ExecutionFunction to run
        metadata: Additional metadata (e.g., num_files, size)
    """

    partition_id: int = Field(
        description="Unique identifier for this partition",
    )
    files: list[str] = Field(
        description="List of file paths or URLs to process",
    )
    execution_function: str = Field(
        description="Name of the ExecutionFunction to run",
    )
    metadata: dict[str, str | int | float] = Field(
        default_factory=dict,
        description="Additional metadata for this partition",
    )
