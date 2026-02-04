"""Orchestration for parallel execution of workflow tasks.

This module provides the abstract ParallelExecutionOrchestrator base class
which defines the interface for managing parallel Argo workflow tasks. It handles:

1. Creating execution templates (Container templates or Hera @script functions)
2. Partitioning input data into parallel chunks
3. Creating DAG tasks with proper dependencies and parameters

The maximum parallelism is controlled at the workflow level, allowing the
Argo workflow engine to automatically schedule tasks as resources become available.

Example:
    orchestrator = ShellParallelExecutionOrchestrator(
        recipe_config=recipe_config,
        execution_function=ExecutionFunction(name="cmd-0", command="process.sh"),
    )
    template = orchestrator.create_execution_template()
    tasks = orchestrator.create_parallel_tasks(template)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from hera.workflows import (
    Task,
)
from loguru import logger

from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
)
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.partitioning import create_partitions


class ParallelExecutionOrchestrator(ABC):
    """Abstract base class for orchestrating parallel execution of workflow tasks.

    This class defines the interface for creating Argo DAG tasks for parallel execution
    of a single execution function. Each execution function should have its own
    orchestrator instance, and the DAG dependencies between different execution
    functions should be managed by the workflow implementation.

    Subclasses must implement:
    - create_execution_template(): Create workflow-specific execution templates
    - _create_tasks_from_partitions(): Create tasks from partitions with specific parameters
    """

    def __init__(
        self,
        recipe_config: RecipeConfig,
        execution_function: ExecutionFunction,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            recipe_config: Recipe configuration
            execution_function: Execution function to run in parallel
        """
        self.recipe_config = recipe_config
        self.execution_function = execution_function

    def create_parallel_tasks(
        self,
        template: Any,
    ) -> list[Task]:
        """Create Argo DAG tasks for parallel execution.

        Must be called after create_execution_template() and can be called within DAG context.

        Args:
            template: Execution template from create_execution_template()

        Returns:
            List of Argo Task objects for parallel execution
        """
        logger.info(
            f"Creating parallel execution tasks for {self.execution_function.name}"
        )

        partitions = self._create_partitions()
        logger.info(
            f"Created {len(partitions)} partitions for {self.execution_function.name}"
        )

        return self._create_tasks_from_partitions(partitions, template)

    def _create_partitions(
        self,
    ) -> list[FilePartition]:
        """Create file partitions based on configuration.

        Returns:
            List of FilePartition objects
        """
        inputs = self.recipe_config.input.params

        return create_partitions(
            inputs=inputs,
            execution_function=self.execution_function,
            parallel_config=self.recipe_config.workflow.parallel,
        )

    @abstractmethod
    def create_execution_template(self) -> Any:
        """Create workflow-specific execution template.

        Returns:
            Execution template (Container, Hera @script function, etc.)

        Raises:
            ValueError: If execution function configuration is invalid
        """
        ...

    @abstractmethod
    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Any,
    ) -> list[Task]:
        """Create Argo tasks from partitions with workflow-specific parameters.

        Args:
            partitions: List of file partitions to process
            template: Execution template to use for all tasks

        Returns:
            List of Task objects ready for DAG execution
        """
        ...
