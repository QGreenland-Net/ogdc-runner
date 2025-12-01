"""Orchestrator for parallel execution of workflows.

This module manages parallel task execution with automatic scheduling.
The MAX_PARALLEL_LIMIT controls how many tasks execute concurrently,
with the workflow engine automatically scheduling remaining tasks as
active tasks complete.

Usage:
    with Workflow(..., parallelism=get_max_parallelism()) as w:
        # Create workflow tasks
        ...
"""

from __future__ import annotations

import json
from typing import Any

from hera.workflows import (
    Artifact,
    Container,
    Parameter,
    Task,
)
from loguru import logger

from ogdc_runner.constants import MAX_PARALLEL_LIMIT
from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
)
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.partitioning import create_partitions


def get_max_parallelism() -> int:
    """Get the maximum parallelism limit for workflow execution.

    Returns:
        int: Maximum number of parallel tasks allowed
    """
    return MAX_PARALLEL_LIMIT


class ParallelExecutionOrchestrator:
    """Orchestrates parallel execution for a single execution function.

    This class handles the creation of Argo DAG tasks for parallel execution
    of a single execution function. Each execution function should have its
    own orchestrator instance, and the DAG dependencies between different
    execution functions should be managed by the workflow implementation.
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

    def create_execution_template(self) -> Container | Any:
        """Create execution template. Must be called before entering DAG context.

        Returns:
            Container template or Hera @script decorated function
        """
        return self._create_execution_template()

    def create_parallel_tasks(
        self,
        template: Container | Any,
        source_task: Task,
        source_artifact_name: str = "output-dir",
        input_artifact_name: str = "input-dir",
        artifact_path: str | None = None,
        file_patterns: list[str] | None = None,
    ) -> list[Task]:
        """Create Argo DAG tasks for parallel execution.

        Must be called after create_execution_template() and can be called within DAG context.

        Args:
            template: Execution template from create_execution_template()
            source_task: Task providing input artifacts
            source_artifact_name: Name of the source artifact (default: "output-dir")
            input_artifact_name: Name for the input artifact (default: "input-dir")
            artifact_path: Optional artifact path for dynamic file discovery
            file_patterns: Optional glob patterns for filtering files

        Returns:
            List of Argo Task objects for parallel execution
        """
        logger.info(
            f"Creating parallel execution tasks for {self.execution_function.name}"
        )

        partitions = self._create_partitions(artifact_path, file_patterns)
        logger.info(
            f"Created {len(partitions)} partitions for {self.execution_function.name}"
        )

        return self._create_tasks_from_partitions(
            partitions, template, source_task, source_artifact_name, input_artifact_name
        )

    def _create_partitions(
        self,
        artifact_path: str | None = None,
        file_patterns: list[str] | None = None,
    ) -> list[FilePartition]:
        """Create file partitions based on configuration.

        Args:
            artifact_path: Optional artifact path for dynamic discovery
            file_patterns: Optional glob patterns for filtering

        Returns:
            List of FilePartition objects
        """
        inputs = None if artifact_path else self.recipe_config.input.params

        return create_partitions(
            inputs=inputs,
            execution_function=self.execution_function,
            parallel_config=self.recipe_config.workflow.parallel,
            artifact_path=artifact_path,
            file_patterns=file_patterns,
        )

    def _create_execution_template(self) -> Container | Any:
        """Create Argo Container template for the execution function.

        Returns:
            Container template or Hera @script decorated function

        Raises:
            ValueError: If execution function has no valid execution type
        """
        func = self.execution_function

        if func.command:
            return self._create_shell_template(func)
        if func.script_module:
            return self._create_script_template(func)
        if func.function:
            return func.function

        msg = f"ExecutionFunction '{func.name}' must have 'command', 'script_module', or 'function'"
        raise ValueError(msg)

    def _create_shell_template(self, func: ExecutionFunction) -> Container:
        """Create a Container template for shell command execution.

        Args:
            func: ExecutionFunction with shell command

        Returns:
            Container template
        """
        command_script = f"""
        set -e
        mkdir -p /output_dir/

        # Read partition manifest from parameter (JSON array of files)
        PARTITION_FILES='{{{{inputs.parameters.partition-manifest}}}}'
        echo "Processing partition with files: $PARTITION_FILES"

        # Parse JSON array and process each file
        FILES=$(echo "$PARTITION_FILES" | sed 's/\\[//g' | sed 's/\\]//g' | sed 's/"//g' | tr ',' '\\n')

        # Process each file
        echo "$FILES" | while IFS= read -r file; do
            # Skip empty lines
            [ -z "$file" ] && continue

            # Trim whitespace
            file=$(echo "$file" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

            echo "Processing file: $file"
            # Extract filename from path
            filename=$(basename "$file")

            # Set environment variables for the command
            export INPUT_FILE="/input_dir/$filename"
            export OUTPUT_FILE="/output_dir/$filename"

            # Run the actual command
            {func.command}
        done
        """

        return Container(
            name=func.name,
            command=["sh", "-c"],
            args=[command_script],
            inputs=[
                Artifact(name="input-dir", path="/input_dir/"),
                Parameter(name="partition-manifest"),
            ],
            outputs=[Artifact(name="output-dir", path="/output_dir/")],
        )

    def _create_script_template(self, func: ExecutionFunction) -> Container:
        """Create a Container template for Python script execution.

        Args:
            func: ExecutionFunction with script module

        Returns:
            Container template
        """
        return Container(
            name=func.name,
            command=["python", "-m", func.script_module],
            args=["--partition-manifest", "{{inputs.parameters.partition-manifest}}"],
            inputs=[
                Artifact(name="input-dir", path="/input_dir/"),
                Parameter(name="partition-manifest"),
            ],
            outputs=[Artifact(name="output-dir", path="/output_dir/")],
        )

    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Container,
        source_task: Task,
        source_artifact_name: str,
        input_artifact_name: str,
    ) -> list[Task]:
        """Create Argo tasks that execute in parallel for a single function.

        All partitions are created as individual tasks. The maximum parallel
        execution limit is controlled by the workflow's parallelism setting,
        which is set at the workflow level. This allows the workflow engine
        to automatically schedule tasks as resources become available.

        For example, with 20 partitions and a parallelism limit of 5:
        - Tasks 1-5 execute immediately
        - Tasks 6-20 are queued
        - As each task completes, the next queued task starts automatically

        Args:
            partitions: List of file partitions
            template: Execution template
            source_task: The task to get input artifacts from
            source_artifact_name: Name of the artifact from the source task
            input_artifact_name: Name to use for the input artifact

        Returns:
            List of Task objects (parallelism controlled by workflow config)
        """
        tasks = []
        func_name = self.execution_function.name

        # Create partition manifests (JSON with list of files)
        partition_manifests = [json.dumps(p.files) for p in partitions]

        # Create tasks - each gets the artifact from the source task
        # and the partition manifest as a parameter
        for partition, manifest in zip(partitions, partition_manifests, strict=True):
            task = Task(
                name=f"{func_name}-partition-{partition.partition_id}",
                template=template,
                arguments=[
                    source_task.get_artifact(source_artifact_name).with_name(
                        input_artifact_name
                    ),
                    Parameter(name="partition-manifest", value=manifest),
                ],
            )
            tasks.append(task)

        logger.info(
            f"Created {len(tasks)} parallel execution tasks for {func_name} "
            f"(parallelism controlled by workflow configuration)"
        )
        return tasks


class SequentialExecutionOrchestrator:
    """Orchestrates sequential (non-parallel) execution for a single function.

    This provides a consistent interface for both parallel and sequential
    execution patterns. Each execution function gets its own orchestrator,
    and DAG dependencies are managed at the workflow level.
    """

    def __init__(
        self,
        recipe_config: RecipeConfig,
        execution_function: ExecutionFunction,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            recipe_config: Recipe configuration
            execution_function: Execution function to run
        """
        self.recipe_config = recipe_config
        self.execution_function = execution_function

    def create_sequential_task(
        self,
        input_artifact_name: str = "input-dir",  # noqa: ARG002
    ) -> Task:
        """Create Argo DAG task for sequential execution.

        Args:
            input_artifact_name: Name of the input artifact (currently unused)

        Returns:
            Task object for this execution function

        Raises:
            ValueError: If execution function is not command-based
        """
        func = self.execution_function

        if not func.command:
            msg = f"Sequential execution only supports 'command' type for ExecutionFunction '{func.name}'"
            raise ValueError(msg)

        template = Container(
            name=func.name,
            command=["sh", "-c"],
            args=[f"mkdir -p /output_dir/ && {func.command}"],
            inputs=[Artifact(name="input-dir", path="/input_dir/")],
            outputs=[Artifact(name="output-dir", path="/output_dir/")],
        )

        return Task(name=func.name, template=template)
