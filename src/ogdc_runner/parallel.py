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
    Container,
    Parameter,
    Task,
)
from hera.workflows.models import VolumeMount
from loguru import logger

from ogdc_runner.argo import OGDC_WORKFLOW_PVC
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

        Args:
            None

        Returns:
            List of FilePartition objects
        """
        inputs = self.recipe_config.input.params

        return create_partitions(
            inputs=inputs,
            execution_function=self.execution_function,
            parallel_config=self.recipe_config.workflow.parallel,
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
        if func.function:
            return func.function

        msg = f"ExecutionFunction '{func.name}' must have 'command' or 'function'"
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

        # Get parameters
        RECIPE_ID="{{{{inputs.parameters.recipe-id}}}}"
        PARTITION_ID="{{{{inputs.parameters.partition-id}}}}"
        CMD_INDEX="{{{{inputs.parameters.cmd-index}}}}"

        # Setup PVC paths
        # Use the initial input directory for the first command
        # and the previous command's output for subsequent commands
        if [ "$CMD_INDEX" -eq 0 ]; then
            INPUT_DIR="/mnt/workflow/$RECIPE_ID/inputs"
        else
            PREV_CMD_INDEX=$((CMD_INDEX - 1))
            INPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$PREV_CMD_INDEX-partition-$PARTITION_ID"
        fi

        OUTPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$CMD_INDEX-partition-$PARTITION_ID"

        mkdir -p "$OUTPUT_DIR"

        # Read partition manifest from parameter (JSON array of files)
        PARTITION_FILES='{{{{inputs.parameters.partition-manifest}}}}'
        echo "Processing partition with files: $PARTITION_FILES"
        echo "Input directory: $INPUT_DIR"
        echo "Output directory: $OUTPUT_DIR"

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
            export INPUT_FILE="$INPUT_DIR/$filename"
            export OUTPUT_FILE="$OUTPUT_DIR/$filename"

            # Run the actual command
            {func.command}
        done
        """

        return Container(
            name=func.name,
            command=["sh", "-c"],
            args=[command_script],
            inputs=[
                Parameter(name="partition-manifest"),
                Parameter(name="recipe-id"),
                Parameter(name="partition-id"),
                Parameter(name="cmd-index"),
            ],
            volume_mounts=[
                VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow")
            ],
        )

    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Container,
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

        Returns:
            List of Task objects (parallelism controlled by workflow config)
        """
        tasks = []
        func_name = self.execution_function.name

        # Extract cmd index from function name (e.g., "cmd-0" -> "0")
        cmd_index = func_name.split("-")[-1]

        # Create partition manifests (JSON with list of files)
        partition_manifests = [json.dumps(p.files) for p in partitions]

        # Create tasks - each gets the partition manifest, recipe-id, partition-id, and cmd-index as parameters
        # Data is accessed directly from PVC instead of artifacts
        for partition, manifest in zip(partitions, partition_manifests, strict=True):
            task = Task(
                name=f"{func_name}-partition-{partition.partition_id}",
                template=template,
                arguments=[
                    Parameter(name="partition-manifest", value=manifest),
                    Parameter(name="recipe-id", value=self.recipe_config.id),
                    Parameter(name="partition-id", value=str(partition.partition_id)),
                    Parameter(name="cmd-index", value=cmd_index),
                ],
            )
            tasks.append(task)

        logger.info(
            f"Created {len(tasks)} parallel execution tasks for {func_name} "
            f"(parallelism controlled by workflow configuration)"
        )
        return tasks
