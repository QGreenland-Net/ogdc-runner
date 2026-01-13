"""Orchestration for parallel execution of workflow tasks.

This module provides the ParallelExecutionOrchestrator class which manages
the creation of parallel Argo workflow tasks. It handles:

1. Creating execution templates (Container templates or Hera @script functions)
2. Partitioning input data into parallel chunks
3. Creating DAG tasks with proper dependencies and parameters

The maximum parallelism is controlled at the workflow level, allowing the
Argo workflow engine to automatically schedule tasks as resources become available.

Example:
    orchestrator = ParallelExecutionOrchestrator(
        recipe_config=recipe_config,
        execution_function=ExecutionFunction(name="cmd-0", command="process.sh"),
    )
    template = orchestrator.create_execution_template()
    tasks = orchestrator.create_parallel_tasks(template)
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
from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
)
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.partitioning import create_partitions


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
            Container template configured for parallel partition processing
        """
        if func.command is None:
            raise ValueError(
                f"ExecutionFunction {func.name} must have a command for shell workflows"
            )
        command_script = self._build_partition_processing_script(func.command)

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

    def _build_partition_processing_script(self, user_command: str) -> str:
        """Build shell script for processing a partition of files.

        The script:
        1. Determines input/output directories based on command index
        2. Processes each file by setting INPUT_FILE and OUTPUT_FILE env vars
        3. Executes the user command for each file

        Args:
            user_command: The shell command to execute for each file

        Returns:
            Complete shell script as a string
        """
        return f"""
set -e

# Get parameters
RECIPE_ID="{{{{inputs.parameters.recipe-id}}}}"
PARTITION_ID="{{{{inputs.parameters.partition-id}}}}"
CMD_INDEX="{{{{inputs.parameters.cmd-index}}}}"

# Determine input directory based on command index
if [ "$CMD_INDEX" -eq 0 ]; then
    export INPUT_DIR="/mnt/workflow/$RECIPE_ID/inputs"
else
    PREV_CMD_INDEX=$((CMD_INDEX - 1))
    export INPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$PREV_CMD_INDEX-partition-$PARTITION_ID"
fi

export OUTPUT_DIR="/mnt/workflow/$RECIPE_ID/cmd-$CMD_INDEX-partition-$PARTITION_ID"
mkdir -p "$OUTPUT_DIR"

# For first command, use partition manifest (original URLs/files)
# For subsequent commands, discover actual files from previous output
if [ "$CMD_INDEX" -eq 0 ]; then
    # Read and parse partition manifest (JSON array of original input files)
    PARTITION_FILES='{{{{inputs.parameters.partition-manifest}}}}'
    echo "Processing partition with files: $PARTITION_FILES"
    echo "Input directory: $INPUT_DIR"
    echo "Output directory: $OUTPUT_DIR"

    # Parse JSON array into individual file paths
    FILES=$(echo "$PARTITION_FILES" | tr -d '[]"' | tr ',' '\\n')

    # Process each file in the partition
    for file in $FILES; do
        [ -z "$file" ] && continue

        file=$(echo "$file" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        filename=$(basename "$file")

        echo "Processing file: $file"
        export INPUT_FILE="$INPUT_DIR/$filename"
        export OUTPUT_FILE="$OUTPUT_DIR/$filename"

        # Execute user command
        {user_command}
    done
else
    # For subsequent commands, process all files from previous output directory
    echo "Discovering files from previous command output: $INPUT_DIR"
    echo "Output directory: $OUTPUT_DIR"

    # Find all files in the input directory (non-recursively)
    if [ ! -d "$INPUT_DIR" ]; then
        echo "ERROR: Input directory does not exist: $INPUT_DIR"
        exit 1
    fi

    # Process each file found in the input directory
    for INPUT_FILE in "$INPUT_DIR"/*; do
        [ -e "$INPUT_FILE" ] || continue  # Skip if no files exist
        [ -f "$INPUT_FILE" ] || continue  # Skip directories

        filename=$(basename "$INPUT_FILE")
        echo "Processing file: $filename"

        export INPUT_FILE
        export OUTPUT_FILE="$OUTPUT_DIR/$filename"

        # Execute user command
        {user_command}
    done
fi
"""

    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Container,
    ) -> list[Task]:
        """Create Argo tasks that execute in parallel for a single function.

        Each partition becomes a separate task. The workflow's parallelism setting
        controls how many tasks execute concurrently, with remaining tasks queued
        and automatically scheduled as resources become available.

        Args:
            partitions: List of file partitions to process
            template: Execution template to use for all tasks

        Returns:
            List of Task objects ready for DAG execution
        """
        func_name = self.execution_function.name
        cmd_index = func_name.split("-")[-1]

        tasks = [
            self._create_partition_task(partition, template, func_name, cmd_index)
            for partition in partitions
        ]

        logger.info(
            f"Created {len(tasks)} parallel tasks for {func_name} "
            f"(parallelism controlled by workflow config)"
        )
        return tasks

    def _create_partition_task(
        self,
        partition: FilePartition,
        template: Container,
        func_name: str,
        cmd_index: str,
    ) -> Task:
        """Create a single task for processing a file partition.

        Args:
            partition: File partition to process
            template: Container template to use
            func_name: Name of the execution function
            cmd_index: Index of the command in the workflow

        Returns:
            Task configured with partition-specific parameters
        """
        partition_manifest = json.dumps(partition.files)

        return Task(
            name=f"{func_name}-partition-{partition.partition_id}",
            template=template,
            arguments=[
                Parameter(name="partition-manifest", value=partition_manifest),
                Parameter(name="recipe-id", value=self.recipe_config.id),
                Parameter(name="partition-id", value=str(partition.partition_id)),
                Parameter(name="cmd-index", value=cmd_index),
            ],
        )
