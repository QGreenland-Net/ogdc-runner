"""Orchestrator for parallel execution of workflows.

This module manages parallel task execution with automatic scheduling.
When multiple partitions are created, the MAX_PARALLEL_LIMIT controls
how many tasks execute concurrently. The workflow engine automatically
schedules remaining tasks as active tasks complete.

To use parallelism in a workflow, set the 'parallelism' parameter when
creating the Workflow object:

    with Workflow(
        ...,
        parallelism=get_max_parallelism(),
    ) as w:
        ...

This ensures that no more than MAX_PARALLEL_LIMIT tasks run simultaneously,
while remaining tasks are automatically queued and scheduled.
"""

from __future__ import annotations

import json

from hera.workflows import (
    Artifact,
    Container,
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

    This value should be used when creating Workflow objects to control
    the maximum number of tasks that can execute concurrently.

    Returns:
        Maximum number of parallel tasks allowed
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
    ):
        """Initialize the orchestrator.

        Args:
            recipe_config: The recipe configuration
            execution_function: Single execution function to run in parallel
        """
        self.recipe_config = recipe_config
        self.execution_function = execution_function

    def create_parallel_tasks(
        self,
        input_artifact_name: str = "input-dir",
        artifact_path: str | None = None,
        file_patterns: list[str] | None = None,
    ) -> list[Task]:
        """Create Argo DAG tasks for parallel execution of a single function.

        Args:
            input_artifact_name: Name of the input artifact containing fetched data
            artifact_path: Optional path to artifact from previous step (for dynamic discovery)
            file_patterns: Optional file patterns for filtering discovered files

        Returns:
            List of Argo Task objects that will execute in parallel
        """
        logger.info(
            f"Creating parallel execution tasks for {self.execution_function.name}"
        )

        # Step 1: Create partitions for this execution function
        partitions = self._create_partitions(artifact_path, file_patterns)
        logger.info(
            f"Created {len(partitions)} partitions for {self.execution_function.name}"
        )

        # Step 2: Create execution template
        template = self._create_execution_template()

        # Step 3: Create parallel tasks from partitions
        tasks = self._create_tasks_from_partitions(
            partitions, template, input_artifact_name
        )

        return tasks

    def _create_partitions(
        self,
        artifact_path: str | None = None,
        file_patterns: list[str] | None = None,
    ) -> list[FilePartition]:
        """Create file partitions based on configuration.

        Args:
            artifact_path: Optional path to artifact from previous step
            file_patterns: Optional file patterns for filtering

        Returns:
            List of FilePartition objects
        """
        # Use artifact_path if provided (dynamic discovery), otherwise use recipe inputs
        inputs = None if artifact_path else self.recipe_config.input.params

        partitions = create_partitions(
            inputs=inputs,
            execution_function=self.execution_function,
            parallel_config=self.recipe_config.workflow.parallel,
            artifact_path=artifact_path,
            file_patterns=file_patterns,
        )

        return partitions

    def _create_execution_template(self) -> Container:
        """Create Argo Container template for the execution function.

        Returns:
            Container template
        """
        func = self.execution_function

        if func.command:
            # Shell command execution
            template = self._create_shell_template(func)
        elif func.script_module:
            # Python script execution (for viz workflows)
            template = self._create_script_template(func)
        else:
            raise ValueError(
                f"ExecutionFunction {func.name} must have either 'command' or 'script_module'"
            )

        return template

    def _create_shell_template(self, func: ExecutionFunction) -> Container:
        """Create a Container template for shell command execution.

        Args:
            func: ExecutionFunction with shell command

        Returns:
            Container template
        """
        # Build command that processes files from partition manifest
        command_script = f"""
        set -e
        mkdir -p /output_dir/

        # Read partition manifest (JSON with list of files)
        PARTITION_FILES=$(cat /partition/manifest.json)
        echo "Processing partition with files: $PARTITION_FILES"

        # Parse JSON and process each file
        echo "$PARTITION_FILES" | jq -r '.[]' | while read -r file; do
            echo "Processing file: $file"
            # Extract filename from path
            filename=$(basename "$file")

            # Execute the command, replacing input/output placeholders
            INPUT_FILE="/input_dir/$filename"
            OUTPUT_FILE="/output_dir/$filename"

            # Run the actual command
            {func.command}
        done
        """

        template = Container(
            name=func.name,
            command=["sh", "-c"],
            args=[command_script],
            inputs=[
                Artifact(name="input-dir", path="/input_dir/"),
                Artifact(name="partition-manifest", path="/partition/manifest.json"),
            ],
            outputs=[Artifact(name="output-dir", path="/output_dir/")],
            env=func.environment,
        )

        return template

    def _create_script_template(self, func: ExecutionFunction) -> Container:
        """Create a Container template for Python script execution.

        Args:
            func: ExecutionFunction with script module

        Returns:
            Container template
        """
        # For script-based execution (visualization workflows)
        # The script should handle reading the partition manifest
        template = Container(
            name=func.name,
            command=["python", "-m", func.script_module],
            args=["--partition-manifest", "/partition/manifest.json"],
            inputs=[
                Artifact(name="input-dir", path="/input_dir/"),
                Artifact(name="partition-manifest", path="/partition/manifest.json"),
            ],
            outputs=[Artifact(name="output-dir", path="/output_dir/")],
            env=func.environment,
        )

        return template

    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Container,
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
            input_artifact_name: Name of the input artifact

        Returns:
            List of Task objects (parallelism controlled by workflow config)
        """
        tasks = []
        func_name = self.execution_function.name

        # Create partition manifests (JSON with list of files)
        partition_manifests = [json.dumps(p.files) for p in partitions]

        # Create tasks with with-items for parallel execution
        # Each partition becomes a separate task instance
        for partition, manifest in zip(partitions, partition_manifests, strict=True):
            task = Task(
                name=f"{func_name}-partition-{partition.partition_id}",
                template=template,
                arguments={
                    input_artifact_name: Artifact(
                        from_=f"{{{{inputs.artifacts.{input_artifact_name}}}}}"
                    ),
                    "partition-manifest": manifest,
                },
            )
            tasks.append(task)

        logger.info(
            f"Created {len(tasks)} parallel execution tasks for {func_name} "
            f"(parallelism controlled by workflow configuration)"
        )
        return tasks

    def create_collection_task(
        self,
        parallel_tasks: list[Task],  # noqa: ARG002
        output_path: str = "/collected_output/",
    ) -> Task:
        """Create a task that collects outputs from all parallel tasks.

        This is useful for aggregating results before publishing, though
        the current design focuses on map-only without reduce.

        Args:
            parallel_tasks: List of parallel tasks whose outputs to collect
            output_path: Path where collected outputs will be stored

        Returns:
            Task that collects all outputs
        """
        # Create a simple collection template
        collect_template = Container(
            name="collect-outputs",
            command=["sh", "-c"],
            args=[
                f"""
                mkdir -p {output_path}
                # Copy all inputs to output
                # In DAG, we'll pass all parallel task outputs as inputs
                cp -r /inputs/* {output_path}/
                """
            ],
            inputs=[Artifact(name="inputs", path="/inputs/")],
            outputs=[Artifact(name="collected", path=output_path)],
        )

        # Create task (actual artifact wiring happens in workflow DAG construction)
        collect_task = Task(
            name="collect-outputs",
            template=collect_template,
        )

        return collect_task


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
    ):
        """Initialize the orchestrator.

        Args:
            recipe_config: The recipe configuration
            execution_function: Single execution function to run
        """
        self.recipe_config = recipe_config
        self.execution_function = execution_function

    def create_sequential_task(
        self,
        input_artifact_name: str = "input-dir",  # noqa: ARG002
    ) -> Task:
        """Create Argo DAG task for sequential execution.

        Args:
            input_artifact_name: Name of the input artifact

        Returns:
            Task object for this execution function
        """
        func = self.execution_function

        if func.command:
            template = Container(
                name=func.name,
                command=["sh", "-c"],
                args=[f"mkdir -p /output_dir/ && {func.command}"],
                inputs=[Artifact(name="input-dir", path="/input_dir/")],
                outputs=[Artifact(name="output-dir", path="/output_dir/")],
                env=func.environment,
            )

        task = Task(name=func.name, template=template)

        return task
