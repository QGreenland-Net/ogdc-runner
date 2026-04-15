from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    Parameter,
    Steps,
    Task,
)
from hera.workflows.models import ValueFrom, VolumeMount
from loguru import logger

from ogdc_runner.argo import (
    OGDC_WORKFLOW_PVC,
    OgdcWorkflow,
    get_input_pvc_volume_mounts,
    get_input_pvc_volumes,
    submit_workflow,
)
from ogdc_runner.constants import MAX_PARALLEL_LIMIT
from ogdc_runner.inputs import make_fetch_input_template
from ogdc_runner.models.parallel_config import ExecutionFunction, FilePartition
from ogdc_runner.models.recipe_config import PvcMountInput, RecipeConfig
from ogdc_runner.parallel import ParallelExecutionOrchestrator
from ogdc_runner.publish import make_publish_template


class ShellParallelExecutionOrchestrator(ParallelExecutionOrchestrator):
    """Orchestrator for parallel execution of shell-based workflows.

    This class implements the ParallelExecutionOrchestrator interface
    specifically for shell command workflows. It handles:

    1. Creating Container templates with shell command execution
    2. Building partition processing scripts
    3. Creating tasks with shell-specific parameters
    """

    def create_execution_template(self) -> Container | Any:
        """Create Argo Container template for shell command execution.

        Returns:
            Container template configured for parallel partition processing

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
        # Read the shell script template from package resources
        script_file = files("ogdc_runner.scripts").joinpath("partition_processing.sh")
        script_template = script_file.read_text()

        # Replace the user command placeholder
        return script_template.replace("{user_command}", user_command)

    def _create_tasks_from_partitions(
        self,
        partitions: list[FilePartition],
        template: Any,
    ) -> list[Task]:
        """Create Argo tasks from partitions with shell-specific parameters.

        Each partition becomes a separate task. The workflow's parallelism setting
        controls how many tasks execute concurrently, with remaining tasks queued
        and automatically scheduled as resources become available.

        Args:
            partitions: List of file partitions to process
            template: Container template to use for all tasks

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
        template: Any,
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


def make_cmd_template(
    name: str,
    command: str,
    extra_volume_mounts: list[VolumeMount] | None = None,
) -> Container:
    """Create a sequential command template.

    Args:
        name: Name of the template
        command: Shell command to execute
        extra_volume_mounts: Additional volume mounts (e.g. input PVC mounts)

    Returns:
        Container template
    """
    return Container(
        name=name,
        command=["sh", "-c"],
        args=[f"mkdir -p /output_dir/ && {command}"],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
        volume_mounts=extra_volume_mounts or None,
    )


def _make_pvc_listing_template(
    pvc_input: PvcMountInput,
    partition_size: int,
    input_pvc_mounts: list[VolumeMount],
) -> Container:
    """Create a container that enumerates PVC files at runtime and outputs partition JSON.

    The container writes a JSON array of partition objects to /tmp/partitions.json,
    which Argo reads as the `partitions` output parameter for `with_param` fan-out.

    Args:
        pvc_input: The PVC input describing mount path and glob pattern
        partition_size: Number of files per partition
        input_pvc_mounts: Read-only volume mounts for the input PVC(s)

    Returns:
        Container template for the listing step
    """
    full_path = pvc_input.full_path
    glob = pvc_input.glob

    listing_cmd = "\n".join([
        "set -eu",
        f"find '{full_path}' -maxdepth 1 -name '{glob}' -type f | sort > /tmp/files.txt",
        f"test -s /tmp/files.txt || {{ echo 'No files found matching {glob} in {full_path}' >&2; exit 1; }}",
        "python3 - << 'PYEOF'",
        "import json",
        "files = [line.rstrip() for line in open('/tmp/files.txt') if line.strip()]",
        f"ps = {partition_size}",
        "parts = [{'partition_id': i, 'files': files[j:j+ps]} for i, j in enumerate(range(0, len(files), ps))]",
        "json.dump(parts, open('/tmp/partitions.json', 'w'))",
        "import sys; sys.stderr.write('Generated ' + str(len(parts)) + ' partitions\\n')",
        "PYEOF",
    ])

    return Container(
        name="list-pvc-files",
        command=["sh", "-c"],
        args=[listing_cmd],
        outputs=[
            Parameter(
                name="partitions",
                value_from=ValueFrom(path="/tmp/partitions.json"),
            ),
        ],
        volume_mounts=[
            VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow"),
            *input_pvc_mounts,
        ],
    )


def _make_pvc_cmd_template(
    name: str,
    command: str,
    extra_volume_mounts: list[VolumeMount],
) -> Container:
    """Create a Container template for a PVC-parallel command step.

    Uses pvc_partition_processing.sh so that at CMD_INDEX=0 the full PVC path
    from the partition manifest is used as INPUT_FILE directly (no download needed).
    For CMD_INDEX > 0 reads from the previous step's output directory as normal.

    Args:
        name: Template name (e.g. "cmd-0")
        command: Shell command to execute per file
        extra_volume_mounts: Read-only input PVC mounts to add

    Returns:
        Container template for a parallel PVC command step
    """
    script_file = files("ogdc_runner.scripts").joinpath("pvc_partition_processing.sh")
    script_template = script_file.read_text()
    command_script = script_template.replace("{user_command}", command)

    return Container(
        name=name,
        command=["sh", "-c"],
        args=[command_script],
        inputs=[
            Parameter(name="partition-manifest"),
            Parameter(name="recipe-id"),
            Parameter(name="partition-id"),
            Parameter(name="cmd-index"),
        ],
        volume_mounts=[
            VolumeMount(name=OGDC_WORKFLOW_PVC.name, mount_path="/mnt/workflow"),
            *extra_volume_mounts,
        ],
    )


def _create_pvc_parallel_workflow(
    recipe_config: RecipeConfig,
    commands: list[str],
) -> None:
    """Create a parallel workflow for PVC-backed inputs using with_param fan-out.

    Files are enumerated at workflow runtime by a listing step, avoiding the need
    to mount the input PVC on the runner Deployment. The listing step's JSON output
    drives a with_param fan-out so each partition is processed independently.

    Pattern:
        [list-pvc-files] → [cmd-0 (with_param)] → [cmd-1 (with_param)] → …

    Args:
        recipe_config: Recipe configuration
        commands: List of shell commands to execute per partition
    """
    pvc_inputs = [
        p for p in recipe_config.input.params if isinstance(p, PvcMountInput)
    ]
    pvc_input = pvc_inputs[0]  # Single PVC input; mixed inputs are out of scope

    parallel_config = recipe_config.workflow.parallel
    partition_size = max(1, parallel_config.partition_size or 1)

    input_pvc_mounts = get_input_pvc_volume_mounts(recipe_config)

    listing_template = _make_pvc_listing_template(
        pvc_input=pvc_input,
        partition_size=partition_size,
        input_pvc_mounts=input_pvc_mounts,
    )
    cmd_templates = [
        _make_pvc_cmd_template(
            name=f"cmd-{idx}",
            command=cmd,
            extra_volume_mounts=input_pvc_mounts,
        )
        for idx, cmd in enumerate(commands)
    ]

    with DAG(name="main"):
        listing_task = Task(name="list-pvc-files", template=listing_template)

        previous_tasks: list[Task] = [listing_task]
        for idx, cmd_template in enumerate(cmd_templates):
            cmd_task = Task(
                name=f"cmd-{idx}",
                template=cmd_template,
                with_param=listing_task.get_parameter("partitions"),
                arguments=[
                    Parameter(name="partition-manifest", value="{{item.files}}"),
                    Parameter(name="recipe-id", value=recipe_config.id),
                    Parameter(name="partition-id", value="{{item.partition_id}}"),
                    Parameter(name="cmd-index", value=str(idx)),
                ],
            )
            for prev_task in previous_tasks:
                prev_task >> cmd_task
            previous_tasks = [cmd_task]


def _create_parallel_workflow(
    recipe_config: RecipeConfig,
    commands: list[str],
) -> None:
    """Create a parallel workflow using DAG structure.

    Dispatches to PVC-specific logic when the recipe has PVC inputs; otherwise
    uses the URL-based partition fetch pattern.

    Args:
        recipe_config: Recipe configuration
        commands: List of shell commands to execute in parallel
    """
    pvc_inputs = [
        p for p in recipe_config.input.params if isinstance(p, PvcMountInput)
    ]
    if pvc_inputs:
        _create_pvc_parallel_workflow(recipe_config, commands)
        return

    fetch_template = make_fetch_input_template(recipe_config, use_pvc=True)

    # Create orchestrators and templates outside DAG context
    orchestrators_with_templates = [
        _create_orchestrator_with_template(recipe_config, idx, command)
        for idx, command in enumerate(commands)
    ]

    # Create DAG structure
    with DAG(name="main"):
        fetch_task = Task(name="fetch", template=fetch_template)
        _build_parallel_task_dependencies(fetch_task, orchestrators_with_templates)


def _create_orchestrator_with_template(
    recipe_config: RecipeConfig,
    idx: int,
    command: str,
) -> tuple[ShellParallelExecutionOrchestrator, Container]:
    """Create an orchestrator and its template for a single command.

    Args:
        recipe_config: Recipe configuration
        idx: Command index
        command: Shell command

    Returns:
        Tuple of (orchestrator, template)
    """
    exec_func = ExecutionFunction(
        name=f"cmd-{idx}",
        command=command,
    )
    orchestrator = ShellParallelExecutionOrchestrator(
        recipe_config=recipe_config,
        execution_function=exec_func,
    )
    template = orchestrator.create_execution_template()
    return orchestrator, template


def _build_parallel_task_dependencies(
    fetch_task: Task,
    orchestrators_with_templates: list[
        tuple[ShellParallelExecutionOrchestrator, Container]
    ],
) -> None:
    """Build task dependencies for parallel execution.

    Args:
        fetch_task: Initial fetch task
        orchestrators_with_templates: List of (orchestrator, template) tuples
    """
    previous_tasks = [fetch_task]

    for orchestrator, template in orchestrators_with_templates:
        parallel_tasks = orchestrator.create_parallel_tasks(template=template)

        # Connect previous tasks to all parallel tasks
        for prev_task in previous_tasks:
            for parallel_task in parallel_tasks:
                prev_task >> parallel_task

        previous_tasks = parallel_tasks


def _create_sequential_workflow(
    recipe_config: RecipeConfig,
    commands: list[str],
) -> None:
    """Create a sequential workflow using Steps structure.

    Args:
        recipe_config: Recipe configuration
        commands: List of shell commands to execute sequentially
    """
    fetch_template = make_fetch_input_template(recipe_config, use_pvc=False)
    publish_template = make_publish_template(recipe_config=recipe_config)

    input_pvc_mounts = get_input_pvc_volume_mounts(recipe_config)
    cmd_templates = [
        make_cmd_template(
            name=f"run-cmd-{idx}",
            command=command,
            extra_volume_mounts=input_pvc_mounts or None,
        )
        for idx, command in enumerate(commands)
    ]

    with Steps(name="main"):
        step = fetch_template()

        for idx, cmd_template in enumerate(cmd_templates):
            step = cmd_template(
                name=f"step-{idx}",
                arguments=step.get_artifact("output-dir").with_name("input-dir")
                if step
                else None,
            )

        if step:
            publish_template(
                name="publish-data",
                arguments=step.get_artifact("output-dir").with_name("input-dir"),
            )


def make_and_submit_shell_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
) -> str:
    """Create and submit an argo workflow based on a shell recipe.

    Args:
        recipe_config: Recipe configuration containing workflow details
        wait: Whether to wait for workflow completion

    Returns:
        Workflow name
    """
    commands = recipe_config.workflow.get_commands_from_sh_file()  # type: ignore[union-attr]
    parallel_config = recipe_config.workflow.parallel

    # Include OGDC_WORKFLOW_PVC alongside any input PVC volumes, since passing
    # volumes= overrides the class-default set by _apply_global_config.
    workflow_volumes = [OGDC_WORKFLOW_PVC, *get_input_pvc_volumes(recipe_config)]

    with OgdcWorkflow(
        name="shell",
        recipe_config=recipe_config,
        archive_workflow=True,
        entrypoint="main",
        parallelism=MAX_PARALLEL_LIMIT if parallel_config.enabled else None,
        volumes=workflow_volumes,
    ) as w:
        if parallel_config.enabled:
            _create_parallel_workflow(recipe_config, commands)
        else:
            _create_sequential_workflow(recipe_config, commands)

    return submit_workflow(w, wait=wait)
