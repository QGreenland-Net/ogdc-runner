from __future__ import annotations

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    Steps,
    Task,
    Workflow,
)

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    submit_workflow,
)
from ogdc_runner.inputs import make_fetch_input_template
from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.parallel import ParallelExecutionOrchestrator, get_max_parallelism
from ogdc_runner.publish import make_publish_template


def make_cmd_template(
    name: str,
    command: str,
) -> Container:
    """
    Creates a command template with an optional custom image.

    Args:
        name: Name of the template
        command: Shell command to execute

    Returns:
        Container template
    """
    return Container(
        name=name,
        command=["sh", "-c"],
        args=[f"mkdir -p /output_dir/ && {command}"],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )


def _create_parallel_workflow(
    recipe_config: RecipeConfig,
    commands: list[str],
) -> None:
    """Create a parallel workflow using DAG structure.

    Args:
        recipe_config: Recipe configuration
        commands: List of shell commands to execute in parallel
    """
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
) -> tuple[ParallelExecutionOrchestrator, Container]:
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
    orchestrator = ParallelExecutionOrchestrator(
        recipe_config=recipe_config,
        execution_function=exec_func,
    )
    template = orchestrator.create_execution_template()
    return orchestrator, template


def _build_parallel_task_dependencies(
    fetch_task: Task,
    orchestrators_with_templates: list[tuple[ParallelExecutionOrchestrator, Container]],
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
    publish_template = make_publish_template(recipe_id=recipe_config.id)

    cmd_templates = [
        make_cmd_template(name=f"run-cmd-{idx}", command=command)
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

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="main",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        parallelism=get_max_parallelism() if parallel_config.enabled else None,
    ) as w:
        if parallel_config.enabled:
            _create_parallel_workflow(recipe_config, commands)
        else:
            _create_sequential_workflow(recipe_config, commands)

    return submit_workflow(w, wait=wait)
