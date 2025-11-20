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
    """Creates a command template with an optional custom image."""
    template = Container(
        name=name,
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {command}",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template


def make_and_submit_shell_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
) -> str:
    """Create and submit an argo workflow based on a shell recipe."""

    commands = recipe_config.workflow.get_commands_from_sh_file()
    parallel_config = recipe_config.workflow.parallel

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="main",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        parallelism=get_max_parallelism() if parallel_config.enabled else None,
    ) as w:
        fetch_template = make_fetch_input_template(recipe_config)
        publish_template = make_publish_template(recipe_id=recipe_config.id)

        if parallel_config.enabled:
            # PARALLEL MODE: Create orchestrators for each command
            with DAG(name="main"):
                fetch_task = Task(name="fetch", template=fetch_template)

                previous_tasks = [fetch_task]
                for idx, command in enumerate(commands):
                    exec_func = ExecutionFunction(
                        name=f"cmd-{idx}",
                        command=command,
                    )
                    orchestrator = ParallelExecutionOrchestrator(
                        recipe_config=recipe_config,
                        execution_function=exec_func,
                    )

                    # Create parallel tasks for this command
                    parallel_tasks = orchestrator.create_parallel_tasks(
                        input_artifact_name="input-dir",
                    )

                    # Set dependencies
                    for prev_task in previous_tasks:
                        for parallel_task in parallel_tasks:
                            prev_task >> parallel_task

                    previous_tasks = parallel_tasks

                # Publish step
                publish_task = Task(name="publish", template=publish_template)
                for prev_task in previous_tasks:
                    prev_task >> publish_task
        else:
            # SEQUENTIAL MODE: Keep existing Steps-based approach
            with Steps(name="main"):
                step = fetch_template()
                for idx, command in enumerate(commands):
                    cmd_template = make_cmd_template(
                        name=f"run-cmd-{idx}",
                        command=command,
                    )
                    step = cmd_template(
                        name=f"step-{idx}",
                        arguments=step.get_artifact("output-dir").with_name(
                            "input-dir"
                        ),
                    )
                publish_template(
                    name="publish-data",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),
                )

    return submit_workflow(w, wait=wait)
