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
            # PARALLEL MODE: Create orchestrators and templates OUTSIDE the DAG
            orchestrators_with_templates = []
            for idx, command in enumerate(commands):
                exec_func = ExecutionFunction(
                    name=f"cmd-{idx}",
                    command=command,
                )
                orchestrator = ParallelExecutionOrchestrator(
                    recipe_config=recipe_config,
                    execution_function=exec_func,
                )
                # Create template outside DAG context
                template = orchestrator.create_execution_template()
                orchestrators_with_templates.append((orchestrator, template))

            # Now create the DAG with tasks
            with DAG(name="main"):
                fetch_task = Task(name="fetch", template=fetch_template)

                previous_tasks = [fetch_task]
                for orchestrator, template in orchestrators_with_templates:
                    # Get the output artifact from the previous task
                    # For the first iteration, it's from fetch_task
                    source_task = previous_tasks[0]

                    # Create parallel tasks for this command
                    parallel_tasks = orchestrator.create_parallel_tasks(
                        template=template,
                        source_task=source_task,
                        source_artifact_name="output-dir",
                        input_artifact_name="input-dir",
                    )

                    # Set dependencies
                    for prev_task in previous_tasks:
                        for parallel_task in parallel_tasks:
                            prev_task >> parallel_task

                    previous_tasks = parallel_tasks

        else:
            # SEQUENTIAL MODE: Create templates outside Steps context
            cmd_templates = []
            for idx, command in enumerate(commands):
                cmd_template = make_cmd_template(
                    name=f"run-cmd-{idx}",
                    command=command,
                )
                cmd_templates.append(cmd_template)

            # Now create Steps with the pre-created templates
            with Steps(name="main"):
                step = fetch_template()
                for idx, cmd_template in enumerate(cmd_templates):
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
