from __future__ import annotations

from ogdc_runner.recipe import get_recipe_config


def test_get_parallel_recipe_config(test_parallel_shell_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_parallel_shell_workflow_recipe_directory,
    )

    assert config.id == "test-parallel-shell-workflow"
    assert config.workflow.parallel.enabled is True
    assert config.workflow.parallel.partition_strategy == "files"
    assert config.workflow.parallel.partition_size == 2


def test_parallel_recipe_partition_calculation(
    test_parallel_shell_workflow_recipe_directory,
):
    config = get_recipe_config(
        recipe_directory=test_parallel_shell_workflow_recipe_directory,
    )
    num_files = len(config.input.params)
    partition_size = config.workflow.parallel.partition_size
    expected_partitions = (num_files + partition_size - 1) // partition_size

    assert expected_partitions == 3
