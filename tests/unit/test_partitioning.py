from __future__ import annotations

import pytest

from ogdc_runner.models.parallel_config import ExecutionFunction, FilePartition
from ogdc_runner.models.recipe_config import InputParam, ParallelConfig
from ogdc_runner.partitioning import create_partitions


class TestPartitioning:
    def test_create_partitions_with_input_params(self):
        inputs = [
            InputParam(value=f"https://example.com/file{i}.txt", type="url")
            for i in range(1, 6)
        ]
        execution_func = ExecutionFunction(name="cmd-0", command="echo test")
        parallel_config = ParallelConfig(
            enabled=True,
            partition_strategy="files",
            partition_size=2,
        )

        partitions = create_partitions(
            inputs=inputs,
            execution_function=execution_func,
            parallel_config=parallel_config,
        )

        assert len(partitions) == 3
        assert all(isinstance(p, FilePartition) for p in partitions)
        assert [len(p.files) for p in partitions] == [2, 2, 1]

    def test_create_partitions_without_parallel_config(self):
        inputs = [
            InputParam(value=f"https://example.com/file{i}.txt", type="url")
            for i in range(1, 3)
        ]
        execution_func = ExecutionFunction(name="cmd-0", command="echo test")

        partitions = create_partitions(
            inputs=inputs,
            execution_function=execution_func,
            parallel_config=None,
        )

        assert len(partitions) == 2
        assert all(len(p.files) == 1 for p in partitions)

    def test_create_partitions_empty_inputs_raises_error(self):
        execution_func = ExecutionFunction(name="cmd-0", command="echo test")

        with pytest.raises(ValueError, match="No files provided"):
            create_partitions(
                inputs=[],
                execution_function=execution_func,
                parallel_config=None,
            )
