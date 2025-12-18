"""Partitioning strategy for dividing work across parallel tasks.

The partitioning strategy creates independent chunks of work (partitions).
This module implements partitioning by grouping files into partitions based on
a configured number of files per partition.

"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
)
from ogdc_runner.models.recipe_config import InputParam, ParallelConfig


def _extract_files_from_inputs(
    inputs: list[InputParam],
) -> list[str]:
    """Extract file paths/URLs from input parameters.

    Args:
        inputs: List of input parameters

    Returns:
        List of file paths or URLs
    """
    return [str(param.value) for param in inputs]


def create_partitions(
    inputs: list[InputParam] | list[Path] | None = None,
    execution_function: ExecutionFunction | None = None,
    parallel_config: ParallelConfig | None = None,
) -> list[FilePartition]:
    """Create partitions by grouping files based on partition_size.

    Args:
        inputs: List of input parameters or file paths
        execution_function: Execution function to create partitions for
        parallel_config: Parallel execution configuration

    Returns:
        List of FilePartition objects

    Raises:
        ValueError: If execution_function is None, no file source provided, or no files found
    """
    if not execution_function:
        msg = "execution_function must be provided"
        raise ValueError(msg)

    # Determine file source: static inputs or dynamic discovery
    if inputs:
        if isinstance(inputs[0], Path):
            files = [str(p) for p in inputs]
            logger.info("Using static file paths from inputs")
        else:
            files = _extract_files_from_inputs(inputs)
            logger.info("Using static files from recipe inputs")
    else:
        msg = "Arg 'inputs' must be provided"
        raise ValueError(msg)

    # Validate that we have files to partition
    if not files:
        msg = f"No files provided for execution function '{execution_function.name}'"
        raise ValueError(msg)

    # Determine number of files per partition
    num_files = (
        parallel_config.partition_size
        if parallel_config and parallel_config.partition_size
        else 1
    )
    num_files = max(1, num_files)  # Ensure at least 1 file per partition

    # Create partitions
    func_name = execution_function.name
    partitions = [
        FilePartition(
            partition_id=partition_id,
            files=files[i : i + num_files],
            execution_function=func_name,
            metadata={"num_files": len(files[i : i + num_files])},
        )
        for partition_id, i in enumerate(range(0, len(files), num_files))
    ]

    logger.info(
        f"Created {len(partitions)} partitions for {func_name} (partition_size={num_files})"
    )

    return partitions
