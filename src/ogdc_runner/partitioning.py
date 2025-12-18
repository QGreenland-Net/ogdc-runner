"""Partitioning strategy for dividing work across parallel tasks.

This module creates independent chunks of work (partitions) by grouping
files based on a configured partition size. Each partition represents
a unit of work that will be executed in parallel.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
)
from ogdc_runner.models.recipe_config import InputParam, ParallelConfig


def create_partitions(
    inputs: list[InputParam] | list[Path],
    execution_function: ExecutionFunction,
    parallel_config: ParallelConfig | None = None,
) -> list[FilePartition]:
    """Create file partitions for parallel execution.

    Groups input files into partitions based on the configured partition size.
    Each partition will be processed independently in parallel.

    Args:
        inputs: List of input parameters or file paths to partition
        execution_function: Execution function to create partitions for
        parallel_config: Optional parallel execution configuration

    Returns:
        List of FilePartition objects, each containing a subset of files

    Raises:
        ValueError: If no files are provided or extracted from inputs
    """
    files = _extract_file_paths(inputs)
    _validate_files(files, execution_function.name)

    partition_size = _get_partition_size(parallel_config)
    partitions = _create_file_partitions(files, execution_function.name, partition_size)

    logger.info(
        f"Created {len(partitions)} partitions for {execution_function.name}"
        f"(partition_size={partition_size}, total_files={len(files)})"
    )

    return partitions


def _extract_file_paths(inputs: list[InputParam] | list[Path]) -> list[str]:
    """Extract file paths or URLs from inputs.

    Handles both InputParam objects (from recipe config) and Path objects
    (from dynamic discovery), converting them to string paths.

    Args:
        inputs: List of input parameters or Path objects

    Returns:
        List of file paths/URLs as strings
    """
    if not inputs:
        return []

    # Type-based dispatch: Path objects vs InputParam objects
    if isinstance(inputs[0], Path):
        return [str(p) for p in inputs]

    return [str(param.value) for param in inputs]


def _validate_files(files: list[str], function_name: str) -> None:
    """Validate that files list is not empty.

    Args:
        files: List of file paths/URLs
        function_name: Name of the execution function (for error messages)

    Raises:
        ValueError: If files list is empty
    """
    if not files:
        msg = f"No files provided for execution function '{function_name}'"
        raise ValueError(msg)


def _get_partition_size(parallel_config: ParallelConfig | None) -> int:
    """Determine the number of files per partition.

    Args:
        parallel_config: Optional parallel configuration

    Returns:
        Number of files per partition (minimum 1)
    """
    if parallel_config and parallel_config.partition_size:
        return max(1, parallel_config.partition_size)
    return 1


def _create_file_partitions(
    files: list[str],
    function_name: str,
    partition_size: int,
) -> list[FilePartition]:
    """Create FilePartition objects by grouping files.

    Args:
        files: List of all file paths/URLs
        function_name: Name of the execution function
        partition_size: Number of files per partition

    Returns:
        List of FilePartition objects
    """
    partitions = []

    for partition_id, start_idx in enumerate(range(0, len(files), partition_size)):
        end_idx = start_idx + partition_size
        partition_files = files[start_idx:end_idx]

        partition = FilePartition(
            partition_id=partition_id,
            files=partition_files,
            execution_function=function_name,
            metadata={"num_files": len(partition_files)},
        )
        partitions.append(partition)

    return partitions
