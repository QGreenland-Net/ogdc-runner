"""Partitioning strategy for dividing work across parallel tasks.

The partitioning strategy creates independent chunks of work (partitions).
This module implements partitioning by grouping files into partitions based on
a configured number of files per partition.

Possible alternative partitioning strategies (not currently implemented):
- By file size: Group files to balance total data size per partition
- By data characteristics: Partition based on spatial/temporal bounds
- By geographic region: Group files by spatial proximity
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path

from loguru import logger

from ogdc_runner.models.parallel_config import (
    ExecutionFunction,
    FilePartition,
    ParallelConfig,
)
from ogdc_runner.models.recipe_config import InputParam


def _discover_files_from_artifact(
    artifact_path: str,
    file_patterns: list[str] | None = None,
) -> list[str]:
    """Discover files from an artifact directory.

    Args:
        artifact_path: Path to artifact directory (e.g., /input_dir/)
        file_patterns: Optional list of glob patterns to filter files

    Returns:
        List of discovered file paths
    """
    discovered_files = []
    artifact_dir = Path(artifact_path)

    if not artifact_dir.exists():
        logger.warning(f"Artifact path does not exist: {artifact_path}")
        return discovered_files

    # Walk the directory to find all files
    for root, _, files in os.walk(artifact_dir):
        for file in files:
            file_path = Path(root) / file
            relative_path = str(file_path.relative_to(artifact_dir))

            # Apply file patterns if specified
            if file_patterns:
                if any(
                    fnmatch.fnmatch(relative_path, pattern) for pattern in file_patterns
                ):
                    discovered_files.append(relative_path)
            else:
                discovered_files.append(relative_path)

    logger.info(f"Discovered {len(discovered_files)} files from {artifact_path}")
    return discovered_files


def _extract_files_from_inputs(
    inputs: list[InputParam],
) -> list[str]:
    """Extract individual file paths/URLs from input parameters.

    Args:
        inputs: List of input parameters

    Returns:
        List of file paths or URLs
    """
    files = []
    for param in inputs:
        # Treat each input as a single file
        files.append(str(param.value))
    return files


def create_partitions(
    inputs: list[InputParam] | list[Path] | None = None,
    execution_function: ExecutionFunction | None = None,
    parallel_config: ParallelConfig | None = None,
    artifact_path: str | None = None,
    file_patterns: list[str] | None = None,
) -> list[FilePartition]:
    """Create partitions by grouping files based on num_files_per_partition.

    Args:
        inputs: List of input parameters or list of file paths
        execution_function: Single execution function to create partitions for
        parallel_config: Parallel execution configuration
        artifact_path: Path to artifact directory from previous step
        file_patterns: Optional list of glob patterns to filter discovered files

    Returns:
        List of FilePartition objects with configured number of files per partition
    """
    if not execution_function:
        msg = "execution_function must be provided"
        raise ValueError(msg)

    # Determine file source: static inputs or dynamic discovery
    if artifact_path:
        # Dynamic: discover files from previous step's output
        files = _discover_files_from_artifact(
            artifact_path,
            file_patterns,
        )
        logger.info(f"Using dynamic file discovery from {artifact_path}")
    elif inputs:
        # Static: use files from recipe config or direct file paths
        if isinstance(inputs[0], Path):
            # List of file paths provided directly
            files = [str(p) for p in inputs]
            logger.info("Using static file paths from inputs")
        else:
            # List of InputParam objects
            files = _extract_files_from_inputs(inputs)
            logger.info("Using static files from recipe inputs")
    else:
        msg = "Either 'inputs' or 'artifact_path' must be provided"
        raise ValueError(msg)

    # Determine number of files per partition
    num_files = parallel_config.num_files_per_partition if parallel_config else 1
    if num_files < 1:
        logger.warning(f"Invalid num_files_per_partition {num_files}, using 1")
        num_files = 1

    # Create partitions for the single execution function
    partitions = []
    func_name = execution_function.name

    for partition_id, i in enumerate(range(0, len(files), num_files)):
        chunk = files[i : i + num_files]
        partition = FilePartition(
            partition_id=partition_id,
            files=chunk,
            execution_function=func_name,
            metadata={
                "num_files": len(chunk),
            },
        )
        partitions.append(partition)

    logger.info(
        f"Created {len(partitions)} partitions for {func_name} (num_files_per_partition={num_files})"
    )

    return partitions
