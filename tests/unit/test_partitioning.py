from __future__ import annotations

from pathlib import Path

import pytest

from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import DataOneInput, ParallelConfig, UrlInput
from ogdc_runner.partitioning import _DEFAULT_PARTITION_SIZE, create_partitions

_EF = ExecutionFunction(name="stage-files", command="bash recipe.sh")


def _url_inputs(n: int) -> list[UrlInput]:
    return [
        UrlInput(value=f"https://example.com/file{i}.fgb", type="url") for i in range(n)
    ]


def _dataone_input(urls: list[str]) -> DataOneInput:
    """Build a DataOneInput with pre-resolved objects, bypassing the network call."""
    obj = DataOneInput.__new__(DataOneInput)
    object.__setattr__(obj, "type", "dataone")
    object.__setattr__(obj, "dataset_identifier", "urn:uuid:fake")
    object.__setattr__(obj, "filename", None)
    object.__setattr__(obj, "dataset_pid", "urn:uuid:fake")
    object.__setattr__(obj, "resolved_objects", [{"url": u} for u in urls])
    return obj


def test_partitions_split_correctly():
    """5 files with partition_size=2 → [2, 2, 1]."""
    partitions = create_partitions(
        _url_inputs(5), _EF, ParallelConfig(partition_size=2)
    )
    assert [len(p.files) for p in partitions] == [2, 2, 1]


def test_default_partition_size_is_1000():
    """No parallel_config → default 1 000; 3 files fit in one partition."""
    partitions = create_partitions(_url_inputs(3), _EF, None)
    assert len(partitions) == 1
    assert _DEFAULT_PARTITION_SIZE == 1000


def test_dataone_urls_are_extracted():
    """DataOneInput resolved_objects[*]['url'] are used as file paths."""
    urls = [f"https://cn.dataone.org/f{i}.fgb" for i in range(4)]
    partitions = create_partitions(
        [_dataone_input(urls)], _EF, ParallelConfig(partition_size=2)
    )
    assert len(partitions) == 2
    assert partitions[0].files == urls[:2]


def test_path_inputs():
    paths = [Path(f"/data/f{i}.fgb") for i in range(4)]
    partitions = create_partitions(paths, _EF, ParallelConfig(partition_size=2))
    assert len(partitions) == 2


def test_empty_inputs_raises():
    with pytest.raises(ValueError, match="No files provided"):
        create_partitions([], _EF, None)


def test_dataone_no_resolved_objects_raises():
    with pytest.raises(ValueError, match="No files provided"):
        create_partitions([_dataone_input([])], _EF, None)
