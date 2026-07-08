from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from typing import Any, TypeAlias, cast

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    Parameter,
    Script,
    Task,
    script,
)
from hera.workflows.models import (
    ResourceRequirements,
    VolumeMount,
)

from ogdc_runner.argo import (
    ARGO_MANAGER,
    OGDC_WORKFLOW_PVC,
    OgdcWorkflow,
    get_input_pvc_volume_mounts,
    get_input_pvc_volumes,
    submit_workflow,
)
from ogdc_runner.constants import MAX_PARALLEL_LIMIT
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.inputs import make_pvc_listing_template
from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import (
    ParallelConfig,
    PvcMountInput,
    RecipeConfig,
    VizWorkflow,
)
from ogdc_runner.partitioning import create_partitions

# ruff: noqa: PLC0415

logger = logging.getLogger(__name__)
WorkflowTask: TypeAlias = Any
WorkflowScript: TypeAlias = Callable[..., WorkflowTask]

# Viz worker container image.  Override via VIZ_WORKFLOW_IMAGE env var.
VIZ_WORKFLOW_IMAGE: str = os.environ.get(
    "VIZ_WORKFLOW_IMAGE",
    "ghcr.io/permafrostdiscoverygateway/viz-workflow:latest",
)
VIZ_WORKFLOW_IMAGE_PULL_POLICY: str = os.environ.get(
    "VIZ_WORKFLOW_IMAGE_PULL_POLICY",
    "IfNotPresent",
)
VIZ_WORKFLOW_SETUP_IMAGE: str = os.environ.get(
    "VIZ_WORKFLOW_SETUP_IMAGE",
    ARGO_MANAGER.config.full_image_path,
)

_DEFAULT_PARTITION_SIZE = int(
    os.environ.get("VIZ_WORKFLOW_DEFAULT_PARTITION_SIZE", "1000")
)

_WORKFLOW_VOLUME_MOUNT = VolumeMount(
    name=OGDC_WORKFLOW_PVC.name,
    mount_path="/mnt/workflow",
)


def _viz_volume_mounts(
    extra_mounts: list[VolumeMount] | None = None,
) -> list[VolumeMount]:
    """Return workflow PVC mount plus any recipe-specific input PVC mounts."""
    mounts = [_WORKFLOW_VOLUME_MOUNT]
    existing_names = {mount.name for mount in mounts}
    for mount in extra_mounts or []:
        if mount.name in existing_names:
            continue
        mounts.append(mount)
        existing_names.add(mount.name)
    return mounts


def _viz_script_kwargs(
    extra_volume_mounts: list[VolumeMount] | None = None,
) -> dict[str, Any]:
    return {
        "image": VIZ_WORKFLOW_IMAGE,
        "image_pull_policy": VIZ_WORKFLOW_IMAGE_PULL_POLICY,
        "command": ["python"],
        "volume_mounts": _viz_volume_mounts(extra_volume_mounts),
    }


def _stage_file_inputs() -> list[Parameter]:
    return [
        Parameter(name="partition-manifest"),
        Parameter(name="recipe-id"),
        Parameter(name="partition-id"),
    ]


def _run_viz_serial_inputs() -> list[Parameter]:
    return [
        Parameter(name="recipe-id"),
        Parameter(name="input-manifest"),
    ]


_DEFAULT_VIZ_WORKFLOW_RESOURCES: dict[str, dict[str, dict[str, str]]] = {
    "stage": {
        "requests": {"cpu": "500m", "memory": "2Gi"},
        "limits": {"cpu": "2", "memory": "6Gi"},
    },
    "raster": {
        "requests": {"cpu": "1", "memory": "4Gi"},
        "limits": {"cpu": "4", "memory": "12Gi"},
    },
    "threedtile": {
        "requests": {"cpu": "2", "memory": "4Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    },
    "discovery": {
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"cpu": "500m", "memory": "1Gi"},
    },
}


def _viz_resource_requirements(resource_name: str) -> ResourceRequirements:
    """Load a viz resource profile from VIZ_WORKFLOW_RESOURCES."""
    raw_resources = os.environ.get("VIZ_WORKFLOW_RESOURCES", "{}")
    try:
        resource_overrides = json.loads(raw_resources)
    except json.JSONDecodeError as e:
        err_msg = "VIZ_WORKFLOW_RESOURCES must be a JSON object"
        raise OgdcInvalidRecipeConfig(err_msg) from e

    if not isinstance(resource_overrides, dict):
        err_msg = "VIZ_WORKFLOW_RESOURCES must be a JSON object"
        raise OgdcInvalidRecipeConfig(err_msg)

    defaults = _DEFAULT_VIZ_WORKFLOW_RESOURCES[resource_name]
    overrides = resource_overrides.get(resource_name, {})
    if not isinstance(overrides, dict):
        raise OgdcInvalidRecipeConfig(
            f"VIZ_WORKFLOW_RESOURCES.{resource_name} must be a JSON object"
        )
    requests = overrides.get("requests", {})
    limits = overrides.get("limits", {})
    if not isinstance(requests, dict) or not isinstance(limits, dict):
        raise OgdcInvalidRecipeConfig(
            f"VIZ_WORKFLOW_RESOURCES.{resource_name}.requests and "
            f"VIZ_WORKFLOW_RESOURCES.{resource_name}.limits must be JSON objects"
        )

    merged_requests = cast(dict[str, Any], {**defaults["requests"], **requests})
    merged_limits = cast(dict[str, Any], {**defaults["limits"], **limits})
    return ResourceRequirements(
        requests=merged_requests,
        limits=merged_limits,
    )


# Resource requirements for each stage.
_STAGE_RESOURCES = _viz_resource_requirements("stage")
_RASTER_RESOURCES = _viz_resource_requirements("raster")
_THREEDTILE_RESOURCES = _viz_resource_requirements("threedtile")
_DISCOVERY_RESOURCES = _viz_resource_requirements("discovery")

# ---------------------------------------------------------------------------
# Stage 1 — Stage input files → max-z vector tiles
# ---------------------------------------------------------------------------


@script(
    name="stage-files",
    inputs=_stage_file_inputs(),
    **_viz_script_kwargs(),
    resources=_STAGE_RESOURCES,
)
def stage_file_parallel() -> None:
    """Stage input files into vector tiles at max z-level.

    Processes one partition of input file URLs.  The partition-manifest
    parameter is a JSON-serialised list of file paths / URLs.
    """
    import json
    import logging
    import os
    import sys
    import urllib.request
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())

    partition_manifest: str = "{{inputs.parameters.partition-manifest}}"
    partition_id: str = "{{inputs.parameters.partition-id}}"
    input_files: list[str] = json.loads(partition_manifest)

    log.info("partition=%s files=%d starting staging", partition_id, len(input_files))

    workflow = WorkflowManager(config)
    dir_input = workflow.config.get("dir_input")
    Path(dir_input).mkdir(parents=True, exist_ok=True)

    for idx, input_file in enumerate(input_files):
        # Download to dir_input if URL; otherwise use the path as-is.
        if input_file.startswith(("http://", "https://")):
            filename = Path(input_file.split("?")[0]).name
            local_path = str(Path(dir_input) / filename)
            log.info(
                "partition=%s [%d/%d] downloading %s -> %s",
                partition_id,
                idx + 1,
                len(input_files),
                input_file,
                local_path,
            )
            urllib.request.urlretrieve(input_file, local_path)
        else:
            local_path = input_file

        log.info(
            "partition=%s [%d/%d] staging %s",
            partition_id,
            idx + 1,
            len(input_files),
            local_path,
        )
        try:
            workflow.stage(local_path)
            log.info(
                "partition=%s [%d/%d] done %s",
                partition_id,
                idx + 1,
                len(input_files),
                local_path,
            )
        except Exception as e:
            log.error(
                "partition=%s [%d/%d] FAILED %s error=%s",
                partition_id,
                idx + 1,
                len(input_files),
                local_path,
                e,
            )
            sys.exit(1)

    log.info("partition=%s staging complete files=%d", partition_id, len(input_files))


# ---------------------------------------------------------------------------
# Discovery 1 — enumerate staged GeoPackage tiles → rasterise + 3D-tile fan-out
# ---------------------------------------------------------------------------


@script(
    name="discover-staged-tiles",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="partition-size", default="1000"),
    ],
    outputs=[
        Artifact(
            name="staged-tiles-manifest",
            path="/tmp/staged_tiles_manifest.json",
        ),
    ],
    **_viz_script_kwargs(),
    resources=_DISCOVERY_RESOURCES,
)
def discover_staged_tiles() -> None:
    """Discover staged GeoPackage tiles at max z-level and emit chunked partition manifests.

    Outputs a JSON array-of-arrays to stdout for Argo withParam fan-out.
    """
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)
    max_z = workflow.config.get_max_z()

    staged_files = workflow.tiles.get_filenames_from_dir("staged", z=max_z)

    log.info("max_z=%d staged_files=%d", max_z, len(staged_files))

    partitions = [
        staged_files[i : i + partition_size]
        for i in range(0, len(staged_files), partition_size)
    ]
    log.info("partitions=%d partition_size=%d", len(partitions), partition_size)

    output_path = Path("{{outputs.artifacts.staged-tiles-manifest.path}}")
    output_path.write_text(json.dumps(partitions))

    # stdout captured by Argo withParam
    print(json.dumps(partitions))


# ---------------------------------------------------------------------------
# Discovery 2 — enumerate parent tiles at a given z-level
# ---------------------------------------------------------------------------


@script(
    name="discover-parent-tiles",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="z-level"),
        Parameter(name="partition-size", default="1000"),
    ],
    outputs=[
        Artifact(
            name="parent-tiles-manifest",
            path="/tmp/parent_tiles_manifest.json",
        ),
    ],
    **_viz_script_kwargs(),
    resources=_DISCOVERY_RESOURCES,
)
def discover_parent_tiles() -> None:
    """Discover parent tile IDs at z-level from child GeoTIFFs at z+1.

    Outputs a JSON array-of-arrays of ``{"z": int, "path": str}`` dicts
    for Argo withParam fan-out.
    """
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())
    z_level = int("{{inputs.parameters.z-level}}")
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)
    child_z = z_level + 1

    child_tiles = workflow.tiles.get_filenames_from_dir("geotiff", z=child_z)

    log.info("z=%d child_z=%d child_tiles=%d", z_level, child_z, len(child_tiles))

    # get_parent_tile returns a morecantile.Tile object; collect unique parents
    # then convert each back to a geotiff file path via path_from_tile.
    parent_tile_objects = set()
    for child_path in child_tiles:
        parent_tile = workflow.tiles.get_parent_tile(child_path)
        if parent_tile is not None:
            parent_tile_objects.add(parent_tile)

    parent_tiles_list = [
        {
            "z": z_level,
            "path": workflow.tiles.path_from_tile(t, base_dir="geotiff"),
        }
        for t in sorted(parent_tile_objects)
    ]
    log.info("z=%d parent_tiles=%d", z_level, len(parent_tiles_list))

    partitions = [
        parent_tiles_list[i : i + partition_size]
        for i in range(0, len(parent_tiles_list), partition_size)
    ]
    log.info("z=%d partitions=%d", z_level, len(partitions))

    output_path = Path("{{outputs.artifacts.parent-tiles-manifest.path}}")
    output_path.write_text(json.dumps(partitions))

    print(json.dumps(partitions))


# ---------------------------------------------------------------------------
# Discovery 3 — enumerate all GeoTIFFs across all z-levels
# ---------------------------------------------------------------------------


@script(
    name="discover-all-geotiffs",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="partition-size", default="1000"),
    ],
    outputs=[
        Artifact(
            name="geotiff-manifest",
            path="/tmp/geotiffs_manifest.json",
        ),
    ],
    **_viz_script_kwargs(),
    resources=_DISCOVERY_RESOURCES,
)
def discover_all_geotiffs() -> None:
    """Discover all GeoTIFF files across every z-level for web-tile fan-out.

    Each partition entry is a dict ``{"z": int, "path": str}`` so the worker
    receives the correct z-level.
    """
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)

    geotiff_entries: list[dict[str, int | str]] = []
    for path in workflow.tiles.get_filenames_from_dir("geotiff"):
        tile = workflow.tiles.dict_from_path(path)
        geotiff_entries.append({"z": int(tile["z"]), "path": path})

    log.info("geotiff_files=%d", len(geotiff_entries))

    partitions = [
        geotiff_entries[i : i + partition_size]
        for i in range(0, len(geotiff_entries), partition_size)
    ]
    log.info("partitions=%d partition_size=%d", len(partitions), partition_size)

    output_path = Path("{{outputs.artifacts.geotiff-manifest.path}}")
    output_path.write_text(json.dumps(partitions))

    print(json.dumps(partitions))


# ---------------------------------------------------------------------------
# Stage 2 — Rasterise staged GeoPackage tiles → GeoTIFF at max z-level
# ---------------------------------------------------------------------------


@script(
    name="rasterize-max-z",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="staged-tiles-manifest"),
    ],
    **_viz_script_kwargs(),
    resources=_RASTER_RESOURCES,
)
def rasterize_max_z_parallel() -> None:
    """Rasterise a partition of staged GeoPackage vector files to GeoTIFF at max z-level."""
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list[str] = json.loads("{{inputs.parameters.staged-tiles-manifest}}")
    log.info("rasterizing tiles=%d", len(manifest))

    for tile_path in manifest:
        log.info("rasterizing %s", tile_path)
        try:
            workflow.rasterize_vector(tile_path)
            log.info("done %s", tile_path)
        except Exception as e:
            log.error("FAILED %s error=%s", tile_path, e)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Stage 3 — Build composite (parent) raster tiles at a given z-level
# ---------------------------------------------------------------------------


@script(
    name="create-composite-z",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="parent-tiles-manifest"),
    ],
    **_viz_script_kwargs(),
    resources=_RASTER_RESOURCES,
)
def create_composite_z_parallel() -> None:
    """Create composite (parent) raster tiles from child GeoTIFFs at z+1."""
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)
    # raster_tiler is None after __init__; initialize it explicitly.
    raster_tiler = workflow.init_raster_tiler()

    manifest: list[dict[str, int | str]] = json.loads(
        "{{inputs.parameters.parent-tiles-manifest}}"
    )
    log.info("composite tiles=%d", len(manifest))

    for item in manifest:
        z_level = int(item["z"])
        parent_path = str(item["path"])
        log.info("z=%d creating composite %s", z_level, parent_path)
        try:
            parent_tile = workflow.tiles.tile_from_path(parent_path)
            raster_tiler.parent_geotiff_from_children(parent_tile)
            log.info("z=%d done %s", z_level, parent_path)
        except Exception as e:
            log.error("z=%d FAILED %s error=%s", z_level, parent_path, e)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Stage 4 — Convert GeoTIFFs → PNG web tiles
# ---------------------------------------------------------------------------


@script(
    name="create-web-tiles",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="geotiff-manifest"),
    ],
    **_viz_script_kwargs(),
    resources=_STAGE_RESOURCES,
)
def create_web_tile_parallel() -> None:
    """Convert GeoTIFFs to PNG web tiles."""
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)
    # raster_tiler is None after __init__; initialize it explicitly.
    raster_tiler = workflow.init_raster_tiler()

    manifest: list[dict[str, int | str] | str] = json.loads(
        "{{inputs.parameters.geotiff-manifest}}"
    )
    log.info("web tiles items=%d", len(manifest))

    for item in manifest:
        if isinstance(item, dict):
            z_level = int(item["z"])
            geotiff_path = str(item["path"])
        else:
            geotiff_path = str(item)
            try:
                z_level = int(workflow.tiles.dict_from_path(geotiff_path)["z"])
            except Exception:
                z_level = -1
        log.info("creating web tile z=%d %s", z_level, geotiff_path)
        try:
            raster_tiler.webtile_from_geotiff(geotiff_path)
            log.info("done z=%d %s", z_level, geotiff_path)
        except Exception as e:
            log.error("FAILED z=%d %s error=%s", z_level, geotiff_path, e)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Stage 5 — Convert staged GeoPackage vectors → Cesium 3D tiles (B3DM/GLB)
# ---------------------------------------------------------------------------


@script(
    name="create-3dtiles",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="staged-tiles-manifest"),
    ],
    **_viz_script_kwargs(),
    resources=_THREEDTILE_RESOURCES,
)
def create_3dtile_parallel() -> None:
    """Convert staged GeoPackage vector tiles to Cesium 3D tiles (B3DM/GLB)."""
    import json
    import logging
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list[str] = json.loads("{{inputs.parameters.staged-tiles-manifest}}")
    log.info("3d tiles items=%d", len(manifest))

    for staged_path in manifest:
        log.info("creating 3d tile %s", staged_path)
        try:
            workflow.staged_to_3dtile(staged_path)
            log.info("done %s", staged_path)
        except Exception as e:
            log.error("FAILED %s error=%s", staged_path, e)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Serial workflow - run all enabled viz stages in one worker
# ---------------------------------------------------------------------------


@script(
    name="run-viz-serial",
    inputs=_run_viz_serial_inputs(),
    **_viz_script_kwargs(),
    resources=_RASTER_RESOURCES,
)
def run_viz_serial() -> None:
    """Run the enabled visualization stages sequentially in one pod."""
    import json
    import logging
    import os
    import sys
    import urllib.request
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    os.chdir(str(config_path.parent))
    config = json.loads(config_path.read_text())
    input_files: list[str] = json.loads("{{inputs.parameters.input-manifest}}")

    workflow = WorkflowManager(config)

    try:
        if workflow.config.is_stager_enabled():
            dir_input = workflow.config.get("dir_input")
            Path(dir_input).mkdir(parents=True, exist_ok=True)

            for idx, input_file in enumerate(input_files):
                if input_file.startswith(("http://", "https://")):
                    filename = Path(input_file.split("?")[0]).name
                    local_path = str(Path(dir_input) / filename)
                    log.info(
                        "serial [%d/%d] downloading %s -> %s",
                        idx + 1,
                        len(input_files),
                        input_file,
                        local_path,
                    )
                    urllib.request.urlretrieve(input_file, local_path)
                else:
                    local_path = input_file

                log.info(
                    "serial [%d/%d] staging %s",
                    idx + 1,
                    len(input_files),
                    local_path,
                )
                workflow.stage(local_path)

        if workflow.config.is_raster_enabled():
            workflow.run_rasterization()

        if workflow.config.is_3dtiles_enabled():
            workflow.run_3d_tiling()

        if workflow.config.is_generate_wmtsCapabilities_enabled() and (
            workflow.config.is_stager_enabled()
            or workflow.config.is_raster_enabled()
            or workflow.config.is_web_tiles_enabled()
        ):
            workflow.generate_wmts_capabilities()
    except Exception as e:
        log.error("serial viz workflow FAILED error=%s", e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Workflow entry point
# ---------------------------------------------------------------------------


def _script_source(task_func: Callable[..., Any]) -> Callable[..., Any]:
    """Return the original Python function behind a Hera @script wrapper."""
    source = getattr(task_func, "wrapped_function", task_func)
    return cast(Callable[..., Any], source)


def _make_stage_file_template(extra_volume_mounts: list[VolumeMount]) -> Script:
    return _make_viz_script_template(
        name="stage-files",
        task_func=stage_file_parallel,
        inputs=_stage_file_inputs(),
        resources=_STAGE_RESOURCES,
        extra_volume_mounts=extra_volume_mounts,
    )


def _make_run_viz_serial_template(extra_volume_mounts: list[VolumeMount]) -> Script:
    return _make_viz_script_template(
        name="run-viz-serial",
        task_func=run_viz_serial,
        inputs=_run_viz_serial_inputs(),
        resources=_RASTER_RESOURCES,
        extra_volume_mounts=extra_volume_mounts,
    )


def _make_viz_script_template(
    *,
    name: str,
    task_func: Callable[..., Any],
    inputs: list[Parameter],
    resources: ResourceRequirements,
    extra_volume_mounts: list[VolumeMount],
) -> Script:
    return Script(
        name=name,
        source=_script_source(task_func),
        inputs=inputs,
        resources=resources,
        **_viz_script_kwargs(extra_volume_mounts),
    )


def _get_viz_pvc_inputs(recipe_config: RecipeConfig) -> list[PvcMountInput]:
    return [p for p in recipe_config.input.params if isinstance(p, PvcMountInput)]


def _link_after(deps: list[WorkflowTask], task: WorkflowTask) -> list[WorkflowTask]:
    for dep in deps:
        dep >> task
    return [task]


def _serial_input_manifest(recipe_config: RecipeConfig) -> str:
    partitions = create_partitions(
        inputs=recipe_config.input.params,
        execution_function=ExecutionFunction(
            name="run-viz-serial", function=run_viz_serial
        ),
        parallel_config=None,
    )
    input_files = [
        input_file for partition in partitions for input_file in partition.files
    ]
    return json.dumps(input_files)


def _create_serial_dag(
    recipe_config: RecipeConfig,
    config_task: Task,
    run_viz_serial_template: WorkflowScript = run_viz_serial,
) -> None:
    pvc_inputs = _get_viz_pvc_inputs(recipe_config)
    if pvc_inputs:
        listing_task = Task(
            name="list-pvc-files",
            template=make_pvc_listing_template(
                pvc_inputs=pvc_inputs,
                partition_size=_DEFAULT_PARTITION_SIZE,
                input_pvc_mounts=get_input_pvc_volume_mounts(recipe_config),
                image=VIZ_WORKFLOW_SETUP_IMAGE,
            ),
        )
        _link_after([config_task], listing_task)
        pvc_serial_task: WorkflowTask = run_viz_serial_template(
            arguments={
                "recipe-id": recipe_config.id,
                "input-manifest": listing_task.get_parameter("files"),
            },
        )
        _link_after([listing_task], pvc_serial_task)
        return

    serial_task: WorkflowTask = run_viz_serial_template(
        arguments={
            "recipe-id": recipe_config.id,
            "input-manifest": _serial_input_manifest(recipe_config),
        },
    )
    _link_after([config_task], serial_task)


def _create_stage_task(
    *,
    recipe_config: RecipeConfig,
    parallel_cfg: ParallelConfig,
    deps: list[WorkflowTask],
    stage_template: WorkflowScript = stage_file_parallel,
) -> list[WorkflowTask]:
    pvc_inputs = _get_viz_pvc_inputs(recipe_config)
    if pvc_inputs:
        input_pvc_mounts = get_input_pvc_volume_mounts(recipe_config)
        listing_task = Task(
            name="list-pvc-files",
            template=make_pvc_listing_template(
                pvc_inputs=pvc_inputs,
                partition_size=parallel_cfg.partition_size or _DEFAULT_PARTITION_SIZE,
                input_pvc_mounts=input_pvc_mounts,
                image=VIZ_WORKFLOW_SETUP_IMAGE,
            ),
        )
        _link_after(deps, listing_task)

        pvc_stage_task: WorkflowTask = stage_template(
            arguments={
                "recipe-id": recipe_config.id,
                "partition-manifest": "{{item.files}}",
                "partition-id": "{{item.partition_id}}",
            },
            with_param=listing_task.get_parameter("partitions"),
        )
        listing_task >> pvc_stage_task
        return [pvc_stage_task]

    partitions = create_partitions(
        inputs=recipe_config.input.params,
        execution_function=ExecutionFunction(
            name="stage-files", function=stage_file_parallel
        ),
        parallel_config=parallel_cfg,
    )
    stage_partitions_json = json.dumps(
        [{"index": i, "files": p.files} for i, p in enumerate(partitions)]
    )
    logger.info(
        "stage1 partitions=%d total_files=%d",
        len(partitions),
        sum(len(p.files) for p in partitions),
    )

    stage_task: WorkflowTask = stage_template(
        arguments={
            "recipe-id": recipe_config.id,
            "partition-manifest": "{{item.files}}",
            "partition-id": "{{item.index}}",
        },
        with_param=stage_partitions_json,
    )
    return _link_after(deps, stage_task)


def _create_staged_tiles_discovery(
    *,
    recipe_config: RecipeConfig,
    partition_size: int,
    deps: list[WorkflowTask],
) -> WorkflowTask:
    staged_tiles_discovery_task: WorkflowTask = discover_staged_tiles(
        arguments={
            "recipe-id": recipe_config.id,
            "partition-size": str(partition_size),
        }
    )
    _link_after(deps, staged_tiles_discovery_task)
    return staged_tiles_discovery_task


def _create_pvc_listing_task(
    *,
    recipe_config: RecipeConfig,
    partition_size: int,
    deps: list[WorkflowTask],
) -> WorkflowTask | None:
    pvc_inputs = _get_viz_pvc_inputs(recipe_config)
    if not pvc_inputs:
        return None

    listing_task = Task(
        name="list-pvc-files",
        template=make_pvc_listing_template(
            pvc_inputs=pvc_inputs,
            partition_size=partition_size,
            input_pvc_mounts=get_input_pvc_volume_mounts(recipe_config),
            image=VIZ_WORKFLOW_SETUP_IMAGE,
        ),
    )
    _link_after(deps, listing_task)
    return listing_task


def _create_rasterize_task(
    *,
    recipe_config: RecipeConfig,
    staged_tiles_with_param: Any,
    deps: list[WorkflowTask],
    staged_tiles_manifest_argument: str = "{{item}}",
    rasterize_template: WorkflowScript = rasterize_max_z_parallel,
) -> WorkflowTask:
    rasterize_task: WorkflowTask = rasterize_template(
        arguments={
            "recipe-id": recipe_config.id,
            "staged-tiles-manifest": staged_tiles_manifest_argument,
        },
        with_param=staged_tiles_with_param,
    )
    _link_after(deps, rasterize_task)
    return rasterize_task


def _create_3dtile_task(
    *,
    recipe_config: RecipeConfig,
    staged_tiles_with_param: Any,
    deps: list[WorkflowTask],
    staged_tiles_manifest_argument: str = "{{item}}",
    threedtile_template: WorkflowScript = create_3dtile_parallel,
) -> None:
    threedtile_task: WorkflowTask = threedtile_template(
        arguments={
            "recipe-id": recipe_config.id,
            "staged-tiles-manifest": staged_tiles_manifest_argument,
        },
        with_param=staged_tiles_with_param,
    )
    _link_after(deps, threedtile_task)


def _create_composite_tasks(
    *,
    recipe_config: RecipeConfig,
    partition_size: int,
    composite_z_levels: list[int],
    deps: list[WorkflowTask],
    rasterize_task: WorkflowTask | None,
    staged_tiles_discovery_task: WorkflowTask | None,
) -> WorkflowTask | None:
    prev_task = rasterize_task or staged_tiles_discovery_task

    for z in composite_z_levels:
        discover_parents_task: WorkflowTask = discover_parent_tiles(
            name=f"discover-parents-z-{z}",
            arguments={
                "recipe-id": recipe_config.id,
                "z-level": str(z),
                "partition-size": str(partition_size),
            },
        )
        create_composites_task: WorkflowTask = create_composite_z_parallel(
            name=f"create-composites-z-{z}",
            arguments={
                "recipe-id": recipe_config.id,
                "parent-tiles-manifest": "{{item}}",
            },
            with_param=discover_parents_task.get_result_as("result"),
        )

        if prev_task is not None:
            _link_after([prev_task], discover_parents_task)
        else:
            _link_after(deps, discover_parents_task)

        _link_after([discover_parents_task], create_composites_task)
        prev_task = create_composites_task

    return prev_task


def _create_web_tile_tasks(
    *,
    recipe_config: RecipeConfig,
    partition_size: int,
    deps: list[WorkflowTask],
    raster_anchor: WorkflowTask | None,
    geotiff_with_param: Any | None = None,
    geotiff_manifest_argument: str = "{{item}}",
    web_tile_template: WorkflowScript = create_web_tile_parallel,
) -> None:
    if geotiff_with_param is not None:
        direct_web_tile_task: WorkflowTask = web_tile_template(
            arguments={
                "recipe-id": recipe_config.id,
                "geotiff-manifest": geotiff_manifest_argument,
            },
            with_param=geotiff_with_param,
        )
        _link_after(deps, direct_web_tile_task)
        return

    discover_geotiffs_task: WorkflowTask = discover_all_geotiffs(
        arguments={
            "recipe-id": recipe_config.id,
            "partition-size": str(partition_size),
        }
    )

    if raster_anchor is not None:
        _link_after([raster_anchor], discover_geotiffs_task)
    else:
        _link_after(deps, discover_geotiffs_task)

    web_tile_task: WorkflowTask = web_tile_template(
        arguments={
            "recipe-id": recipe_config.id,
            "geotiff-manifest": "{{item}}",
        },
        with_param=discover_geotiffs_task.get_result_as("result"),
    )
    _link_after([discover_geotiffs_task], web_tile_task)


def _relative_workflow_dir(base: str, path: str | None) -> str | None:
    if not path:
        return None
    stripped = path.rstrip("/")
    if not stripped or stripped.startswith("/"):
        return None
    return f"{base}/{stripped}"


def _setup_directories(
    *,
    base: str,
    workflow_config: dict[str, Any],
    enable_stager: bool,
    enable_raster: bool,
    enable_web_tiles: bool,
    enable_3dtiles: bool,
) -> set[str]:
    dirs = {base}
    maybe_dirs = []

    if enable_stager:
        maybe_dirs.extend(
            [
                workflow_config.get("dir_input"),
                workflow_config.get("dir_staged"),
            ]
        )
    if enable_raster:
        maybe_dirs.append(workflow_config.get("dir_geotiff"))
    if enable_web_tiles:
        maybe_dirs.append(workflow_config.get("dir_web_tiles"))
    if enable_3dtiles:
        maybe_dirs.append(workflow_config.get("dir_3dtiles", "output/3dtiles"))

    for configured_dir in maybe_dirs:
        if not isinstance(configured_dir, str):
            continue
        workflow_dir = _relative_workflow_dir(base, configured_dir)
        if workflow_dir is not None:
            dirs.add(workflow_dir)

    return dirs


def make_and_submit_viz_workflow(
    recipe_config: RecipeConfig,
    wait: bool = False,
) -> str:
    """Create and submit a parallel Argo workflow for viz processing on K8s.

    Implements a 5-stage parallel pipeline with strict z-level ordering:

    Stage 1  — Stage input files → max-z GeoPackage tiles      (with_param fan-out)
    Stage 2  — Rasterise max-z GeoPackage tiles → GeoTIFFs     (discovery → with_param)
    Stage 3  — Build composite parent tiles from max-z-1→min-z (sequential z, with_param tiles)
    Stage 4  — Convert GeoTIFFs → PNG web tiles                (discovery → with_param)
    Stage 5  — Convert staged vectors → Cesium 3D tiles        (discovery reuse → with_param)

    All stages use Argo ``withParam`` for dynamic fan-out so the number of DAG
    nodes stays bounded regardless of dataset size.

    For extreme-scale datasets (> ~10 M files) Stage 1 currently embeds the
    partition manifest in the workflow spec.  A future hardening step writes
    manifests to the workflow PVC during setup and passes only partition
    indices via withParam to stay within etcd size limits.

    Args:
        recipe_config: Recipe configuration with ``workflow.type == "visualization"``.
        wait: If ``True``, block until the workflow completes.  Defaults to
            ``False`` for async submission.

    Returns:
        The Argo workflow name (e.g. ``"my-recipe-visualization-xyz12"``).

    Raises:
        OgdcInvalidRecipeConfig: If the workflow type is not ``"visualization"``.
    """
    if recipe_config.workflow.type != "visualization":
        raise OgdcInvalidRecipeConfig(
            f"Expected recipe configuration with workflow type `visualization`. "
            f"Got: {recipe_config.workflow.type}"
        )
    if not isinstance(recipe_config.workflow, VizWorkflow):
        raise OgdcInvalidRecipeConfig(
            f"Expected VizWorkflow configuration. Got: {recipe_config.workflow}"
        )

    workflow_config_model = recipe_config.workflow
    parallel_cfg = workflow_config_model.parallel
    parallelism: int | None = (
        (parallel_cfg.max_parallelism or MAX_PARALLEL_LIMIT)
        if parallel_cfg.enabled
        else None
    )
    input_pvc_mounts = get_input_pvc_volume_mounts(recipe_config)
    workflow_volumes = [OGDC_WORKFLOW_PVC, *get_input_pvc_volumes(recipe_config)]
    stage_template: WorkflowScript = cast(
        WorkflowScript,
        _make_stage_file_template(input_pvc_mounts)
        if input_pvc_mounts
        else stage_file_parallel,
    )
    run_viz_serial_template: WorkflowScript = cast(
        WorkflowScript,
        _make_run_viz_serial_template(input_pvc_mounts)
        if input_pvc_mounts
        else run_viz_serial,
    )
    rasterize_template: WorkflowScript = cast(
        WorkflowScript,
        _make_viz_script_template(
            name="rasterize-max-z",
            task_func=rasterize_max_z_parallel,
            inputs=[
                Parameter(name="recipe-id"),
                Parameter(name="staged-tiles-manifest"),
            ],
            resources=_RASTER_RESOURCES,
            extra_volume_mounts=input_pvc_mounts,
        )
        if input_pvc_mounts
        else rasterize_max_z_parallel,
    )
    threedtile_template: WorkflowScript = cast(
        WorkflowScript,
        _make_viz_script_template(
            name="create-3dtiles",
            task_func=create_3dtile_parallel,
            inputs=[
                Parameter(name="recipe-id"),
                Parameter(name="staged-tiles-manifest"),
            ],
            resources=_THREEDTILE_RESOURCES,
            extra_volume_mounts=input_pvc_mounts,
        )
        if input_pvc_mounts
        else create_3dtile_parallel,
    )
    web_tile_template: WorkflowScript = cast(
        WorkflowScript,
        _make_viz_script_template(
            name="create-web-tiles",
            task_func=create_web_tile_parallel,
            inputs=[
                Parameter(name="recipe-id"),
                Parameter(name="geotiff-manifest"),
            ],
            resources=_STAGE_RESOURCES,
            extra_volume_mounts=input_pvc_mounts,
        )
        if input_pvc_mounts
        else create_web_tile_parallel,
    )

    with OgdcWorkflow(
        name="visualization",
        recipe_config=recipe_config,
        archive_workflow=True,
        entrypoint="main",
        volumes=workflow_volumes,
        parallelism=parallelism,
        annotations={
            "workflows.argoproj.io/description": (
                "Parallel Viz Workflow (stage→rasterise→composite→web-tile→3d-tile)"
            ),
        },
        labels={
            "workflows.argoproj.io/archive-strategy": "false",
        },
    ) as w:
        config_content = workflow_config_model.get_config_file_json()

        partition_size: int = parallel_cfg.partition_size or _DEFAULT_PARTITION_SIZE

        # Parse workflow config for z-range and feature flags.
        workflow_config: dict[str, Any] = json.loads(config_content)
        z_range: list[int] = workflow_config.get("z_range", [0, 12])
        min_z, max_z = z_range[0], z_range[1]

        enable_stager: bool = workflow_config.get("enable_stager", True)
        enable_raster: bool = workflow_config.get("enable_raster", True)
        enable_raster_parents: bool = workflow_config.get("enable_raster_parents", True)
        enable_web_tiles: bool = workflow_config.get("enable_web_tiles", True)
        enable_3dtiles: bool = workflow_config.get("enable_3dtiles", True)

        # z = max_z-1 down to min_z (inclusive), processed strictly in order.
        composite_z_levels: list[int] = list(range(max_z - 1, min_z - 1, -1))

        # ----------------------------------------------------------------
        # Setup container — write config.json and create output directories
        # Only create workflow-local directories for enabled stages. Absolute
        # paths may refer to mounted input PVCs and must not be pre-created.
        # ----------------------------------------------------------------
        _base = f"/mnt/workflow/{recipe_config.id}"
        _dirs = _setup_directories(
            base=_base,
            workflow_config=workflow_config,
            enable_stager=enable_stager,
            enable_raster=enable_raster,
            enable_web_tiles=enable_web_tiles,
            enable_3dtiles=enable_3dtiles,
        )
        _mkdir_cmds = " && \\\n".join(f"mkdir -p {d}" for d in sorted(_dirs))

        setup_template = Container(
            name="stage-viz-config",
            image=VIZ_WORKFLOW_SETUP_IMAGE,
            command=["sh", "-c"],
            args=[
                f"""mkdir -p {_base} && \
{_mkdir_cmds} && \
cat > {_base}/config.json << 'EOF'
{config_content}
EOF"""
            ],
            volume_mounts=[
                VolumeMount(
                    name=OGDC_WORKFLOW_PVC.name,
                    mount_path="/mnt/workflow/",
                )
            ],
        )

        with DAG(name="main"):
            config_task = Task(
                name="setup-config",
                template=setup_template,
            )

            if not parallel_cfg.enabled:
                _create_serial_dag(
                    recipe_config,
                    config_task,
                    run_viz_serial_template=run_viz_serial_template,
                )
            else:
                current_deps: list[WorkflowTask] = [config_task]
                pvc_listing_task: WorkflowTask | None = None

                if enable_stager:
                    current_deps = _create_stage_task(
                        recipe_config=recipe_config,
                        parallel_cfg=parallel_cfg,
                        deps=current_deps,
                        stage_template=stage_template,
                    )
                else:
                    pvc_listing_task = _create_pvc_listing_task(
                        recipe_config=recipe_config,
                        partition_size=partition_size,
                        deps=current_deps,
                    )

                staged_tiles_source_tasks: list[WorkflowTask] = []
                staged_tiles_with_param: Any | None = None
                staged_tiles_manifest_argument = "{{item}}"
                if pvc_listing_task is not None:
                    staged_tiles_source_tasks = [pvc_listing_task]
                    staged_tiles_with_param = pvc_listing_task.get_parameter(
                        "partitions"
                    )
                    staged_tiles_manifest_argument = "{{item.files}}"
                if (
                    enable_raster or enable_3dtiles
                ) and staged_tiles_with_param is None:
                    # Without a stager, PVC manifests can directly seed raster and
                    # 3D tile work. URL/DataONE inputs still require discovery from
                    # staged workflow output.
                    staged_tiles_discovery_task = _create_staged_tiles_discovery(
                        recipe_config=recipe_config,
                        partition_size=partition_size,
                        deps=current_deps,
                    )
                    staged_tiles_source_tasks = [staged_tiles_discovery_task]
                    staged_tiles_with_param = staged_tiles_discovery_task.get_result_as(
                        "result"
                    )

                rasterize_task = None
                if enable_raster and staged_tiles_with_param is not None:
                    rasterize_task = _create_rasterize_task(
                        recipe_config=recipe_config,
                        staged_tiles_with_param=staged_tiles_with_param,
                        deps=staged_tiles_source_tasks,
                        staged_tiles_manifest_argument=staged_tiles_manifest_argument,
                        rasterize_template=rasterize_template,
                    )

                # Stage 5 uses the same staged vector manifest as rasterization,
                # so attach it next to Step 1 discovery instead of as a separate
                # tail block.
                if enable_3dtiles and staged_tiles_with_param is not None:
                    _create_3dtile_task(
                        recipe_config=recipe_config,
                        staged_tiles_with_param=staged_tiles_with_param,
                        deps=staged_tiles_source_tasks,
                        staged_tiles_manifest_argument=staged_tiles_manifest_argument,
                        threedtile_template=threedtile_template,
                    )

                final_composite_task = None
                # PVC geotiff inputs can seed web tile generation directly.
                # Parent-raster discovery still expects workflow-local geotiffs,
                # so do not put that discovery in front of direct PVC web tiles.
                can_create_raster_parents = not (
                    pvc_listing_task is not None and not enable_raster
                )
                if enable_raster_parents and can_create_raster_parents:
                    final_composite_task = _create_composite_tasks(
                        recipe_config=recipe_config,
                        partition_size=partition_size,
                        composite_z_levels=composite_z_levels,
                        deps=current_deps,
                        rasterize_task=rasterize_task,
                        staged_tiles_discovery_task=staged_tiles_source_tasks[0]
                        if staged_tiles_source_tasks
                        else None,
                    )

                if enable_web_tiles:
                    raster_anchor = final_composite_task or rasterize_task
                    geotiff_with_param = (
                        pvc_listing_task.get_parameter("partitions")
                        if pvc_listing_task is not None and raster_anchor is None
                        else None
                    )
                    geotiff_manifest_argument = (
                        "{{item.files}}"
                        if geotiff_with_param is not None
                        else "{{item}}"
                    )
                    _create_web_tile_tasks(
                        recipe_config=recipe_config,
                        partition_size=partition_size,
                        deps=current_deps,
                        raster_anchor=raster_anchor,
                        geotiff_with_param=geotiff_with_param,
                        geotiff_manifest_argument=geotiff_manifest_argument,
                        web_tile_template=web_tile_template,
                    )

        workflow_name: str = submit_workflow(w, wait=wait)
    return workflow_name
