from __future__ import annotations

import json
import logging
import os

from hera.workflows import (
    DAG,
    Artifact,
    Container,
    Parameter,
    Task,
    script,
)
from hera.workflows.models import (
    ResourceRequirements,
    RetryStrategy,
    VolumeMount,
)

from ogdc_runner.argo import (
    OGDC_WORKFLOW_PVC,
    OgdcWorkflow,
    submit_workflow,
)
from ogdc_runner.constants import MAX_PARALLEL_LIMIT
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.partitioning import create_partitions

# ruff: noqa: PLC0415

logger = logging.getLogger(__name__)

# Set up constants

# Viz worker container image.  Override via VIZ_WORKFLOW_IMAGE env var.
VIZ_WORKFLOW_IMAGE: str = os.environ.get(
    "VIZ_WORKFLOW_IMAGE",
    "ghcr.io/permafrostdiscoverygateway/viz-workflow:latest",
)

_DEFAULT_PARTITION_SIZE = 1000

_WORKFLOW_VOLUME_MOUNT = VolumeMount(
    name=OGDC_WORKFLOW_PVC.name,
    mount_path="/mnt/workflow",
)

# Shared kwargs applied to every viz @script decorator.
_VIZ_SCRIPT_KWARGS: dict = {
    "image": VIZ_WORKFLOW_IMAGE,
    "image_pull_policy": "IfNotPresent",
    "command": ["python"],
    "volume_mounts": [_WORKFLOW_VOLUME_MOUNT],
}

# Resource requirements for each stage.
_STAGE_RESOURCES = ResourceRequirements(
    requests={"cpu": "500m", "memory": "2Gi"},
    limits={"cpu": "2", "memory": "6Gi"},
)
_RASTER_RESOURCES = ResourceRequirements(
    requests={"cpu": "1", "memory": "4Gi"},
    limits={"cpu": "4", "memory": "12Gi"},
)
_THREEDTILE_RESOURCES = ResourceRequirements(
    requests={"cpu": "2", "memory": "4Gi"},
    limits={"cpu": "4", "memory": "8Gi"},
)
_DISCOVERY_RESOURCES = ResourceRequirements(
    requests={"cpu": "250m", "memory": "512Mi"},
    limits={"cpu": "500m", "memory": "1Gi"},
)
_WORKER_RETRY = RetryStrategy(
    limit=3,
    retry_policy="OnTransientError",
)

# ---------------------------------------------------------------------------
# Stage 1 — Stage input files → max-z vector tiles
# ---------------------------------------------------------------------------

@script(
    name="stage-files",
    inputs=[
        Parameter(name="partition-manifest"),
        Parameter(name="recipe-id"),
        Parameter(name="partition-id"),
    ],
    **_VIZ_SCRIPT_KWARGS,
    resources=_STAGE_RESOURCES,
    retry_strategy=_WORKER_RETRY,
)
def stage_file_parallel() -> None:
    """Stage input files into vector tiles at max z-level.

    Processes one partition of input file URLs.  The partition-manifest
    parameter is a JSON-serialised list of file paths / URLs.
    """
    import json
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())

    partition_manifest: str = "{{inputs.parameters.partition-manifest}}"
    partition_id: str = "{{inputs.parameters.partition-id}}"
    input_files: list = json.loads(partition_manifest)

    log.info("partition=%s files=%d starting staging", partition_id, len(input_files))

    workflow = WorkflowManager(config)

    for idx, input_file in enumerate(input_files):
        log.info(
            "partition=%s [%d/%d] staging %s",
            partition_id, idx + 1, len(input_files), input_file,
        )
        try:
            workflow.stage(input_file)
            log.info(
                "partition=%s [%d/%d] done %s",
                partition_id, idx + 1, len(input_files), input_file,
            )
        except Exception as e:
            log.error(
                "partition=%s [%d/%d] FAILED %s error=%s",
                partition_id, idx + 1, len(input_files), input_file, e,
            )
            sys.exit(1)

    log.info("partition=%s staging complete files=%d", partition_id, len(input_files))


# ---------------------------------------------------------------------------
# Discovery 1 — enumerate staged FGB tiles → rasterise + 3D-tile fan-out
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
    **_VIZ_SCRIPT_KWARGS,
    resources=_DISCOVERY_RESOURCES,
)
def discover_staged_tiles() -> None:
    """Discover staged FGB tiles at max z-level and emit chunked partition manifests.

    Uses os.scandir (lazy, no full-buffer glob) to avoid OOM on large dirs.
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

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)
    max_z = workflow.config.get_max_z()

    staged_dir = workflow.config.get_dir("staged", z=max_z)
    staged_files = [
        e.path
        for e in os.scandir(staged_dir)
        if e.is_file() and e.name.endswith(".fgb")
    ]

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
    **_VIZ_SCRIPT_KWARGS,
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

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())
    z_level = int("{{inputs.parameters.z-level}}")
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)
    child_z = z_level + 1

    child_dir = workflow.config.get_dir("geotiff", z=child_z)
    child_tiles = [
        e.path
        for e in os.scandir(child_dir)
        if e.is_file() and e.name.endswith(".tif")
    ]

    log.info("z=%d child_z=%d child_tiles=%d", z_level, child_z, len(child_tiles))

    parent_tiles: set = set()
    for child_path in child_tiles:
        parent_path = workflow.tiles.get_parent_tile(child_path)
        if parent_path:
            parent_tiles.add(parent_path)

    parent_tiles_list = [
        {"z": z_level, "path": p} for p in sorted(parent_tiles)
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
    **_VIZ_SCRIPT_KWARGS,
    resources=_DISCOVERY_RESOURCES,
)
def discover_all_geotiffs() -> None:
    """Discover all GeoTIFF files across every z-level for web-tile fan-out.

    Uses os.scandir for lazy directory walk.  Each partition entry is a dict
    ``{"z": int, "path": str}`` so the worker receives the correct z-level.
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

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())
    partition_size = int("{{inputs.parameters.partition-size}}")

    workflow = WorkflowManager(config)

    geotiff_dir = workflow.config.get_dir("geotiff")
    geotiff_entries: list = []
    # Walk one level: geotiff_dir/<z>/<tile>.tif  (lazy, no recursion)
    for z_entry in os.scandir(geotiff_dir):
        if not z_entry.is_dir():
            continue
        try:
            z_level = int(z_entry.name)
        except ValueError:
            continue
        for tile_entry in os.scandir(z_entry.path):
            if tile_entry.is_file() and tile_entry.name.endswith(".tif"):
                geotiff_entries.append({"z": z_level, "path": tile_entry.path})

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
# Stage 2 — Rasterise staged FGB tiles → GeoTIFF at max z-level
# ---------------------------------------------------------------------------


@script(
    name="rasterize-max-z",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="staged-tiles-manifest"),
    ],
    **_VIZ_SCRIPT_KWARGS,
    resources=_RASTER_RESOURCES,
    retry_strategy=_WORKER_RETRY,
)
def rasterize_max_z_parallel() -> None:
    """Rasterise a partition of staged FGB vector files to GeoTIFF at max z-level."""
    import json
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list = json.loads("{{inputs.parameters.staged-tiles-manifest}}")
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
    **_VIZ_SCRIPT_KWARGS,
    resources=_RASTER_RESOURCES,
    retry_strategy=_WORKER_RETRY,
)
def create_composite_z_parallel() -> None:
    """Create composite (parent) raster tiles from child GeoTIFFs at z+1."""
    import json
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list = json.loads("{{inputs.parameters.parent-tiles-manifest}}")
    log.info("composite tiles=%d", len(manifest))

    for item in manifest:
        z_level: int = item["z"]
        parent_path: str = item["path"]
        log.info("z=%d creating composite %s", z_level, parent_path)
        try:
            workflow.raster_tiler.create_parent_tile(parent_path, z_level)
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
    **_VIZ_SCRIPT_KWARGS,
    resources=_STAGE_RESOURCES,
    retry_strategy=_WORKER_RETRY,
)
def create_web_tile_parallel() -> None:
    """Convert GeoTIFFs to PNG web tiles."""
    import json
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list = json.loads("{{inputs.parameters.geotiff-manifest}}")
    log.info("web tiles items=%d", len(manifest))

    for item in manifest:
        z_level: int = item["z"]
        geotiff_path: str = item["path"]
        log.info("creating web tile z=%d %s", z_level, geotiff_path)
        try:
            workflow.raster_tiler.geotiff_to_webtile(geotiff_path, z_level)
            log.info("done z=%d %s", z_level, geotiff_path)
        except Exception as e:
            log.error("FAILED z=%d %s error=%s", z_level, geotiff_path, e)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Stage 5 — Convert staged FGB vectors → Cesium 3D tiles (B3DM/GLB)
# ---------------------------------------------------------------------------


@script(
    name="create-3dtiles",
    inputs=[
        Parameter(name="recipe-id"),
        Parameter(name="staged-tiles-manifest"),
    ],
    **_VIZ_SCRIPT_KWARGS,
    resources=_THREEDTILE_RESOURCES,
    retry_strategy=_WORKER_RETRY,
)
def create_3dtile_parallel() -> None:
    """Convert staged FGB vector tiles to Cesium 3D tiles (B3DM/GLB)."""
    import json
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)

    from pdgworkflow import WorkflowManager  # type: ignore[import-not-found]

    config_path = Path("/mnt/workflow/{{inputs.parameters.recipe-id}}/config.json")
    config = json.loads(config_path.read_text())

    workflow = WorkflowManager(config)

    manifest: list = json.loads("{{inputs.parameters.staged-tiles-manifest}}")
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
# Workflow entry point
# ---------------------------------------------------------------------------


def make_and_submit_viz_workflow(
    recipe_config: RecipeConfig,
    wait: bool = False,
) -> str:
    """Create and submit a parallel Argo workflow for viz processing on K8s.

    Implements a 5-stage parallel pipeline with strict z-level ordering:

    Stage 1  — Stage input files → max-z FGB vector tiles      (with_param fan-out)
    Stage 2  — Rasterise max-z FGB tiles → GeoTIFFs            (discovery → with_param)
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

    viz_image = recipe_config.workflow.viz_image  # type: ignore[union-attr]
    if viz_image != VIZ_WORKFLOW_IMAGE:
        logger.warning(
            "recipe viz_image=%r differs from module-level VIZ_WORKFLOW_IMAGE=%r; "
            "the module-level constant is used in @script decorators. "
            "Set VIZ_WORKFLOW_IMAGE env var before starting ogdc-runner.",
            viz_image,
            VIZ_WORKFLOW_IMAGE,
        )

    parallel_cfg = recipe_config.workflow.parallel  # type: ignore[union-attr]
    parallelism: int | None = (
        (parallel_cfg.max_parallelism or MAX_PARALLEL_LIMIT)
        if parallel_cfg.enabled
        else None
    )

    with OgdcWorkflow(
        name="visualization",
        recipe_config=recipe_config,
        archive_workflow=True,
        entrypoint="main",
        volumes=[OGDC_WORKFLOW_PVC],
        parallelism=parallelism,
        annotations={
            "workflows.argoproj.io/description": (
                "Parallel 5-stage viz workflow "
                "(stage→rasterise→composite→web-tile→3d-tile)"
            ),
        },
        labels={
            "workflows.argoproj.io/archive-strategy": "false",
        },
    ) as w:
        config_content = recipe_config.workflow.get_config_file_json()  # type: ignore[union-attr]

        partition_size: int = parallel_cfg.partition_size or _DEFAULT_PARTITION_SIZE

        # Parse workflow config for z-range and feature flags.
        workflow_config: dict = json.loads(config_content)
        z_range: list = workflow_config.get("z_range", [0, 12])
        min_z, max_z = z_range[0], z_range[1]

        enable_stager: bool = workflow_config.get("enable_stager", True)
        enable_raster: bool = workflow_config.get("enable_raster", True)
        enable_raster_parents: bool = workflow_config.get("enable_raster_parents", True)
        enable_web_tiles: bool = workflow_config.get("enable_web_tiles", True)
        enable_3dtiles: bool = workflow_config.get("enable_3dtiles", True)

        # z = max_z-1 down to min_z (inclusive), processed strictly in order.
        composite_z_levels: list = list(range(max_z - 1, min_z - 1, -1))

        # ----------------------------------------------------------------
        # Setup container — write config.json and create output directories
        # ----------------------------------------------------------------
        setup_template = Container(
            name="stage-viz-config",
            image="alpine:latest",
            command=["sh", "-c"],
            args=[
                f"""mkdir -p /mnt/workflow/{recipe_config.id}/input && \
mkdir -p /mnt/workflow/{recipe_config.id}/output/staged && \
mkdir -p /mnt/workflow/{recipe_config.id}/output/geotiff && \
mkdir -p /mnt/workflow/{recipe_config.id}/output/3dtiles && \
mkdir -p /mnt/workflow/{recipe_config.id}/output/web_tiles && \
cat > /mnt/workflow/{recipe_config.id}/config.json << 'EOF'
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

            # Track tail(s) of the in-flight pipeline for dependency chaining.
            current_deps: list = [config_task]

            # ============================================================
            # STAGE 1 — Stage input files at max-z   (with_param fan-out)
            # ============================================================
            if enable_stager:
                # Compute partitions at submission time and serialise as
                # JSON array-of-arrays for Argo withParam.
                #
                # NOTE: For datasets with > ~10 000 input files the manifest
                # will grow large in the workflow spec.  A future hardening
                # step will write manifests to the PVC during setup and pass
                # only partition indices via withParam.
                partitions = create_partitions(
                    inputs=recipe_config.input.params,
                    execution_function=ExecutionFunction(
                        name="stage-files", function=stage_file_parallel
                    ),
                    parallel_config=parallel_cfg,
                )
                stage_partitions_json = json.dumps([p.files for p in partitions])
                logger.info(
                    "stage1 partitions=%d total_files=%d",
                    len(partitions),
                    sum(len(p.files) for p in partitions),
                )

                stage_task = stage_file_parallel(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "partition-manifest": "{{item}}",
                        "partition-id": "{{loop.index}}",
                    },
                    with_param=stage_partitions_json,
                )
                for dep in current_deps:
                    dep >> stage_task
                current_deps = [stage_task]

            # ============================================================
            # Discovery — enumerate staged tiles (shared by Stage 2 + 5)
            # ============================================================
            staged_tiles_discovery_task: Task | None = None
            if enable_raster or enable_3dtiles:
                staged_tiles_discovery_task = discover_staged_tiles(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "partition-size": str(partition_size),
                    }
                )
                for dep in current_deps:
                    dep >> staged_tiles_discovery_task

            # ============================================================
            # STAGE 2 — Rasterise at max-z   (discovery → with_param)
            # ============================================================
            rasterize_task: Task | None = None
            if enable_raster and staged_tiles_discovery_task is not None:
                rasterize_task = rasterize_max_z_parallel(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "staged-tiles-manifest": "{{item}}",
                    },
                    with_param=staged_tiles_discovery_task.get_result_as("result"),
                )
                staged_tiles_discovery_task >> rasterize_task

            # ============================================================
            # STAGE 3 — Composite tiles  (sequential z, with_param tiles)
            # ============================================================
            final_composite_task: Task | None = None
            if enable_raster_parents:
                prev_task: Task | None = (
                    rasterize_task or staged_tiles_discovery_task
                )

                for z in composite_z_levels:
                    discover_parents_task = discover_parent_tiles(
                        name=f"discover-parents-z-{z}",
                        arguments={
                            "recipe-id": recipe_config.id,
                            "z-level": str(z),
                            "partition-size": str(partition_size),
                        },
                    )
                    create_composites_task = create_composite_z_parallel(
                        name=f"create-composites-z-{z}",
                        arguments={
                            "recipe-id": recipe_config.id,
                            "parent-tiles-manifest": "{{item}}",
                        },
                        with_param=discover_parents_task.get_result_as("result"),
                    )

                    if prev_task is not None:
                        prev_task >> discover_parents_task
                    else:
                        for dep in current_deps:
                            dep >> discover_parents_task
                        current_deps = []  # consumed on first z iteration only

                    discover_parents_task >> create_composites_task
                    prev_task = create_composites_task

                final_composite_task = prev_task

            # ============================================================
            # Discovery 3 + STAGE 4 — Web tiles  (discovery → with_param)
            # ============================================================
            if enable_web_tiles:
                discover_geotiffs_task = discover_all_geotiffs(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "partition-size": str(partition_size),
                    }
                )

                raster_anchor: Task | None = (
                    final_composite_task
                    or rasterize_task
                    or staged_tiles_discovery_task
                )
                if raster_anchor is not None:
                    raster_anchor >> discover_geotiffs_task
                else:
                    for dep in current_deps:
                        dep >> discover_geotiffs_task

                web_tile_task = create_web_tile_parallel(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "geotiff-manifest": "{{item}}",
                    },
                    with_param=discover_geotiffs_task.get_result_as("result"),
                )
                discover_geotiffs_task >> web_tile_task

            # ============================================================
            # STAGE 5 — 3D tiles, reuses Stage-1 discovery (with_param)
            # ============================================================
            if enable_3dtiles and staged_tiles_discovery_task is not None:
                threedtile_task = create_3dtile_parallel(
                    arguments={
                        "recipe-id": recipe_config.id,
                        "staged-tiles-manifest": "{{item}}",
                    },
                    with_param=staged_tiles_discovery_task.get_result_as("result"),
                )
                staged_tiles_discovery_task >> threedtile_task

    workflow_name: str = submit_workflow(w, wait=wait)
    return workflow_name
