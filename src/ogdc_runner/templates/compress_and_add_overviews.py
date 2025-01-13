"""Template for compressing and adding overviews to GeoTIFF.

This template is utliized by QGreenland Core to produce consistently compressed
rasters with overviews.
"""

from __future__ import annotations

from hera.workflows import (
    Artifact,
    Workflow,
)

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.templates.bash_command import make_cmd_template


def compress_and_add_overviews(
    input_dir: Artifact,
    input_file: str,
    output_file: str,
    dtype_is_float: bool | None = None,
    resampling_algorithm: str = "average",
    compress_type: str = "DEFLATE",
) -> Workflow:
    dtype_unexp_not_passed = compress_type == "DEFLATE" and dtype_is_float is None
    dtype_unexp_passed = compress_type != "DEFLATE" and dtype_is_float is not None
    if dtype_unexp_passed or dtype_unexp_not_passed:
        raise RuntimeError(
            "`dtype_is_float` may only be specified for DEFLATE compression" " type.",
        )

    compress_creation_options = [
        "-co",
        "TILED=YES",
        "-co",
        f"COMPRESS={compress_type}",
    ]
    if compress_type == "DEFLATE":
        predictor_value = 3 if dtype_is_float else 2
        compress_creation_options.extend(
            [
                "-co",
                f"PREDICTOR={predictor_value}",
            ]
        )

    compress = [
        "gdal_translate",
        *compress_creation_options,
        input_file,
        "/output_dir/compressed.tif",
    ]

    copy_into_place = [
        "cp",
        "/input_dir/compressed.tif",
        output_file,
    ]

    add_overviews = [
        "gdaladdo",
        "-r",
        resampling_algorithm,
        output_file,
        "2",
        "4",
        "8",
        "16",
    ]

    compress_template = make_cmd_template(
        name="compress-raster",
        command=" ".join(compress),
    )
    build_overviews_template = make_cmd_template(
        name="build_overviews",
        command=" ".join([*copy_into_place, "&&", *add_overviews]),
    )

    with Workflow(
        generate_name="compress-and-add-overviews-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        compress_step = compress_template(arguments=input_dir.with_name("input-dir"))
        build_overviews_template(
            arguments=compress_step.get_artifact("output-dir").with_name("input-dir")
        )

    return w
