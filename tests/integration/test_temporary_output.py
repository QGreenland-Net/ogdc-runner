from __future__ import annotations

import shutil
from pathlib import Path

from ogdc_runner.__main__ import _download_output_for_workflow
from ogdc_runner.api import submit_ogdc_recipe


def test_temporary_output_recipe(test_temp_output_recipe_directory, tmpdir):
    """Test that an ogdc recipe with a temporary output can be submitted, executes successfully, and outputs are accessible for download as a zip package.

    TODO/NOTE: this test and the recipe that's invoked is similar to that in the
    `test_ogdc_recipe.py::test_submit_ogdc_recipe` integration test. The key
    difference is the output type. In the `test_ogdc_recipe.py` module, there's
    another test, `test_submit_ogdc_recipe_fails_already_published`, which
    expects to have an `OgdcDataAlreadyPublished` when the same recipe is
    submitted again directly after the one in
    `test_submit_ogdc_recipe`. Temporary outputs are (currently) always treated
    as they are not published. Once work to track recipe executions in the
    database is complete, the separation of this test from
    `test_submit_ogdc_recipe` can be removed.
    """
    tmpdir_path = Path(tmpdir)

    # Note: `overwrite` is set here to ensure that outptus from a previous test
    # run are overwritten. This is not ideal. Tests that create data should
    # cleanup after themselves.
    workflow_name = submit_ogdc_recipe(
        recipe_dir=test_temp_output_recipe_directory,
        overwrite=True,
        wait=True,
    )

    # retrieve the data and assret that it is correct.
    _download_output_for_workflow(workflow_name, tmpdir_path)

    # There should be one zip file with the workflow output contents
    zip_files = list(tmpdir_path.glob("*.zip"))
    assert len(zip_files) == 1
    zip_file = zip_files[0]
    # Unzip the package and ensure the expected gpkg file is present.
    shutil.unpack_archive(zip_file, tmpdir_path)
    gpkg_files = list(tmpdir_path.glob("*.gpkg"))
    assert len(gpkg_files) == 1
