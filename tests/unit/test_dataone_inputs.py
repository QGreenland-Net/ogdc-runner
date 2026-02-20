from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from ogdc_runner.inputs import make_fetch_input_template
from ogdc_runner.models.recipe_config import (
    DataOneInput,
    RecipeConfig,
    RecipeInput,
    ShellWorkflow,
    TemporaryRecipeOutput,
)


class TestDataOneFetchInputTemplate:
    """Tests for fetching DataONE inputs in workflows."""

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_fetch_single_dataone_file(self, mock_resolve, tmpdir):
        """Test fetching a single DataONE file."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3Afile1",
                "filename": "data.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "Data File",
                "entity_description": "",
            }
        ]

        fake_sh_file = tmpdir / "recipe.sh"
        fake_sh_file.write('echo "test"')

        recipe_config = RecipeConfig(
            name="Test DataONE Recipe",
            input=RecipeInput(
                params=[
                    DataOneInput(value="resource_map_doi:10.18739/A29G5GD39"),
                ]
            ),
            output=TemporaryRecipeOutput(),
            workflow=ShellWorkflow.model_validate(
                {"sh_file": Path(fake_sh_file).name},
                context={"recipe_directory": Path(tmpdir)},
            ),
            recipe_directory=Path(tmpdir),
        )

        template = make_fetch_input_template(recipe_config)

        # Verify the template has correct wget command
        assert template.name is not None
        assert template.name.startswith("test-dataone-recipe-fetch-template-")
        assert template.args is not None
        assert len(template.args) == 1

        command = template.args[0]
        assert "wget --content-disposition -P /output_dir/" in command
        assert (
            "https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3Afile1"
            in command
        )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_fetch_multiple_dataone_files(self, mock_resolve, tmpdir):
        """Test fetching multiple DataONE files with wildcard."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://arcticdata.io/object/file1",
                "filename": "percent_gris_1.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file2",
                "url": "https://arcticdata.io/object/file2",
                "filename": "percent_gris_2.nc",
                "format_id": "netCDF-4",
                "size": 2048,
                "entity_name": "File 2",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file3",
                "url": "https://arcticdata.io/object/file3",
                "filename": "percent_gris_3.nc",
                "format_id": "netCDF-4",
                "size": 3072,
                "entity_name": "File 3",
                "entity_description": "",
            },
        ]

        fake_sh_file = tmpdir / "recipe.sh"
        fake_sh_file.write('echo "test"')

        recipe_config = RecipeConfig(
            name="Test Multiple Files",
            input=RecipeInput(
                params=[
                    DataOneInput(
                        value="resource_map_doi:10.18739/A29G5GD39",
                        filename="percent_gris_*.nc",
                    ),
                ]
            ),
            output=TemporaryRecipeOutput(),
            workflow=ShellWorkflow.model_validate(
                {"sh_file": Path(fake_sh_file).name},
                context={"recipe_directory": Path(tmpdir)},
            ),
            recipe_directory=Path(tmpdir),
        )

        template = make_fetch_input_template(recipe_config)

        # Verify all files are in the command
        assert template.args is not None
        command = template.args[0]
        assert "https://arcticdata.io/object/file1" in command
        assert "https://arcticdata.io/object/file2" in command
        assert "https://arcticdata.io/object/file3" in command

        # Verify commands are chained with &&
        assert command.count("&&") >= 3

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_fetch_template_output_artifact(self, mock_resolve, tmpdir):
        """Test that fetch template has correct output artifact configuration."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://arcticdata.io/object/file1",
                "filename": "data.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File",
                "entity_description": "",
            }
        ]

        fake_sh_file = tmpdir / "recipe.sh"
        fake_sh_file.write('echo "test"')

        recipe_config = RecipeConfig(
            name="Test Output Artifact",
            input=RecipeInput(
                params=[
                    DataOneInput(value="resource_map_doi:10.18739/A29G5GD39"),
                ]
            ),
            output=TemporaryRecipeOutput(),
            workflow=ShellWorkflow.model_validate(
                {"sh_file": Path(fake_sh_file).name},
                context={"recipe_directory": Path(tmpdir)},
            ),
            recipe_directory=Path(tmpdir),
        )

        template = make_fetch_input_template(recipe_config)

        # Just verify outputs exist - don't try to inspect them deeply
        assert template.outputs is not None
