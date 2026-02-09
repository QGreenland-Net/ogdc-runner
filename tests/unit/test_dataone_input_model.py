"""Unit tests for DataOneInput model."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pydantic import AnyUrl

from ogdc_runner.models.recipe_config import DataOneInput, RecipeInput, UrlInput


class TestDataOneInput:
    """Tests for DataOneInput model."""

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_basic(self, mock_resolve):
        """Test basic DataOneInput creation and resolution."""
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

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
        )

        assert input_param.type == "dataone"
        assert input_param.value == "resource_map_doi:10.18739/A29G5GD39"
        assert len(input_param._resolved_objects) == 1
        assert input_param._resolved_objects[0]["filename"] == "data.nc"
        mock_resolve.assert_called_once_with(
            dataset_identifier="resource_map_doi:10.18739/A29G5GD39"
        )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_with_filename_pattern(self, mock_resolve):
        """Test DataOneInput with filename pattern filtering."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "percent_gris_1.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file2",
                "url": "https://test.org/file2",
                "filename": "percent_gris_2.nc",
                "format_id": "netCDF-4",
                "size": 2048,
                "entity_name": "File 2",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file3",
                "url": "https://test.org/file3",
                "filename": "other_data.csv",
                "format_id": "text/csv",
                "size": 512,
                "entity_name": "Other File",
                "entity_description": "",
            },
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
            filename="percent_gris_*.nc",
        )

        # Should only match the two .nc files with percent_gris prefix
        assert len(input_param._resolved_objects) == 2
        assert all(
            obj["filename"].startswith("percent_gris")
            for obj in input_param._resolved_objects
        )
        assert all(
            obj["filename"].endswith(".nc") for obj in input_param._resolved_objects
        )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_exact_filename(self, mock_resolve):
        """Test DataOneInput with exact filename match."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "specific_file.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "Specific File",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file2",
                "url": "https://test.org/file2",
                "filename": "other_file.nc",
                "format_id": "netCDF-4",
                "size": 2048,
                "entity_name": "Other File",
                "entity_description": "",
            },
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
            filename="specific_file.nc",
        )

        # Should only match exact filename
        assert len(input_param._resolved_objects) == 1
        assert input_param._resolved_objects[0]["filename"] == "specific_file.nc"

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_wildcard_question_mark(self, mock_resolve):
        """Test DataOneInput with ? wildcard (single character)."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "data1.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file2",
                "url": "https://test.org/file2",
                "filename": "data2.nc",
                "format_id": "netCDF-4",
                "size": 2048,
                "entity_name": "File 2",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file3",
                "url": "https://test.org/file3",
                "filename": "data10.nc",  # Two digits - shouldn't match
                "format_id": "netCDF-4",
                "size": 512,
                "entity_name": "File 3",
                "entity_description": "",
            },
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
            filename="data?.nc",
        )

        # Should match data1.nc and data2.nc but not data10.nc
        assert len(input_param._resolved_objects) == 2
        filenames = [obj["filename"] for obj in input_param._resolved_objects]
        assert "data1.nc" in filenames
        assert "data2.nc" in filenames
        assert "data10.nc" not in filenames

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_no_filename_uses_all(self, mock_resolve):
        """Test DataOneInput without filename uses all files."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "file1.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
            {
                "identifier": "urn:uuid:file2",
                "url": "https://test.org/file2",
                "filename": "file2.csv",
                "format_id": "text/csv",
                "size": 2048,
                "entity_name": "File 2",
                "entity_description": "",
            },
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
            # No filename specified
        )

        # Should include all files
        assert len(input_param._resolved_objects) == 2

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_case_insensitive_matching(self, mock_resolve):
        """Test that filename matching is case-insensitive."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "DATA.NC",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
            filename="data.nc",  # lowercase pattern
        )

        # Should match despite case difference
        assert len(input_param._resolved_objects) == 1
        assert input_param._resolved_objects[0]["filename"] == "DATA.NC"

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_no_matching_files(self, mock_resolve):
        """Test DataOneInput raises error when no files match pattern."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "other_file.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File 1",
                "entity_description": "",
            },
        ]

        with pytest.raises(
            ValueError, match=r"No data objects matching pattern 'nonexistent.nc'"
        ):
            DataOneInput(
                value="resource_map_doi:10.18739/A29G5GD39",
                filename="nonexistent.nc",
            )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_empty_dataset(self, mock_resolve):
        """Test DataOneInput raises error when dataset has no objects."""
        mock_resolve.return_value = []

        with pytest.raises(
            ValueError, match="No data objects found in dataset resource_map"
        ):
            DataOneInput(
                value="resource_map_doi:10.18739/A29G5GD39",
            )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_resolver_failure(self, mock_resolve):
        """Test DataOneInput handles resolver failures gracefully."""
        mock_resolve.side_effect = Exception("Network error")

        with pytest.raises(ValueError, match="Failed to resolve DataONE package"):
            DataOneInput(
                value="resource_map_doi:10.18739/A29G5GD39",
            )

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_dataone_input_stores_dataset_pid(self, mock_resolve):
        """Test that DataOneInput stores the dataset PID."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "data.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File",
                "entity_description": "",
            }
        ]

        input_param = DataOneInput(
            value="resource_map_doi:10.18739/A29G5GD39",
        )

        assert input_param._dataset_pid == "resource_map_doi:10.18739/A29G5GD39"

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_recipe_input_with_dataone(self, mock_resolve):
        """Test RecipeInput accepts DataOneInput."""
        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "data.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File",
                "entity_description": "",
            }
        ]

        recipe_input = RecipeInput(
            params=[
                DataOneInput(value="resource_map_doi:10.18739/A29G5GD39"),
            ]
        )

        assert len(recipe_input.params) == 1
        assert isinstance(recipe_input.params[0], DataOneInput)

    @patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
    def test_recipe_input_mixed_types(self, mock_resolve):
        """Test RecipeInput with both URL and DataONE inputs."""

        mock_resolve.return_value = [
            {
                "identifier": "urn:uuid:file1",
                "url": "https://test.org/file1",
                "filename": "data.nc",
                "format_id": "netCDF-4",
                "size": 1024,
                "entity_name": "File",
                "entity_description": "",
            }
        ]

        recipe_input = RecipeInput(
            params=[
                UrlInput(value=AnyUrl("http://example.com/data.zip")),
                DataOneInput(value="resource_map_doi:10.18739/A29G5GD39"),
            ]
        )

        assert len(recipe_input.params) == 2
        assert isinstance(recipe_input.params[0], UrlInput)
        assert isinstance(recipe_input.params[1], DataOneInput)
