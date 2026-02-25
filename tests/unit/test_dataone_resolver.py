"""Unit tests for DataONE resolver functionality."""

from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from ogdc_runner.dataone import resolver
from ogdc_runner.dataone.resolver import (
    DataONEResolver,
    _filter_by_filename,
    resolve_dataone_input,
)
from ogdc_runner.exceptions import OgdcDataOneError, OgdcMissingEnvvar


class TestDataONEResolver:
    """Tests for DataONEResolver class."""

    def test_resolver_initialization(self):
        """Test that resolver initializes with correct member node."""
        # The resolver gets DATAONE_NODE_URL from the environment at module load time
        # So we just verify it has a member_node attribute
        resolver = DataONEResolver()
        assert resolver.member_node is not None
        assert isinstance(resolver.member_node, str)
        assert "http" in resolver.member_node  # Should be a URL

    @patch.dict("os.environ", {}, clear=True)
    def test_resolver_missing_envvar(self):
        """Test that resolver raises error when DATAONE_NODE_URL is missing."""
        # Need to reload the module to trigger the env check
        with pytest.raises(OgdcMissingEnvvar, match="Must have DATAONE_NODE_URL"):
            importlib.reload(resolver)

    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_success(self, mock_get):
        """Test successful dataset resolution with multiple objects."""
        # Mock Solr response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "response": {
                "docs": [
                    {
                        "id": "urn:uuid:file1",
                        "fileName": "data1.nc",
                        "title": "Data File 1",
                        "formatId": "netCDF-4",
                        "size": 1024,
                    },
                    {
                        "id": "urn:uuid:file2",
                        "fileName": ["data2.nc"],  # Test list format
                        "title": "Data File 2",
                        "formatId": "netCDF-4",
                        "size": 2048,
                    },
                ]
            }
        }
        mock_get.return_value = mock_response

        resolver = DataONEResolver()
        result = resolver.resolve_dataset("resource_map_urn:uuid:test-123")

        # Verify request was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "v2/query/solr/" in call_args[0][0]
        assert (
            'resourceMap:"resource_map_urn:uuid:test-123"'
            in call_args[1]["params"]["q"]
        )

        # Verify results
        assert len(result) == 2
        assert result[0]["identifier"] == "urn:uuid:file1"
        assert result[0]["filename"] == "data1.nc"
        assert result[0]["entity_name"] == "Data File 1"
        assert "urn%3Auuid%3Afile1" in result[0]["url"]

        assert result[1]["identifier"] == "urn:uuid:file2"
        assert result[1]["filename"] == "data2.nc"
        assert result[1]["entity_name"] == "Data File 2"

    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_empty(self, mock_get):
        """Test dataset resolution when no objects found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"response": {"docs": []}}
        mock_get.return_value = mock_response

        resolver = DataONEResolver()
        result = resolver.resolve_dataset("resource_map_urn:uuid:empty")

        assert result == []

    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_network_error(self, mock_get):
        """Test dataset resolution handles network errors."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

        resolver = DataONEResolver()
        # Override member_node for testing
        resolver.member_node = "https://test.dataone.org"

        with pytest.raises(OgdcDataOneError, match="Failed to resolve dataset"):
            resolver.resolve_dataset("resource_map_urn:uuid:test-123")

    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_http_error(self, mock_get):
        """Test dataset resolution handles HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_get.return_value = mock_response

        resolver = DataONEResolver()
        # Override member_node for testing
        resolver.member_node = "https://test.dataone.org"

        with pytest.raises(OgdcDataOneError, match="Failed to resolve dataset"):
            resolver.resolve_dataset("resource_map_urn:uuid:nonexistent")

    def test_build_object_url(self):
        """Test URL building with proper encoding."""
        resolver = DataONEResolver()
        # Override member_node for testing
        resolver.member_node = "https://test.dataone.org"

        # Test with special characters that need encoding
        url = resolver._build_object_url("urn:uuid:test-123")
        assert url == "https://test.dataone.org/v2/object/urn%3Auuid%3Atest-123"

        # Test with colons and slashes
        url = resolver._build_object_url("doi:10.18739/A29G5GD39")
        assert "doi%3A10.18739" in url

    def test_get_filename_from_field(self):
        """Test filename extraction from fileName field."""
        resolver = DataONEResolver()

        # String filename
        doc_str = {"id": "urn:uuid:123", "fileName": "myfile.nc"}
        assert resolver._get_filename(doc_str) == "myfile.nc"

        # List filename (take first)
        doc_list: dict[str, Any] = {
            "id": "urn:uuid:123",
            "fileName": ["file1.nc", "file2.nc"],
        }
        assert resolver._get_filename(doc_list) == "file1.nc"

    def test_get_entity_name(self):
        """Test entity name extraction."""
        resolver = DataONEResolver()

        # String title
        doc_str = {"id": "urn:uuid:123", "title": "My Dataset", "fileName": "data.nc"}
        assert resolver._get_entity_name(doc_str) == "My Dataset"

        # No title - fallback to filename
        doc_no_title = {"id": "urn:uuid:123", "fileName": "fallback.nc"}
        assert resolver._get_entity_name(doc_no_title) == "fallback.nc"

    @patch("ogdc_runner.dataone.resolver.DataONEResolver.resolve_dataset")
    def test_resolve_dataone_input_function(self, mock_resolve):
        """Test the convenience function wrapper."""
        mock_resolve.return_value = [{"identifier": "test", "url": "http://test"}]
        result = resolve_dataone_input("resource_map_urn:uuid:test")

        mock_resolve.assert_called_once_with("resource_map_urn:uuid:test")
        assert len(result) == 1
        assert result[0]["identifier"] == "test"

    def test_filter_by_filename(self):
        """Test _filter_by_filename raises error when no files match."""

        data_objects = [
            {"filename": "other_file.nc", "identifier": "urn:uuid:1"},
        ]

        with pytest.raises(
            ValueError, match=r"No data objects matching pattern 'nonexistent.nc'"
        ):
            _filter_by_filename(data_objects, "nonexistent.nc")
