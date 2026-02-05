"""Unit tests for DataONE resolver functionality."""

from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from ogdc_runner.dataone import resolver
from ogdc_runner.dataone.resolver import DataONEResolver, resolve_dataone_input
from ogdc_runner.exceptions import OgdcDataOneError, OgdcMissingEnvvar


class TestDataONEResolver:
    """Tests for DataONEResolver class."""

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    def test_resolver_initialization(self):
        """Test that resolver initializes with correct member node."""
        resolver = DataONEResolver()
        assert resolver.member_node == "https://test.dataone.org"

    @patch.dict("os.environ", {}, clear=True)
    def test_resolver_missing_envvar(self):
        """Test that resolver raises error when DATAONE_NODE_URL is missing."""
        # Need to reload the module to trigger the env check
        with pytest.raises(OgdcMissingEnvvar, match="Must have DATAONE_NODE_URL"):
            importlib.reload(resolver)

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
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
                        "title": ["Data File 2"],
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

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_empty(self, mock_get):
        """Test dataset resolution when no objects found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"response": {"docs": []}}
        mock_get.return_value = mock_response

        resolver = DataONEResolver()
        result = resolver.resolve_dataset("resource_map_urn:uuid:empty")

        assert result == []

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_network_error(self, mock_get):
        """Test dataset resolution handles network errors."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

        resolver = DataONEResolver()
        with pytest.raises(OgdcDataOneError, match="Failed to resolve dataset"):
            resolver.resolve_dataset("resource_map_urn:uuid:test-123")

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    @patch("ogdc_runner.dataone.resolver.requests.get")
    def test_resolve_dataset_http_error(self, mock_get):
        """Test dataset resolution handles HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_get.return_value = mock_response

        resolver = DataONEResolver()
        with pytest.raises(OgdcDataOneError, match="Failed to resolve dataset"):
            resolver.resolve_dataset("resource_map_urn:uuid:nonexistent")

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    def test_build_object_url(self):
        """Test URL building with proper encoding."""
        resolver = DataONEResolver()

        # Test with special characters that need encoding
        url = resolver._build_object_url("urn:uuid:test-123")
        assert url == "https://test.dataone.org/v2/object/urn%3Auuid%3Atest-123"

        # Test with colons and slashes
        url = resolver._build_object_url("doi:10.18739/A29G5GD39")
        assert "doi%3A10.18739" in url

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    def test_get_filename_from_field(self):
        """Test filename extraction from fileName field."""
        resolver = DataONEResolver()

        # String filename
        doc = {"id": "urn:uuid:123", "fileName": "myfile.nc"}
        assert resolver._get_filename(doc) == "myfile.nc"

        # List filename (take first)
        doc: dict[str, Any] = {
            "id": "urn:uuid:123",
            "fileName": ["file1.nc", "file2.nc"],
        }
        assert resolver._get_filename(doc) == "file1.nc"

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    def test_get_filename_fallback(self):
        """Test filename fallback to identifier."""
        resolver = DataONEResolver()

        # No fileName field - use identifier
        doc = {"id": "urn:uuid:my-data-file.nc"}
        assert resolver._get_filename(doc) == "my-data-file.nc"

        # Missing both should raise error
        doc = {}
        with pytest.raises(ValueError, match="missing required 'id' field"):
            resolver._get_filename(doc)

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    def test_get_entity_name(self):
        """Test entity name extraction."""
        resolver = DataONEResolver()

        # String title
        doc = {"id": "urn:uuid:123", "title": "My Dataset", "fileName": "data.nc"}
        assert resolver._get_entity_name(doc) == "My Dataset"

        # List title
        doc: dict[str, Any] = {
            "id": "urn:uuid:123",
            "title": ["Dataset 1", "Dataset 2"],
        }
        assert resolver._get_entity_name(doc) == "Dataset 1"

        # No title - fallback to filename
        doc = {"id": "urn:uuid:123", "fileName": "fallback.nc"}
        assert resolver._get_entity_name(doc) == "fallback.nc"

    @patch.dict("os.environ", {"DATAONE_NODE_URL": "https://test.dataone.org"})
    @patch("ogdc_runner.dataone.resolver.DataONEResolver.resolve_dataset")
    def test_resolve_dataone_input_function(self, mock_resolve):
        """Test the convenience function wrapper."""
        mock_resolve.return_value = [{"identifier": "test", "url": "http://test"}]

        result = resolve_dataone_input("resource_map_urn:uuid:test")

        mock_resolve.assert_called_once_with("resource_map_urn:uuid:test")
        assert len(result) == 1
        assert result[0]["identifier"] == "test"
