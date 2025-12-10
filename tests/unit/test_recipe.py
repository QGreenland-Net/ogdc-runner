from __future__ import annotations

from unittest.mock import Mock, patch

import requests

from ogdc_runner.recipe import get_recipe_config, validate_input_urls


def test_get_recipe_config(test_shell_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_shell_workflow_recipe_directory,
    )

    assert config.id == "test-ogdc-workflow"


class TestValidateInputUrls:
    """Tests for validate_input_urls function."""

    def test_valid_url_returns_empty_list(self, test_shell_workflow_recipe_directory):
        """A recipe with accessible URLs should return no errors."""
        config = get_recipe_config(test_shell_workflow_recipe_directory)

        with patch("ogdc_runner.recipe.requests.head") as mock_head:
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_head.return_value = mock_response

            errors = validate_input_urls(config)

            assert errors == []
            mock_head.assert_called_once()

    def test_http_404_returns_error(self, test_shell_workflow_recipe_directory):
        """A 404 response should return an error."""
        config = get_recipe_config(test_shell_workflow_recipe_directory)

        with patch("ogdc_runner.recipe.requests.head") as mock_head:
            mock_response = Mock()
            mock_response.status_code = 404
            http_error = requests.exceptions.HTTPError(response=mock_response)
            mock_response.raise_for_status.side_effect = http_error
            mock_head.return_value = mock_response

            errors = validate_input_urls(config)

            assert len(errors) == 1
            assert "HTTP 404" in errors[0][1]

    def test_connection_error_returns_error(self, test_shell_workflow_recipe_directory):
        """A connection error should return an error."""
        config = get_recipe_config(test_shell_workflow_recipe_directory)

        with patch("ogdc_runner.recipe.requests.head") as mock_head:
            mock_head.side_effect = requests.exceptions.ConnectionError()

            errors = validate_input_urls(config)

            assert len(errors) == 1
            assert "Connection failed" in errors[0][1]

    def test_timeout_returns_error(self, test_shell_workflow_recipe_directory):
        """A timeout should return an error."""
        config = get_recipe_config(test_shell_workflow_recipe_directory)

        with patch("ogdc_runner.recipe.requests.head") as mock_head:
            mock_head.side_effect = requests.exceptions.Timeout()

            errors = validate_input_urls(config, timeout=10)

            assert len(errors) == 1
            assert "Timeout after 10s" in errors[0][1]

    def test_skips_non_url_params(self, test_viz_workflow_recipe_directory):
        """Non-URL params should be skipped."""
        config = get_recipe_config(test_viz_workflow_recipe_directory)

        # Check if this recipe has non-url params
        url_params = [p for p in config.input.params if p.type == "url"]

        with patch("ogdc_runner.recipe.requests.head") as mock_head:
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_head.return_value = mock_response

            errors = validate_input_urls(config)

            assert errors == []
            # Should only be called for URL-type params
            assert mock_head.call_count == len(url_params)
