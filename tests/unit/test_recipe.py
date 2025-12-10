from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests

from ogdc_runner.recipe import get_recipe_config, validate_input_urls


def test_get_recipe_config(test_shell_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_shell_workflow_recipe_directory,
    )

    assert config.id == "test-ogdc-workflow"


def _http_error(status_code):
    """Create an HTTPError with the given status code."""
    response = Mock()
    response.status_code = status_code
    return requests.exceptions.HTTPError(response=response)


@pytest.mark.parametrize(
    ("side_effect", "expected_error"),
    [
        (_http_error(404), "HTTP 404"),
        (requests.exceptions.ConnectionError(), "Connection failed"),
        (requests.exceptions.Timeout(), "Timeout"),
    ],
)
def test_validate_input_urls_errors(
    test_shell_workflow_recipe_directory, side_effect, expected_error
):
    """URL validation returns appropriate errors for various failure modes."""
    config = get_recipe_config(test_shell_workflow_recipe_directory)

    with patch("ogdc_runner.recipe.requests.head") as mock_head:
        mock_head.side_effect = side_effect
        errors = validate_input_urls(config)

    assert len(errors) == 1
    assert expected_error in errors[0][1]


def test_validate_input_urls_success(test_shell_workflow_recipe_directory):
    """Accessible URLs return no errors."""
    config = get_recipe_config(test_shell_workflow_recipe_directory)

    with patch("ogdc_runner.recipe.requests.head") as mock_head:
        mock_head.return_value = Mock()
        errors = validate_input_urls(config)

    assert errors == []
