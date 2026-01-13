from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests  # type: ignore[import-untyped]
from pydantic import ValidationError

from ogdc_runner.recipe import get_recipe_config


def test_get_recipe_config(test_shell_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_shell_workflow_recipe_directory,
    )

    assert config.id == "test-ogdc-workflow"


@pytest.mark.parametrize(
    ("side_effect", "expected_error"),
    [
        (requests.exceptions.HTTPError(response=Mock(status_code=404)), "HTTP 404"),
        (requests.exceptions.ConnectionError(), "Connection failed"),
        (requests.exceptions.Timeout(), "Timeout"),
    ],
)
def test_get_recipe_config_url_validation_errors(
    test_shell_workflow_recipe_directory, side_effect, expected_error
):
    """URL validation returns appropriate errors for various failure modes."""
    with patch("ogdc_runner.models.recipe_config.requests.head") as mock_head:
        mock_head.side_effect = side_effect
        with pytest.raises(ValidationError) as exc_info:
            get_recipe_config(test_shell_workflow_recipe_directory, check_urls=True)

    assert expected_error in str(exc_info.value)


def test_get_recipe_config_url_validation_success(test_shell_workflow_recipe_directory):
    """Accessible URLs return no errors."""
    with patch("ogdc_runner.models.recipe_config.requests.head") as mock_head:
        mock_head.return_value = Mock()
        config = get_recipe_config(
            test_shell_workflow_recipe_directory, check_urls=True
        )

    assert config.id == "test-ogdc-workflow"
