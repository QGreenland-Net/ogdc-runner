from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import AnyUrl, ValidationError

from ogdc_runner.models.recipe_config import (
    InputParam,
    RecipeConfig,
    RecipeInput,
    RecipeOutput,
    ShellWorkflow,
)


def test_recipe_meta(tmpdir):
    fake_sh_file = tmpdir / "foo.sh"
    fake_sh_file.write('echo "FAKE PROCESSING"')
    recipe_input = RecipeInput(
        params=[InputParam(value=AnyUrl("http://www.example.com"), type="url")]
    )
    recipe_output = RecipeOutput(dataone_id="12345")
    name = "Test Recipe"
    recipe_id = "test-recipe"

    recipe_meta = RecipeConfig(
        name=name,
        input=recipe_input,
        output=recipe_output,
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(fake_sh_file).name},
            context={"recipe_directory": Path(tmpdir)},
        ),
        recipe_directory=Path(tmpdir),
    )

    assert recipe_meta.name == name
    assert recipe_meta.id == recipe_id
    assert recipe_meta.input == recipe_input
    assert recipe_meta.output == recipe_output


def test_recipe_meta_failure_bad_id():
    recipe_input = RecipeInput(
        params=[InputParam(value=AnyUrl("http://www.example.com"), type="url")]
    )
    recipe_output = RecipeOutput(dataone_id="12345")

    # This name should raise a validation error, as `*` is not allowed.
    name = "Test Recipe*"

    with pytest.raises(ValidationError):
        RecipeConfig(
            name=name,
            input=recipe_input,
            output=recipe_output,
            workflow=ShellWorkflow(),
            recipe_directory=Path("/foo/"),
        )


@pytest.fixture
def mock_dataone_response():
    """Mock successful DataONE resolver response."""
    return [
        {
            "identifier": "urn:uuid:data-object-1",
            "url": "https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3Adata-object-1",
            "filename": "test_data.csv",
            "format_id": "text/csv",
            "size": 1000,
            "entity_name": "Test Data File",
            "entity_description": "A test data file",
        }
    ]


@pytest.fixture
def recipe_with_dataone_input(tmpdir):
    """Create a recipe directory with DataONE input config."""
    fake_sh_file = tmpdir / "foo.sh"
    fake_sh_file.write('echo "FAKE PROCESSING"')
    return {
        "tmpdir": tmpdir,
        "sh_file": fake_sh_file,
    }


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_successful_dataone_resolution(
    mock_resolve,
    recipe_with_dataone_input,
    mock_dataone_response,
):
    """Test successful resolution of DataONE package identifier."""
    mock_resolve.return_value = mock_dataone_response

    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="dataone",
                value="urn:uuid:test-package-id",
                member_node="https://test.dataone.org/mn",
            )
        ]
    )

    config = RecipeConfig(
        name="Test Recipe",
        input=recipe_input,
        output=RecipeOutput(dataone_id="12345"),
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
            context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
        ),
        recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
    )

    # Verify resolver was called
    mock_resolve.assert_called_once_with(
        dataset_identifier="urn:uuid:test-package-id",
        member_node="https://test.dataone.org/mn",
    )

    # Verify resolved fields were populated
    param = config.input.params[0]
    assert param._resolved_url == mock_dataone_response[0]["url"]
    assert param._entity_name == mock_dataone_response[0]["entity_name"]
    assert param._entity_description == mock_dataone_response[0]["entity_description"]
    assert param._format_id == mock_dataone_response[0]["format_id"]
    assert param._dataset_pid == "urn:uuid:test-package-id"


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_dataone_resolution_with_default_member_node(
    mock_resolve,
    recipe_with_dataone_input,
    mock_dataone_response,
):
    """Test that default member node is used when not specified."""
    mock_resolve.return_value = mock_dataone_response

    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="dataone",
                value="urn:uuid:test-package-id",
            )
        ]
    )

    RecipeConfig(
        name="Test Recipe",
        input=recipe_input,
        output=RecipeOutput(dataone_id="12345"),
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
            context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
        ),
        recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
    )

    # Verify default member node was used
    mock_resolve.assert_called_once()
    call_kwargs = mock_resolve.call_args[1]
    assert call_kwargs["member_node"] == "https://arcticdata.io/metacat/d1/mn"


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_dataone_resolution_no_data_objects(
    mock_resolve,
    recipe_with_dataone_input,
):
    """Test handling when no data objects are found in package."""
    mock_resolve.return_value = []

    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="dataone",
                value="urn:uuid:empty-package-id",
            )
        ]
    )

    with pytest.raises(ValueError, match="No data objects found in dataset"):
        RecipeConfig(
            name="Test Recipe",
            input=recipe_input,
            output=RecipeOutput(dataone_id="12345"),
            workflow=ShellWorkflow.model_validate(
                {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
                context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
            ),
            recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
        )


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_dataone_resolution_api_error(
    mock_resolve,
    recipe_with_dataone_input,
):
    """Test handling when DataONE API is unavailable."""
    mock_resolve.side_effect = Exception("Connection timeout")

    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="dataone",
                value="urn:uuid:test-package-id",
            )
        ]
    )

    with pytest.raises(ValueError, match="Failed to resolve DataONE package"):
        RecipeConfig(
            name="Test Recipe",
            input=recipe_input,
            output=RecipeOutput(dataone_id="12345"),
            workflow=ShellWorkflow.model_validate(
                {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
                context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
            ),
            recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
        )


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_dataone_resolution_only_for_dataone_type(
    mock_resolve,
    recipe_with_dataone_input,
    mock_dataone_response,
):
    """Test that resolution only happens for dataone input type."""
    mock_resolve.return_value = mock_dataone_response

    # Use URL type instead
    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="url",
                value=AnyUrl("https://example.com/data.csv"),
            )
        ]
    )

    config = RecipeConfig(
        name="Test Recipe",
        input=recipe_input,
        output=RecipeOutput(dataone_id="12345"),
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
            context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
        ),
        recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
    )

    # Resolver should NOT have been called
    mock_resolve.assert_not_called()

    # Resolved fields should be None
    param = config.input.params[0]
    assert param._resolved_url is None


@patch("ogdc_runner.models.recipe_config.resolve_dataone_input")
def test_dataone_resolution_uses_first_object(
    mock_resolve,
    recipe_with_dataone_input,
):
    """Test that first data object is used when multiple are returned."""
    # Mock response with multiple objects
    mock_resolve.return_value = [
        {
            "identifier": "urn:uuid:first-object",
            "url": "https://example.com/first",
            "filename": "first.csv",
            "format_id": "text/csv",
            "size": 1000,
            "entity_name": "First Object",
            "entity_description": "First",
        },
        {
            "identifier": "urn:uuid:second-object",
            "url": "https://example.com/second",
            "filename": "second.csv",
            "format_id": "text/csv",
            "size": 2000,
            "entity_name": "Second Object",
            "entity_description": "Second",
        },
    ]

    recipe_input = RecipeInput(
        params=[
            InputParam(
                type="dataone",
                value="urn:uuid:test-package-id",
            )
        ]
    )

    config = RecipeConfig(
        name="Test Recipe",
        input=recipe_input,
        output=RecipeOutput(dataone_id="12345"),
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(recipe_with_dataone_input["sh_file"]).name},
            context={"recipe_directory": Path(recipe_with_dataone_input["tmpdir"])},
        ),
        recipe_directory=Path(recipe_with_dataone_input["tmpdir"]),
    )

    # Should use first object
    param = config.input.params[0]
    assert param._resolved_url == "https://example.com/first"
    assert param._entity_name == "First Object"
