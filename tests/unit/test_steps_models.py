"""Tests for the experimental steps/templates recipe schema."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

from ogdc_runner.models.steps import StepsMeta, Template, VizTemplate

REPO = Path(__file__).parents[1] / "test_steps_recipe_repo"
TEMPLATES = TypeAdapter(list[Template])


def base_doc() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load(
        (REPO / "recipes/sea-ice-conc/meta.yml").read_text()
    )
    return doc


def test_fixture_recipe_parses():
    meta = StepsMeta.model_validate(base_doc())
    assert [s.name for s in meta.recipe.steps] == ["to-percent", "reproject", "package"]
    assert meta.recipe.steps[2].with_ == {"COMPRESS": "DEFLATE", "PREDICTOR": 2}
    assert meta.templates_from == ["templates/raster/compress-overviews.yml"]


def test_shared_template_file_parses():
    doc = yaml.safe_load((REPO / "templates/raster/compress-overviews.yml").read_text())
    (tpl,) = TEMPLATES.validate_python(doc["templates"])
    assert tpl.name == "compress-overviews"
    assert tpl.mode == "map"


def test_viz_call_template_parses():
    (tpl,) = TEMPLATES.validate_python(
        [
            {
                "name": "render-band1",
                "type": "visualization",
                "call": {
                    "function": "pdgraster.operations:render_web_tile",
                    "kwargs": {"input_path": {"$ref": "item.path"}, "band": 1},
                },
            }
        ]
    )
    assert isinstance(tpl, VizTemplate)
    assert tpl.call is not None
    assert tpl.call.function == "pdgraster.operations:render_web_tile"


def _viz(**over: Any) -> dict[str, Any]:
    return {"name": "v", "type": "visualization", **over}


@pytest.mark.parametrize(
    "mutate",
    [
        # legacy `workflow` together with the new form
        lambda d: d.update(workflow={"type": "shell"}),
        lambda d: d["recipe"].update(steps=[]),
        lambda d: d["recipe"].update(steps=d["recipe"]["steps"] * 7),  # > 20 steps
        lambda d: d["recipe"]["steps"][0].update(name="Not_K8s"),
        lambda d: d["recipe"]["steps"][0].update(unexpected=1),
        lambda d: d["recipe"]["steps"][2].update(**{"with": {"X": [1]}}),
        lambda d: d.update(templates=[{"name": "x", "type": "shell"}]),  # no sh_file
        lambda d: d["templates"][0].update(sh_file="../a.sh"),
        lambda d: d["templates"][0].update(parameters={"lower": {}}),
        lambda d: d.update(templates_from=["../templates/a.yml"]),
        lambda d: d.update(templates_from=["https://x.test/templates/a.yml"]),
        lambda d: d.update(templates_from=["templates/a.txt"]),
        lambda d: d.update(templates=[_viz()]),  # neither call nor config_file
        lambda d: d.update(
            templates=[_viz(config_file="c.json", call={"function": "a:b"})]
        ),
        lambda d: d.update(templates=[_viz(call={"factory": "a:B"})]),  # no method
    ],
)
def test_structural_rejections(mutate):
    doc = base_doc()
    mutate(doc)
    with pytest.raises(ValidationError):
        StepsMeta.model_validate(doc)
