from __future__ import annotations

from click.testing import CliRunner

from ogdc_runner.__main__ import cli


def test_submit_greenland_ice_sheet_recipe(monkeypatch):
    """Test submitting greenland-ice-sheet recipe with DataONE input.

    This recipe demonstrates:
    - DataONE input with wildcard filename pattern (percent_gris_*.nc)
    - Fetching multiple files (3 NetCDF)
    - Processing NetCDF raster data with GDAL
    - Converting to GeoTIFF format
    """
    monkeypatch.setenv("ENVIRONMENT", "local")

    # Submit greenland-ice-sheet recipe and wait until completion.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "submit",
            "--wait",
            "--overwrite",
            # TODO: change to main once ogdc-recipe PR is merged
            "github://qgreenland-net:ogdc-recipes@dataone-type/recipes/greenland-ice-sheet",
        ],
    )

    assert result.exit_code == 0
