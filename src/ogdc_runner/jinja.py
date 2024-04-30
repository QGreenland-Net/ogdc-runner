from __future__ import annotations

from jinja2 import Environment, PackageLoader, StrictUndefined

j2_environment = Environment(
    loader=PackageLoader("ogdc_runner"), undefined=StrictUndefined
)
