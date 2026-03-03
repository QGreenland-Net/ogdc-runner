"""Base Pydantic model for ogdc-runner."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class OgdcBaseModel(BaseModel):
    """Base pydantic model for the ogdc-runner."""

    # Disallow "extra" config that we do not expect. We want users to know if
    # they've made a mistake and added something that has no effect.
    model_config = ConfigDict(extra="forbid")
