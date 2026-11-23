from __future__ import annotations

import pytest

from ogdc_runner.service import db


@pytest.fixture(autouse=True)
def _fresh_engine_cache():
    db.get_engine.cache_clear()
    yield
    db.get_engine.cache_clear()
