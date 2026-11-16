from __future__ import annotations

from unittest.mock import patch

import pytest

from ogdc_runner.service import db


@pytest.fixture(autouse=True)
def _fresh_engine_cache():
    db.get_engine.cache_clear()
    yield
    db.get_engine.cache_clear()


def test_engine_url_pins_psycopg2_driver(monkeypatch):
    # Bare `postgresql` resolves to psycopg (v3) on SQLAlchemy 2.1, which is
    # not installed. The declared dependency is psycopg2.
    monkeypatch.setenv("OGDC_DB_USERNAME", "u")
    monkeypatch.setenv("OGDC_DB_PASSWORD", "p")
    monkeypatch.setenv("OGDC_DB_HOST", "h")

    with patch.object(db, "create_engine") as create_engine:
        db.get_engine()

    url = create_engine.call_args.args[0]
    assert url.drivername == "postgresql+psycopg2"
