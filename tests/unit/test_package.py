from __future__ import annotations

import importlib.metadata

import ogdc_runner as m


def test_version():
    assert importlib.metadata.version("ogdc_runner") == m.__version__
