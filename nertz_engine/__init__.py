"""Nertz engine — packaged trading system."""

from __future__ import annotations

import sys
from pathlib import Path

__version__ = "0.1.0"

_ROOT = Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
for _path in (_ROOT, _SRC):
    _entry = str(_path)
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from nertz_engine.storage import create_storage

__all__ = ["__version__", "create_storage", "NertzEngine"]


def __getattr__(name: str):
    if name == "NertzEngine":
        from Nertzh import NertzMetalEngine

        return NertzMetalEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")