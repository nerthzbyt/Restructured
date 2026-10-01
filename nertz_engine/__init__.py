"""Nertz engine — packaged trading system."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

__version__ = "0.1.0"

_ROOT = Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
for _path in (_ROOT, _SRC):
    _entry = str(_path)
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from nertz_engine.storage import create_storage

if TYPE_CHECKING:
    # Se resuelve de forma perezosa en __getattr__ (evita importar el motor al cargar el paquete).
    from nertz_core.engine import NertzMetalEngine as NertzEngine

__all__ = ["__version__", "create_storage", "NertzEngine"]


def __getattr__(name: str):
    if name == "NertzEngine":
        from Nertzh import NertzMetalEngine

        return NertzMetalEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")