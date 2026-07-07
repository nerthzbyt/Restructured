"""
Shim de compatibilidad — configuración canónica en src/settings.py.
"""
from __future__ import annotations

import os
import sys

_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from settings import ConfigSettings  # noqa: E402

__all__ = ["ConfigSettings"]