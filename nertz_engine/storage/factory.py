"""Storage backend factory."""

from __future__ import annotations

from typing import Any

from nertz_engine.storage.base import StorageBackend
from nertz_engine.storage.duckdb_backend import DuckDBBackend

_SUPPORTED_BACKENDS = frozenset({"duckdb", "duck", "sqlite_legacy", "sqlite", "legacy"})


def create_storage(backend: str, path: str, **kwargs: Any) -> StorageBackend | None:
    """Create a storage backend instance.

    Args:
        backend: ``duckdb`` for DuckDB; ``sqlite_legacy`` keeps legacy SQLite in Nertzh.py.
        path: Database file path (used by DuckDB).
        **kwargs: Forwarded to :class:`DuckDBBackend` (e.g. ``flush_interval_ms``).

    Returns:
        A started-ready backend, or ``None`` when legacy SQLite should be used.
    """
    normalized = (backend or "").strip().lower()

    if normalized in {"duckdb", "duck"}:
        return DuckDBBackend(path=path, **kwargs)

    if normalized in {"sqlite_legacy", "sqlite", "legacy"}:
        return None

    raise ValueError(
        f"Unsupported storage backend {backend!r}. "
        f"Supported values: {', '.join(sorted(_SUPPORTED_BACKENDS))}"
    )