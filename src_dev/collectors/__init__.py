"""Collectors — imports perezosos para no bloquear order lab si DB local falta."""

from __future__ import annotations

from typing import Any

__all__ = [
    "build_snapshot_from_rest",
    "build_snapshot_from_ws",
    "build_exchange_metrics_context",
    "load_from_db",
    "load_jsonl_tail",
]


def __getattr__(name: str) -> Any:
    if name == "load_from_db":
        from src_dev.collectors.db_sources import load_from_db

        return load_from_db
    if name == "load_jsonl_tail":
        from src_dev.collectors.db_sources import load_jsonl_tail

        return load_jsonl_tail
    if name == "build_exchange_metrics_context":
        from src_dev.collectors.exchange_snapshot import build_exchange_metrics_context

        return build_exchange_metrics_context
    if name == "build_snapshot_from_rest":
        from src_dev.collectors.snapshot_builder import build_snapshot_from_rest

        return build_snapshot_from_rest
    if name == "build_snapshot_from_ws":
        from src_dev.collectors.snapshot_builder import build_snapshot_from_ws

        return build_snapshot_from_ws
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")