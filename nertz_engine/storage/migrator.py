"""Migrate legacy SQLite / JSONL data into DuckDB."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

from nertz_engine.storage.duckdb_backend import DuckDBBackend
from nertz_engine.storage.base import MetricRow


def migrate_metrics_jsonl(
    jsonl_path: str,
    duckdb_path: str,
    *,
    limit: int = 500_000,
) -> dict[str, Any]:
    """Import metrics_snapshots.jsonl rows into DuckDB metric_snapshots."""
    path = os.path.abspath(str(jsonl_path))
    if not os.path.exists(path):
        return {"ok": False, "error": "jsonl_not_found", "path": path}

    backend = DuckDBBackend(duckdb_path, flush_interval_ms=100.0)
    backend.connect()

    imported = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if imported >= int(limit):
                break
            s = str(line or "").strip()
            if not s:
                continue
            try:
                rec = json.loads(s)
            except ValueError:
                continue
            if not isinstance(rec, dict):
                continue
            ts_raw = rec.get("timestamp") or rec.get("ts")
            try:
                if isinstance(ts_raw, (int, float)):
                    ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                elif isinstance(ts_raw, str) and ts_raw:
                    ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                else:
                    ts = datetime.now(timezone.utc)
            except (TypeError, ValueError, OverflowError, OSError):
                ts = datetime.now(timezone.utc)
            metrics_raw = rec.get("metrics")
            metrics: dict[str, Any] = metrics_raw if isinstance(metrics_raw, dict) else {}
            thresholds_raw = rec.get("thresholds")
            thresholds: dict[str, Any] = thresholds_raw if isinstance(thresholds_raw, dict) else {}
            row = MetricRow(
                timestamp=ts,
                symbol=str(rec.get("symbol") or ""),
                last_price=float(rec.get("last_price") or 0.0),
                decision=str(rec.get("decision") or "hold"),
                combined=float(metrics.get("combined") or rec.get("combined") or 0.0),
                ild=float(metrics.get("ild") or 0.0),
                egm=float(metrics.get("egm") or 0.0),
                rol=float(metrics.get("rol") or 0.0),
                pio=float(metrics.get("pio") or 0.0),
                ogm=float(metrics.get("ogm") or 0.0),
                volatility=float(metrics.get("volatility") or 0.0),
                thresholds=thresholds,
                metrics=metrics,
            )
            backend.insert_metrics_now([row])
            imported += 1

    backend.close()
    return {"ok": True, "imported": imported, "duckdb_path": os.path.abspath(duckdb_path)}


def migrate_sqlite_trades(
    sqlite_path: str,
    duckdb_path: str,
    *,
    limit: int = 100_000,
) -> dict[str, Any]:
    """Copy trades table from legacy SQLite into DuckDB engine_events for audit."""
    sp = os.path.abspath(str(sqlite_path))
    if not os.path.exists(sp):
        return {"ok": False, "error": "sqlite_not_found", "path": sp}

    con = sqlite3.connect(sp)
    try:
        rows = con.execute(
            "SELECT timestamp, symbol, action, profit_loss, combined FROM trades ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    finally:
        con.close()

    backend = DuckDBBackend(duckdb_path, flush_interval_ms=100.0)
    backend.connect()
    from nertz_engine.storage.base import EventRow

    imported = 0
    batch: list[EventRow] = []
    for ts_s, symbol, action, pl, combined in rows:
        try:
            ts = datetime.fromisoformat(str(ts_s).replace("Z", "+00:00"))
        except ValueError:
            ts = datetime.now(timezone.utc)
        batch.append(
            EventRow(
                timestamp=ts,
                event_type="legacy_trade",
                symbol=str(symbol or ""),
                payload={
                    "action": str(action or ""),
                    "profit_loss": float(pl or 0.0),
                    "combined": float(combined or 0.0),
                },
            )
        )
        if len(batch) >= 500:
            backend.insert_events_now(batch)
            imported += len(batch)
            batch = []
    if batch:
        backend.insert_events_now(batch)
        imported += len(batch)
    backend.close()
    return {"ok": True, "imported": imported, "duckdb_path": os.path.abspath(duckdb_path)}