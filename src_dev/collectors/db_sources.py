"""Carga datos del sistema productivo: SQLite, JSONL, DuckDB."""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from src_dev.config import DATA_DIR, DevSettings

try:
    from utils import load_metrics_raw_history_from_jsonl
except ImportError:
    load_metrics_raw_history_from_jsonl = None  # type: ignore


def load_jsonl_tail(
    symbol: str,
    *,
    limit: int = 5,
    settings: Optional[DevSettings] = None,
) -> List[Dict[str, Any]]:
    cfg = settings or DevSettings.from_env()
    path = cfg.jsonl_path
    if not os.path.isfile(path):
        return []
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("symbol", "")).strip() == symbol:
                rows.append(row)
    return rows[-limit:]


def load_metric_history(symbol: str, settings: Optional[DevSettings] = None) -> List[Dict[str, float]]:
    cfg = settings or DevSettings.from_env()
    if load_metrics_raw_history_from_jsonl is None:
        return []
    window_s = max(60.0, float(cfg.metrics_window_minutes) * 60.0)
    return load_metrics_raw_history_from_jsonl(DATA_DIR, symbol, window_s=window_s)


def load_from_db(
    symbol: str,
    *,
    candle_limit: int = 50,
    settings: Optional[DevSettings] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Lee market_data, orderbook y ticker de trading.db si existen."""
    cfg = settings or DevSettings.from_env()
    notes: List[str] = []
    if not os.path.isfile(cfg.sqlite_path):
        notes.append(f"SQLite no encontrado: {cfg.sqlite_path}")
        return None, notes

    conn = sqlite3.connect(cfg.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        candles = _query_candles(conn, symbol, candle_limit)
        if not candles:
            notes.append("Sin velas en market_data — usar REST como fallback")

        ob = _query_latest_orderbook(conn, symbol)
        ticker = _query_latest_ticker(conn, symbol)
        if not ob.get("bids"):
            notes.append("Orderbook DB vacío — usar REST")

        return {
            "symbol": symbol,
            "candles": candles,
            "orderbook": ob,
            "ticker": ticker or {"last_price": 0.0},
            "recent_trades": [],
            "instrument_rules": {},
            "open_interest_linear": None,
        }, notes
    finally:
        conn.close()


def _query_candles(conn: sqlite3.Connection, symbol: str, limit: int) -> List[Dict[str, float]]:
    try:
        cur = conn.execute(
            """
            SELECT open, high, low, close, volume
            FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        return [
            {
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            }
            for r in cur.fetchall()
        ]
    except sqlite3.Error:
        return []


def _query_latest_orderbook(conn: sqlite3.Connection, symbol: str) -> Dict[str, Any]:
    try:
        cur = conn.execute(
            """
            SELECT bids, asks FROM orderbook
            WHERE symbol = ?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if not row:
            return {"bids": [], "asks": []}
        bids = json.loads(row["bids"]) if row["bids"] else []
        asks = json.loads(row["asks"]) if row["asks"] else []
        return {"bids": bids, "asks": asks}
    except (sqlite3.Error, json.JSONDecodeError):
        return {"bids": [], "asks": []}


def _query_latest_ticker(conn: sqlite3.Connection, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        cur = conn.execute(
            """
            SELECT last_price, volume_24h, high_24h, low_24h
            FROM market_ticker
            WHERE symbol = ?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "last_price": float(row["last_price"] or 0.0),
            "volume_24h": float(row["volume_24h"] or 0.0),
            "high_24h": float(row["high_24h"] or 0.0),
            "low_24h": float(row["low_24h"] or 0.0),
        }
    except sqlite3.Error:
        return None