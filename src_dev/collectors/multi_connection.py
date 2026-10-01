"""Agrega datos de todas las conexiones exchange — sin fuentes locales."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from src_dev.bybit.rest import BybitRestClient
from src_dev.bybit.ws import BybitWsCollector
from src_dev.collectors.exchange_snapshot import build_exchange_metrics_context
from src_dev.config import DevSettings
from src_dev.orders.credential_validator import validate_all_connections
from src_dev.orders.exchange_catalog import fetch_exchange_orders, fetch_instrument_constraints


async def build_multi_connection_context(
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
    *,
    ws_duration_s: float = 10.0,
) -> Dict[str, Any]:
    """
    Contexto unificado:
    - REST público (velas, book, ticker, trades)
    - WS público (book/ticker/trades live)
    - REST privado (órdenes, historial) vía credenciales
    - Métricas utils con historial acumulado en memoria (muestreos REST)
    """
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol

    connection_debug = await validate_all_connections(sym, cfg, ws_probe_s=ws_duration_s)
    metrics_ctx = await build_exchange_metrics_context(
        sym,
        cfg,
        history_samples=cfg.lab_history_samples,
        history_interval_s=cfg.lab_history_interval_s,
    )
    constraints = await fetch_instrument_constraints(sym, cfg)
    exchange_orders = await fetch_exchange_orders(sym, settings=cfg)

    ws = BybitWsCollector(cfg)
    ws.symbol = sym
    ws_snapshot = await ws.collect(duration_s=ws_duration_s)

    rest_snap: Dict[str, Any] = {}
    async with BybitRestClient(cfg) as client:
        rest_snap = await client.fetch_market_snapshot(sym, include_oi=False)

    merged_market = _merge_market(rest_snap, ws_snapshot, metrics_ctx.get("market") or {})

    return {
        "ts": time.time(),
        "symbol": sym,
        "local_sources_used": False,
        "connection_debug": connection_debug,
        "constraints": constraints,
        "exchange_orders": exchange_orders,
        "metrics": metrics_ctx.get("metrics") or {},
        "orderbook_stats": metrics_ctx.get("orderbook_stats") or {},
        "metric_history_len": metrics_ctx.get("metric_history_len"),
        "market": merged_market,
        "sources": {
            "rest_public": bool(rest_snap.get("candles")),
            "ws_public": bool(ws_snapshot.get("ready")),
            "rest_private_orders": bool(exchange_orders.get("authenticated")),
            "metrics_rest_series": int(metrics_ctx.get("history_samples_taken") or 0),
        },
    }


def _merge_market(
    rest: Dict[str, Any],
    ws: Dict[str, Any],
    metrics_market: Dict[str, Any],
) -> Dict[str, Any]:
    """Prioridad: WS para book/ticker reciente, REST para velas completas."""
    candles = rest.get("candles") or metrics_market.get("candles") or ws.get("candles") or []
    orderbook = ws.get("orderbook") if ws.get("ready") else (rest.get("orderbook") or {})
    ticker = ws.get("ticker") if float((ws.get("ticker") or {}).get("last_price") or 0) > 0 else (
        rest.get("ticker") or {}
    )
    trades = ws.get("recent_trades") or rest.get("recent_trades") or []
    if len(trades) < 5:
        trades = rest.get("recent_trades") or trades
    return {
        "candles": candles,
        "orderbook": orderbook,
        "ticker": ticker,
        "recent_trades": trades,
        "instrument_rules": rest.get("instrument_rules") or {},
    }