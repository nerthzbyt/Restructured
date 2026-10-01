"""Construye MarketSnapshot desde REST, WS o DB+REST híbrido."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src_dev.bybit.rest import BybitRestClient
from src_dev.bybit.ws import BybitWsCollector
from src_dev.collectors.db_sources import load_from_db, load_metric_history
from src_dev.config import DevSettings
from src_dev.models.market import MarketSnapshot


async def build_snapshot_from_rest(
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
) -> MarketSnapshot:
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    async with BybitRestClient(cfg) as client:
        raw = await client.fetch_market_snapshot(sym)
    history = load_metric_history(sym, cfg)
    prev_liq = history[-1].get("pio") if history else None
    return MarketSnapshot(
        symbol=sym,
        source="rest",
        candles=raw["candles"],
        orderbook=raw["orderbook"],
        ticker=raw["ticker"],
        recent_trades=raw["recent_trades"],
        instrument_rules=raw["instrument_rules"],
        open_interest_linear=raw.get("open_interest_linear"),
        metric_history=history,
        prev_weighted_liquidity=None,
        rol_dt_s=None,
    )


async def build_snapshot_from_ws(
    duration_s: float = 15.0,
    *,
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
    rest_fallback: bool = True,
) -> MarketSnapshot:
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    collector = BybitWsCollector(cfg)
    collector.symbol = sym
    ws_data = await collector.collect(duration_s=duration_s)

    candles = ws_data.get("candles") or []
    if rest_fallback and (len(candles) < 2 or not ws_data.get("ready")):
        async with BybitRestClient(cfg) as client:
            rest = await client.fetch_market_snapshot(sym)
        if len(candles) < 2:
            candles = rest["candles"]
        if not ws_data.get("ready"):
            ws_data["orderbook"] = rest["orderbook"]
            ws_data["ticker"] = rest["ticker"]
            ws_data["recent_trades"] = rest["recent_trades"]

    history = load_metric_history(sym, cfg)
    return MarketSnapshot(
        symbol=sym,
        source="ws",
        candles=candles,
        orderbook=ws_data.get("orderbook") or {"bids": [], "asks": []},
        ticker=ws_data.get("ticker") or {"last_price": 0.0},
        recent_trades=ws_data.get("recent_trades") or [],
        instrument_rules={},
        metric_history=history,
    )


async def build_snapshot_from_db_hybrid(
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
) -> Tuple[MarketSnapshot, List[str]]:
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    db_raw, notes = load_from_db(sym, settings=cfg)
    async with BybitRestClient(cfg) as client:
        rest = await client.fetch_market_snapshot(sym)

    candles = (db_raw or {}).get("candles") or []
    if len(candles) < 2:
        candles = rest["candles"]
        notes.append("Velas completadas desde REST")

    ob = (db_raw or {}).get("orderbook") or {"bids": [], "asks": []}
    if not ob.get("bids"):
        ob = rest["orderbook"]
        notes.append("Orderbook completado desde REST")

    ticker = (db_raw or {}).get("ticker") or rest["ticker"]
    history = load_metric_history(sym, cfg)

    snap = MarketSnapshot(
        symbol=sym,
        source="db_hybrid",
        candles=candles,
        orderbook=ob,
        ticker=ticker,
        recent_trades=rest["recent_trades"],
        instrument_rules=rest["instrument_rules"],
        open_interest_linear=rest.get("open_interest_linear"),
        metric_history=history,
    )
    return snap, notes