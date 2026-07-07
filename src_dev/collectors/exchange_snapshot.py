"""Snapshot 100% exchange — sin SQLite, JSONL ni DuckDB locales."""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from src_dev.analysis.orderbook_stats import analyze_orderbook
from src_dev.bybit.rest import BybitRestClient
from src_dev.config import DevSettings

from utils import calculate_metrics


async def _single_fetch(symbol: str, settings: DevSettings) -> Dict[str, Any]:
    async with BybitRestClient(settings) as client:
        return await client.fetch_market_snapshot(symbol, include_oi=False)


async def build_exchange_metrics_context(
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
    *,
    history_samples: int = 6,
    history_interval_s: float = 2.0,
) -> Dict[str, Any]:
    """
    Construye contexto de métricas solo desde Bybit REST.
    Acumula historial in-memory (ventana corta) para calibrar z-scores.
    """
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    metric_history: List[Dict[str, float]] = []
    last_wl: Optional[float] = None
    last_ts: Optional[float] = None
    latest_raw: Optional[Dict[str, Any]] = None
    latest_metrics: Dict[str, Any] = {}

    samples = max(2, int(history_samples))
    for i in range(samples):
        raw = await _single_fetch(sym, cfg)
        latest_raw = raw
        now_ts = time.time()

        ticker_payload = {
            **(raw.get("ticker") or {}),
            "last_price": float((raw.get("ticker") or {}).get("last_price") or 0.0),
            "orderbook_lambda": cfg.orderbook_lambda,
            "orderbook_pct_band": cfg.orderbook_pct_band,
            "ild_target_move": cfg.ild_target_move,
            "metric_history": list(metric_history),
            "prev_weighted_liquidity": last_wl,
            "rol_dt_s": (now_ts - last_ts) if last_ts else None,
            "combined_weights": dict(cfg.combined_weights),
        }
        candles = raw.get("candles") or []
        orderbook = raw.get("orderbook") or {"bids": [], "asks": []}
        trades = raw.get("recent_trades") or []

        metrics = calculate_metrics(
            candles,
            orderbook,
            ticker_payload,
            depth=int(cfg.orderbook_depth),
            recent_trades=trades,
        )
        latest_metrics = metrics

        if metrics.get("data_ok"):
            metric_history.append(
                {
                    "ts": now_ts,
                    "pio": float(metrics.get("pio_raw") or 0.0),
                    "ild": float(metrics.get("ild_raw") or 0.0),
                    "egm": float(metrics.get("egm_raw") or 0.0),
                    "rol": float(metrics.get("rol_raw") or 0.0),
                    "ogm": float(metrics.get("ogm_raw") or 0.0),
                    "mom_raw": float(metrics.get("mom_raw") or 0.0),
                    "asymmetry": float(metrics.get("asymmetry") or 0.0),
                    "spread_pct": float(metrics.get("spread_pct") or 0.0),
                }
            )
            wl = metrics.get("weighted_liquidity")
            if wl is not None:
                last_wl = float(wl)
            last_ts = now_ts

        if i < samples - 1:
            await asyncio.sleep(max(0.5, float(history_interval_s)))

    assert latest_raw is not None
    ob_stats = analyze_orderbook(
        latest_raw.get("orderbook") or {},
        depth=int(cfg.orderbook_depth),
        last_price=float((latest_raw.get("ticker") or {}).get("last_price") or 0.0),
    )

    return {
        "symbol": sym,
        "ts": time.time(),
        "source": "bybit_exchange_only",
        "market": latest_raw,
        "metrics": latest_metrics,
        "orderbook_stats": ob_stats,
        "metric_history_len": len(metric_history),
        "history_samples_taken": samples,
    }