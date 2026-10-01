"""Estructuras de datos unificadas para validación."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MarketSnapshot:
    symbol: str
    source: str
    ts: float = field(default_factory=time.time)
    candles: List[Dict[str, float]] = field(default_factory=list)
    orderbook: Dict[str, Any] = field(default_factory=dict)
    ticker: Dict[str, Any] = field(default_factory=dict)
    recent_trades: List[Dict[str, Any]] = field(default_factory=list)
    instrument_rules: Dict[str, float] = field(default_factory=dict)
    open_interest_linear: Optional[float] = None
    metric_history: List[Dict[str, float]] = field(default_factory=list)
    prev_weighted_liquidity: Optional[float] = None
    rol_dt_s: Optional[float] = None

    @property
    def last_price(self) -> float:
        lp = float(self.ticker.get("last_price") or 0.0)
        if lp > 0:
            return lp
        if self.candles:
            return float(self.candles[0].get("close") or 0.0)
        return 0.0

    @property
    def data_ready(self) -> bool:
        return (
            len(self.candles) >= 2
            and bool(self.orderbook.get("bids"))
            and bool(self.orderbook.get("asks"))
            and self.last_price > 0
        )

    def to_ticker_payload(self, settings: Any) -> Dict[str, Any]:
        return {
            **self.ticker,
            "last_price": self.last_price,
            "orderbook_lambda": float(getattr(settings, "orderbook_lambda", 0.03)),
            "orderbook_pct_band": float(getattr(settings, "orderbook_pct_band", 0.015)),
            "ild_target_move": float(getattr(settings, "ild_target_move", 0.002)),
            "metric_history": list(self.metric_history),
            "prev_weighted_liquidity": self.prev_weighted_liquidity,
            "rol_dt_s": self.rol_dt_s,
            "combined_weights": dict(getattr(settings, "combined_weights", {})),
        }


@dataclass
class MetricValidationReport:
    symbol: str
    source: str
    ts: float
    data_ok: bool
    metrics_calibrated: bool
    utils_metrics: Dict[str, Any]
    reference_raw: Dict[str, float]
    raw_checks: Dict[str, Dict[str, Any]]
    jsonl_compare: Optional[Dict[str, Any]] = None
    live_api_compare: Optional[Dict[str, Any]] = None
    orderbook_stats: Optional[Dict[str, Any]] = None
    passed: bool = False
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "source": self.source,
            "ts": self.ts,
            "data_ok": self.data_ok,
            "metrics_calibrated": self.metrics_calibrated,
            "passed": self.passed,
            "notes": self.notes,
            "utils_combined": self.utils_metrics.get("combined"),
            "utils_pio_raw": self.utils_metrics.get("pio_raw"),
            "reference_pio_raw": self.reference_raw.get("pio_raw"),
            "raw_checks": self.raw_checks,
            "jsonl_compare": self.jsonl_compare,
            "live_api_compare": self.live_api_compare,
            "orderbook_stats": self.orderbook_stats,
        }