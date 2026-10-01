"""Calcula métricas y señales por perfil de horizonte — reutiliza utils/signal_engine."""
from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src_dev.horizons.profiles import HorizonProfile

if TYPE_CHECKING:
    from src_dev.config import DevSettings
    from src_dev.models.market import MarketSnapshot

_PROJECT_ROOT = __file__
for _ in range(3):
    _PROJECT_ROOT = __import__("os").path.dirname(_PROJECT_ROOT)
_SRC = __import__("os").path.join(_PROJECT_ROOT, "src")
_NERT_PRO = __import__("os").path.join(_PROJECT_ROOT, "NerT_AI_PRO")
for _p in (_SRC, _NERT_PRO):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils import calculate_metrics  # noqa: E402
from signal_engine import evaluate_signal, check_execution_gates  # noqa: E402
from intelligence_catalog import compute_prediction_level  # noqa: E402


def _filter_history(
    buffer: List[Dict[str, Any]],
    window_min: float,
    *,
    now_ts: Optional[float] = None,
) -> List[Dict[str, float]]:
    cutoff = float(now_ts or time.time()) - max(60.0, float(window_min) * 60.0)
    out: List[Dict[str, float]] = []
    for row in buffer:
        if not isinstance(row, dict):
            continue
        ts = row.get("ts")
        if ts is not None and float(ts) < cutoff:
            continue
        payload = {k: v for k, v in row.items() if k != "ts"}
        out.append({str(k): float(v) for k, v in payload.items() if _is_finite(v)})
    return out


def _is_finite(v: Any) -> bool:
    try:
        import math

        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def _prev_weighted_liquidity(buffer: List[Dict[str, Any]]) -> tuple[Optional[float], Optional[float]]:
    if not buffer:
        return None, None
    last = buffer[-1]
    wl = last.get("weighted_liquidity")
    ts = last.get("ts")
    try:
        return float(wl), float(ts)
    except (TypeError, ValueError):
        return None, None


def compute_metrics_for_profile(
    snapshot: "MarketSnapshot",
    profile: HorizonProfile,
    settings: "DevSettings",
    *,
    history_buffer: List[Dict[str, Any]],
    now_ts: Optional[float] = None,
) -> Dict[str, Any]:
    ts = float(now_ts or time.time())
    history_payload = _filter_history(history_buffer, profile.history_window_min, now_ts=ts)
    prev_liq, prev_ts = _prev_weighted_liquidity(history_buffer)

    candles = list(snapshot.candles or [])[: profile.candle_limit]
    trades = list(snapshot.recent_trades or [])[-profile.tfi_window :]

    ticker_payload = {
        **(snapshot.ticker or {}),
        "last_price": snapshot.last_price,
        "orderbook_lambda": float(settings.orderbook_lambda),
        "orderbook_pct_band": float(settings.orderbook_pct_band),
        "ild_target_move": float(settings.ild_target_move),
        "metric_history": history_payload,
        "prev_weighted_liquidity": prev_liq,
        "rol_dt_s": (ts - float(prev_ts)) if prev_ts else None,
        "combined_weights": dict(settings.combined_weights or {}),
    }

    metrics = calculate_metrics(
        candles,
        snapshot.orderbook or {"bids": [], "asks": []},
        ticker_payload,
        depth=int(profile.orderbook_depth),
        recent_trades=trades,
    )
    return metrics if isinstance(metrics, dict) else {}


def evaluate_profile_signal(
    metrics: Dict[str, Any],
    thresholds: Dict[str, float],
) -> Dict[str, Any]:
    buy_th = float(thresholds.get("combined_buy") or 4.5)
    sell_th = float(thresholds.get("combined_sell") or -4.5)
    hold_band = float(thresholds.get("combined_hold_band") or 1.5)

    ev = evaluate_signal(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
    )
    level = compute_prediction_level(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
    )
    gate_ok, gate_reason = check_execution_gates(metrics)
    return {
        "signal": ev,
        "prediction_level": level,
        "execution_gate": {"ok": gate_ok, "reason": gate_reason},
        "thresholds": {
            "buy": buy_th,
            "sell": sell_th,
            "hold_band": hold_band,
        },
    }


def append_history_from_metrics(
    buffer: List[Dict[str, Any]],
    metrics: Dict[str, Any],
    *,
    now_ts: Optional[float] = None,
    max_len: int = 500,
) -> None:
    if not metrics.get("data_ok"):
        return
    buffer.append(
        {
            "ts": float(now_ts or time.time()),
            "pio": float(metrics.get("pio_raw") or 0.0),
            "ild": float(metrics.get("ild_raw") or 0.0),
            "egm": float(metrics.get("egm_raw") or 0.0),
            "rol": float(metrics.get("rol_raw") or 0.0),
            "ogm": float(metrics.get("ogm_raw") or 0.0),
            "mom_raw": float(metrics.get("mom_raw") or 0.0),
            "asymmetry": float(metrics.get("asymmetry") or 0.0),
            "spread_pct": float(metrics.get("spread_pct") or 0.0),
            "weighted_liquidity": float(metrics.get("weighted_liquidity") or 0.0),
        }
    )
    if len(buffer) > max_len:
        del buffer[: len(buffer) - max_len]


def compute_horizon_grid(
    snapshot: "MarketSnapshot",
    profiles: List[HorizonProfile],
    settings: "DevSettings",
    *,
    history_buffer: List[Dict[str, Any]],
    thresholds: Dict[str, float],
    now_ts: Optional[float] = None,
) -> Dict[str, Any]:
    rows: Dict[str, Any] = {}
    for profile in profiles:
        metrics = compute_metrics_for_profile(
            snapshot,
            profile,
            settings,
            history_buffer=history_buffer,
            now_ts=now_ts,
        )
        evaluation = evaluate_profile_signal(metrics, thresholds)
        rows[profile.name] = {
            "profile": profile.as_dict(),
            "metrics": {
                "combined": metrics.get("combined"),
                "combined_z": metrics.get("combined_z"),
                "pio": metrics.get("pio"),
                "egm": metrics.get("egm"),
                "tfi": metrics.get("tfi"),
                "mom": metrics.get("mom"),
                "metrics_calibrated": metrics.get("metrics_calibrated"),
                "data_ok": metrics.get("data_ok"),
                "pio_raw": metrics.get("pio_raw"),
                "ild_raw": metrics.get("ild_raw"),
            },
            "decision": evaluation["signal"].get("decision"),
            "market_state": evaluation["signal"].get("market_state"),
            "blockers": evaluation["signal"].get("blockers") or [],
            "confirmations": evaluation["signal"].get("confirmations") or {},
            "level": evaluation["prediction_level"].get("level"),
            "level_name": evaluation["prediction_level"].get("name"),
            "confidence_pct": evaluation["prediction_level"].get("confidence_pct"),
            "execution_gate": evaluation["execution_gate"],
        }
    return rows