"""Registro completo de señales + comparación con bot :8787."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src_dev.horizons.multi_metrics import (
    append_history_from_metrics,
    compute_horizon_grid,
    compute_metrics_for_profile,
    evaluate_profile_signal,
)
from src_dev.horizons.profiles import HorizonProfile, load_horizon_profiles

if TYPE_CHECKING:
    from src_dev.config import DevSettings
    from src_dev.models.market import MarketSnapshot


def fetch_live_bot_state(
    symbol: str,
    base_url: str,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"symbol": symbol, "reachable": False}
    base = base_url.rstrip("/")
    endpoints = {
        "metrics": f"{base}/api/metrics/{symbol}",
        "prediction": f"{base}/agent/prediction-level/{symbol}",
        "status": f"{base}/api/status",
    }
    for key, url in endpoints.items():
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                out[key] = json.loads(resp.read().decode())
            out["reachable"] = True
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            out[key] = {"error": str(exc)}
    return out


def compare_with_live_bot(
    production_eval: Dict[str, Any],
    live: Dict[str, Any],
) -> Dict[str, Any]:
    live_metrics = (live.get("metrics") or {}).get("metrics") or {}
    live_pred = live.get("prediction") or {}

    dev_decision = str((production_eval.get("signal") or {}).get("decision") or "hold")
    live_decision = str(live_pred.get("decision") or "hold")
    dev_level = str((production_eval.get("prediction_level") or {}).get("level") or "L0")
    live_level = str(live_pred.get("level") or "L0")

    dev_combined = float((production_eval.get("signal") or {}).get("combined") or 0.0)
    live_combined = float(live_metrics.get("combined") or 0.0)

    blockers_dev = (production_eval.get("signal") or {}).get("blockers") or []
    blockers_live = live_pred.get("blockers") or []

    return {
        "decision_match": dev_decision == live_decision,
        "level_match": dev_level == live_level,
        "dev_decision": dev_decision,
        "live_decision": live_decision,
        "dev_level": dev_level,
        "live_level": live_level,
        "dev_combined": dev_combined,
        "live_combined": live_combined,
        "combined_abs_diff": abs(dev_combined - live_combined),
        "dev_blockers": blockers_dev,
        "live_blockers": blockers_live,
        "blockers_only_in_dev": [b for b in blockers_dev if b not in blockers_live],
        "blockers_only_in_live": [b for b in blockers_live if b not in blockers_dev],
        "live_calibrated": bool(live_metrics.get("metrics_calibrated")),
        "interpretation": _interpret_mismatch(
            dev_decision,
            live_decision,
            dev_level,
            live_level,
            blockers_dev,
            blockers_live,
            dev_combined,
            live_combined,
        ),
    }


def _interpret_mismatch(
    dev_decision: str,
    live_decision: str,
    dev_level: str,
    live_level: str,
    blockers_dev: List[str],
    blockers_live: List[str],
    dev_combined: float,
    live_combined: float,
) -> str:
    if dev_decision == live_decision and dev_level == live_level:
        return "alineado"
    if abs(dev_combined - live_combined) > 2.0:
        return "divergencia_calibracion_o_timing"
    if blockers_dev and not blockers_live:
        return "dev_mas_conservador_o_sin_calibrar"
    if blockers_live and not blockers_dev:
        return "live_mas_conservador"
    if dev_level != live_level and dev_decision == live_decision:
        return "mismo_decision_distinto_nivel"
    return "revision_manual"


def build_observation(
    snapshot: "MarketSnapshot",
    settings: "DevSettings",
    *,
    history_buffer: List[Dict[str, Any]],
    thresholds: Dict[str, float],
    live_state: Optional[Dict[str, Any]] = None,
    ml_proba: Optional[float] = None,
    forward_label: Optional[int] = None,
    forward_return_bps: Optional[float] = None,
) -> Dict[str, Any]:
    profiles = load_horizon_profiles(settings)
    prod_profile = HorizonProfile(
        name="production",
        orderbook_depth=int(settings.orderbook_depth),
        tfi_window=10,
        candle_limit=50,
        history_window_min=float(settings.metrics_window_minutes),
    )

    ts = time.time()
    prod_metrics = compute_metrics_for_profile(
        snapshot,
        prod_profile,
        settings,
        history_buffer=history_buffer,
        now_ts=ts,
    )
    prod_eval = evaluate_profile_signal(prod_metrics, thresholds)
    append_history_from_metrics(history_buffer, prod_metrics, now_ts=ts)

    horizons = compute_horizon_grid(
        snapshot,
        [p for p in profiles if p.name != "production"],
        settings,
        history_buffer=history_buffer,
        thresholds=thresholds,
        now_ts=ts,
    )
    horizons["production"] = {
        "profile": prod_profile.as_dict(),
        "metrics": {
            "combined": prod_metrics.get("combined"),
            "combined_z": prod_metrics.get("combined_z"),
            "pio": prod_metrics.get("pio"),
            "egm": prod_metrics.get("egm"),
            "tfi": prod_metrics.get("tfi"),
            "mom": prod_metrics.get("mom"),
            "metrics_calibrated": prod_metrics.get("metrics_calibrated"),
            "data_ok": prod_metrics.get("data_ok"),
        },
        "decision": prod_eval["signal"].get("decision"),
        "market_state": prod_eval["signal"].get("market_state"),
        "blockers": prod_eval["signal"].get("blockers") or [],
        "level": prod_eval["prediction_level"].get("level"),
        "level_name": prod_eval["prediction_level"].get("name"),
        "confidence_pct": prod_eval["prediction_level"].get("confidence_pct"),
        "execution_gate": prod_eval["execution_gate"],
    }

    obs: Dict[str, Any] = {
        "ts": ts,
        "symbol": snapshot.symbol,
        "price": snapshot.last_price,
        "production": {
            "metrics": prod_metrics,
            "signal": prod_eval["signal"],
            "prediction_level": prod_eval["prediction_level"],
            "execution_gate": prod_eval["execution_gate"],
        },
        "horizons": horizons,
        "thresholds": thresholds,
        "history_len": len(history_buffer),
    }

    if live_state:
        obs["live_bot"] = live_state
        obs["comparison"] = compare_with_live_bot(prod_eval, live_state)

    if ml_proba is not None:
        obs["ml_dev"] = {"p_success": ml_proba}

    if forward_label is not None:
        obs["forward_label"] = int(forward_label)
    if forward_return_bps is not None:
        obs["forward_return_bps"] = float(forward_return_bps)

    return obs


def append_observation(path: str, obs: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obs, ensure_ascii=False) + "\n")