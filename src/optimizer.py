from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from signal_engine import (
    DEFAULT_COMBINED_WEIGHTS,
    CombinedWeights,
    Thresholds,
    determine_decision_from_metrics,
    recompute_combined,
    symmetrize_threshold_values,
)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return float(v)


def _extract_trade_metrics(trade: Any) -> Dict[str, float]:
    raw = getattr(trade, "bybit_raw", None)
    if isinstance(raw, dict):
        snap = raw.get("metrics_snapshot")
        if isinstance(snap, dict):
            m = snap.get("metrics")
            if isinstance(m, dict):
                return {str(k): _safe_float(v, 0.0) for k, v in m.items()}
    return {
        "combined": _safe_float(getattr(trade, "combined", 0.0), 0.0),
        "ild": _safe_float(getattr(trade, "ild", 0.0), 0.0),
        "egm": _safe_float(getattr(trade, "egm", 0.0), 0.0),
        "rol": _safe_float(getattr(trade, "rol", 0.0), 0.0),
        "pio": _safe_float(getattr(trade, "pio", 0.0), 0.0),
        "ogm": _safe_float(getattr(trade, "ogm", 0.0), 0.0),
        "mom": _safe_float(getattr(trade, "mom", 0.0), 0.0),
        "tfi": _safe_float(getattr(trade, "tfi", 0.0), 0.0),
        "volatility": _safe_float(getattr(trade, "volatility", 0.0), 0.0),
        "ema_diff_rel": _safe_float(getattr(trade, "ema_diff_rel", 0.0), 0.0),
        "igd_n5_n20": _safe_float(getattr(trade, "igd_n5_n20", 0.0), 0.0),
        "cbd_n20": _safe_float(getattr(trade, "cbd_n20", 0.0), 0.0),
        "rvol": _safe_float(getattr(trade, "rvol", 0.0), 0.0),
        "spread_bps": _safe_float(getattr(trade, "spread_bps", 0.0), 0.0),
        "microprice_offset_bps": _safe_float(
            getattr(trade, "microprice_offset_bps", 0.0), 0.0
        ),
    }


@dataclass(frozen=True)
class ThresholdOptimizationResult:
    success: bool
    baseline: Dict[str, Any]
    best: Dict[str, Any]
    searched: int
    timestamp: str


def _evaluate_system(
    trades: Iterable[Any], th: Thresholds, w: Optional[CombinedWeights]
) -> Dict[str, Any]:
    selected = 0
    wins = 0
    losses = 0
    net_profit = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    total = 0
    running_pl = 0.0
    peak = 0.0
    max_dd = 0.0

    sym_th = th.symmetrized()

    for t in trades:
        total += 1
        action = str(getattr(t, "action", "") or "").lower()
        if action not in {"buy", "sell"}:
            continue
        pl = _safe_float(getattr(t, "profit_loss", 0.0), 0.0)
        metrics = _extract_trade_metrics(t)
        if isinstance(w, CombinedWeights):
            metrics = dict(metrics)
            metrics["combined"] = recompute_combined(metrics, w)
        pred = determine_decision_from_metrics(
            metrics,
            buy_th=float(sym_th.combined_buy_threshold),
            sell_th=float(sym_th.combined_sell_threshold),
            hold_band=float(sym_th.combined_hold_band),
        )
        if pred != action:
            continue
        selected += 1
        net_profit += float(pl)
        running_pl += float(pl)
        if running_pl > peak:
            peak = running_pl
        dd = peak - running_pl
        if dd > max_dd:
            max_dd = dd
        if pl > 0:
            wins += 1
            gross_profit += float(pl)
        elif pl < 0:
            losses += 1
            gross_loss += float(pl)

    win_rate = float(wins) / float(selected) if selected > 0 else 0.0
    avg_profit = float(net_profit) / float(selected) if selected > 0 else 0.0
    return {
        "total_trades": int(total),
        "selected": int(selected),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": float(win_rate),
        "net_profit": float(net_profit),
        "gross_profit": float(gross_profit),
        "gross_loss": float(gross_loss),
        "avg_profit": float(avg_profit),
        "max_drawdown": float(max_dd),
        "thresholds": {
            "combined_buy_threshold": float(sym_th.combined_buy_threshold),
            "combined_sell_threshold": float(sym_th.combined_sell_threshold),
            "combined_hold_band": float(sym_th.combined_hold_band),
        },
        "weights": (w.as_dict() if isinstance(w, CombinedWeights) else None),
    }


def _score_system(ev: Dict[str, Any]) -> float:
    net = _safe_float(ev.get("net_profit"), 0.0)
    win_rate = _safe_float(ev.get("win_rate"), 0.0)
    max_dd = _safe_float(ev.get("max_drawdown"), 0.0)
    selected = int(ev.get("selected") or 0)
    total = int(ev.get("total_trades") or 0)
    min_sel = max(5, int(0.05 * total))
    penalty = float(min_sel - selected) * 0.5 if selected < min_sel else 0.0
    bonus = win_rate * 0.4 if win_rate > 0.6 else 0.0
    consistency = 1.0 if max_dd < 5.0 else 0.0
    return float(net) + float(bonus) + float(consistency) - float(penalty)


def _clamp_symmetric(magnitude: float, hold: float) -> Thresholds:
    mag = float(max(1.0, min(15.0, magnitude)))
    hb = float(max(0.5, min(6.0, hold)))
    return Thresholds(mag, -mag, hb)


def optimize_thresholds_from_trades(
    trades: list[Any],
    *,
    start: Thresholds,
    iterations: int = 400,
    seed: Optional[int] = None,
) -> ThresholdOptimizationResult:
    if not isinstance(trades, list) or not trades:
        return ThresholdOptimizationResult(
            success=False,
            baseline={"ok": False, "error": "no_trades"},
            best={"ok": False, "error": "no_trades"},
            searched=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    rng = random.Random(int(seed) if seed is not None else None)
    start_sym = start.symmetrized()
    base_eval = _evaluate_system(trades, start_sym, None)
    best_th = start_sym
    best_eval = base_eval
    best_score = _score_system(best_eval)
    start_mag = abs(best_th.combined_buy_threshold)

    for i in range(max(0, int(iterations))):
        if i % 10 == 0:
            cand = _clamp_symmetric(
                magnitude=rng.uniform(1.0, 15.0),
                hold=rng.uniform(0.5, 6.0),
            )
        else:
            cand = _clamp_symmetric(
                magnitude=float(start_mag if i < 2 else abs(best_th.combined_buy_threshold))
                + rng.gauss(0.0, 0.9),
                hold=float(best_th.combined_hold_band) + rng.gauss(0.0, 0.35),
            )
        ev = _evaluate_system(trades, cand, None)
        sc = _score_system(ev)
        if sc > best_score:
            best_score = sc
            best_th = cand
            best_eval = ev

    return ThresholdOptimizationResult(
        success=True,
        baseline=base_eval,
        best=best_eval,
        searched=int(max(0, int(iterations))),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@dataclass(frozen=True)
class SystemOptimizationResult:
    success: bool
    baseline: Dict[str, Any]
    best: Dict[str, Any]
    searched: int
    timestamp: str


def optimize_system_from_trades(
    trades: list[Any],
    *,
    start_thresholds: Thresholds,
    start_weights: Optional[CombinedWeights] = None,
    iterations: int = 900,
    seed: Optional[int] = None,
) -> SystemOptimizationResult:
    if not isinstance(trades, list) or not trades:
        return SystemOptimizationResult(
            success=False,
            baseline={"ok": False, "error": "no_trades"},
            best={"ok": False, "error": "no_trades"},
            searched=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    rng = random.Random(int(seed) if seed is not None else None)
    start_w = (
        start_weights
        if isinstance(start_weights, CombinedWeights)
        else DEFAULT_COMBINED_WEIGHTS
    )
    start_w = CombinedWeights.normalize(
        pio=start_w.pio,
        egm=start_w.egm,
        ild=start_w.ild,
        rol=start_w.rol,
        ogm=start_w.ogm,
        mom=start_w.mom,
        tfi=start_w.tfi,
        scale=start_w.scale,
    )
    start_th = start_thresholds.symmetrized()
    baseline = _evaluate_system(trades, start_th, start_w)
    best_th = start_th
    best_w = start_w
    best_eval = baseline
    best_score = _score_system(best_eval)

    for i in range(max(0, int(iterations))):
        if i % 10 == 0:
            cand_th = _clamp_symmetric(
                magnitude=rng.uniform(1.0, 15.0),
                hold=rng.uniform(0.5, 6.0),
            )
            cand_w = CombinedWeights.normalize(
                pio=rng.uniform(-1.0, 1.0),
                egm=rng.uniform(-1.0, 1.0),
                ild=rng.uniform(-1.0, 1.0),
                rol=rng.uniform(-1.0, 1.0),
                ogm=rng.uniform(-1.0, 1.0),
                mom=rng.uniform(-1.0, 1.0),
                tfi=rng.uniform(-1.0, 1.0),
                scale=rng.uniform(6.0, 18.0),
            )
        else:
            cand_th = _clamp_symmetric(
                magnitude=float(best_th.combined_buy_threshold) + rng.gauss(0.0, 0.9),
                hold=float(best_th.combined_hold_band) + rng.gauss(0.0, 0.35),
            )
            cand_w = CombinedWeights.normalize(
                pio=float(best_w.pio) + rng.gauss(0.0, 0.08),
                egm=float(best_w.egm) + rng.gauss(0.0, 0.08),
                ild=float(best_w.ild) + rng.gauss(0.0, 0.06),
                rol=float(best_w.rol) + rng.gauss(0.0, 0.06),
                ogm=float(best_w.ogm) + rng.gauss(0.0, 0.04),
                mom=float(best_w.mom) + rng.gauss(0.0, 0.05),
                tfi=float(best_w.tfi) + rng.gauss(0.0, 0.08),
                scale=float(best_w.scale) + rng.gauss(0.0, 1.0),
            )

        ev = _evaluate_system(trades, cand_th, cand_w)
        sc = _score_system(ev)
        if sc > best_score:
            best_score = sc
            best_th = cand_th
            best_w = cand_w
            best_eval = ev

    best_payload = dict(best_eval)
    best_payload["thresholds"] = {
        "combined_buy_threshold": float(best_th.combined_buy_threshold),
        "combined_sell_threshold": float(best_th.combined_sell_threshold),
        "combined_hold_band": float(best_th.combined_hold_band),
    }
    best_payload["weights"] = best_w.as_dict()

    return SystemOptimizationResult(
        success=True,
        baseline=baseline,
        best=best_payload,
        searched=int(max(0, int(iterations))),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def thresholds_from_config(
    buy: float, sell: float, hold: float
) -> Thresholds:
    return symmetrize_threshold_values(buy, sell, hold)