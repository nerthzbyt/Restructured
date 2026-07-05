from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import numpy as np


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return float(v) if bool(np.isfinite(v)) else float(default)


def _extract_trade_metrics(trade: Any) -> Dict[str, float]:
    raw = getattr(trade, "bybit_raw", None)
    if isinstance(raw, dict):
        snap = raw.get("metrics_snapshot")
        if isinstance(snap, dict):
            m = snap.get("metrics")
            if isinstance(m, dict):
                out: Dict[str, float] = {}
                for k, v in m.items():
                    out[str(k)] = _safe_float(v, 0.0)
                return out
    return {
        "combined": _safe_float(getattr(trade, "combined", 0.0), 0.0),
        "ild": _safe_float(getattr(trade, "ild", 0.0), 0.0),
        "egm": _safe_float(getattr(trade, "egm", 0.0), 0.0),
        "rol": _safe_float(getattr(trade, "rol", 0.0), 0.0),
        "pio": _safe_float(getattr(trade, "pio", 0.0), 0.0),
        "ogm": _safe_float(getattr(trade, "ogm", 0.0), 0.0),
        "mom": _safe_float(getattr(trade, "mom", 0.0), 0.0),
        "volatility": 0.0,
        "ema_diff_rel": 0.0,
        "igd_n5_n20": 0.0,
        "cbd_n20": 0.0,
    }


def determine_decision_from_metrics(
    metrics: Dict[str, float],
    *,
    buy_th: float = 6.5,
    sell_th: float = -6.5,
    hold_band: float = 1.5,
    min_egm_filter: float = 0.01
) -> str:
    combined = _safe_float(metrics.get("combined"), 0.0)
    egm = _safe_float(metrics.get("egm"), 0.0)

    if abs(combined) < hold_band:
        return "hold"

    if combined >= buy_th:
        if egm > -min_egm_filter:
            return "buy"
    if combined <= sell_th:
        if egm < min_egm_filter:
            return "sell"

    return "hold"


@dataclass(frozen=True)
class Thresholds:
    combined_buy_threshold: float
    combined_sell_threshold: float
    combined_hold_band: float


@dataclass(frozen=True)
class ThresholdOptimizationResult:
    success: bool
    baseline: Dict[str, Any]
    best: Dict[str, Any]
    searched: int
    timestamp: str


@dataclass(frozen=True)
class CombinedWeights:
    pio: float
    egm: float
    ild: float
    rol: float
    ogm: float
    mom: float
    scale: float = 10.0

    def as_dict(self) -> Dict[str, float]:
        return {
            "pio": float(self.pio),
            "egm": float(self.egm),
            "ild": float(self.ild),
            "rol": float(self.rol),
            "ogm": float(self.ogm),
            "mom": float(self.mom),
            "scale": float(self.scale),
        }


DEFAULT_COMBINED_WEIGHTS = CombinedWeights(
    pio=0.25, egm=0.30, ild=-0.15, rol=0.10, ogm=0.05, mom=0.16, scale=10.0
)


def _recompute_combined(metrics: Dict[str, float], w: CombinedWeights) -> float:
    pio = _safe_float(metrics.get("pio"), 0.0)
    egm = _safe_float(metrics.get("egm"), 0.0)
    ild = _safe_float(metrics.get("ild"), 0.0)
    rol = _safe_float(metrics.get("rol"), 0.0)
    ogm = _safe_float(metrics.get("ogm"), 0.0)
    mom = _safe_float(metrics.get("mom"), 0.0)
    z = (
        float(w.pio) * float(pio)
        + float(w.egm) * float(egm)
        + float(w.ild) * float(ild)
        + float(w.rol) * float(rol)
        + float(w.ogm) * float(ogm)
        + float(w.mom) * float(mom)
    )
    return float(z) * float(w.scale)


def _evaluate_system(trades: Iterable[Any], th: Thresholds, w: Optional[CombinedWeights]) -> Dict[str, Any]:
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

    for t in trades:
        total += 1
        action = str(getattr(t, "action", "") or "").lower()
        if action not in {"buy", "sell"}:
            continue
        pl = _safe_float(getattr(t, "profit_loss", 0.0), 0.0)
        metrics = _extract_trade_metrics(t)
        if isinstance(w, CombinedWeights):
            metrics = dict(metrics)
            metrics["combined"] = _recompute_combined(metrics, w)
        pred = determine_decision_from_metrics(
            metrics,
            buy_th=float(th.combined_buy_threshold),
            sell_th=float(th.combined_sell_threshold),
            hold_band=float(th.combined_hold_band),
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
            "combined_buy_threshold": float(th.combined_buy_threshold),
            "combined_sell_threshold": float(th.combined_sell_threshold),
            "combined_hold_band": float(th.combined_hold_band),
        },
        "weights": (w.as_dict() if isinstance(w, CombinedWeights) else None),
    }


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
    base_eval = _evaluate_system(trades, start, None)

    best_th = start
    best_eval = base_eval

    def score(ev: Dict[str, Any]) -> float:
        net = _safe_float(ev.get("net_profit"), 0.0)
        win_rate = _safe_float(ev.get("win_rate"), 0.0)
        max_dd = _safe_float(ev.get("max_drawdown"), 0.0)
        selected = int(ev.get("selected") or 0)
        total = int(ev.get("total_trades") or 0)
        
        min_sel = max(5, int(0.05 * total))
        penalty = 0.0
        if selected < min_sel:
            penalty = float(min_sel - selected) * 0.5
            
        bonus = 0.0
        if win_rate > 0.6:
            bonus = win_rate * 0.4
            
        consistency = 1.0 if max_dd < 5.0 else 0.0
        return float(net) + float(bonus) + float(consistency) - float(penalty)

    best_score = score(best_eval)

    def clamp(buy: float, sell: float, hold: float) -> Thresholds:
        buy = float(max(1.0, min(15.0, buy)))
        sell = float(-max(1.0, min(15.0, abs(sell))))
        hold = float(max(0.5, min(6.0, hold)))
        return Thresholds(buy, sell, hold)

    for i in range(max(0, int(iterations))):
        explore_global = (i % 10 == 0)
        if explore_global:
            cand = clamp(
                buy=rng.uniform(1.0, 15.0),
                sell=-rng.uniform(1.0, 15.0),
                hold=rng.uniform(0.5, 6.0),
            )
        else:
            cand = clamp(
                buy=float(best_th.combined_buy_threshold) + rng.gauss(0.0, 0.9),
                sell=float(best_th.combined_sell_threshold) + rng.gauss(0.0, 0.9),
                hold=float(best_th.combined_hold_band) + rng.gauss(0.0, 0.35),
            )

        ev = _evaluate_system(trades, cand, None)
        sc = score(ev)
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
    start_w = start_weights if isinstance(start_weights, CombinedWeights) else DEFAULT_COMBINED_WEIGHTS
    vec0 = np.array(
        [start_w.pio, start_w.egm, start_w.ild, start_w.rol, start_w.ogm, start_w.mom],
        dtype=np.float64,
    )
    if not np.all(np.isfinite(vec0)):
        vec0 = np.nan_to_num(vec0, nan=0.0, posinf=0.0, neginf=0.0)
    denom0 = float(np.sum(np.abs(vec0)))
    if denom0 <= 1e-12:
        vec0 = np.array([0.45, 0.30, -0.15, 0.10, 0.05, 0.16], dtype=np.float64)
        denom0 = float(np.sum(np.abs(vec0)))
    vec0 = vec0 / float(denom0)
    start_w = CombinedWeights(
        pio=float(vec0[0]),
        egm=float(vec0[1]),
        ild=float(vec0[2]),
        rol=float(vec0[3]),
        ogm=float(vec0[4]),
        mom=float(vec0[5]),
        scale=float(max(1.0, min(25.0, float(start_w.scale)))),
    )
    baseline = _evaluate_system(trades, start_thresholds, start_w)

    best_th = start_thresholds
    best_w = start_w
    best_eval = baseline

    def score(ev: Dict[str, Any]) -> float:
        net = _safe_float(ev.get("net_profit"), 0.0)
        win_rate = _safe_float(ev.get("win_rate"), 0.0)
        max_dd = _safe_float(ev.get("max_drawdown"), 0.0)
        selected = int(ev.get("selected") or 0)
        total = int(ev.get("total_trades") or 0)
        
        min_sel = max(5, int(0.05 * total))
        penalty = 0.0
        if selected < min_sel:
            penalty = float(min_sel - selected) * 0.5
            
        bonus = 0.0
        if win_rate > 0.6:
            bonus = win_rate * 0.4
            
        consistency = 1.0 if max_dd < 5.0 else 0.0
        return float(net) + float(bonus) + float(consistency) - float(penalty)

    best_score = score(best_eval)

    def clamp_th(buy: float, sell: float, hold: float) -> Thresholds:
        buy = float(max(1.0, min(15.0, buy)))
        sell = float(-max(1.0, min(15.0, abs(sell))))
        hold = float(max(0.5, min(6.0, hold)))
        return Thresholds(buy, sell, hold)

    def normalize_w(
        pio: float, egm: float, ild: float, rol: float, ogm: float, mom: float, scale: float
    ) -> CombinedWeights:
        vec = np.array([pio, egm, ild, rol, ogm, mom], dtype=np.float64)
        if not np.all(np.isfinite(vec)):
            vec = np.nan_to_num(vec, nan=0.0, posinf=0.0, neginf=0.0)
        denom = float(np.sum(np.abs(vec)))
        if denom <= 1e-12:
            vec = np.array([0.45, 0.30, -0.15, 0.10, 0.05, 0.16], dtype=np.float64)
            denom = float(np.sum(np.abs(vec)))
        vec = vec / float(denom)
        scale = float(max(1.0, min(25.0, scale)))
        return CombinedWeights(
            pio=float(vec[0]),
            egm=float(vec[1]),
            ild=float(vec[2]),
            rol=float(vec[3]),
            ogm=float(vec[4]),
            mom=float(vec[5]),
            scale=float(scale),
        )

    for i in range(max(0, int(iterations))):
        explore_global = (i % 10 == 0)
        if explore_global:
            cand_th = clamp_th(
                buy=rng.uniform(1.0, 15.0),
                sell=-rng.uniform(1.0, 15.0),
                hold=rng.uniform(0.5, 6.0),
            )
            cand_w = normalize_w(
                pio=rng.uniform(-1.0, 1.0),
                egm=rng.uniform(-1.0, 1.0),
                ild=rng.uniform(-1.0, 1.0),
                rol=rng.uniform(-1.0, 1.0),
                ogm=rng.uniform(-1.0, 1.0),
                mom=rng.uniform(-1.0, 1.0),
                scale=rng.uniform(6.0, 18.0),
            )
        else:
            cand_th = clamp_th(
                buy=float(best_th.combined_buy_threshold) + rng.gauss(0.0, 0.9),
                sell=float(best_th.combined_sell_threshold) + rng.gauss(0.0, 0.9),
                hold=float(best_th.combined_hold_band) + rng.gauss(0.0, 0.35),
            )
            cand_w = normalize_w(
                pio=float(best_w.pio) + rng.gauss(0.0, 0.08),
                egm=float(best_w.egm) + rng.gauss(0.0, 0.08),
                ild=float(best_w.ild) + rng.gauss(0.0, 0.06),
                rol=float(best_w.rol) + rng.gauss(0.0, 0.06),
                ogm=float(best_w.ogm) + rng.gauss(0.0, 0.04),
                mom=float(best_w.mom) + rng.gauss(0.0, 0.05),
                scale=float(best_w.scale) + rng.gauss(0.0, 1.0),
            )

        ev = _evaluate_system(trades, cand_th, cand_w)
        sc = score(ev)
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
