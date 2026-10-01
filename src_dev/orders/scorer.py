"""Scoring data-driven — métricas live + historial real del exchange, sin pesos mágicos."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src_dev.orders.combinator import OrderCombination
from src_dev.orders.exchange_schema import SpotInstrumentConstraints


@dataclass
class ScoredCombo:
    combo: OrderCombination
    score: float
    rank_factors: Dict[str, float]
    body_preview: Dict[str, object]
    rationale: List[str]


def combo_exchange_key(combo: OrderCombination) -> str:
    flt = combo.order_filter or "Order"
    return f"{combo.order_type}|{combo.time_in_force}|{flt}"


def _observed_frequency(
    combo: OrderCombination,
    observed_counts: Dict[str, int],
) -> float:
    key = combo_exchange_key(combo)
    total = sum(int(v) for v in observed_counts.values())
    if total <= 0:
        return 0.0
    return float(observed_counts.get(key, 0)) / float(total)


def _clamp01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))


def score_combination(
    combo: OrderCombination,
    metrics: Dict[str, Any],
    ob_stats: Dict[str, Any],
    constraints: SpotInstrumentConstraints,
    *,
    body_preview: Dict[str, object],
    thresholds: Dict[str, float],
    observed_combo_counts: Dict[str, int],
    min_calibration_samples: int,
    metric_history_len: int,
) -> ScoredCombo:
    combined = float(metrics.get("combined") or 0.0)
    volatility = float(metrics.get("volatility") or 0.0)
    spread_bps = float(ob_stats.get("spread_bps") or metrics.get("spread_bps") or 0.0)
    pio = float(metrics.get("pio") or 0.0)
    egm = float(metrics.get("egm") or 0.0)
    tfi = float(metrics.get("tfi") or metrics.get("recent_trades_imbalance_qty_pct") or 0.0)
    depth_imb = float(ob_stats.get("depth_imbalance_qty") or 0.0)
    calibrated = bool(metrics.get("metrics_calibrated", False))

    buy_th = float(thresholds["combined_buy"])
    sell_th = float(thresholds["combined_sell"])
    hold_band = float(thresholds.get("combined_hold_band") or abs(buy_th))

    signal_strength = _clamp01(abs(combined) / max(abs(buy_th), 1e-12))
    is_buy = combined >= buy_th
    is_sell = combined <= sell_th
    in_hold = not is_buy and not is_sell

    rationale: List[str] = []
    factors: Dict[str, float] = {}

    cal_ratio = _clamp01(float(metric_history_len) / max(1, int(min_calibration_samples)))
    factors["calibration"] = cal_ratio if calibrated else cal_ratio * 0.5
    if not calibrated:
        rationale.append(f"calibración incompleta ({metric_history_len}/{min_calibration_samples} muestras)")

    if combo.order_type == "Limit":
        factors["signal_type_fit"] = _clamp01(1.0 - signal_strength) if in_hold else signal_strength * 0.7 + 0.3
    else:
        factors["signal_type_fit"] = signal_strength

    if combo.time_in_force == "PostOnly":
        factors["spread_fit"] = _clamp01(1.0 - spread_bps / max(spread_bps + 1.0, 1e-9))
    else:
        factors["spread_fit"] = _clamp01(1.0 - spread_bps / (hold_band * 100.0 + 1e-9))

    vol_ref = max(volatility, 1e-9)
    if combo.order_type == "Market":
        factors["vol_regime"] = _clamp01(volatility / (vol_ref + float(thresholds["sl_pct"]) / 100.0))
    else:
        factors["vol_regime"] = _clamp01(1.0 - volatility / (vol_ref + 0.002))

    side_buy = combo.side_hint == "Buy"
    micro_raw = (pio + depth_imb + tfi) / 3.0 if side_buy else -(pio + depth_imb + tfi) / 3.0
    factors["microstructure"] = _clamp01(0.5 + 0.5 * micro_raw) * _clamp01(0.5 + abs(egm))

    if combo.price_anchor in ("best_bid", "microprice", "combined_chase") and side_buy:
        factors["price_side"] = 0.9 if is_buy else 0.5
    elif combo.price_anchor in ("best_ask",) and not side_buy:
        factors["price_side"] = 0.9 if is_sell else 0.5
    elif combo.price_anchor:
        factors["price_side"] = 0.65
    else:
        factors["price_side"] = 0.7

    obs = _observed_frequency(combo, observed_combo_counts)
    factors["exchange_observed"] = obs
    if obs > 0:
        rationale.append(f"perfil observado en exchange: {obs:.2%} del historial")

    factors["instrument_ok"] = 1.0 if constraints.status == "Trading" else 0.0

    # Market spot: TP/SL nativo no monitoreable en panel (tpsl_panel_verify)
    if combo.order_type == "Market":
        factors["exchange_tpsl_fit"] = 0.4 if combo.tp_sl_mode != "none" else 0.72
        if combo.tp_sl_mode != "none":
            rationale.append("Market+TP/SL: sin monitoreo fiable en panel Bybit spot")
    elif combo.tp_sl_mode == "bracket_on_limit":
        factors["exchange_tpsl_fit"] = 0.95
    elif combo.tp_sl_mode == "none":
        factors["exchange_tpsl_fit"] = 0.88
    else:
        factors["exchange_tpsl_fit"] = 0.75

    active = [v for v in factors.values()]
    score = (sum(active) / len(active)) if active else 0.0
    score = round(score * 100.0, 4)

    return ScoredCombo(
        combo=combo,
        score=score,
        rank_factors={k: round(v, 4) for k, v in factors.items()},
        body_preview=body_preview,
        rationale=rationale,
    )


def rank_all(scored: List[ScoredCombo]) -> List[ScoredCombo]:
    seen: set[str] = set()
    ranked: List[ScoredCombo] = []
    for item in sorted(scored, key=lambda x: x.score, reverse=True):
        key = item.combo.combo_id()
        if key in seen:
            continue
        seen.add(key)
        ranked.append(item)
    return ranked


def rank_top_n(scored: List[ScoredCombo], n: int) -> List[ScoredCombo]:
    return rank_all(scored)[: max(1, int(n))]