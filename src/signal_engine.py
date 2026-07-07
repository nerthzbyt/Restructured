"""
Motor unificado de señales: umbrales simétricos, clasificación de mercado,
vetos de flujo tóxico y gates de ejecución.

Usado por Nertzh (runtime), optimizer (backtest) y NerT_AI_PRO (agente).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# --- Referencias calibradas (validación exchange 2026-07-04) ---
BASE_VOL_REF = 0.002
RVOL_MIN = 5e-6
TRADE_AGE_MAX_S = 4.0
TFI_VETO_EXTREME = 0.8
TFI_ALIGN_OPTIMAL = 0.8
TFI_CHOP_BAND = 0.3
VOL_CHOP_MAX = 0.0004
VOL_OPTIMAL_MIN = 0.0008
COMBINED_Z_OPTIMAL = 1.2
COMBINED_Z_CHOP = 0.8
MOM_BREAKOUT = 0.5
TFI_BREAKOUT = 0.9
PIO_SPOOF_Z_MIN = 0.8
MICROPRICE_VETO_BPS = 0.003
SPREAD_VETO_MULT = 1.5

_WEIGHT_FALLBACK = (0.25, 0.30, -0.15, 0.10, 0.05, 0.16, 0.25)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return float(v) if bool(np.isfinite(v)) else float(default)


def _metric(metrics: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for k in keys:
        if k in metrics and metrics[k] is not None:
            return _safe_float(metrics[k], default)
    return float(default)


class MarketState(str, Enum):
    OPTIMAL = "optimal"
    CHOP = "chop"
    TOXIC = "toxic"
    BREAKOUT = "breakout"
    NEUTRAL = "neutral"


@dataclass(frozen=True)
class Thresholds:
    combined_buy_threshold: float
    combined_sell_threshold: float
    combined_hold_band: float

    def symmetrized(self) -> Thresholds:
        base = (abs(self.combined_buy_threshold) + abs(self.combined_sell_threshold)) / 2.0
        return Thresholds(
            combined_buy_threshold=float(base),
            combined_sell_threshold=float(-base),
            combined_hold_band=float(self.combined_hold_band),
        )

    def scaled_by_volatility(self, volatility: float) -> Thresholds:
        th = self.symmetrized()
        vol = _safe_float(volatility, 0.0)
        if vol <= 0 or vol >= BASE_VOL_REF:
            return th
        scale = float((BASE_VOL_REF / vol) ** 0.5)
        scale = max(1.0, min(2.0, scale))
        return Thresholds(
            combined_buy_threshold=th.combined_buy_threshold * scale,
            combined_sell_threshold=th.combined_sell_threshold * scale,
            combined_hold_band=th.combined_hold_band * min(2.0, scale),
        )


@dataclass(frozen=True)
class CombinedWeights:
    pio: float
    egm: float
    ild: float
    rol: float
    ogm: float
    mom: float
    tfi: float
    scale: float = 10.0

    def as_dict(self) -> Dict[str, float]:
        return {
            "pio": float(self.pio),
            "egm": float(self.egm),
            "ild": float(self.ild),
            "rol": float(self.rol),
            "ogm": float(self.ogm),
            "mom": float(self.mom),
            "tfi": float(self.tfi),
            "scale": float(self.scale),
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> CombinedWeights:
        d = data if isinstance(data, dict) else {}
        return cls.normalize(
            pio=_safe_float(d.get("pio"), _WEIGHT_FALLBACK[0]),
            egm=_safe_float(d.get("egm"), _WEIGHT_FALLBACK[1]),
            ild=_safe_float(d.get("ild"), _WEIGHT_FALLBACK[2]),
            rol=_safe_float(d.get("rol"), _WEIGHT_FALLBACK[3]),
            ogm=_safe_float(d.get("ogm"), _WEIGHT_FALLBACK[4]),
            mom=_safe_float(d.get("mom"), _WEIGHT_FALLBACK[5]),
            tfi=_safe_float(d.get("tfi"), _WEIGHT_FALLBACK[6]),
            scale=_safe_float(d.get("scale"), 10.0),
        )

    @classmethod
    def normalize(
        cls,
        *,
        pio: float,
        egm: float,
        ild: float,
        rol: float,
        ogm: float,
        mom: float,
        tfi: float,
        scale: float = 10.0,
    ) -> CombinedWeights:
        vec = np.array([pio, egm, ild, rol, ogm, mom, tfi], dtype=np.float64)
        if not np.all(np.isfinite(vec)):
            vec = np.nan_to_num(vec, nan=0.0, posinf=0.0, neginf=0.0)
        denom = float(np.sum(np.abs(vec)))
        if denom <= 1e-12:
            vec = np.array(_WEIGHT_FALLBACK, dtype=np.float64)
            denom = float(np.sum(np.abs(vec)))
        vec = vec / denom
        scale = float(max(1.0, min(25.0, scale)))
        return cls(
            pio=float(vec[0]),
            egm=float(vec[1]),
            ild=float(vec[2]),
            rol=float(vec[3]),
            ogm=float(vec[4]),
            mom=float(vec[5]),
            tfi=float(vec[6]),
            scale=scale,
        )


DEFAULT_COMBINED_WEIGHTS = CombinedWeights.normalize(
    pio=0.25,
    egm=0.30,
    ild=-0.15,
    rol=0.10,
    ogm=0.05,
    mom=0.16,
    tfi=0.25,
    scale=10.0,
)


def symmetrize_threshold_values(
    buy_th: float, sell_th: float, hold_band: float
) -> Thresholds:
    return Thresholds(
        combined_buy_threshold=float(buy_th),
        combined_sell_threshold=float(sell_th),
        combined_hold_band=float(hold_band),
    ).symmetrized()


def recompute_combined(metrics: Dict[str, float], w: CombinedWeights) -> float:
    z = (
        float(w.pio) * _metric(metrics, "pio")
        + float(w.egm) * _metric(metrics, "egm")
        + float(w.ild) * _metric(metrics, "ild")
        + float(w.rol) * _metric(metrics, "rol")
        + float(w.ogm) * _metric(metrics, "ogm")
        + float(w.mom) * _metric(metrics, "mom")
        + float(w.tfi) * _metric(metrics, "tfi", "recent_trades_imbalance_qty_pct")
    )
    return float(z) * float(w.scale)


def normalize_signal_metrics(metrics: Dict[str, Any]) -> Dict[str, float]:
    m = metrics if isinstance(metrics, dict) else {}
    scale = _metric(m, "combined_weights.scale", default=10.0)
    if scale <= 0:
        scale = 10.0
    combined = _metric(m, "combined")
    combined_z = _metric(m, "combined_z", default=combined / scale if scale else 0.0)
    tfi = _metric(m, "tfi", "recent_trades_imbalance_qty_pct")
    last_age = m.get("recent_trades_last_trade_age_s")
    if last_age is None:
        last_age = m.get("last_trade_age_s")
    return {
        "combined": combined,
        "combined_z": combined_z,
        "pio": _metric(m, "pio"),
        "egm": _metric(m, "egm"),
        "ild": _metric(m, "ild"),
        "rol": _metric(m, "rol"),
        "ogm": _metric(m, "ogm"),
        "mom": _metric(m, "mom"),
        "tfi": tfi,
        "volatility": _metric(m, "volatility"),
        "ema_diff_rel": _metric(m, "ema_diff_rel"),
        "igd_n5_n20": _metric(m, "igd_n5_n20"),
        "cbd_n20": _metric(m, "cbd_n20"),
        "rvol": _metric(m, "rvol", "recent_trades_rvol"),
        "spread_bps": _metric(m, "spread_bps"),
        "obi": _metric(m, "obi", "obi_notional"),
        "microprice_offset_bps": _metric(m, "microprice_offset_bps", "microprice_offset"),
        "metrics_calibrated": 1.0 if bool(m.get("metrics_calibrated", True)) else 0.0,
        "data_ok": 1.0 if bool(m.get("data_ok", True)) else 0.0,
        "recent_trades_last_trade_age_s": _safe_float(last_age, -1.0)
        if last_age is not None
        else -1.0,
    }


def is_spoof_trap(sig: Dict[str, float]) -> bool:
    """PIO/combined fuerte en un sentido con TFI agresivo opuesto (spoofing)."""
    pio = sig["pio"]
    tfi = sig["tfi"]
    combined = sig["combined"]
    rvol = sig["rvol"]

    bullish_book = pio >= PIO_SPOOF_Z_MIN or combined >= 6.0
    bearish_book = pio <= -PIO_SPOOF_Z_MIN or combined <= -6.0

    if bullish_book and tfi <= -TFI_VETO_EXTREME:
        return rvol < RVOL_MIN * 10 or abs(pio) >= PIO_SPOOF_Z_MIN
    if bearish_book and tfi >= TFI_VETO_EXTREME:
        return rvol < RVOL_MIN * 10 or abs(pio) >= PIO_SPOOF_Z_MIN
    return False


def microprice_conflicts_signal(sig: Dict[str, float], side: str) -> bool:
    offset = sig["microprice_offset_bps"]
    if abs(offset) < MICROPRICE_VETO_BPS:
        return False
    if side == "buy" and offset < -MICROPRICE_VETO_BPS:
        return True
    if side == "sell" and offset > MICROPRICE_VETO_BPS:
        return True
    return False


def classify_market_state(sig: Dict[str, float], th: Thresholds) -> MarketState:
    if is_spoof_trap(sig):
        return MarketState.TOXIC

    vol = sig["volatility"]
    tfi = sig["tfi"]
    cz = abs(sig["combined_z"])
    mom = sig["mom"]

    if (
        mom >= MOM_BREAKOUT
        and tfi >= TFI_BREAKOUT
        and sig["ema_diff_rel"] > 0
    ) or (
        mom <= -MOM_BREAKOUT
        and tfi <= -TFI_BREAKOUT
        and sig["ema_diff_rel"] < 0
    ):
        return MarketState.BREAKOUT

    if vol > 0 and vol < VOL_CHOP_MAX and abs(tfi) < TFI_CHOP_BAND:
        return MarketState.CHOP

    if (
        cz >= COMBINED_Z_OPTIMAL
        and vol >= VOL_OPTIMAL_MIN
        and sig["rvol"] >= RVOL_MIN
        and (
            (sig["combined"] >= th.combined_buy_threshold and tfi >= TFI_ALIGN_OPTIMAL)
            or (sig["combined"] <= th.combined_sell_threshold and tfi <= -TFI_ALIGN_OPTIMAL)
        )
    ):
        return MarketState.OPTIMAL

    if cz < COMBINED_Z_CHOP and abs(tfi) < TFI_CHOP_BAND:
        return MarketState.CHOP

    return MarketState.NEUTRAL


def _classic_buy(sig: Dict[str, float]) -> bool:
    return sig["pio"] > 0 and sig["egm"] > 0 and sig["mom"] > 0.05


def _classic_sell(sig: Dict[str, float]) -> bool:
    return sig["pio"] < 0 and sig["egm"] < 0 and sig["mom"] < -0.05


def _ok_v2_buy(sig: Dict[str, float]) -> bool:
    return (
        sig["ema_diff_rel"] >= 0.0
        and sig["igd_n5_n20"] >= 0.0
        and sig["cbd_n20"] >= 0.0
    )


def _ok_v2_sell(sig: Dict[str, float]) -> bool:
    return (
        sig["ema_diff_rel"] <= 0.0
        and sig["igd_n5_n20"] <= 0.0
        and sig["cbd_n20"] >= 0.0
    )


def _tfi_allows(side: str, tfi: float) -> bool:
    if side == "buy":
        return tfi >= -TFI_VETO_EXTREME
    if side == "sell":
        return tfi <= TFI_VETO_EXTREME
    return True


def evaluate_signal(
    metrics: Dict[str, Any],
    *,
    buy_th: float,
    sell_th: float,
    hold_band: float,
) -> Dict[str, Any]:
    sig = normalize_signal_metrics(metrics)
    raw_th = symmetrize_threshold_values(buy_th, sell_th, hold_band)
    th = raw_th.scaled_by_volatility(sig["volatility"])
    state = classify_market_state(sig, th)
    blockers: List[str] = []

    if not sig["data_ok"] or not sig["metrics_calibrated"]:
        blockers.append("datos_no_calibrados")
    if state == MarketState.TOXIC:
        blockers.append("spoof_trap_tfi_divergente")
    if state == MarketState.CHOP:
        blockers.append("mercado_chop_baja_volatilidad")

    decision = "hold"

    if abs(sig["combined"]) < th.combined_hold_band:
        blockers.append("combined_dentro_hold_band")
    elif sig["combined"] >= th.combined_buy_threshold:
        confirmed = _classic_buy(sig) or (_ok_v2_buy(sig) and sig["mom"] > 0.05)
        if not confirmed:
            if sig["mom"] <= 0.05:
                blockers.append("buy_requiere_mom_gt_0.05")
            if not _classic_buy(sig) and not _ok_v2_buy(sig):
                blockers.append("buy_sin_confirmacion_pio_egm_o_v2")
        elif not _tfi_allows("buy", sig["tfi"]):
            blockers.append("tfi_veto_extremo_contra_compra")
        elif microprice_conflicts_signal(sig, "buy"):
            blockers.append("microprice_offset_contra_compra")
        elif state in {MarketState.TOXIC, MarketState.CHOP}:
            pass
        else:
            decision = "buy"
    elif sig["combined"] <= th.combined_sell_threshold:
        confirmed = _classic_sell(sig) or (_ok_v2_sell(sig) and sig["mom"] < -0.05)
        if not confirmed:
            if sig["mom"] >= -0.05:
                blockers.append("sell_requiere_mom_lt_-0.05")
            if not _classic_sell(sig) and not _ok_v2_sell(sig):
                blockers.append("sell_sin_confirmacion_pio_egm_o_v2")
        elif not _tfi_allows("sell", sig["tfi"]):
            blockers.append("tfi_veto_extremo_contra_venta")
        elif microprice_conflicts_signal(sig, "sell"):
            blockers.append("microprice_offset_contra_venta")
        elif state in {MarketState.TOXIC, MarketState.CHOP}:
            pass
        elif state == MarketState.BREAKOUT and sig["mom"] * sig["combined"] < 0:
            blockers.append("breakout_contra_momentum")
        else:
            decision = "sell"
    else:
        blockers.append("combined_entre_umbrales")

    if decision == "hold" and not blockers:
        blockers.append("sin_confirmacion")

    return {
        "decision": decision,
        "market_state": state.value,
        "blockers": blockers if decision == "hold" else [],
        "combined": sig["combined"],
        "combined_z": sig["combined_z"],
        "pio": sig["pio"],
        "egm": sig["egm"],
        "mom": sig["mom"],
        "tfi": sig["tfi"],
        "rvol": sig["rvol"],
        "volatility": sig["volatility"],
        "microprice_offset_bps": sig["microprice_offset_bps"],
        "thresholds_effective": {
            "buy": th.combined_buy_threshold,
            "sell": th.combined_sell_threshold,
            "hold_band": th.combined_hold_band,
        },
        "thresholds_symmetric_base": (
            abs(th.combined_buy_threshold) + abs(th.combined_sell_threshold)
        )
        / 2.0,
        "confirmations": {
            "ok_v2_buy": _ok_v2_buy(sig),
            "ok_v2_sell": _ok_v2_sell(sig),
            "classic_buy": _classic_buy(sig),
            "classic_sell": _classic_sell(sig),
            "tfi_aligned_buy": sig["tfi"] >= TFI_ALIGN_OPTIMAL,
            "tfi_aligned_sell": sig["tfi"] <= -TFI_ALIGN_OPTIMAL,
        },
    }


def determine_decision_from_metrics(
    metrics: Dict[str, float],
    *,
    buy_th: float = 4.5,
    sell_th: float = -4.5,
    hold_band: float = 3.0,
) -> str:
    return evaluate_signal(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
    )["decision"]


def check_execution_gates(
    metrics: Dict[str, Any],
    *,
    spread_avg_bps: float = 1.5,
) -> Tuple[bool, Optional[str]]:
    """True = permitido ejecutar. Breakouts con alto rvol no se penalizan."""
    sig = normalize_signal_metrics(metrics)
    spread_bps = sig["spread_bps"]
    rvol = sig["rvol"]
    last_age = sig["recent_trades_last_trade_age_s"]

    if spread_bps > spread_avg_bps * SPREAD_VETO_MULT:
        return False, "spread_expandido"
    if rvol < RVOL_MIN:
        return False, "rvol_bajo_sin_participacion"
    if last_age >= 0 and last_age > TRADE_AGE_MAX_S:
        return False, "trade_age_stale"
    if is_spoof_trap(sig):
        return False, "spoof_trap"
    return True, None


def relax_thresholds_symmetric(
    buy_th: float, sell_th: float, hold_band: float, factor: float = 0.85
) -> Thresholds:
    th = symmetrize_threshold_values(buy_th, sell_th, hold_band)
    base = (abs(th.combined_buy_threshold) + abs(th.combined_sell_threshold)) / 2.0
    new_base = max(1.0, min(15.0, base * factor))
    new_hold = max(0.5, min(6.0, th.combined_hold_band * factor))
    return Thresholds(new_base, -new_base, new_hold)


def blend_thresholds_symmetric(
    current: Thresholds, target: Thresholds, alpha: float
) -> Thresholds:
    a = float(max(0.0, min(1.0, alpha)))
    cur = current.symmetrized()
    tgt = target.symmetrized()
    base_cur = (abs(cur.combined_buy_threshold) + abs(cur.combined_sell_threshold)) / 2.0
    base_tgt = (abs(tgt.combined_buy_threshold) + abs(tgt.combined_sell_threshold)) / 2.0
    new_base = (1.0 - a) * base_cur + a * base_tgt
    new_hold = (1.0 - a) * cur.combined_hold_band + a * tgt.combined_hold_band
    new_base = max(1.0, min(15.0, new_base))
    new_hold = max(0.5, min(6.0, new_hold))
    return Thresholds(new_base, -new_base, new_hold)
