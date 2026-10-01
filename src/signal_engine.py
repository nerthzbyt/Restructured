"""
Motor unificado de señales: umbrales simétricos, clasificación de mercado,
vetos de flujo tóxico y gates de ejecución.

Usado por Nertzh (runtime), optimizer (backtest) y NerT_AI_PRO (agente).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np


@dataclass(frozen=True)
class SignalParams:
    """Parámetros del motor de señal (defaults calibrados, validación exchange 2026-07-04).

    Se pueden sobreescribir sin tocar código vía ``SIGNAL_PARAMS_JSON`` en .env,
    p.ej. ``{"rvol_min": 1e-5, "trade_age_max_s": 6}``.
    """

    base_vol_ref: float = 0.002
    vol_scale_max: float = 2.0
    rvol_min: float = 5e-6
    trade_age_max_s: float = 4.0
    tfi_veto_extreme: float = 0.8
    tfi_align_optimal: float = 0.8
    tfi_chop_band: float = 0.3
    vol_chop_max: float = 0.0004
    vol_optimal_min: float = 0.0008
    combined_z_optimal: float = 1.2
    combined_z_chop: float = 0.8
    mom_breakout: float = 0.5
    tfi_breakout: float = 0.9
    pio_spoof_z_min: float = 0.8
    combined_spoof_abs: float = 6.0
    spoof_rvol_mult: float = 10.0
    microprice_veto_bps: float = 0.003
    spread_veto_mult: float = 1.5
    mom_confirm: float = 0.05
    threshold_min: float = 1.0
    threshold_max: float = 15.0
    hold_band_min: float = 0.5
    hold_band_max: float = 6.0
    weight_scale_min: float = 1.0
    weight_scale_max: float = 25.0

    @classmethod
    def from_overrides(cls, overrides: Optional[Mapping[str, Any]] = None) -> "SignalParams":
        if not overrides:
            return DEFAULT_SIGNAL_PARAMS
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(overrides) - known)
        if unknown:
            raise ValueError(f"SignalParams: parámetros desconocidos {unknown}")
        return replace(DEFAULT_SIGNAL_PARAMS, **{k: float(v) for k, v in overrides.items()})

    def as_dict(self) -> Dict[str, float]:
        return asdict(self)


DEFAULT_SIGNAL_PARAMS = SignalParams()

# Alias de compatibilidad (módulos externos importan estas constantes).
BASE_VOL_REF = DEFAULT_SIGNAL_PARAMS.base_vol_ref
RVOL_MIN = DEFAULT_SIGNAL_PARAMS.rvol_min
TRADE_AGE_MAX_S = DEFAULT_SIGNAL_PARAMS.trade_age_max_s
TFI_VETO_EXTREME = DEFAULT_SIGNAL_PARAMS.tfi_veto_extreme
TFI_ALIGN_OPTIMAL = DEFAULT_SIGNAL_PARAMS.tfi_align_optimal
TFI_CHOP_BAND = DEFAULT_SIGNAL_PARAMS.tfi_chop_band
VOL_CHOP_MAX = DEFAULT_SIGNAL_PARAMS.vol_chop_max
VOL_OPTIMAL_MIN = DEFAULT_SIGNAL_PARAMS.vol_optimal_min
COMBINED_Z_OPTIMAL = DEFAULT_SIGNAL_PARAMS.combined_z_optimal
COMBINED_Z_CHOP = DEFAULT_SIGNAL_PARAMS.combined_z_chop
MOM_BREAKOUT = DEFAULT_SIGNAL_PARAMS.mom_breakout
TFI_BREAKOUT = DEFAULT_SIGNAL_PARAMS.tfi_breakout
PIO_SPOOF_Z_MIN = DEFAULT_SIGNAL_PARAMS.pio_spoof_z_min
MICROPRICE_VETO_BPS = DEFAULT_SIGNAL_PARAMS.microprice_veto_bps
SPREAD_VETO_MULT = DEFAULT_SIGNAL_PARAMS.spread_veto_mult

# Pesos crudos por defecto (orden: pio, egm, ild, rol, ogm, mom, tfi) y escala.
RAW_DEFAULT_WEIGHTS: Dict[str, float] = {
    "pio": 0.25,
    "egm": 0.30,
    "ild": -0.15,
    "rol": 0.10,
    "ogm": 0.05,
    "mom": 0.16,
    "tfi": 0.25,
    "scale": 10.0,
}
_WEIGHT_KEYS = ("pio", "egm", "ild", "rol", "ogm", "mom", "tfi")
_WEIGHT_FALLBACK = tuple(RAW_DEFAULT_WEIGHTS[k] for k in _WEIGHT_KEYS)


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

    def scaled_by_volatility(
        self, volatility: float, params: SignalParams = DEFAULT_SIGNAL_PARAMS
    ) -> Thresholds:
        th = self.symmetrized()
        vol = _safe_float(volatility, 0.0)
        if vol <= 0 or vol >= params.base_vol_ref:
            return th
        scale = float((params.base_vol_ref / vol) ** 0.5)
        scale = max(1.0, min(params.vol_scale_max, scale))
        return Thresholds(
            combined_buy_threshold=th.combined_buy_threshold * scale,
            combined_sell_threshold=th.combined_sell_threshold * scale,
            combined_hold_band=th.combined_hold_band * min(params.vol_scale_max, scale),
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
    def from_raw(cls, data: Optional[Mapping[str, Any]] = None) -> CombinedWeights:
        """Pesos tal como los usa producción (``raw_weights``): sin normalizar ni recortar ``scale``."""
        return cls(**raw_weights(data))

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> CombinedWeights:
        d = data if isinstance(data, dict) else {}
        return cls.normalize(
            **{k: _safe_float(d.get(k), RAW_DEFAULT_WEIGHTS[k]) for k in _WEIGHT_KEYS},
            scale=_safe_float(d.get("scale"), RAW_DEFAULT_WEIGHTS["scale"]),
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
        p = DEFAULT_SIGNAL_PARAMS
        scale = float(max(p.weight_scale_min, min(p.weight_scale_max, scale)))
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


# Pesos por defecto normalizados (sum|w| = 1). Compatibilidad: NO son los que usa
# el runtime; para evaluar con semántica de producción usar RUNTIME_COMBINED_WEIGHTS.
DEFAULT_COMBINED_WEIGHTS = CombinedWeights.normalize(**RAW_DEFAULT_WEIGHTS)


def raw_weights(data: Optional[Mapping[str, Any]] = None) -> Dict[str, float]:
    """Pesos crudos (sin normalizar) con fallback por componente.

    A diferencia del patrón ``x or default``, un peso 0.0 explícito se respeta.
    """
    out = dict(RAW_DEFAULT_WEIGHTS)
    if isinstance(data, Mapping):
        for k in out:
            v = data.get(k)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if np.isfinite(fv):
                out[k] = fv
    return out


# Pesos por defecto exactamente como los usa el runtime (crudos, sin normalizar).
RUNTIME_COMBINED_WEIGHTS = CombinedWeights.from_raw()


def symmetrize_threshold_values(
    buy_th: float, sell_th: float, hold_band: float
) -> Thresholds:
    return Thresholds(
        combined_buy_threshold=float(buy_th),
        combined_sell_threshold=float(sell_th),
        combined_hold_band=float(hold_band),
    ).symmetrized()


# Clave de peso -> clave del snapshot con la variable z que usa el runtime.
# Ojo: metrics["tfi"] es el TFI crudo (fórmula DEFAULT_FORMULAS, usado por los
# vetos); el combined usa tfi_z. Nunca se cae al TFI crudo.
COMBINED_INPUT_KEYS: Dict[str, str] = {
    "pio": "pio",
    "egm": "egm",
    "ild": "ild",
    "rol": "rol",
    "ogm": "ogm",
    "mom": "mom",
    "tfi": "tfi_z",
}


@dataclass(frozen=True)
class CombinedComposition:
    combined_z_micro: float
    combined_z: float
    combined: float
    components: Dict[str, float]


def compose_combined(
    z: Mapping[str, Any], weights: Mapping[str, Any]
) -> CombinedComposition:
    """Composición canónica del combined: fuente única para runtime, optimizer y backtest.

    ``z``: componentes z por clave de peso (pio, egm, ild, rol, ogm, mom, tfi).
    ``weights``: pesos tal cual (crudos, sin normalizar) más ``scale``.
    El orden de las operaciones es el del runtime validado (bit a bit).
    """
    combined_z_micro = (
        weights["pio"] * z["pio"] + weights["egm"] * z["egm"] + weights["ild"] * z["ild"]
        + weights["rol"] * z["rol"] + weights["ogm"] * z["ogm"] + weights["tfi"] * z["tfi"]
    )
    combined_z = float(combined_z_micro) + float(weights["mom"]) * float(z["mom"])
    combined = float(combined_z * float(weights["scale"]))
    return CombinedComposition(
        combined_z_micro=float(combined_z_micro),
        combined_z=float(combined_z),
        combined=combined,
        components={k: float(weights[k]) * float(z[k]) for k in _WEIGHT_KEYS},
    )


def combined_inputs_from_metrics(metrics: Mapping[str, Any]) -> Dict[str, float]:
    """Variables z del snapshot que usa el runtime para el combined (tfi -> tfi_z)."""
    m = metrics if isinstance(metrics, Mapping) else {}
    return {k: _metric(m, src) for k, src in COMBINED_INPUT_KEYS.items()}


def recompute_composition(
    metrics: Mapping[str, Any],
    w: Optional[CombinedWeights | Mapping[str, Any]] = None,
) -> CombinedComposition:
    """Recalcula el combined de un snapshot con la semántica exacta de producción.

    ``w``: ``CombinedWeights`` (se usa tal cual, sin normalizar), un dict de pesos
    crudos (vía ``raw_weights``, igual que el runtime) o ``None`` para usar los
    ``combined_weights`` guardados en el propio snapshot.
    """
    if isinstance(w, CombinedWeights):
        weights = w.as_dict()
    else:
        if w is None and isinstance(metrics, Mapping):
            cw = metrics.get("combined_weights")
            w = cw if isinstance(cw, Mapping) else None
        weights = raw_weights(w)
    return compose_combined(combined_inputs_from_metrics(metrics), weights)


def recompute_combined(
    metrics: Mapping[str, Any],
    w: Optional[CombinedWeights | Mapping[str, Any]] = None,
) -> float:
    return recompute_composition(metrics, w).combined


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


def is_spoof_trap(sig: Dict[str, float], params: SignalParams = DEFAULT_SIGNAL_PARAMS) -> bool:
    """PIO/combined fuerte en un sentido con TFI agresivo opuesto (spoofing)."""
    p = params
    pio = sig["pio"]
    tfi = sig["tfi"]
    combined = sig["combined"]
    rvol = sig["rvol"]

    bullish_book = pio >= p.pio_spoof_z_min or combined >= p.combined_spoof_abs
    bearish_book = pio <= -p.pio_spoof_z_min or combined <= -p.combined_spoof_abs

    if bullish_book and tfi <= -p.tfi_veto_extreme:
        return rvol < p.rvol_min * p.spoof_rvol_mult or abs(pio) >= p.pio_spoof_z_min
    if bearish_book and tfi >= p.tfi_veto_extreme:
        return rvol < p.rvol_min * p.spoof_rvol_mult or abs(pio) >= p.pio_spoof_z_min
    return False


def microprice_conflicts_signal(
    sig: Dict[str, float], side: str, params: SignalParams = DEFAULT_SIGNAL_PARAMS
) -> bool:
    veto = params.microprice_veto_bps
    offset = sig["microprice_offset_bps"]
    if abs(offset) < veto:
        return False
    if side == "buy" and offset < -veto:
        return True
    if side == "sell" and offset > veto:
        return True
    return False


def classify_market_state(
    sig: Dict[str, float], th: Thresholds, params: SignalParams = DEFAULT_SIGNAL_PARAMS
) -> MarketState:
    p = params
    if is_spoof_trap(sig, p):
        return MarketState.TOXIC

    vol = sig["volatility"]
    tfi = sig["tfi"]
    cz = abs(sig["combined_z"])
    mom = sig["mom"]

    if (
        mom >= p.mom_breakout
        and tfi >= p.tfi_breakout
        and sig["ema_diff_rel"] > 0
    ) or (
        mom <= -p.mom_breakout
        and tfi <= -p.tfi_breakout
        and sig["ema_diff_rel"] < 0
    ):
        return MarketState.BREAKOUT

    if vol > 0 and vol < p.vol_chop_max and abs(tfi) < p.tfi_chop_band:
        return MarketState.CHOP

    if (
        cz >= p.combined_z_optimal
        and vol >= p.vol_optimal_min
        and sig["rvol"] >= p.rvol_min
        and (
            (sig["combined"] >= th.combined_buy_threshold and tfi >= p.tfi_align_optimal)
            or (sig["combined"] <= th.combined_sell_threshold and tfi <= -p.tfi_align_optimal)
        )
    ):
        return MarketState.OPTIMAL

    if cz < p.combined_z_chop and abs(tfi) < p.tfi_chop_band:
        return MarketState.CHOP

    return MarketState.NEUTRAL


def _classic_buy(sig: Dict[str, float], mom_confirm: float = DEFAULT_SIGNAL_PARAMS.mom_confirm) -> bool:
    return sig["pio"] > 0 and sig["egm"] > 0 and sig["mom"] > mom_confirm


def _classic_sell(sig: Dict[str, float], mom_confirm: float = DEFAULT_SIGNAL_PARAMS.mom_confirm) -> bool:
    return sig["pio"] < 0 and sig["egm"] < 0 and sig["mom"] < -mom_confirm


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


def _tfi_allows(side: str, tfi: float, veto: float = DEFAULT_SIGNAL_PARAMS.tfi_veto_extreme) -> bool:
    if side == "buy":
        return tfi >= -veto
    if side == "sell":
        return tfi <= veto
    return True


def evaluate_signal(
    metrics: Dict[str, Any],
    *,
    buy_th: float,
    sell_th: float,
    hold_band: float,
    params: Optional[SignalParams] = None,
) -> Dict[str, Any]:
    p = params or DEFAULT_SIGNAL_PARAMS
    mc = p.mom_confirm
    sig = normalize_signal_metrics(metrics)
    raw_th = symmetrize_threshold_values(buy_th, sell_th, hold_band)
    th = raw_th.scaled_by_volatility(sig["volatility"], p)
    state = classify_market_state(sig, th, p)
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
        confirmed = _classic_buy(sig, mc) or (_ok_v2_buy(sig) and sig["mom"] > mc)
        if not confirmed:
            if sig["mom"] <= mc:
                blockers.append(f"buy_requiere_mom_gt_{mc:g}")
            if not _classic_buy(sig, mc) and not _ok_v2_buy(sig):
                blockers.append("buy_sin_confirmacion_pio_egm_o_v2")
        elif not _tfi_allows("buy", sig["tfi"], p.tfi_veto_extreme):
            blockers.append("tfi_veto_extremo_contra_compra")
        elif microprice_conflicts_signal(sig, "buy", p):
            blockers.append("microprice_offset_contra_compra")
        elif state in {MarketState.TOXIC, MarketState.CHOP}:
            pass
        else:
            decision = "buy"
    elif sig["combined"] <= th.combined_sell_threshold:
        confirmed = _classic_sell(sig, mc) or (_ok_v2_sell(sig) and sig["mom"] < -mc)
        if not confirmed:
            if sig["mom"] >= -mc:
                blockers.append(f"sell_requiere_mom_lt_-{mc:g}")
            if not _classic_sell(sig, mc) and not _ok_v2_sell(sig):
                blockers.append("sell_sin_confirmacion_pio_egm_o_v2")
        elif not _tfi_allows("sell", sig["tfi"], p.tfi_veto_extreme):
            blockers.append("tfi_veto_extremo_contra_venta")
        elif microprice_conflicts_signal(sig, "sell", p):
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
            "classic_buy": _classic_buy(sig, mc),
            "classic_sell": _classic_sell(sig, mc),
            "tfi_aligned_buy": sig["tfi"] >= p.tfi_align_optimal,
            "tfi_aligned_sell": sig["tfi"] <= -p.tfi_align_optimal,
        },
    }


def determine_decision_from_metrics(
    metrics: Dict[str, float],
    *,
    buy_th: float = 4.5,
    sell_th: float = -4.5,
    hold_band: float = 3.0,
    params: Optional[SignalParams] = None,
) -> str:
    return evaluate_signal(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
        params=params,
    )["decision"]


def check_execution_gates(
    metrics: Dict[str, Any],
    *,
    spread_avg_bps: float = 1.5,
    params: Optional[SignalParams] = None,
) -> Tuple[bool, Optional[str]]:
    """True = permitido ejecutar. Breakouts con alto rvol no se penalizan."""
    p = params or DEFAULT_SIGNAL_PARAMS
    sig = normalize_signal_metrics(metrics)
    spread_bps = sig["spread_bps"]
    rvol = sig["rvol"]
    last_age = sig["recent_trades_last_trade_age_s"]

    if spread_bps > spread_avg_bps * p.spread_veto_mult:
        return False, "spread_expandido"
    if rvol < p.rvol_min:
        return False, "rvol_bajo_sin_participacion"
    if last_age >= 0 and last_age > p.trade_age_max_s:
        return False, "trade_age_stale"
    if is_spoof_trap(sig, p):
        return False, "spoof_trap"
    return True, None


def clamp_thresholds(
    magnitude: float, hold: float, params: SignalParams = DEFAULT_SIGNAL_PARAMS
) -> Thresholds:
    """Umbrales simétricos acotados a los rangos operativos de ``params``."""
    mag = float(max(params.threshold_min, min(params.threshold_max, magnitude)))
    hb = float(max(params.hold_band_min, min(params.hold_band_max, hold)))
    return Thresholds(mag, -mag, hb)


def relax_thresholds_symmetric(
    buy_th: float,
    sell_th: float,
    hold_band: float,
    factor: float = 0.85,
    params: SignalParams = DEFAULT_SIGNAL_PARAMS,
) -> Thresholds:
    th = symmetrize_threshold_values(buy_th, sell_th, hold_band)
    base = (abs(th.combined_buy_threshold) + abs(th.combined_sell_threshold)) / 2.0
    return clamp_thresholds(base * factor, th.combined_hold_band * factor, params)


def blend_thresholds_symmetric(
    current: Thresholds,
    target: Thresholds,
    alpha: float,
    params: SignalParams = DEFAULT_SIGNAL_PARAMS,
) -> Thresholds:
    a = float(max(0.0, min(1.0, alpha)))
    cur = current.symmetrized()
    tgt = target.symmetrized()
    base_cur = (abs(cur.combined_buy_threshold) + abs(cur.combined_sell_threshold)) / 2.0
    base_tgt = (abs(tgt.combined_buy_threshold) + abs(tgt.combined_sell_threshold)) / 2.0
    new_base = (1.0 - a) * base_cur + a * base_tgt
    new_hold = (1.0 - a) * cur.combined_hold_band + a * tgt.combined_hold_band
    return clamp_thresholds(new_base, new_hold, params)