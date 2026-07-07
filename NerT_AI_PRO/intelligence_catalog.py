"""Catálogo de indicadores, niveles de predicción y perfiles de orden validados."""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SWEEP_SUMMARY = os.path.join(_BASE, "src_dev", "output", "full_system_sweep_summary.json")


INDICATORS: List[Dict[str, Any]] = [
    {
        "id": "pio",
        "name": "Pressure Imbalance Orderbook",
        "category": "microstructure",
        "range": "z-score",
        "weight_default": 0.25,
        "description": "Presión neta del libro ponderada por distancia al mid (exp decay). Positivo = bid dominance.",
        "inputs": ["orderbook bids/asks", "lambda", "pct_band"],
        "validated": True,
    },
    {
        "id": "egm",
        "name": "Enhanced Gravity Metric",
        "category": "microstructure",
        "range": "z-score",
        "weight_default": 0.30,
        "description": "Señal compuesta PIO + asimetría + bonus ROL cuando liquidez acelera (>1.5σ). Filtro direccional principal.",
        "inputs": ["pio_z", "asymmetry", "rol_z"],
        "validated": True,
    },
    {
        "id": "ild",
        "name": "Implied Liquidity Depth",
        "category": "liquidity",
        "range": "z-score",
        "weight_default": -0.15,
        "description": "Coste notional para mover precio ±target_move. Peso negativo: alta liquidez reduce urgencia de market.",
        "inputs": ["orderbook depth", "target_move"],
        "validated": True,
    },
    {
        "id": "rol",
        "name": "Rate of Liquidity",
        "category": "microstructure",
        "range": "z-score",
        "weight_default": 0.10,
        "description": "Δ liquidez ponderada / Δt. Detecta absorción o retirada rápida del libro.",
        "inputs": ["prev_weighted_liquidity", "rol_dt_s"],
        "validated": True,
    },
    {
        "id": "ogm",
        "name": "Order Gap Metric",
        "category": "microstructure",
        "range": "z-score",
        "weight_default": 0.05,
        "description": "Diferencia de gaps grandes vs medianos entre bid/ask. Identifica vacíos estructurales.",
        "inputs": ["orderbook level gaps"],
        "validated": True,
    },
    {
        "id": "tfi",
        "name": "Trade Flow Imbalance",
        "category": "flow",
        "range": "z-score",
        "weight_default": 0.25,
        "description": "Desequilibrio de cantidad en últimos trades (ventana 10). Integrado en combined desde v5.",
        "inputs": ["recent_trades"],
        "validated": True,
    },
    {
        "id": "mom",
        "name": "Momentum Composite",
        "category": "momentum",
        "range": "z-score",
        "weight_default": 0.16,
        "description": "EMA diff + retornos 1m/5m/20m + IGD normalizado por volatilidad. Veto en decisiones extremas.",
        "inputs": ["candles", "volatility", "ema5", "ema20"],
        "validated": True,
    },
    {
        "id": "combined",
        "name": "Combined Signal",
        "category": "decision",
        "range": "scaled z",
        "weight_default": 10.0,
        "description": "Σ(w_i × z_i) × scale. Umbral buy/sell/hold define acción. Optimizable por Optuna.",
        "inputs": ["pio", "egm", "ild", "rol", "ogm", "tfi", "mom"],
        "validated": True,
    },
]

PREDICTION_LEVELS: List[Dict[str, Any]] = [
    {
        "level": "L0",
        "name": "Sin señal",
        "confidence_pct": 0,
        "requirements": ["data_ok=false", "metrics_calibrated=false"],
        "action": "No operar. Esperar calibración (≥5 muestras Welford).",
        "validated_basis": "Gate laboratorio: sin historial suficiente devuelve combined=0.",
    },
    {
        "level": "L1",
        "name": "Observación",
        "confidence_pct": 25,
        "requirements": ["metrics_calibrated=true", "|combined| < hold_band"],
        "action": "Monitorear. Perfiles Limit+PostOnly o Limit+GTC preferidos.",
        "validated_basis": "29,184 perfiles evaluados; hold_band evita churn en rango lateral.",
    },
    {
        "level": "L2",
        "name": "Direccional débil",
        "confidence_pct": 50,
        "requirements": ["|combined| ≥ hold_band", "EGM veto activo o ML p<0.55"],
        "action": "Preparar orden; no ejecutar hasta confirmación microestructura.",
        "validated_basis": "optimizer determine_decision con min_egm_filter=0.01.",
    },
    {
        "level": "L3",
        "name": "Accionable",
        "confidence_pct": 75,
        "requirements": [
            "combined ≥ buy_th o ≤ sell_th",
            "egm coherente con lado",
            "perfil orden score ≥ 80",
        ],
        "action": "Ejecutar perfil validado (Market+IOC 86.8% obs. exchange o Limit+GTC).",
        "validated_basis": "Live verify demo: score 82.49, slippage 2.16 bps, filled=true.",
    },
    {
        "level": "L4",
        "name": "Alta convicción",
        "confidence_pct": 92,
        "requirements": [
            "L3 + metrics_calibrated",
            "composite_score ≥ 85.6 (sweep)",
            "execution_score ≥ 90",
            "nertzh_production_fit ≥ 0.85",
        ],
        "action": "Ejecución prioritaria con TP/SL optimizado (R:R 1.33 validado).",
        "validated_basis": "full_system_sweep rank #1: composite 85.62, execution 90.0.",
    },
]

ORDER_PROFILES_VALIDATED: List[Dict[str, Any]] = [
    {
        "rank": 1,
        "profile_id": "Market|IOC|baseCoin|Order",
        "order_type": "Market",
        "time_in_force": "IOC",
        "market_unit": "baseCoin",
        "exchange_observed_pct": 86.8,
        "lab_score": 84.03,
        "execution_score": 90.0,
        "composite_score": 85.62,
        "use_case": "Entrada/salida rápida con señal L3-L4 en spot BTCUSDT.",
        "validated_at": "2026-07-04",
        "source": "bybit_exchange_order_history n=500",
    },
    {
        "rank": 2,
        "profile_id": "Limit|GTC|Order",
        "order_type": "Limit",
        "time_in_force": "GTC",
        "market_unit": None,
        "exchange_observed_pct": 12.8,
        "lab_score": 85.78,
        "execution_score": 85.0,
        "composite_score": 82.0,
        "use_case": "Maker en rangos L1-L2; menor slippage, mayor latencia de fill.",
        "validated_at": "2026-07-04",
        "source": "old_results + exchange cross-check",
    },
    {
        "rank": 3,
        "profile_id": "Limit|PostOnly|Order",
        "order_type": "Limit",
        "time_in_force": "PostOnly",
        "market_unit": None,
        "exchange_observed_pct": 0.2,
        "lab_score": 78.0,
        "execution_score": 70.0,
        "composite_score": 74.0,
        "use_case": "Spread tight + señal débil; garantía maker fee.",
        "validated_at": "2026-07-04",
        "source": "exchange_api_order_history",
    },
]

QWEN_BACKENDS: List[Dict[str, Any]] = [
    {
        "backend": "qwen_desktop",
        "models": ["qwen3.7-plus", "qwen3.7-max", "qwen3.5-flash", "qwen3-coder-plus"],
        "auth": "JWT desde Qwen Desktop (Google login)",
        "use_case": "Agente ReAct, propuesta de estrategia, síntesis de diagnósticos",
    },
    {
        "backend": "openai_compat",
        "models": ["qwen-plus-latest", "qwen-max-latest"],
        "auth": "DASHSCOPE_API_KEY",
        "use_case": "Producción cloud DashScope",
    },
    {
        "backend": "ollama",
        "models": ["qwen2.5-coder:latest"],
        "auth": "local",
        "use_case": "Fallback offline / desarrollo",
    },
]

FUTURE_ROADMAP: List[Dict[str, Any]] = [
    {"phase": "Q3 2026", "item": "Factorización Nertzh.py → nertz_engine (reducción ~70% LOC)", "status": "planned"},
    {"phase": "Q3 2026", "item": "API pública v5 REST + WebSocket unificados", "status": "in_progress"},
    {"phase": "Q4 2026", "item": "ML ensemble XGBoost + calibración isotónica por símbolo", "status": "planned"},
    {"phase": "Q4 2026", "item": "Multi-exchange (Bybit + Binance) con perfiles normalizados", "status": "planned"},
    {"phase": "2027", "item": "Agente autónomo con memoria episódica y backtest en vivo", "status": "research"},
]


def _load_sweep_summary() -> Optional[Dict[str, Any]]:
    if not os.path.isfile(_SWEEP_SUMMARY):
        return None
    try:
        with open(_SWEEP_SUMMARY, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def compute_prediction_level(
    metrics: Dict[str, Any],
    *,
    buy_th: float = 4.5,
    sell_th: float = -4.5,
    hold_band: float = 3.0,
    ml_p_buy: Optional[float] = None,
    ml_p_sell: Optional[float] = None,
) -> Dict[str, Any]:
    """Calcula nivel L0-L4 alineado con signal_engine."""
    import sys

    _src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
    if _src not in sys.path:
        sys.path.insert(0, _src)
    from signal_engine import MarketState, evaluate_signal  # noqa: WPS433

    ev = evaluate_signal(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
    )
    state = str(ev.get("market_state") or "neutral")
    decision = str(ev.get("decision") or "hold")
    calibrated = bool(metrics.get("metrics_calibrated", False))
    data_ok = bool(metrics.get("data_ok", True))

    if not data_ok or not calibrated:
        level, meta = "L0", PREDICTION_LEVELS[0]
    elif state == MarketState.TOXIC.value:
        level, meta = "L0", PREDICTION_LEVELS[0]
    elif state == MarketState.CHOP.value:
        level, meta = "L1", PREDICTION_LEVELS[1]
    elif state == MarketState.BREAKOUT.value and decision in {"buy", "sell"}:
        level, meta = "L4", PREDICTION_LEVELS[4]
    elif state == MarketState.OPTIMAL.value and decision in {"buy", "sell"}:
        level, meta = "L4", PREDICTION_LEVELS[4]
    elif decision in {"buy", "sell"}:
        level, meta = "L3", PREDICTION_LEVELS[3]
    else:
        level, meta = "L2", PREDICTION_LEVELS[2]

    if ml_p_buy is not None or ml_p_sell is not None:
        max_p = max(ml_p_buy or 0.0, ml_p_sell or 0.0)
        if max_p < 0.55 and level in {"L3", "L4"}:
            level, meta = "L2", PREDICTION_LEVELS[2]

    return {
        "level": level,
        "name": meta["name"],
        "confidence_pct": meta["confidence_pct"],
        "action": meta["action"],
        "market_state": state,
        "decision": decision,
        "combined": ev.get("combined"),
        "combined_z": ev.get("combined_z"),
        "egm": ev.get("egm"),
        "tfi": ev.get("tfi"),
        "metrics_calibrated": calibrated,
        "blockers": ev.get("blockers") or [],
        "thresholds": ev.get("thresholds_effective"),
        "recommended_profiles": [
            p["profile_id"]
            for p in ORDER_PROFILES_VALIDATED
            if (level in {"L3", "L4"} and p["order_type"] == "Market")
            or (level in {"L1", "L2"} and p["order_type"] == "Limit")
        ][:2],
    }


def full_catalog() -> Dict[str, Any]:
    sweep = _load_sweep_summary()
    out: Dict[str, Any] = {
        "version": "5.0.0",
        "indicators": INDICATORS,
        "prediction_levels": PREDICTION_LEVELS,
        "order_profiles": ORDER_PROFILES_VALIDATED,
        "qwen_backends": QWEN_BACKENDS,
        "roadmap": FUTURE_ROADMAP,
        "default_weights": {
            "pio": 0.25,
            "egm": 0.30,
            "ild": -0.15,
            "rol": 0.10,
            "ogm": 0.05,
            "tfi": 0.25,
            "mom": 0.16,
            "scale": 10.0,
        },
    }
    if sweep:
        out["validation_summary"] = {
            "generated_at": sweep.get("generated_at"),
            "symbol": sweep.get("symbol"),
            "total_scored": sweep.get("sweep_dimensions", {}).get("total_scored"),
            "top_recommendation": (sweep.get("top_recommendations") or {}).get("best_overall", [{}])[0],
            "production_env": sweep.get("production_env_current"),
        }
    return out