"""
Registro único de configuración del motor.

Cada parámetro se declara UNA vez en ``SETTINGS`` (clave, tipo, default, rango,
descripción). De ahí salen:

* la carga desde variables de entorno (``.env``),
* la validación/coerción (al arrancar y en cada cambio en caliente),
* los overrides por símbolo (``SYMBOL_OVERRIDES_JSON``),
* el esquema que expone la API (``/config/schema``) y el ``.env.example``.

Añadir un parámetro nuevo = añadir un ``Setting`` a la lista; el resto del
sistema lo lee como ``config.CLAVE`` o ``config.for_symbol(sym, "CLAVE")``.

CLI: ``python src/settings.py --env-example`` imprime un .env documentado.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Tuple

from signal_engine import RAW_DEFAULT_WEIGHTS, SignalParams

logger = logging.getLogger("NertzMetalEngine")

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,30}$")

# Intervalos de kline soportados por Bybit v5 (alias humano -> código API).
TIMEFRAME_TO_BYBIT: Dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}

# Endpoints por entorno. Cualquier URL puede sobreescribirse por env
# (BYBIT_REST_URL / BYBIT_PUBLIC_REST_URL / BYBIT_WS_PUBLIC_URL).
# Demo trading usa datos de mercado de mainnet y REST privado api-demo.
BYBIT_ENVIRONMENTS: Dict[str, Dict[str, str]] = {
    "mainnet": {
        "rest_private": "https://api.bybit.com",
        "rest_public": "https://api.bybit.com",
        "ws_public": "wss://stream.bybit.com/v5/public/{category}",
    },
    "demo": {
        "rest_private": "https://api-demo.bybit.com",
        "rest_public": "https://api.bybit.com",
        "ws_public": "wss://stream.bybit.com/v5/public/{category}",
    },
    "testnet": {
        "rest_private": "https://api-testnet.bybit.com",
        "rest_public": "https://api-testnet.bybit.com",
        "ws_public": "wss://stream-testnet.bybit.com/v5/public/{category}",
    },
}

# Pesos crudos del combined: fuente única en signal_engine (la escala de los
# umbrales COMBINED_* está calibrada contra estos pesos sin normalizar).
DEFAULT_COMBINED_WEIGHTS_RAW: Dict[str, float] = dict(RAW_DEFAULT_WEIGHTS)

DEFAULT_FORMULAS: Dict[str, str] = {
    "basis": "(mark_price - index_price) / (index_price + 1e-12)",
    "obi": "(bid_notional_sum_k - ask_notional_sum_k) / (bid_notional_sum_k + ask_notional_sum_k + 1e-12)",
    "tfi": "(taker_buy_qty - taker_sell_qty) / (taker_buy_qty + taker_sell_qty + 1e-12)",
    "spread_rel": "(best_ask - best_bid) / (mid_price + 1e-12)",
    "microprice_offset": "(microprice - mid_price) / (mid_price + 1e-12)",
    "rvol": "RecentTrades:rvol",
    "spread_bps": "SpreadRel * 10000",
    "microprice_offset_bps": "((MicroPrice - MidPrice) / (MidPrice + 1e-12)) * 10000",
}


class ConfigError(ValueError):
    """Valor de configuración inválido."""


@dataclass(frozen=True)
class Setting:
    key: str
    kind: str  # str | int | float | bool | json | symbols | csv
    default: Any
    description: str
    group: str = "general"
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    choices: Optional[Tuple[str, ...]] = None
    secret: bool = False
    normalize: Optional[Callable[[Any], Any]] = None

    def coerce(self, raw: Any) -> Any:
        value = _coerce_kind(self.kind, raw, self.key)
        if self.normalize is not None:
            value = self.normalize(value)
        if self.choices is not None and value not in self.choices:
            raise ConfigError(f"{self.key}={value!r} inválido. Valores permitidos: {list(self.choices)}")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if self.min_value is not None and value < self.min_value:
                raise ConfigError(f"{self.key}={value} por debajo del mínimo {self.min_value}")
            if self.max_value is not None and value > self.max_value:
                raise ConfigError(f"{self.key}={value} por encima del máximo {self.max_value}")
        return value

    def describe(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "type": self.kind,
            "default": None if self.secret else self.default,
            "group": self.group,
            "description": self.description,
            "min": self.min_value,
            "max": self.max_value,
            "choices": list(self.choices) if self.choices else None,
            "secret": self.secret,
        }


_TRUE = {"1", "true", "yes", "on", "y", "si", "sí"}
_FALSE = {"0", "false", "no", "off", "n", ""}


def _coerce_kind(kind: str, raw: Any, key: str) -> Any:
    try:
        if kind == "str":
            return "" if raw is None else str(raw).strip()
        if kind == "bool":
            if isinstance(raw, bool):
                return raw
            s = str(raw).strip().lower()
            if s in _TRUE:
                return True
            if s in _FALSE:
                return False
            raise ConfigError(f"{key}={raw!r} no es booleano (true/false/1/0)")
        if kind == "int":
            if isinstance(raw, bool):
                raise ConfigError(f"{key} espera entero")
            f = float(raw)
            if not f.is_integer():
                raise ConfigError(f"{key}={raw!r} no es entero")
            return int(f)
        if kind == "float":
            if isinstance(raw, bool):
                raise ConfigError(f"{key} espera número")
            v = float(raw)
            if v != v or v in (float("inf"), float("-inf")):
                raise ConfigError(f"{key}={raw!r} no es finito")
            return v
        if kind == "json":
            if isinstance(raw, (dict, list)):
                return raw
            s = str(raw or "").strip()
            if not s:
                return {}
            return json.loads(s)
        if kind == "symbols":
            items = raw if isinstance(raw, (list, tuple)) else str(raw or "").split(",")
            symbols: List[str] = []
            for item in items:
                sym = str(item).strip().upper()
                if not sym:
                    continue
                if not _SYMBOL_RE.match(sym):
                    raise ConfigError(f"{key}: símbolo con formato inválido {item!r}")
                if sym not in symbols:
                    symbols.append(sym)
            if not symbols:
                raise ConfigError(f"{key} no puede estar vacío")
            return ",".join(symbols)
        if kind == "csv":
            items = raw if isinstance(raw, (list, tuple)) else str(raw or "").split(",")
            return ",".join(str(i).strip() for i in items if str(i).strip())
    except ConfigError:
        raise
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{key}={raw!r} inválido para tipo {kind}: {e}") from e
    raise ConfigError(f"{key}: tipo desconocido {kind}")


def _norm_env(v: str) -> str:
    return str(v).strip().lower() or "mainnet"


def _norm_order_type(v: str) -> str:
    return {"limit": "Limit", "market": "Market"}.get(str(v).strip().lower(), str(v).strip())


_TIF_ALIASES = {
    "gtc": "GTC",
    "goodtillcancel": "GTC",
    "ioc": "IOC",
    "immediateorcancel": "IOC",
    "fok": "FOK",
    "fillorkill": "FOK",
    "postonly": "PostOnly",
}


def _norm_tif(v: str) -> str:
    return _TIF_ALIASES.get(str(v).strip().lower(), str(v).strip())


def _norm_backend(v: str) -> str:
    s = str(v).strip().lower()
    return {"duck": "duckdb", "sqlite": "sqlite_legacy", "legacy": "sqlite_legacy"}.get(s, s)


def _weights_dict(v: Any) -> Dict[str, float]:
    if not isinstance(v, dict):
        raise ConfigError("COMBINED_WEIGHTS_JSON debe ser un objeto JSON")
    out = dict(DEFAULT_COMBINED_WEIGHTS_RAW)
    for k, val in v.items():
        if k not in out:
            raise ConfigError(f"COMBINED_WEIGHTS_JSON: componente desconocido {k!r}")
        out[k] = float(val)
    return out


def _formulas_dict(v: Any) -> Dict[str, str]:
    if not isinstance(v, dict):
        raise ConfigError("FORMULAS_JSON debe ser un objeto JSON")
    out = dict(DEFAULT_FORMULAS)
    out.update({str(k): str(val) for k, val in v.items()})
    return out


def _dict_of_dicts(v: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(v, dict):
        raise ConfigError("Se esperaba un objeto JSON {clave: valor}")
    return {str(k): dict(val) if isinstance(val, dict) else val for k, val in v.items()}


def _signal_params(v: Any) -> Dict[str, float]:
    if not isinstance(v, dict):
        raise ConfigError("SIGNAL_PARAMS_JSON debe ser un objeto JSON")
    try:
        SignalParams.from_overrides(v)
    except (TypeError, ValueError) as e:
        raise ConfigError(str(e)) from e
    return {str(k): float(val) for k, val in v.items()}


S = Setting
SETTINGS: Tuple[Setting, ...] = (
    # --- Exchange / credenciales ---
    S("BYBIT_API_KEY", "str", "", "API key de Bybit.", "exchange", secret=True),
    S("BYBIT_API_SECRET", "str", "", "API secret de Bybit.", "exchange", secret=True),
    S("BYBIT_ENV", "str", "mainnet", "Entorno Bybit.", "exchange",
      choices=tuple(BYBIT_ENVIRONMENTS), normalize=_norm_env),
    S("BYBIT_CATEGORY", "str", "spot",
      "Categoría de producto Bybit. El ciclo de ejecución actual implementa spot.", "exchange",
      choices=("spot",)),
    S("BYBIT_REST_URL", "str", "", "Override REST privado (vacío = según BYBIT_ENV).", "exchange"),
    S("BYBIT_PUBLIC_REST_URL", "str", "", "Override REST público de mercado.", "exchange"),
    S("BYBIT_WS_PUBLIC_URL", "str", "", "Override WebSocket público.", "exchange"),
    S("BYBIT_RECV_WINDOW", "int", 5000, "recv_window de firma (ms).", "exchange", 1000, 60000),
    S("BYBIT_HTTP_TIMEOUT_S", "float", 15.0, "Timeout HTTP REST.", "exchange", 1.0, 120.0),
    S("BYBIT_HTTP_MAX_RETRIES", "int", 3, "Reintentos HTTP (429/5xx/red).", "exchange", 0, 10),
    S("LIVE_TRADING_ENABLED", "bool", False, "Envía órdenes reales al exchange.", "exchange"),
    S("QUOTE_COIN", "str", "USDT", "Moneda de cotización/cuenta para balance.", "exchange",
      normalize=lambda v: str(v).upper()),
    S("ACCOUNT_TYPE", "str", "UNIFIED", "Tipo de cuenta preferido para wallet-balance.", "exchange",
      normalize=lambda v: str(v).upper()),
    S("ACCOUNT_TYPE_FALLBACKS", "csv", "UNIFIED,SPOT",
      "Tipos de cuenta a probar en orden si el preferido no devuelve balance.", "exchange"),
    S("ORDER_LINK_PREFIX", "str", "nertzh-",
      "Prefijo de orderLinkId que identifica órdenes propias del bot.", "exchange"),
    S("CLOCK_DRIFT_MAX_S", "float", 10.0, "Deriva de reloj máxima tolerada en preflight.", "exchange", 0.5, 120.0),
    S("INSTRUMENT_RULES_TTL_S", "float", 3600.0, "Cache de reglas de instrumento (tick/lot).", "exchange", 10.0, 86400.0),

    # --- Mercado ---
    S("SYMBOL", "symbols", "BTCUSDT",
      "Símbolos a operar separados por coma (cualquier par válido del exchange).", "market"),
    S("TIMEFRAME", "str", "1m", "Intervalo de velas.", "market", choices=tuple(TIMEFRAME_TO_BYBIT)),
    S("ORDERBOOK_DEPTH", "int", 50, "Niveles de libro usados en métricas.", "market", 1, 200),
    S("CANDLE_BUFFER_SIZE", "int", 50, "Velas mantenidas en memoria por símbolo.", "market", 21, 1000),
    S("RECENT_TRADES_BUFFER", "int", 500, "Trades públicos en memoria por símbolo.", "market", 10, 20000),
    S("RECENT_TRADES_METRICS_N", "int", 50, "Trades públicos pasados al cálculo de métricas.", "market", 3, 5000),
    S("WS_RECONNECT_DELAY_S", "float", 5.0, "Espera antes de reconectar el WebSocket.", "market", 0.5, 300.0),
    S("INITIAL_FETCH_ATTEMPTS", "int", 3, "Reintentos de carga REST inicial.", "market", 1, 20),

    # --- Órdenes ---
    S("ORDER_TYPE", "str", "Limit", "Tipo de orden de entrada.", "orders",
      choices=("Limit", "Market"), normalize=_norm_order_type),
    S("TIME_IN_FORCE", "str", "GTC", "Time in force de órdenes Limit.", "orders",
      choices=("GTC", "IOC", "FOK", "PostOnly"), normalize=_norm_tif),
    S("MAX_CONCURRENT_ORDERS", "int", 20, "Órdenes simultáneas en vuelo (todas las monedas).", "orders", 1, 1000),
    S("ALLOW_MULTIPLE_ACTIVE_TRADES", "bool", True, "Permite varios trades abiertos por símbolo.", "orders"),
    S("TRADE_COOLDOWN_S", "float", 0.0, "Pausa mínima entre trades de un símbolo.", "orders", 0.0, 86400.0),
    S("COOLDOWN_BYPASS_STRONG_SIGNAL", "bool", True, "Ignora el cooldown con señal fuerte.", "orders"),
    S("COOLDOWN_BYPASS_MULT", "float", 1.25, "Múltiplo del umbral que se considera señal fuerte.", "orders", 1.0, 10.0),
    S("ORDERS_SYNC_INTERVAL_S", "float", 5.0, "Intervalo de sincronización de órdenes.", "orders", 0.5, 300.0),
    S("ORDERS_SYNC_UPDATE_AFTER_S", "float", 5.0, "Edad a partir de la cual se re-precia una Limit.", "orders", 0.5, 3600.0),
    S("ORDERS_SYNC_TIMEOUT_S", "float", 15.0, "Edad a partir de la cual se cancela una orden abierta.", "orders", 1.0, 86400.0),
    S("ORDERS_SYNC_LIMIT", "int", 100, "Órdenes por consulta de sync.", "orders", 1, 50000),
    S("TPSL_CANCEL_AFTER_S", "float", 90.0, "Cancela órdenes tpsl nativas más viejas que esto (0=off).", "orders", 0.0, 86400.0),
    S("MAX_CHASE_ATTEMPTS", "int", 3, "Reintentos al colocar orden (rate limit / banda de precio).", "orders", 1, 20),
    S("CHASE_INTERVAL", "float", 2.0, "Base de backoff entre reintentos de orden (s).", "orders", 0.0, 60.0),

    # --- Riesgo / sizing ---
    S("CAPITAL_USDT", "float", 2000.0, "Capital simulado cuando no hay wallet real.", "risk", 0.0, None),
    S("RISK_FACTOR", "float", 0.01, "Fracción del capital arriesgada por trade.", "risk", 0.0, 1.0),
    S("MAX_POSITION_NOTIONAL_PCT", "float", 0.10,
      "Tope de nocional por trade como fracción del capital.", "risk", 0.0, 1.0),
    S("MAX_TRADE_SIZE", "float", 0.0,
      "Tope opcional en unidades base (0 = sin tope; usar SYMBOL_OVERRIDES_JSON por par).", "risk", 0.0, None),
    S("MIN_TRADE_SIZE", "float", 0.0,
      "Mínimo opcional en unidades base (0 = mínimo del exchange).", "risk", 0.0, None),
    S("MIN_NOTIONAL_BUFFER", "float", 1.1, "Margen sobre el nocional mínimo del exchange.", "risk", 1.0, 5.0),
    S("VOLATILITY_FALLBACK", "float", 0.01, "Volatilidad usada si la medida es 0/ inválida.", "risk", 1e-6, 1.0),
    S("FEE_RATE", "float", 0.002, "Comisión por lado (fracción del nocional).", "risk", 0.0, 0.1),
    S("TP_PERCENTAGE", "float", 1.5, "Multiplicador de volatilidad para TP.", "risk", 0.0, None),
    S("SL_PERCENTAGE", "float", 0.5, "Multiplicador de volatilidad para SL.", "risk", 0.0, None),
    S("MAX_ITERATIONS", "int", 0, "Trades máximos antes de parar (0 = ilimitado).", "risk", 0, None),
    S("OUTCOME_HORIZON_S", "float", 15.0, "Horizonte para cerrar el resultado de trades simulados.", "risk", 1.0, 86400.0),

    # --- Señal ---
    S("COMBINED_BUY_THRESHOLD", "float", 4.5, "Umbral de compra del combined.", "signal", -100.0, 100.0),
    S("COMBINED_SELL_THRESHOLD", "float", -4.5, "Umbral de venta del combined.", "signal", -100.0, 100.0),
    S("COMBINED_HOLD_BAND", "float", 3.0, "Banda |combined| sin operar.", "signal", 0.0, 100.0),
    S("EGM_BUY_THRESHOLD", "float", 0.02, "Umbral EGM (legacy, calibración).", "signal"),
    S("EGM_SELL_THRESHOLD", "float", -0.02, "Umbral EGM (legacy, calibración).", "signal"),
    S("PIO_THRESHOLD", "float", 0.0, "Umbral PIO (legacy).", "signal"),
    S("AVG_SPREAD_BPS", "float", 1.5, "Spread medio de referencia para el gate de ejecución.", "signal", 0.01, 500.0),
    S("COMBINED_WEIGHTS_JSON", "json", dict(DEFAULT_COMBINED_WEIGHTS_RAW),
      "Pesos por defecto del combined (pio, egm, ild, rol, ogm, mom, tfi, scale).", "signal",
      normalize=_weights_dict),
    S("SIGNAL_PARAMS_JSON", "json", {},
      "Overrides de parámetros del motor de señal (ver SignalParams en signal_engine).", "signal",
      normalize=_signal_params),
    S("FORMULAS_JSON", "json", dict(DEFAULT_FORMULAS), "Fórmulas TSM derivadas.", "signal",
      normalize=_formulas_dict),
    S("RSI_UPPER_THRESHOLD", "float", 80.0, "Legacy.", "signal", 0.0, 100.0),
    S("RSI_LOWER_THRESHOLD", "float", 20.0, "Legacy.", "signal", 0.0, 100.0),
    S("PRICE_SHIFT_FACTOR", "float", 0.003, "Legacy.", "signal", 0.0, 0.1),
    S("VOLUME_THRESHOLD", "float", 1.0, "Legacy.", "signal", 0.0, None),

    # --- Métricas ---
    S("ORDERBOOK_LAMBDA", "float", 0.03, "Decaimiento exponencial por distancia al mid (PIO).", "metrics", 0.0, None),
    S("ORDERBOOK_PCT_BAND", "float", 0.015, "Banda relativa al mid usada en el libro.", "metrics", 0.0, 0.25),
    S("ILD_TARGET_MOVE", "float", 0.002, "Movimiento objetivo para ILD.", "metrics", 0.0001, 0.05),
    S("METRICS_WINDOW_MINUTES", "float", 15.0, "Ventana de historia para z-scores.", "metrics", 1.0, 1440.0),
    S("METRICS_WINDOW_BUFFER", "int", 2500, "Decisiones recientes mantenidas por símbolo.", "metrics", 10, 100000),
    S("METRICS_LIVE_REFRESH_S", "float", 5.0, "Recalcular métricas en vivo cada N s.", "metrics", 1.0, 3600.0),
    S("METRICS_SNAPSHOT_INTERVAL_S", "float", 55.0, "Snapshot persistido de métricas cada N s.", "metrics", 1.0, 3600.0),
    S("METRICS_SNAPSHOT_DEDUP_S", "float", 3.0, "Separación mínima entre snapshots.", "metrics", 0.0, 3600.0),
    S("METRICS_RESULTS_EVENT_MIN_S", "float", 0.0, "Separación mínima de eventos metrics en results.json.", "metrics", 0.0, 86400.0),

    # --- Loops / automatismos ---
    S("SUPPORT_LOOP_INTERVAL_S", "float", 1.0, "Periodo del loop de soporte.", "automation", 0.25, 30.0),
    S("BALANCE_SYNC_INTERVAL_S", "float", 60.0, "Refresco periódico del balance.", "automation", 1.0, 3600.0),
    S("BALANCE_DIRTY_SYNC_S", "float", 2.0, "Refresco tras operar (balance sucio).", "automation", 0.0, 600.0),
    S("AUTO_ENABLE_SECONDARY_SYSTEMS", "bool", False,
      "Activa AUTO_AGENT tras SECONDARY_SYSTEMS_DELAY_S de arranque.", "automation"),
    S("SECONDARY_SYSTEMS_DELAY_S", "float", 20.0, "Retardo para auto-activar sistemas secundarios.", "automation", 0.0, 3600.0),
    S("AUTO_AGENT_ENABLED", "bool", False, "Agente interno (relajar umbrales, reentrenar ML).", "automation"),
    S("AUTO_AGENT_TRAIN_INTERVAL_MIN", "float", 5.0, "Intervalo de reentreno ML.", "automation", 1.0, 1440.0),
    S("AGENT_TICK_MIN_S", "float", 0.5, "Separación mínima entre ticks del agente.", "automation", 0.0, 600.0),
    S("AGENT_RELAX_INTERVAL_S", "float", 120.0, "Separación mínima entre relajaciones.", "automation", 1.0, 86400.0),
    S("AGENT_RELAX_MIN_SNAPSHOTS", "int", 80, "Decisiones mínimas en ventana para relajar.", "automation", 1, 100000),
    S("AGENT_RELAX_HOLD_RATIO", "float", 0.92, "Ratio de hold que dispara la relajación.", "automation", 0.0, 1.0),
    S("AGENT_RELAX_IDLE_S", "float", 600.0, "Segundos sin trades para permitir relajar.", "automation", 0.0, 86400.0),
    S("AGENT_RELAX_FACTOR", "float", 0.85, "Factor multiplicativo de relajación.", "automation", 0.1, 1.0),
    S("AGENT_DECISIONS_MAX", "int", 250, "Decisiones máximas analizadas por tick.", "automation", 1, 100000),
    S("AUTO_TUNE_THRESHOLDS", "bool", False, "Auto-calibración de umbrales con trades finales.", "automation"),
    S("AUTO_TUNE_INTERVAL_S", "float", 60.0, "Periodo de auto-calibración.", "automation", 1.0, 86400.0),
    S("AUTO_TUNE_MIN_TRADES", "int", 20, "Trades finales mínimos para calibrar.", "automation", 1, 100000),
    S("AUTO_TUNE_SAMPLE", "int", 200, "Trades usados por calibración.", "automation", 1, 1000000),
    S("AUTO_TUNE_ALPHA", "float", 0.1, "Mezcla hacia el objetivo por calibración.", "automation", 0.0, 1.0),
    S("PERSIST_THRESHOLDS_TO_ENV", "bool", False, "Escribe umbrales optimizados en .env.", "automation"),
    S("FULL_RESET_ON_BOOT", "bool", False, "Borra BD y results.json al arrancar.", "automation"),
    S("AUTO_GIT_COMMIT", "bool", False, "Auto-commit de resultados (desarrollo).", "automation"),

    # --- HFT ---
    S("AUTO_HFT_ENABLED", "bool", False, "Arranque/parada automática de loops HFT.", "hft"),
    S("AUTO_HFT_TICK_S", "float", 2.0, "Evaluación del auto-HFT cada N s.", "hft", 0.25, 3600.0),
    S("AUTO_HFT_WINDOW_S", "float", 60.0, "Ventana de decisiones analizada.", "hft", 10.0, 86400.0),
    S("AUTO_HFT_MIN_SNAPSHOTS", "int", 30, "Decisiones mínimas para actuar.", "hft", 5, 100000),
    S("AUTO_HFT_START_RATIO", "float", 0.35, "Ratio buy/sell que arranca HFT.", "hft", 0.0, 1.0),
    S("AUTO_HFT_STOP_RATIO", "float", 0.15, "Ratio buy/sell que detiene HFT.", "hft", 0.0, 1.0),
    S("AUTO_HFT_COMBINED_ABS_THRESHOLD", "float", 10.0, "|combined| medio mínimo para HFT.", "hft", 0.0, None),
    S("AUTO_HFT_INTERVAL_MS", "int", 250, "Periodo de ciclo HFT.", "hft", 0, 60000),
    S("AUTO_HFT_COLLECT_ONLY", "bool", True, "HFT solo recolecta (no opera).", "hft"),
    S("AUTO_HFT_COOLDOWN_S", "float", 60.0, "Separación mínima entre cambios de estado HFT.", "hft", 5.0, 86400.0),

    # --- Auto TP/SL virtual ---
    S("AUTO_TPSL_ENABLED", "bool", True, "Gestión de TP/SL virtual (spot).", "tpsl"),
    S("AUTO_TPSL_INTERVAL_S", "float", 3.0, "Periodo del gestor TP/SL.", "tpsl", 0.25, 60.0),
    S("AUTO_TPSL_MIN_TP_MOVE_TICKS", "int", 1, "Ticks mínimos para mover TP.", "tpsl", 1, 10000),
    S("AUTO_TPSL_MIN_SL_MOVE_TICKS", "int", 1, "Ticks mínimos para mover SL.", "tpsl", 1, 10000),
    S("AUTO_TPSL_TRAIL_GAP_MULT", "float", 1.2, "Gap de trailing = volatilidad × mult.", "tpsl", 0.0, 10.0),
    S("AUTO_TPSL_TRAIL_GAP_MIN", "float", 0.001, "Gap de trailing mínimo (fracción).", "tpsl", 0.0, 0.2),
    S("AUTO_TPSL_TP_EXT_MULT", "float", 1.25, "Extensión de TP al acercarse.", "tpsl", 1.0, 5.0),
    S("AUTO_TPSL_ML_TP_BOOST", "float", 1.0, "Boost de extensión TP si ML confía.", "tpsl", 0.0, 10.0),
    S("AUTO_TPSL_TP_PROXIMITY", "float", 0.005, "Distancia relativa al TP que activa la extensión.", "tpsl", 0.0, 0.2),
    S("AUTO_TPSL_MIN_EXT", "float", 0.001, "Extensión/gap mínimo del TP (fracción).", "tpsl", 0.0, 0.2),
    S("AUTO_TPSL_LOW_ML_GAP_MULT", "float", 0.85, "Reduce el gap si ML < 0.5.", "tpsl", 0.1, 1.0),
    S("AUTO_TPSL_MAX_VOLATILITY", "float", 0.25, "Cota de volatilidad usada para el gap.", "tpsl", 0.0, 1.0),
    S("AUTO_TPSL_CLOSE_ORDER_TYPE", "str", "Market", "Tipo de orden al disparar TP/SL virtual.", "tpsl",
      choices=("Limit", "Market"), normalize=_norm_order_type),

    # --- ML ---
    S("ML_ENABLED", "bool", False, "Filtro ML de probabilidad de acierto.", "ml"),
    S("ML_MIN_SAMPLES", "int", 50, "Trades finales mínimos para entrenar.", "ml", 5, 1000000),
    S("ML_PROB_THRESHOLD", "float", 0.6, "Probabilidad mínima para operar.", "ml", 0.5, 0.99),
    S("ML_EPOCHS", "int", 250, "Épocas de la regresión logística.", "ml", 10, 100000),
    S("ML_LEARNING_RATE", "float", 0.15, "Learning rate.", "ml", 1e-6, 10.0),
    S("ML_L2", "float", 0.02, "Regularización L2.", "ml", 0.0, 10.0),

    # --- Persistencia ---
    S("DATA_DIR", "str", "data", "Directorio de datos (relativo a la raíz del proyecto).", "storage"),
    S("LOGS_DIR", "str", "logs", "Directorio de logs/results.json.", "storage"),
    S("SQLITE_PATH", "str", "", "Ruta SQLite (vacío = DATA_DIR/trading.db).", "storage"),
    S("STORAGE_BACKEND", "str", "duckdb", "Backend de series de alta frecuencia.", "storage",
      choices=("duckdb", "sqlite_legacy"), normalize=_norm_backend),
    S("STORAGE_PATH", "str", "data/nertz.duckdb", "Ruta del archivo DuckDB.", "storage"),
    S("STORAGE_BATCH_INTERVAL_MS", "float", 50.0, "Flush por lotes DuckDB.", "storage", 10.0, 5000.0),
    S("ORDERBOOK_PERSIST_INTERVAL_MS", "float", 200.0, "Persistencia de libro cada N ms.", "storage", 50.0, 600000.0),
    S("TICKER_PERSIST_INTERVAL_MS", "float", 200.0, "Persistencia de ticker cada N ms.", "storage", 50.0, 600000.0),
    S("STORAGE_DISABLE_JSONL", "bool", None,
      "Desactiva metrics_snapshots.jsonl (por defecto sí con duckdb).", "storage"),
    S("STORAGE_SQLITE_MIRROR", "bool", True, "Espejo SQLite de ticks/libro.", "storage"),
    S("RESULTS_MAX_EVENTS", "int", 2000, "Eventos conservados en results.json.", "storage", 0, 1000000),
    S("RESULTS_PRECISION", "int", 6, "Decimales en resúmenes de results.json.", "storage", 0, 12),

    # --- Operación / API ---
    S("MAX_CANDLES_API", "int", 200, "Velas máximas por consulta API.", "api", 1, 10000),
    S("DISCOVERY_CANDLES_LOOKBACK", "int", 500, "Velas de BD usadas por /discovery (soportes/resistencias).", "api", 10, 100000),
    S("VALIDATION_MARKET_MAX_AGE_S", "float", 15.0, "Edad máxima de datos de mercado en /validation.", "api", 1.0, 3600.0),
    S("API_HOST", "str", "0.0.0.0", "Host del servidor standalone.", "api"),
    S("API_PORT", "int", 8081, "Puerto del servidor standalone.", "api", 1, 65535),
    S("SYMBOL_OVERRIDES_JSON", "json", {},
      'Overrides por símbolo, p. ej. {"XRPUSDT": {"MAX_TRADE_SIZE": 500, "RISK_FACTOR": 0.02}}.', "general",
      normalize=_dict_of_dicts),
    S("DEFAULT_SLEEP_TIME", "int", 10, "Legacy.", "general", 0, None),
    S("RATE_LIMIT_DELAY", "int", 50, "Legacy.", "general", 0, None),
)
del S

SETTINGS_BY_KEY: Dict[str, Setting] = {s.key: s for s in SETTINGS}

# Alias de variables de entorno históricas.
_ENV_ALIASES: Dict[str, str] = {
    "COMBINED_WEIGHTS_JSON": "COMBINED_WEIGHTS",
}


@dataclass(frozen=True)
class BybitEndpoints:
    env: str
    category: str
    rest_private: str
    rest_public: str
    ws_public: str


class ConfigSettings:
    """Configuración viva del motor.

    Atributos en MAYÚSCULAS = valores validados de ``SETTINGS``. Asignar un
    atributo registrado (``config.X = v``) aplica la misma validación y coerción que el archivo .env.
    """

    # Solo anotaciones (sin valor): los atributos reales se crean en __init__.
    _changes: Deque[Dict[str, Any]]
    FORMULAS_JSON: Dict[str, str]

    if TYPE_CHECKING:
        # Las claves de SETTINGS se asignan dinámicamente con object.__setattr__;
        # esto le indica al IDE / type checker que existen (sin efecto en runtime).
        def __getattr__(self, name: str) -> Any:
            ...

    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        source = os.environ if env is None else env
        object.__setattr__(self, "_changes", deque(maxlen=500))
        errors: List[str] = []
        for setting in SETTINGS:
            raw = source.get(setting.key)
            if raw is None and setting.key in _ENV_ALIASES:
                raw = source.get(_ENV_ALIASES[setting.key])
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                raw = setting.default
            try:
                value = None if raw is None else setting.coerce(raw)
            except ConfigError as e:
                errors.append(str(e))
                continue
            object.__setattr__(self, setting.key, value)
        if errors:
            for e in errors:
                logger.error("Configuración inválida: %s", e)
            raise ConfigError("; ".join(errors))
        if self.STORAGE_DISABLE_JSONL is None:
            object.__setattr__(self, "STORAGE_DISABLE_JSONL", self.STORAGE_BACKEND == "duckdb")
        self._validate_cross()
        self._log_config()

    # ------------------------------------------------------------------ core
    def __setattr__(self, key: str, value: Any) -> None:
        setting = SETTINGS_BY_KEY.get(key)
        if setting is not None:
            value = setting.coerce(value)
        elif isinstance(getattr(type(self), key, None), property):
            getattr(type(self), key).fset(self, value)
            return
        object.__setattr__(self, key, value)

    def update(self, values: Mapping[str, Any], *, source: str = "runtime") -> Dict[str, Any]:
        """Aplica cambios validados de forma atómica. Devuelve {clave: (antes, después)}."""
        staged: Dict[str, Any] = {}
        for key, raw in (values or {}).items():
            k = str(key).upper()
            setting = SETTINGS_BY_KEY.get(k)
            if setting is None:
                raise ConfigError(f"Parámetro desconocido: {key}")
            staged[k] = setting.coerce(raw)
        changes: Dict[str, Any] = {}
        for k, v in staged.items():
            before = getattr(self, k, None)
            object.__setattr__(self, k, v)
            if before != v:
                changes[k] = {"before": before, "after": v}
        if changes:
            self._changes.append({"ts": time.time(), "source": source, "changes": changes})
            logger.info("Configuración actualizada (%s): %s", source, sorted(changes))
        return changes

    def for_symbol(self, symbol: Optional[str], key: str) -> Any:
        """Valor efectivo de ``key`` para ``symbol`` (SYMBOL_OVERRIDES_JSON > global)."""
        base = getattr(self, key)
        overrides = self.SYMBOL_OVERRIDES_JSON.get(str(symbol or "").upper()) if symbol else None
        if isinstance(overrides, dict) and key in overrides:
            setting = SETTINGS_BY_KEY.get(key)
            return setting.coerce(overrides[key]) if setting else overrides[key]
        return base

    @property
    def symbols(self) -> List[str]:
        return [s for s in str(self.SYMBOL).split(",") if s]

    @property
    def FORMULAS(self) -> Dict[str, str]:  # noqa: N802 - compat
        return self.FORMULAS_JSON

    @FORMULAS.setter
    def FORMULAS(self, value: Any) -> None:  # noqa: N802 - compat
        self.FORMULAS_JSON = value

    @property
    def signal_params(self) -> SignalParams:
        return SignalParams.from_overrides(self.SIGNAL_PARAMS_JSON)

    @property
    def kline_interval(self) -> str:
        return TIMEFRAME_TO_BYBIT[self.TIMEFRAME]

    @property
    def ws_orderbook_depth(self) -> int:
        """Menor profundidad WS soportada por Bybit que cubre ORDERBOOK_DEPTH."""
        supported = (1, 50, 200) if self.BYBIT_CATEGORY == "spot" else (1, 50, 200, 500)
        for d in supported:
            if d >= int(self.ORDERBOOK_DEPTH):
                return d
        return supported[-1]

    def endpoints(self) -> BybitEndpoints:
        table = BYBIT_ENVIRONMENTS[self.BYBIT_ENV]
        cat = self.BYBIT_CATEGORY
        return BybitEndpoints(
            env=self.BYBIT_ENV,
            category=cat,
            rest_private=(self.BYBIT_REST_URL or table["rest_private"]).rstrip("/"),
            rest_public=(self.BYBIT_PUBLIC_REST_URL or table["rest_public"]).rstrip("/"),
            ws_public=(self.BYBIT_WS_PUBLIC_URL or table["ws_public"].format(category=cat)),
        )

    def account_types(self) -> List[str]:
        out = [self.ACCOUNT_TYPE]
        for a in str(self.ACCOUNT_TYPE_FALLBACKS).split(","):
            a = a.strip().upper()
            if a and a not in out:
                out.append(a)
        return out

    def is_bot_order_link(self, link: Any) -> bool:
        return isinstance(link, str) and bool(link) and link.startswith(self.ORDER_LINK_PREFIX)

    @staticmethod
    def resolve_path(raw: str, project_root: str) -> str:
        path = str(raw or "").strip()
        if not os.path.isabs(path):
            path = os.path.join(project_root, path)
        return os.path.abspath(path)

    # ------------------------------------------------------------ introspect
    @staticmethod
    def schema() -> List[Dict[str, Any]]:
        return [s.describe() for s in SETTINGS]

    def as_dict(self, *, include_secrets: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for s in SETTINGS:
            v = getattr(self, s.key, None)
            if s.secret and not include_secrets:
                v = "SET" if v else "NOT_SET"
            out[s.key] = v
        return out

    def recent_changes(self) -> List[Dict[str, Any]]:
        return list(self._changes)

    def _validate_cross(self) -> None:
        if self.COMBINED_BUY_THRESHOLD <= self.COMBINED_SELL_THRESHOLD:
            logger.warning(
                "COMBINED_BUY_THRESHOLD (%s) <= COMBINED_SELL_THRESHOLD (%s); el motor los simetriza.",
                self.COMBINED_BUY_THRESHOLD,
                self.COMBINED_SELL_THRESHOLD,
            )

    def _log_config(self) -> None:
        logger.info("Configuration loaded:")
        logger.info("  - SYMBOL: %s, TIMEFRAME: %s, ORDER_TYPE: %s", self.SYMBOL, self.TIMEFRAME, self.ORDER_TYPE)
        logger.info(
            "  - BYBIT_ENV: %s, LIVE_TRADING_ENABLED: %s, BYBIT_API_KEY: %s",
            self.BYBIT_ENV,
            self.LIVE_TRADING_ENABLED,
            "SET" if self.BYBIT_API_KEY else "NOT_SET",
        )
        logger.info(
            "  - STORAGE: %s @ %s, batch_ms=%s",
            self.STORAGE_BACKEND,
            self.STORAGE_PATH,
            self.STORAGE_BATCH_INTERVAL_MS,
        )


def render_env_example(settings: Iterable[Setting] = SETTINGS) -> str:
    lines = [
        "# Generado por: python src/settings.py --env-example",
        "# Todas las claves son opcionales; el valor mostrado es el default.",
        "",
    ]
    group = None
    for s in settings:
        if s.group != group:
            group = s.group
            lines += ["", f"# ===== {group} ====="]
        rng = ""
        if s.choices:
            rng = f" [{' | '.join(s.choices)}]"
        elif s.min_value is not None or s.max_value is not None:
            rng = f" [{s.min_value if s.min_value is not None else '-inf'} .. {s.max_value if s.max_value is not None else 'inf'}]"
        lines.append(f"# {s.description}{rng}")
        if s.secret or s.default is None:
            default = ""
        elif s.kind == "json":
            default = json.dumps(s.default, separators=(",", ":")) if s.default else ""
        elif s.kind == "bool":
            default = "true" if s.default else "false"
        else:
            default = str(s.default)
        lines.append(f"{s.key}={default}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    if "--env-example" in sys.argv:
        sys.stdout.write(render_env_example())
    else:
        json.dump(ConfigSettings().schema(), sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
