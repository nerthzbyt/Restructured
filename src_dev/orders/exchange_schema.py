"""
Esquema spot Bybit v5 — extraído de documentación oficial (docs/v5/order/create-order).
Actualizado según API vigente; no usar tablas locales.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Tuple

# Fuente: https://bybit-exchange.github.io/docs/v5/order/create-order (spot)

SPOT_ORDER_TYPES: Tuple[str, ...] = ("Limit", "Market")

SPOT_TIME_IN_FORCE: Tuple[str, ...] = ("GTC", "IOC", "FOK", "PostOnly", "RPI")

SPOT_ORDER_FILTERS: Tuple[str, ...] = ("Order", "tpslOrder", "StopOrder")

SPOT_MARKET_UNITS: Tuple[str, ...] = ("baseCoin", "quoteCoin")

SPOT_IS_LEVERAGE: Tuple[int, ...] = (0, 1)

# Alias retrocompat
ORDER_FILTERS = SPOT_ORDER_FILTERS
IS_LEVERAGE = SPOT_IS_LEVERAGE
MARKET_UNITS = SPOT_MARKET_UNITS

SPOT_SLIPPAGE_TYPES: Tuple[str, ...] = ("TickSize", "Percent")

# Anclas de precio para Limit (derivadas del orderbook live del exchange)
PRICE_ANCHORS: Tuple[str, ...] = (
    "best_bid",
    "best_ask",
    "mid",
    "microprice",
    "combined_chase",
)

# Modos TP/SL spot (docs: Limit soporta TP/SL; tpslOrder / StopOrder vía orderFilter)
TP_SL_MODES: Tuple[str, ...] = (
    "none",
    "bracket_on_limit",
    "order_filter_tpsl",
    "conditional_stop",
)

# Parámetros obligatorios create-order spot
REQUIRED_CREATE_KEYS: FrozenSet[str] = frozenset(
    {"category", "symbol", "side", "orderType", "qty"}
)

# Parámetros opcionales spot relevantes para el laboratorio
OPTIONAL_SPOT_KEYS: Tuple[str, ...] = (
    "price",
    "timeInForce",
    "marketUnit",
    "orderFilter",
    "isLeverage",
    "orderLinkId",
    "takeProfit",
    "stopLoss",
    "tpOrderType",
    "slOrderType",
    "triggerPrice",
    "triggerDirection",
    "slippageToleranceType",
    "slippageTolerance",
    "rpiTakerAccess",
)


@dataclass(frozen=True)
class SpotInstrumentConstraints:
    """Límites reales del par — desde instruments-info del exchange."""

    symbol: str
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional: float
    max_order_qty: float
    max_mkt_order_qty: float
    status: str = "Trading"
    base_coin: str = ""
    quote_coin: str = ""


@dataclass
class OrderCombination:
    """Perfil de orden spot — combinación válida según reglas Bybit + contexto métricas."""

    order_type: str
    time_in_force: str
    market_unit: str | None
    order_filter: str
    is_leverage: int
    price_anchor: str | None
    tp_sl_mode: str
    slippage_type: str | None
    slippage_value: float | None
    side_hint: str
    body_template: Dict[str, object] = field(default_factory=dict)
    valid: bool = True
    invalid_reason: str = ""

    def combo_id(self) -> str:
        parts = [
            self.order_type,
            self.time_in_force,
            self.market_unit or "-",
            self.order_filter,
            f"lev{self.is_leverage}",
            self.price_anchor or "-",
            self.tp_sl_mode,
            self.slippage_type or "-",
            self.side_hint,
        ]
        return "|".join(parts)


def spot_tif_for_order_type(order_type: str) -> Tuple[str, ...]:
    if order_type == "Market":
        return ("IOC",)
    return ("GTC", "IOC", "FOK", "PostOnly")


def is_valid_spot_combo(
    order_type: str,
    time_in_force: str,
    market_unit: str | None,
    order_filter: str,
    price_anchor: str | None,
    tp_sl_mode: str,
    slippage_type: str | None,
) -> Tuple[bool, str]:
    if order_type == "Market":
        if time_in_force != "IOC":
            return False, "Market spot fuerza IOC en Bybit"
        if price_anchor is not None:
            return False, "Market no usa price anchor"
        if tp_sl_mode == "bracket_on_limit":
            return False, "bracket_on_limit solo Limit"
    if order_type == "Limit":
        if market_unit is not None:
            return False, "marketUnit solo Market"
        if price_anchor is None:
            return False, "Limit requiere price_anchor"
        if time_in_force == "PostOnly" and tp_sl_mode not in ("none", "bracket_on_limit"):
            return False, "PostOnly incompatible con orderFilter condicional"
    if time_in_force == "PostOnly" and order_type != "Limit":
        return False, "PostOnly solo Limit"
    if order_filter in ("tpslOrder", "StopOrder") and tp_sl_mode == "none":
        return False, "orderFilter condicional requiere tp_sl_mode"
    if order_filter == "Order" and tp_sl_mode in ("order_filter_tpsl", "conditional_stop"):
        return False, "tp_sl condicional requiere orderFilter tpslOrder/StopOrder"
    if slippage_type and order_type != "Market":
        return False, "slippageTolerance solo Market"
    if tp_sl_mode == "conditional_stop" and order_filter != "StopOrder":
        return False, "conditional_stop usa StopOrder"
    if tp_sl_mode == "order_filter_tpsl" and order_filter != "tpslOrder":
        return False, "order_filter_tpsl usa tpslOrder"
    return True, ""