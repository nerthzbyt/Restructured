"""Combinaciones completas de órdenes spot — itertools + reglas Bybit v5."""
from __future__ import annotations

import itertools
import uuid
from typing import Dict, Iterator, List, Optional

from src_dev.orders.exchange_schema import (
    PRICE_ANCHORS,
    SPOT_IS_LEVERAGE,
    SPOT_MARKET_UNITS,
    SPOT_ORDER_FILTERS,
    SPOT_ORDER_TYPES,
    SPOT_SLIPPAGE_TYPES,
    TP_SL_MODES,
    OrderCombination,
    SpotInstrumentConstraints,
    is_valid_spot_combo,
    spot_tif_for_order_type,
)

SLIPPAGE_VALUES = {
    "TickSize": (1, 5, 10),
    "Percent": (0.05, 0.1, 0.5),
}


def iter_spot_combinations(
    *,
    side_hint: str = "Buy",
    include_slippage: bool = True,
) -> Iterator[OrderCombination]:
    """
    Genera todas las combinaciones spot válidas según reglas del exchange.
    Usa itertools.product para el espacio combinatorio completo filtrado.
    """
    slippage_opts: List[tuple[Optional[str], Optional[float]]] = [(None, None)]
    if include_slippage:
        for st in SPOT_SLIPPAGE_TYPES:
            for val in SLIPPAGE_VALUES.get(st, ()):
                slippage_opts.append((st, float(val)))

    for order_type in SPOT_ORDER_TYPES:
        tifs = spot_tif_for_order_type(order_type)
        market_units: List[Optional[str]] = list(SPOT_MARKET_UNITS) if order_type == "Market" else [None]
        price_anchors: List[Optional[str]] = list(PRICE_ANCHORS) if order_type == "Limit" else [None]

        for tif, market_unit, order_filter, is_lev, price_anchor, tp_sl_mode, (slip_t, slip_v) in itertools.product(
            tifs,
            market_units,
            SPOT_ORDER_FILTERS,
            SPOT_IS_LEVERAGE,
            price_anchors,
            TP_SL_MODES,
            slippage_opts,
        ):
            ok, reason = is_valid_spot_combo(
                order_type,
                tif,
                market_unit,
                order_filter,
                price_anchor,
                tp_sl_mode,
                slip_t,
            )
            combo = OrderCombination(
                order_type=order_type,
                time_in_force=tif,
                market_unit=market_unit,
                order_filter=order_filter,
                is_leverage=is_lev,
                price_anchor=price_anchor,
                tp_sl_mode=tp_sl_mode,
                slippage_type=slip_t,
                slippage_value=slip_v,
                side_hint=side_hint,
                valid=ok,
                invalid_reason=reason if not ok else "",
            )
            if ok:
                yield combo


def build_order_body(
    combo: OrderCombination,
    *,
    symbol: str,
    side: str,
    qty: str,
    price: Optional[str] = None,
    trigger_price: Optional[str] = None,
    take_profit: Optional[str] = None,
    stop_loss: Optional[str] = None,
) -> Dict[str, object]:
    """Plantilla de body POST /v5/order/create — no envía la orden, solo documenta payload."""
    body: Dict[str, object] = {
        "category": "spot",
        "symbol": symbol,
        "side": side,
        "orderType": combo.order_type,
        "qty": qty,
        "timeInForce": combo.time_in_force,
        "orderLinkId": f"devlab-{uuid.uuid4().hex[:16]}",
    }
    if combo.is_leverage:
        body["isLeverage"] = combo.is_leverage
    if combo.order_filter != "Order":
        body["orderFilter"] = combo.order_filter
    if combo.order_type == "Limit" and price:
        body["price"] = price
    if combo.order_type == "Market" and combo.market_unit:
        body["marketUnit"] = combo.market_unit
    if combo.slippage_type and combo.slippage_value is not None:
        body["slippageToleranceType"] = combo.slippage_type
        body["slippageTolerance"] = str(combo.slippage_value)
    if combo.tp_sl_mode in ("bracket_on_limit", "order_filter_tpsl") and take_profit and stop_loss:
        body["takeProfit"] = take_profit
        body["stopLoss"] = stop_loss
        body["tpOrderType"] = "Market"
        body["slOrderType"] = "Market"
    if combo.tp_sl_mode == "conditional_stop" and trigger_price:
        body["triggerPrice"] = trigger_price
    return body


def resolve_limit_price(anchor: str, ob_stats: Dict[str, float], combined: float) -> float:
    """Precio Limit desde orderbook live del exchange."""
    bid = float(ob_stats.get("best_bid") or 0.0)
    ask = float(ob_stats.get("best_ask") or 0.0)
    mid = float(ob_stats.get("mid") or (bid + ask) / 2.0)
    micro = float(ob_stats.get("microprice") or mid)
    tick = float(ob_stats.get("tick_size") or 0.01)

    if anchor == "best_bid":
        return bid
    if anchor == "best_ask":
        return ask
    if anchor == "mid":
        return mid
    if anchor == "microprice":
        return micro
    if anchor == "combined_chase":
        offset_ticks = 1 if combined >= 0 else -1
        return mid + offset_ticks * tick
    return mid


def qty_for_notional(
    notional_usdt: float,
    price: float,
    constraints: SpotInstrumentConstraints,
) -> str:
    if price <= 0:
        return "0"
    raw = notional_usdt / price
    step = max(constraints.qty_step, 1e-12)
    qty = max(constraints.min_qty, int(raw / step) * step)
    return f"{qty:.8f}".rstrip("0").rstrip(".")


def list_all_valid_combinations(side_hint: str = "Buy") -> List[OrderCombination]:
    return list(iter_spot_combinations(side_hint=side_hint))