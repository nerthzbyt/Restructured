"""Análisis estructural del orderbook — independiente de utils."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _parse_side(rows: List, depth: int) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for row in (rows or [])[:depth]:
        try:
            p, q = float(row[0]), float(row[1])
            if p > 0 and q > 0:
                out.append((p, q))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def analyze_orderbook(
    orderbook: Dict[str, Any],
    *,
    depth: int = 50,
    last_price: float = 0.0,
) -> Dict[str, Any]:
    bids = _parse_side(orderbook.get("bids"), depth)
    asks = _parse_side(orderbook.get("asks"), depth)
    if not bids or not asks:
        return {"valid": False, "reason": "empty_book"}

    best_bid, best_bid_qty = bids[0]
    best_ask, best_ask_qty = asks[0]
    mid = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    spread_pct = spread / mid if mid > 0 else 0.0

    bid_notional = sum(p * q for p, q in bids)
    ask_notional = sum(p * q for p, q in asks)
    total_qty_bid = sum(q for _, q in bids)
    total_qty_ask = sum(q for _, q in asks)

    microprice = (
        (best_ask * best_bid_qty + best_bid * best_ask_qty) / (best_bid_qty + best_ask_qty)
        if (best_bid_qty + best_ask_qty) > 0
        else mid
    )

    depth_imbalance = (total_qty_bid - total_qty_ask) / (total_qty_bid + total_qty_ask + 1e-12)

    return {
        "valid": True,
        "levels_bid": len(bids),
        "levels_ask": len(asks),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "spread_bps": spread_pct * 1e4,
        "microprice": microprice,
        "bid_notional_top": bid_notional,
        "ask_notional_top": ask_notional,
        "depth_imbalance_qty": depth_imbalance,
        "last_price_delta_bps": ((last_price - mid) / mid * 1e4) if mid > 0 and last_price > 0 else 0.0,
    }