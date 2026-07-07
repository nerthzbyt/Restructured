"""Recálculo independiente de métricas RAW — cross-check contra src/utils.py."""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


def _parse_book(
    orderbook: Dict[str, Any],
    depth: int,
    pct_band: float,
) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], float, float]:
    bids_in: List[Tuple[float, float]] = []
    asks_in: List[Tuple[float, float]] = []
    for row in (orderbook.get("bids") or [])[:depth]:
        try:
            p, q = float(row[0]), float(row[1])
            if p > 0 and q > 0:
                bids_in.append((p, q))
        except (TypeError, ValueError, IndexError):
            continue
    for row in (orderbook.get("asks") or [])[:depth]:
        try:
            p, q = float(row[0]), float(row[1])
            if p > 0 and q > 0:
                asks_in.append((p, q))
        except (TypeError, ValueError, IndexError):
            continue

    if not bids_in or not asks_in:
        return [], [], 0.0, 0.0

    best_bid = bids_in[0][0]
    best_ask = asks_in[0][0]
    mid = (best_bid + best_ask) / 2.0
    band = mid * pct_band
    lo, hi = mid - band, mid + band
    bids = [(p, q) for p, q in bids_in if lo <= p <= hi]
    asks = [(p, q) for p, q in asks_in if lo <= p <= hi]
    return bids, asks, mid, (best_ask - best_bid)


def compute_reference_raw(
    candles: List[Dict[str, float]],
    orderbook: Dict[str, Any],
    ticker_payload: Dict[str, Any],
    *,
    depth: int = 50,
) -> Dict[str, float]:
    """
    Replica las fórmulas de pio_raw, ild_raw, ogm_raw, rol_raw de utils.calculate_metrics.
    No calcula z-scores — solo valida la capa física/raw.
    """
    lambda_ = float(ticker_payload.get("orderbook_lambda") or 0.03)
    pct_band = float(ticker_payload.get("orderbook_pct_band") or 0.015)
    target_move = float(ticker_payload.get("ild_target_move") or 0.002)

    bids, asks, mid, spread = _parse_book(orderbook, depth, pct_band)
    if not bids or not asks or mid <= 0:
        return {"pio_raw": 0.0, "ild_raw": 0.0, "ogm_raw": 0.0, "rol_raw": 0.0, "valid": 0.0}

    bid_w_sum = 0.0
    ask_w_sum = 0.0
    for p, q in bids:
        bid_w_sum += q * math.exp(-lambda_ * max(0.0, mid - p))
    for p, q in asks:
        ask_w_sum += q * math.exp(-lambda_ * max(0.0, p - mid))

    pio_raw = bid_w_sum - ask_w_sum
    weighted_liquidity = bid_w_sum + ask_w_sum

    up_target = mid * (1.0 + target_move)
    down_target = mid * (1.0 - target_move)

    up_notional = 0.0
    for p, q in sorted(asks, key=lambda x: x[0]):
        up_notional += p * q
        if p >= up_target:
            break

    down_notional = 0.0
    for p, q in sorted(bids, key=lambda x: x[0], reverse=True):
        down_notional += p * q
        if p <= down_target:
            break

    ild_raw = (up_notional + down_notional) / 2.0

    def _gap_stats(levels: List[Tuple[float, float]], ascending: bool) -> Tuple[float, float]:
        if len(levels) < 3:
            return 0.0, 0.0
        prices = np.array([p for p, _ in sorted(levels, key=lambda x: x[0], reverse=not ascending)])
        qtys = np.array([q for _, q in sorted(levels, key=lambda x: x[0], reverse=not ascending)])
        gaps = np.abs(np.diff(prices))
        if len(gaps) == 0:
            return 0.0, 0.0
        med_gap = float(np.median(gaps))
        q_thr = float(np.quantile(qtys, 0.9))
        large_idx = np.where(qtys[:-1] >= q_thr)[0]
        if len(large_idx) == 0:
            return med_gap, med_gap
        return med_gap, float(np.mean(gaps[large_idx]))

    ask_med, ask_large = _gap_stats(asks, True)
    bid_med, bid_large = _gap_stats(bids, False)
    ogm_raw = (ask_large - ask_med) - (bid_large - bid_med)

    rol_raw = 0.0
    prev_liq = ticker_payload.get("prev_weighted_liquidity")
    dt_s = ticker_payload.get("rol_dt_s")
    try:
        if prev_liq is not None and dt_s is not None and float(dt_s) > 0:
            rol_raw = (weighted_liquidity - float(prev_liq)) / float(dt_s)
    except (TypeError, ValueError):
        rol_raw = 0.0

    return {
        "pio_raw": float(pio_raw),
        "ild_raw": float(ild_raw),
        "ogm_raw": float(ogm_raw),
        "rol_raw": float(rol_raw),
        "weighted_liquidity": float(weighted_liquidity),
        "mid_price": float(mid),
        "spread": float(spread),
        "valid": 1.0,
    }