"""Rutas REST v5 Bybit usadas por el validador dev."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketPaths:
    time: str = "/v5/market/time"
    kline: str = "/v5/market/kline"
    orderbook: str = "/v5/market/orderbook"
    tickers: str = "/v5/market/tickers"
    recent_trade: str = "/v5/market/recent-trade"
    instruments_info: str = "/v5/market/instruments-info"
    open_interest: str = "/v5/market/open-interest"
    mark_price_kline: str = "/v5/market/mark-price-kline"
    index_price_kline: str = "/v5/market/index-price-kline"


PATHS = MarketPaths()

TIMEFRAME_TO_INTERVAL = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "1d": "D",
}