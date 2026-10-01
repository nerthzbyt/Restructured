"""Estado de mercado en memoria: libro incremental y parsers de mensajes Bybit."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple


class OrderBook:
    """Libro L2 mantenido incrementalmente (snapshot + deltas).

    Antes cada delta reconstruía dos dicts parseando los strings de todo el
    libro; aquí el estado vive como ``{precio: cantidad}`` y la vista en listas
    ``[[precio, qty], ...]`` (formato Bybit) se cachea hasta el siguiente cambio.
    """

    __slots__ = ("depth", "_bids", "_asks", "_view", "updated_ts", "seq")

    def __init__(self, depth: int = 50) -> None:
        self.depth = max(1, int(depth))
        self._bids: Dict[float, Tuple[str, str]] = {}
        self._asks: Dict[float, Tuple[str, str]] = {}
        self._view: Optional[Dict[str, List[List[str]]]] = None
        self.updated_ts: float = 0.0
        self.seq: Optional[int] = None

    @staticmethod
    def _apply(side: Dict[float, Tuple[str, str]], rows: Any) -> None:
        for row in rows or ():
            try:
                p_s, q_s = str(row[0]), str(row[1])
                price = float(p_s)
                qty = float(q_s)
            except (TypeError, ValueError, IndexError):
                continue
            if qty > 0:
                side[price] = (p_s, q_s)
            else:
                side.pop(price, None)

    def apply_snapshot(self, bids: Any, asks: Any, seq: Optional[int] = None) -> None:
        self._bids.clear()
        self._asks.clear()
        self._apply(self._bids, bids)
        self._apply(self._asks, asks)
        self._touch(seq)

    def apply_delta(self, bids: Any, asks: Any, seq: Optional[int] = None) -> bool:
        if not self._bids and not self._asks:
            return False
        self._apply(self._bids, bids)
        self._apply(self._asks, asks)
        self._touch(seq)
        return True

    def _touch(self, seq: Optional[int]) -> None:
        self._view = None
        self.updated_ts = time.time()
        self.seq = seq

    def _render(self) -> Dict[str, List[List[str]]]:
        view = self._view
        if view is None:
            d = self.depth
            bids = [list(self._bids[p]) for p in sorted(self._bids, reverse=True)[:d]]
            asks = [list(self._asks[p]) for p in sorted(self._asks)[:d]]
            view = {"bids": bids, "asks": asks}
            self._view = view
        return view

    # Interfaz dict (compat: el resto del sistema usa orderbook_data[sym]["bids"]).
    def __getitem__(self, key: str) -> List[List[str]]:
        return self._render()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._render().get(key, default)

    def keys(self):
        return self._render().keys()

    def items(self):
        return self._render().items()

    def __iter__(self):
        return iter(self._render())

    def __contains__(self, key: object) -> bool:
        return key in self._render()

    def as_dict(self) -> Dict[str, List[List[str]]]:
        v = self._render()
        return {"bids": list(v["bids"]), "asks": list(v["asks"])}

    def best_bid(self) -> float:
        return max(self._bids) if self._bids else 0.0

    def best_ask(self) -> float:
        return min(self._asks) if self._asks else 0.0

    def is_ready(self) -> bool:
        return bool(self._bids) and bool(self._asks)


@dataclass
class Candle:
    """Vela en memoria (mismos atributos que el modelo MarketData)."""

    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    def as_metric_input(self) -> Dict[str, float]:
        return {"open": self.open, "high": self.high, "low": self.low, "close": self.close, "volume": self.volume}


def candle_from_bybit_row(symbol: str, row: Any) -> Optional[Candle]:
    """Fila REST ``[start, open, high, low, close, volume, turnover]``."""
    try:
        return Candle(
            timestamp=datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc),
            symbol=symbol,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
    except (TypeError, ValueError, IndexError):
        return None


def candle_from_ws(symbol: str, kline: Mapping[str, Any]) -> Optional[Candle]:
    start = kline.get("start")
    if start is None or not str(start).isdigit():
        return None
    try:
        return Candle(
            timestamp=datetime.fromtimestamp(int(start) / 1000, tz=timezone.utc),
            symbol=symbol,
            open=float(kline.get("open", 0)),
            high=float(kline.get("high", 0)),
            low=float(kline.get("low", 0)),
            close=float(kline.get("close", 0)),
            volume=float(kline.get("volume", 0)),
        )
    except (TypeError, ValueError):
        return None


def candle_inputs(candles: List[Any]) -> List[Dict[str, float]]:
    return [
        {"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in candles
    ]


_TICKER_FIELDS = {
    "last_price": "lastPrice",
    "volume_24h": "volume24h",
    "high_24h": "highPrice24h",
    "low_24h": "lowPrice24h",
    "turnover_24h": "turnover24h",
    "usd_index_price": "usdIndexPrice",
    "bid1_price": "bid1Price",
    "ask1_price": "ask1Price",
}
_TICKER_REQUIRED = ("lastPrice", "volume24h", "highPrice24h", "lowPrice24h")


def parse_ticker(ticker: Mapping[str, Any]) -> Optional[Dict[str, float]]:
    """Normaliza un ticker Bybit (REST o WS). ``None`` si faltan campos requeridos."""
    if not isinstance(ticker, Mapping) or not all(k in ticker for k in _TICKER_REQUIRED):
        return None
    out: Dict[str, float] = {}
    for dst, src in _TICKER_FIELDS.items():
        raw = ticker.get(src)
        try:
            v = float(raw) if raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            v = 0.0
        out[dst] = v if math.isfinite(v) else 0.0
    return out


def parse_public_trade(t: Mapping[str, Any], now_s: float) -> Optional[Dict[str, Any]]:
    """Trade público Bybit (``v``/``p``/``T``/``S``) con alias tolerantes."""

    def _first(keys: Tuple[str, ...]) -> Any:
        for k in keys:
            if k in t:
                return t.get(k)
        return None

    try:
        qty = float(_first(("v", "size", "qty", "q")) or 0.0)
    except (TypeError, ValueError):
        qty = 0.0
    if qty <= 0:
        return None
    try:
        price = float(_first(("p", "price", "px")) or 0.0)
    except (TypeError, ValueError):
        price = 0.0
    ts_raw = _first(("T", "ts", "time", "timestamp"))
    try:
        val = float(ts_raw) if ts_raw is not None else None
        ts_s = (val / 1000.0 if val > 10_000_000_000 else val) if val is not None else now_s
    except (TypeError, ValueError):
        ts_s = now_s
    side_raw = _first(("S", "side", "m"))
    if isinstance(side_raw, bool):
        side: Optional[str] = "Sell" if side_raw else "Buy"
    elif isinstance(side_raw, str):
        side = side_raw
    else:
        side = None
    return {"ts": float(ts_s), "qty": qty, "price": price, "side": side}
