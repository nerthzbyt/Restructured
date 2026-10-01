"""Colector WebSocket público Bybit v5 — merge de orderbook, ticker, trades, kline."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from src_dev.bybit.endpoints import TIMEFRAME_TO_INTERVAL
from src_dev.config import DevSettings

logger = logging.getLogger("src_dev.bybit.ws")


class BybitWsCollector:
    """Acumula estado de mercado desde WS spot (misma URL que Nertzh)."""

    def __init__(self, settings: Optional[DevSettings] = None):
        self.settings = settings or DevSettings.from_env()
        self.symbol = self.settings.symbol
        self._orderbook: Dict[str, Any] = {"bids": [], "asks": []}
        self._ticker: Dict[str, Any] = {"last_price": 0.0}
        self._recent_trades: List[Dict[str, Any]] = []
        self._candles: Dict[str, Dict[str, float]] = {}
        self._message_count = 0
        self._last_msg_ts = 0.0

    @property
    def ws_url(self) -> str:
        return self.settings.endpoints.ws_spot_public

    def _topics(self) -> List[str]:
        sym = self.symbol
        interval = TIMEFRAME_TO_INTERVAL.get(self.settings.timeframe, "1")
        depth = min(50, max(1, int(self.settings.orderbook_depth)))
        return [
            f"orderbook.{depth}.{sym}",
            f"tickers.{sym}",
            f"publicTrade.{sym}",
            f"kline.{interval}.{sym}",
        ]

    def _apply_orderbook(self, payload: Dict[str, Any]) -> None:
        data = payload.get("data") or {}
        msg_type = str(data.get("type") or payload.get("type") or "").lower()
        bids = data.get("b") or data.get("bids") or []
        asks = data.get("a") or data.get("asks") or []

        if msg_type == "snapshot" or not self._orderbook.get("bids"):
            self._orderbook = {"bids": list(bids), "asks": list(asks)}
            return

        bid_map = {float(r[0]): float(r[1]) for r in self._orderbook.get("bids", []) if len(r) >= 2}
        ask_map = {float(r[0]): float(r[1]) for r in self._orderbook.get("asks", []) if len(r) >= 2}
        for row in bids:
            if len(row) < 2:
                continue
            p, q = float(row[0]), float(row[1])
            if q <= 0:
                bid_map.pop(p, None)
            else:
                bid_map[p] = q
        for row in asks:
            if len(row) < 2:
                continue
            p, q = float(row[0]), float(row[1])
            if q <= 0:
                ask_map.pop(p, None)
            else:
                ask_map[p] = q
        self._orderbook = {
            "bids": [[p, q] for p, q in sorted(bid_map.items(), reverse=True)],
            "asks": [[p, q] for p, q in sorted(ask_map.items())],
        }

    def _apply_ticker(self, payload: Dict[str, Any]) -> None:
        data = payload.get("data")
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            lp = row.get("lastPrice")
            if lp is not None:
                self._ticker = {
                    "last_price": float(lp),
                    "volume_24h": float(row.get("volume24h") or 0.0),
                    "high_24h": float(row.get("highPrice24h") or 0.0),
                    "low_24h": float(row.get("lowPrice24h") or 0.0),
                    "bid1": float(row.get("bid1Price") or 0.0),
                    "ask1": float(row.get("ask1Price") or 0.0),
                }

    def _apply_trade(self, payload: Dict[str, Any]) -> None:
        data = payload.get("data")
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            side = str(row.get("S") or row.get("side") or "").lower()
            self._recent_trades.append(
                {
                    "price": float(row.get("p") or row.get("price") or 0.0),
                    "qty": float(row.get("v") or row.get("size") or 0.0),
                    "side": side,
                    "time_ms": int(row.get("T") or row.get("time") or 0),
                    "is_buy": side == "buy",
                }
            )
        if len(self._recent_trades) > 500:
            self._recent_trades = self._recent_trades[-500:]

    def _apply_kline(self, payload: Dict[str, Any]) -> None:
        rows = payload.get("data")
        if not isinstance(rows, list):
            rows = [rows] if isinstance(rows, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            confirm = row.get("confirm")
            if confirm is False:
                continue
            key = str(row.get("start") or row.get("timestamp") or "")
            self._candles[key] = {
                "open": float(row.get("open") or 0.0),
                "high": float(row.get("high") or 0.0),
                "low": float(row.get("low") or 0.0),
                "close": float(row.get("close") or 0.0),
                "volume": float(row.get("volume") or 0.0),
            }

    def _dispatch(self, payload: Dict[str, Any]) -> None:
        topic = str(payload.get("topic") or "")
        if topic.startswith("orderbook."):
            self._apply_orderbook(payload)
        elif topic.startswith("tickers."):
            self._apply_ticker(payload)
        elif topic.startswith("publicTrade."):
            self._apply_trade(payload)
        elif topic.startswith("kline."):
            self._apply_kline(payload)
        self._message_count += 1
        self._last_msg_ts = time.time()

    async def collect(self, duration_s: float = 15.0) -> Dict[str, Any]:
        import websockets

        messages: List[Dict[str, Any]] = []
        t0 = time.time()
        async with websockets.connect(self.ws_url, open_timeout=12) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": self._topics()}))
            while time.time() - t0 < duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, duration_s))
                except asyncio.TimeoutError:
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if payload.get("op") in {"subscribe", "ping", "pong"}:
                    continue
                messages.append(payload)
                self._dispatch(payload)

        candles = sorted(
            self._candles.values(),
            key=lambda c: c.get("close", 0.0),
            reverse=True,
        )
        return {
            "symbol": self.symbol,
            "duration_s": duration_s,
            "message_count": self._message_count,
            "ws_messages_sample": messages[:5],
            "orderbook": self._orderbook,
            "ticker": self._ticker,
            "recent_trades": list(self._recent_trades[-self.settings.recent_trades_limit :]),
            "candles": candles,
            "ready": bool(
                self._orderbook.get("bids")
                and self._orderbook.get("asks")
                and float(self._ticker.get("last_price") or 0.0) > 0
            ),
        }