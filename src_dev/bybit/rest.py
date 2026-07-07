"""Cliente REST Bybit v5 (aiohttp) — snapshot completo de mercado."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import aiohttp

from src_dev.bybit.endpoints import PATHS, TIMEFRAME_TO_INTERVAL
from src_dev.config import BybitEndpoints, DevSettings

logger = logging.getLogger("src_dev.bybit.rest")


class BybitRestClient:
    def __init__(
        self,
        settings: Optional[DevSettings] = None,
        endpoints: Optional[BybitEndpoints] = None,
        *,
        timeout_s: float = 20.0,
    ):
        self.settings = settings or DevSettings.from_env()
        self.endpoints = endpoints or self.settings.endpoints
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "BybitRestClient":
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def _session_or_raise(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise RuntimeError("BybitRestClient: usar dentro de 'async with'")
        return self._session

    async def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        session = self._session_or_raise()
        url = self.endpoints.rest(path)
        async with session.get(url, params=params) as resp:
            payload = await resp.json(content_type=None)
            return {
                "http_status": resp.status,
                "url": str(resp.url),
                **(payload if isinstance(payload, dict) else {"raw": payload}),
            }

    async def server_time(self) -> Dict[str, Any]:
        return await self.get(PATHS.time, {})

    async def klines(
        self,
        symbol: str,
        *,
        category: str = "spot",
        interval: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, float]]:
        tf = interval or TIMEFRAME_TO_INTERVAL.get(self.settings.timeframe, "1")
        payload = await self.get(
            PATHS.kline,
            {"category": category, "symbol": symbol, "interval": tf, "limit": limit},
        )
        if payload.get("retCode") != 0:
            logger.warning("kline retCode=%s msg=%s", payload.get("retCode"), payload.get("retMsg"))
            return []
        rows = (payload.get("result") or {}).get("list") or []
        candles: List[Dict[str, float]] = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            candles.append(
                {
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "start_ms": int(row[0]),
                }
            )
        return candles

    async def orderbook(
        self,
        symbol: str,
        *,
        category: str = "spot",
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        depth = int(limit or self.settings.orderbook_depth)
        depth = max(1, min(depth, 200 if category == "linear" else 50))
        payload = await self.get(
            PATHS.orderbook,
            {"category": category, "symbol": symbol, "limit": depth},
        )
        if payload.get("retCode") != 0:
            return {"bids": [], "asks": [], "raw": payload}
        result = payload.get("result") or {}
        return {
            "bids": result.get("b") or [],
            "asks": result.get("a") or [],
            "ts": result.get("ts"),
            "u": result.get("u"),
            "raw": payload,
        }

    async def ticker(self, symbol: str, *, category: str = "spot") -> Dict[str, Any]:
        payload = await self.get(
            PATHS.tickers,
            {"category": category, "symbol": symbol},
        )
        if payload.get("retCode") != 0:
            return {"raw": payload}
        lst = (payload.get("result") or {}).get("list") or []
        row = lst[0] if lst else {}
        return {
            "last_price": float(row.get("lastPrice") or 0.0),
            "volume_24h": float(row.get("volume24h") or 0.0),
            "high_24h": float(row.get("highPrice24h") or 0.0),
            "low_24h": float(row.get("lowPrice24h") or 0.0),
            "bid1": float(row.get("bid1Price") or 0.0),
            "ask1": float(row.get("ask1Price") or 0.0),
            "mark_price": float(row.get("markPrice") or row.get("lastPrice") or 0.0),
            "index_price": float(row.get("indexPrice") or row.get("lastPrice") or 0.0),
            "open_interest": float(row.get("openInterest") or 0.0),
            "funding_rate": float(row.get("fundingRate") or 0.0),
            "symbol": row.get("symbol") or symbol,
            "raw": row,
        }

    async def recent_trades(
        self,
        symbol: str,
        *,
        category: str = "spot",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        payload = await self.get(
            PATHS.recent_trade,
            {"category": category, "symbol": symbol, "limit": limit},
        )
        if payload.get("retCode") != 0:
            return []
        rows = (payload.get("result") or {}).get("list") or []
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            side = str(row.get("side") or "").lower()
            out.append(
                {
                    "price": float(row.get("price") or 0.0),
                    "qty": float(row.get("size") or row.get("qty") or 0.0),
                    "side": side,
                    "time_ms": int(row.get("time") or 0),
                    "is_buy": side == "buy",
                }
            )
        return out

    async def instrument_rules(
        self,
        symbol: str,
        *,
        category: str = "spot",
    ) -> Dict[str, float]:
        payload = await self.get(
            PATHS.instruments_info,
            {"category": category, "symbol": symbol},
        )
        rules = {
            "tick_size": 0.01,
            "qty_step": 0.0001,
            "min_qty": 0.0001,
            "min_notional": 1.0,
        }
        if payload.get("retCode") != 0:
            return rules
        lst = (payload.get("result") or {}).get("list") or []
        row = lst[0] if lst else {}
        pf = row.get("priceFilter") or {}
        lf = row.get("lotSizeFilter") or {}
        try:
            if pf.get("tickSize") is not None:
                rules["tick_size"] = float(pf["tickSize"])
            if lf.get("qtyStep") is not None:
                rules["qty_step"] = float(lf["qtyStep"])
            elif lf.get("basePrecision") is not None:
                rules["qty_step"] = float(lf["basePrecision"])
            if lf.get("minOrderQty") is not None:
                rules["min_qty"] = float(lf["minOrderQty"])
            mn = lf.get("minNotionalValue") or lf.get("minOrderAmt")
            if mn is not None:
                rules["min_notional"] = float(mn)
        except (TypeError, ValueError):
            pass
        return rules

    async def open_interest(
        self,
        symbol: str,
        *,
        category: str = "linear",
        interval_time: str = "5min",
        limit: int = 1,
    ) -> Optional[float]:
        """Open interest solo disponible en linear/inverse, no en spot."""
        payload = await self.get(
            PATHS.open_interest,
            {
                "category": category,
                "symbol": symbol,
                "intervalTime": interval_time,
                "limit": limit,
            },
        )
        if payload.get("retCode") != 0:
            return None
        lst = (payload.get("result") or {}).get("list") or []
        if not lst:
            return None
        try:
            return float(lst[0].get("openInterest") or 0.0)
        except (TypeError, ValueError):
            return None

    async def fetch_market_snapshot(
        self,
        symbol: Optional[str] = None,
        *,
        include_oi: bool = True,
    ) -> Dict[str, Any]:
        """Snapshot unificado: todo lo necesario para calculate_metrics."""
        sym = symbol or self.settings.symbol
        candles, orderbook, ticker, trades, rules = await _gather(
            self.klines(sym),
            self.orderbook(sym),
            self.ticker(sym),
            self.recent_trades(sym, limit=self.settings.recent_trades_limit),
            self.instrument_rules(sym),
        )
        oi = None
        if include_oi:
            oi = await self.open_interest(sym, category="linear")
        return {
            "symbol": sym,
            "candles": candles,
            "orderbook": orderbook,
            "ticker": ticker,
            "recent_trades": trades,
            "instrument_rules": rules,
            "open_interest_linear": oi,
        }


async def _gather(*coros):
    import asyncio

    return await asyncio.gather(*coros)