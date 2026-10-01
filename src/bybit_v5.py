"""Cliente asíncrono Bybit v5 (REST firmado + endpoints públicos de mercado)."""
import asyncio
import json
import random
import time
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlencode

import aiohttp

from utils import generate_signature


def _canonical_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _query(**params: Any) -> str:
    """Query string estable (orden de inserción) omitiendo valores vacíos."""
    clean = {k: v for k, v in params.items() if v is not None and v != ""}
    return urlencode(clean, safe=",")


class BybitV5Client:
    # Filtros de órdenes abiertas que Bybit separa por tipo.
    OPEN_ORDER_FILTERS: Tuple[str, ...] = (
        "Order",
        "StopOrder",
        "tpslOrder",
        "OcoOrder",
        "BidirectionalTpslOrder",
    )

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        recv_window: str = "5000",
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_s: float = 15.0,
        max_retries: int = 3,
        backoff_base_s: float = 0.4,
        backoff_max_s: float = 4.0,
        public_base_url: Optional[str] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.public_base_url = (public_base_url or base_url).rstrip("/")
        self.recv_window = str(recv_window)
        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session: bool = session is None
        self.timeout_s = float(timeout_s)
        self.max_retries = int(max_retries)
        self.backoff_base_s = float(backoff_base_s)
        self.backoff_max_s = float(backoff_max_s)

    @staticmethod
    def _timestamp_ms() -> str:
        return str(int(time.time() * 1000))

    def _sign(self, payload: str, timestamp_ms: str) -> str:
        prehash = f"{timestamp_ms}{self.api_key}{self.recv_window}{payload}"
        return generate_signature(self.api_secret, prehash)

    # Compatibilidad con llamadas existentes.
    def _sign_get(self, query_string: str, timestamp_ms: str) -> str:
        return self._sign(query_string, timestamp_ms)

    def _sign_post(self, body_str: str, timestamp_ms: str) -> str:
        return self._sign(body_str, timestamp_ms)

    def _headers(self, signature: str, timestamp_ms: str) -> Dict[str, str]:
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp_ms,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type": "application/json",
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        session = self._session
        if session is None or session.closed:
            timeout = aiohttp.ClientTimeout(total=max(1.0, float(self.timeout_s)))
            session = aiohttp.ClientSession(timeout=timeout)
            self._session = session
            self._owns_session = True
        return session

    async def aclose(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _should_retry_http(status: int) -> bool:
        if status == 429:
            return True
        if status in (408, 409):
            return True
        return 500 <= int(status) <= 599

    def _retry_delay_s(self, attempt: int, *, retry_after_s: Optional[float] = None) -> float:
        if isinstance(retry_after_s, (int, float)) and retry_after_s > 0:
            return float(min(self.backoff_max_s, retry_after_s))
        base = float(self.backoff_base_s) * (2 ** max(0, int(attempt)))
        jitter = random.uniform(0.0, 0.2)
        return float(min(self.backoff_max_s, base + jitter))

    @staticmethod
    def _parse_retry_after_s(headers: Mapping[str, Any]) -> Optional[float]:
        raw = headers.get("Retry-After") if isinstance(headers, Mapping) else None
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        query_string: str = "",
        body_str: str = "",
        signed: bool = True,
    ) -> Tuple[int, Dict[str, Any]]:
        session = await self._get_session()
        is_get = method.upper() == "GET"
        base = self.base_url if signed else self.public_base_url
        url = f"{base}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        last_status: int = 0
        last_payload: Dict[str, Any] = {"retCode": -1, "retMsg": "request_failed"}

        for attempt in range(max(0, int(self.max_retries)) + 1):
            headers: Optional[Dict[str, str]] = None
            if signed:
                ts = self._timestamp_ms()
                headers = self._headers(self._sign(query_string if is_get else body_str, ts), ts)
            try:
                if is_get:
                    request = session.get(url, headers=headers)
                else:
                    request = session.post(url, data=body_str.encode("utf-8"), headers=headers)
                async with request as resp:
                    last_status = int(resp.status)
                    try:
                        last_payload = await resp.json(content_type=None)
                    except ValueError:
                        txt = await resp.text()
                        last_payload = {"retCode": -1, "retMsg": "non_json_response", "text": txt}
                    if self._should_retry_http(last_status) and attempt < int(self.max_retries):
                        await asyncio.sleep(
                            self._retry_delay_s(attempt, retry_after_s=self._parse_retry_after_s(resp.headers))
                        )
                        continue
                    return last_status, last_payload
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_payload = {"retCode": -1, "retMsg": str(e)}
                if attempt < int(self.max_retries):
                    await asyncio.sleep(self._retry_delay_s(attempt))
                    continue
                return last_status, last_payload

        return last_status, last_payload

    async def get(self, path: str, query_string: str) -> Tuple[int, Dict[str, Any]]:
        return await self._request_json("GET", path, query_string=query_string)

    async def post(self, path: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        body_str = _canonical_json(body)
        return await self._request_json("POST", path, body_str=body_str)

    async def _signed_get(self, path: str, **params: Any) -> Dict[str, Any]:
        status, data = await self.get(path, _query(**params))
        return {"http_status": status, **(data or {})}

    async def _signed_post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        status, data = await self.post(path, body)
        return {"http_status": status, **(data or {})}

    async def _public_get(self, path: str, **params: Any) -> Dict[str, Any]:
        status, data = await self._request_json("GET", path, query_string=_query(**params), signed=False)
        return {"http_status": status, **(data or {})}

    # ------------------------------------------------------------ market
    async def get_server_time(self) -> Dict[str, Any]:
        return await self._public_get("/v5/market/time")

    async def get_kline(self, category: str, symbol: str, interval: str, limit: int = 200) -> Dict[str, Any]:
        return await self._public_get(
            "/v5/market/kline", category=category, symbol=symbol, interval=interval, limit=int(limit)
        )

    async def get_orderbook(self, category: str, symbol: str, limit: int = 50) -> Dict[str, Any]:
        return await self._public_get("/v5/market/orderbook", category=category, symbol=symbol, limit=int(limit))

    async def get_tickers(self, category: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        return await self._public_get("/v5/market/tickers", category=category, symbol=symbol)

    async def get_instruments_info(self, category: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        return await self._public_get("/v5/market/instruments-info", category=category, symbol=symbol)

    # ------------------------------------------------------------ account
    async def wallet_balance(self, account_type: str = "UNIFIED", coin: Optional[str] = None) -> Dict[str, Any]:
        return await self._signed_get("/v5/account/wallet-balance", accountType=account_type, coin=coin)

    # ------------------------------------------------------------ orders
    async def create_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return await self._signed_post("/v5/order/create", body)

    async def cancel_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return await self._signed_post("/v5/order/cancel", body)

    async def amend_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return await self._signed_post("/v5/order/amend", body)

    async def order_realtime(
        self,
        category: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None,
        order_filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self._signed_get(
            "/v5/order/realtime",
            category=category,
            symbol=symbol,
            orderId=order_id,
            orderLinkId=order_link_id,
            orderFilter=order_filter,
            limit=int(limit) if isinstance(limit, int) and limit > 0 else None,
        )

    async def order_history(
        self,
        category: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None,
        limit: Optional[int] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._signed_get(
            "/v5/order/history",
            category=category,
            symbol=symbol,
            orderId=order_id,
            orderLinkId=order_link_id,
            limit=int(limit) if isinstance(limit, int) and limit > 0 else None,
            startTime=int(start_time_ms) if start_time_ms is not None else None,
            endTime=int(end_time_ms) if end_time_ms is not None else None,
            cursor=cursor,
        )

    async def execution_list(
        self,
        category: str,
        *,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._signed_get(
            "/v5/execution/list",
            category=category,
            symbol=symbol,
            orderId=order_id,
            orderLinkId=order_link_id,
            startTime=int(start_time_ms) if start_time_ms is not None else None,
            endTime=int(end_time_ms) if end_time_ms is not None else None,
            limit=int(limit) if isinstance(limit, int) and limit > 0 else None,
            cursor=cursor,
        )

    async def get_open_orders(
        self,
        category: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_filter: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        return await self.order_realtime(
            category=category,
            symbol=symbol,
            order_id=order_id,
            order_filter=order_filter,
            limit=int(limit),
        )

    async def get_open_orders_merged(
        self,
        category: str,
        *,
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        async def _one(order_filter: str) -> Dict[str, Any]:
            try:
                return await self.get_open_orders(
                    category=category, symbol=symbol, order_filter=order_filter, limit=int(limit)
                )
            except Exception as e:
                return {"http_status": 0, "retCode": -1, "retMsg": str(e), "result": {"list": []}}

        # Los filtros son independientes: se consultan en paralelo.
        responses = list(await asyncio.gather(*(_one(f) for f in self.OPEN_ORDER_FILTERS)))

        merged: Dict[str, Dict[str, Any]] = {}
        for payload in responses:
            if payload.get("retCode") != 0:
                continue
            for row in ((payload.get("result", {}) or {}).get("list", []) or []):
                if not isinstance(row, dict):
                    continue
                oid = row.get("orderId")
                if isinstance(oid, str) and oid:
                    merged[oid] = row

        http_status = max((int(p.get("http_status") or 0) for p in responses), default=0)
        ok_any = any(p.get("retCode") == 0 for p in responses)
        if ok_any:
            ret_code: Any = 0
            ret_msg: Any = "OK"
        else:
            bad = next((p for p in responses if p.get("retCode") not in (0, None)), (responses[0] if responses else {}))
            ret_code = bad.get("retCode")
            ret_msg = bad.get("retMsg")

        return {
            "http_status": http_status,
            "retCode": ret_code,
            "retMsg": ret_msg,
            "result": {"list": list(merged.values())},
        }
