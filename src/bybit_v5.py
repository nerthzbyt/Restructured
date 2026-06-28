import asyncio
import json
import random
import time
from typing import Any, Dict, Optional, Tuple

import aiohttp
from urllib.parse import quote

from utils import generate_signature


def _canonical_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    
class BybitV5Client:
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
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window = str(recv_window)
        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session: bool = session is None
        self.timeout_s = float(timeout_s)
        self.max_retries = int(max_retries)
        self.backoff_base_s = float(backoff_base_s)
        self.backoff_max_s = float(backoff_max_s)

    def _timestamp_ms(self) -> str:
        return str(int(time.time() * 1000))

    def _sign_get(self, query_string: str, timestamp_ms: str) -> str:
        prehash = f"{timestamp_ms}{self.api_key}{self.recv_window}{query_string}"
        return generate_signature(self.api_secret, prehash)

    def _sign_post(self, body_str: str, timestamp_ms: str) -> str:
        prehash = f"{timestamp_ms}{self.api_key}{self.recv_window}{body_str}"
        return generate_signature(self.api_secret, prehash)

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
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=max(1.0, float(self.timeout_s)))
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def aclose(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    def _should_retry_http(self, status: int) -> bool:
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

    def _parse_retry_after_s(self, headers: "aiohttp.typedefs.LooseHeaders") -> Optional[float]:
        try:
            raw = headers.get("Retry-After")  # type: ignore[attr-defined]
        except Exception:
            raw = None
        if raw is None:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        query_string: str = "",
        body_str: str = "",
    ) -> Tuple[int, Dict[str, Any]]:
        session = await self._get_session()
        url = f"{self.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        last_status: int = 0
        last_payload: Dict[str, Any] = {"retCode": -1, "retMsg": "request_failed"}

        for attempt in range(max(0, int(self.max_retries)) + 1):
            ts = self._timestamp_ms()
            if method.upper() == "GET":
                sign = self._sign_get(query_string, ts)
            else:
                sign = self._sign_post(body_str, ts)
            headers = self._headers(sign, ts)

            try:
                if method.upper() == "GET":
                    async with session.get(url, headers=headers) as resp:
                        last_status = int(resp.status)
                        try:
                            last_payload = await resp.json(content_type=None)
                        except Exception:
                            txt = await resp.text()
                            last_payload = {"retCode": -1, "retMsg": "non_json_response", "text": txt}
                        if self._should_retry_http(last_status) and attempt < int(self.max_retries):
                            await asyncio.sleep(self._retry_delay_s(attempt, retry_after_s=self._parse_retry_after_s(resp.headers)))
                            continue
                        return last_status, last_payload
                async with session.post(url, data=body_str.encode("utf-8"), headers=headers) as resp:
                    last_status = int(resp.status)
                    try:
                        last_payload = await resp.json(content_type=None)
                    except Exception:
                        txt = await resp.text()
                        last_payload = {"retCode": -1, "retMsg": "non_json_response", "text": txt}
                    if self._should_retry_http(last_status) and attempt < int(self.max_retries):
                        await asyncio.sleep(self._retry_delay_s(attempt, retry_after_s=self._parse_retry_after_s(resp.headers)))
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

    async def wallet_balance(self, account_type: str = "UNIFIED", coin: Optional[str] = None) -> Dict[str, Any]:
        qs_parts = [f"accountType={account_type}"]
        if coin:
            qs_parts.append(f"coin={coin}")
        status, data = await self.get("/v5/account/wallet-balance", "&".join(qs_parts))
        return {"http_status": status, **(data or {})}

    async def get_server_time(self) -> Dict[str, Any]:
        status, data = await self.get("/v5/market/time", "")
        return {"http_status": status, **(data or {})}

    async def create_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        status, data = await self.post("/v5/order/create", body)
        return {"http_status": status, **(data or {})}

    async def cancel_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        status, data = await self.post("/v5/order/cancel", body)
        return {"http_status": status, **(data or {})}

    async def amend_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        status, data = await self.post("/v5/order/amend", body)
        return {"http_status": status, **(data or {})}

    async def order_realtime(
        self,
        category: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None,
        order_filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        qs_parts = [f"category={category}"]
        if symbol:
            qs_parts.append(f"symbol={symbol}")
        if order_id:
            qs_parts.append(f"orderId={order_id}")
        if order_link_id:
            qs_parts.append(f"orderLinkId={order_link_id}")
        if order_filter:
            qs_parts.append(f"orderFilter={order_filter}")
        if isinstance(limit, int) and limit > 0:
            qs_parts.append(f"limit={limit}")
        status, data = await self.get("/v5/order/realtime", "&".join(qs_parts))
        return {"http_status": status, **(data or {})}

    async def order_history(
        self,
        category: str,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        qs_parts = [f"category={category}"]
        if symbol:
            qs_parts.append(f"symbol={symbol}")
        if order_id:
            qs_parts.append(f"orderId={order_id}")
        if order_link_id:
            qs_parts.append(f"orderLinkId={order_link_id}")
        if isinstance(limit, int) and limit > 0:
            qs_parts.append(f"limit={limit}")
        status, data = await self.get("/v5/order/history", "&".join(qs_parts))
        return {"http_status": status, **(data or {})}

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
        qs_parts = [f"category={category}"]
        if symbol:
            qs_parts.append(f"symbol={symbol}")
        if order_id:
            qs_parts.append(f"orderId={order_id}")
        if order_link_id:
            qs_parts.append(f"orderLinkId={order_link_id}")
        if start_time_ms is not None:
            qs_parts.append(f"startTime={int(start_time_ms)}")
        if end_time_ms is not None:
            qs_parts.append(f"endTime={int(end_time_ms)}")
        if isinstance(limit, int) and limit > 0:
            qs_parts.append(f"limit={int(limit)}")
        if cursor:
            qs_parts.append(f"cursor={quote(str(cursor), safe='')}")
        status, data = await self.get("/v5/execution/list", "&".join(qs_parts))
        return {"http_status": status, **(data or {})}

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
        order_filters = [
            "Order",
            "StopOrder",
            "tpslOrder",
            "OcoOrder",
            "BidirectionalTpslOrder",
        ]
        responses: list[Dict[str, Any]] = []
        for order_filter in order_filters:
            try:
                responses.append(
                    await self.get_open_orders(
                        category=category,
                        symbol=symbol,
                        order_filter=order_filter,
                        limit=int(limit),
                    )
                )
            except Exception as e:
                responses.append({"http_status": 0, "retCode": -1, "retMsg": str(e), "result": {"list": []}})

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
