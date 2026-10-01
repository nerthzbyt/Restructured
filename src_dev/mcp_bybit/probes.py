"""Generación de argumentos de prueba y clasificación de herramientas MCP."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

# Endpoints que usa Nertzh/src hoy (bybit_v5 + Nertzh)
NERTZH_USED_ENDPOINTS = {
    "/v5/market/time": ["getServerTime"],
    "/v5/account/wallet-balance": ["getWalletBalance"],
    "/v5/order/create": ["createOrder", "wsPlaceOrder", "batchCreateOrders"],
    "/v5/order/cancel": ["cancelOrder", "wsCancelOrder", "batchCancelOrders", "cancelAllOrders"],
    "/v5/order/amend": ["amendOrder", "wsAmendOrder", "batchAmendOrders"],
    "/v5/order/realtime": ["getOpenOrders"],
    "/v5/order/history": ["getOrderHistory"],
    "/v5/execution/list": ["getExecutionList", "subscribeExecution", "subscribeExecutionFast"],
}

MUTATION_PREFIXES = (
    "create",
    "amend",
    "cancel",
    "place",
    "withdraw",
    "stake",
    "redeem",
    "close",
    "distribute",
    "delete",
    "freeze",
    "update",
    "remove",
    "buy",
    "sell",
    "add",
    "modify",
    "set",
    "execute",
    "accept",
    "batch",
    "repay",
    "borrow",
    "reinvest",
    "claim",
    "convert",
    "wsplace",
    "wscancel",
    "wsamend",
    "wsbatch",
)

READ_PREFIXES = (
    "get",
    "query",
    "list",
    "subscribe",
    "rec",
    "fetch",
    "search",
)


def tool_category(name: str) -> str:
    low = name.lower()
    if low.startswith("subscribe"):
        return "websocket"
    if low.startswith("ws"):
        return "wstrade"
    for p in MUTATION_PREFIXES:
        if low.startswith(p):
            return "mutation"
    for p in READ_PREFIXES:
        if low.startswith(p):
            return "read"
    return "other"


def is_mutation_tool(name: str) -> bool:
    return tool_category(name) == "mutation"


def infer_category_from_schema(tool: Dict[str, Any]) -> Optional[str]:
    schema = tool.get("inputSchema") or {}
    props = schema.get("properties") or {}
    if "category" in props:
        enum = (props.get("category") or {}).get("enum")
        if enum:
            return str(enum[0])
    return None


def _default_for_prop(name: str, spec: Dict[str, Any]) -> Any:
    if "default" in spec:
        return spec["default"]
    if "enum" in spec and spec["enum"]:
        return spec["enum"][0]
    t = spec.get("type")
    if t == "string":
        low = name.lower()
        if low == "symbol":
            return "BTCUSDT"
        if low == "category":
            return "spot"
        if low == "coin":
            return "USDT"
        if low == "basecoin":
            return "BTC"
        if low in ("accounttype", "account_type"):
            return "UNIFIED"
        if low in ("interval", "timeframe"):
            return "1"
        if low == "side":
            return "Buy"
        if low == "ordertype":
            return "Market"
        if low in ("timeinforce", "time_in_force"):
            return "IOC"
        if low == "settlecoin":
            return "USDT"
        if low in ("orderid", "order_id"):
            return "00000000-0000-0000-0000-000000000000"
        if low in ("orderlinkid", "order_link_id"):
            return "probe-invalid-link-id"
        return "probe"
    if t == "integer":
        return int(spec.get("minimum", 1) or 1)
    if t == "number":
        return float(spec.get("minimum", 1.0) or 1.0)
    if t == "boolean":
        return False
    if t == "array":
        return []
    if t == "object":
        return {}
    return None


def build_probe_args(tool: Dict[str, Any], *, symbol: str = "BTCUSDT") -> Dict[str, Any]:
    schema = tool.get("inputSchema") or {}
    props = schema.get("properties") or {}
    required = list(schema.get("required") or [])
    args: Dict[str, Any] = {}

    for key in required:
        spec = props.get(key) or {}
        val = _default_for_prop(key, spec)
        if key.lower() == "symbol" and val == "BTCUSDT":
            val = symbol
        if val is not None:
            args[key] = val

    # Campos opcionales útiles para lectura
    if "category" in props and "category" not in args:
        args["category"] = infer_category_from_schema(tool) or "spot"
    if "symbol" in props and "symbol" not in args:
        args["symbol"] = symbol
    if "limit" in props and "limit" not in args:
        args["limit"] = 5
    if "messageCount" in props and "messageCount" not in args:
        args["messageCount"] = 1
    if "timeoutMs" in props and "timeoutMs" not in args:
        args["timeoutMs"] = 3000

    return args


def classify_result(
    *,
    tool_name: str,
    is_error: bool,
    text: str,
    category: str,
) -> Tuple[str, str]:
    """Retorna (status, detail)."""
    low = text.lower()
    if not is_error:
        if "retcode" in low:
            m = re.search(r"retcode[\"']?\s*[:=]\s*(-?\d+)", low)
            if m and int(m.group(1)) != 0:
                code = int(m.group(1))
                if code in (10003, 10004, 10005, 33004, 10010):
                    return "auth_error", text[:400]
                if code in (10001, 10002, 10006, 10016, 110001, 110003, 110004, 110005, 110006, 110007, 110008):
                    return "api_reject", text[:400]
                return "api_error", text[:400]
        return "ok", "success"

    if "validation error" in low or "required" in low:
        if category == "mutation":
            return "schema_ok", "mutation tool reachable; schema validation works"
        return "schema_error", text[:400]
    if "bybit_api_key must be set" in low or "authenticated" in low:
        return "auth_required", text[:400]
    if "http 401" in low or "http 403" in low or "sign" in low:
        return "auth_error", text[:400]
    if "timeout" in low or "aborted" in low:
        return "timeout", text[:400]
    if "tool not found" in low:
        return "missing", text[:400]
    if category == "mutation":
        return "schema_ok", text[:400]
    return "error", text[:400]


def map_tool_to_nertzh_usage(tool_name: str) -> List[str]:
    hits: List[str] = []
    for endpoint, tools in NERTZH_USED_ENDPOINTS.items():
        if tool_name in tools:
            hits.append(endpoint)
    return hits