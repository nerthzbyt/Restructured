"""Estadísticas de tipos de orden — solo exchange API o snapshot exchange (old_results)."""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

MS_7D = 7 * 24 * 3600 * 1000


def combo_key_from_row(row: Dict[str, Any]) -> str:
    ot = str(row.get("orderType") or "?")
    tif = str(row.get("timeInForce") or "?")
    flt = str(row.get("orderFilter") or "Order")
    return f"{ot}|{tif}|{flt}"


def count_combo_stats(rows: List[Dict[str, Any]], *, dedupe_order_id: bool = True) -> Dict[str, Any]:
    stats: Dict[str, int] = {}
    seen_ids: set[str] = set()
    used = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        oid = str(row.get("orderId") or "").strip()
        if dedupe_order_id and oid:
            if oid in seen_ids:
                continue
            seen_ids.add(oid)
        key = combo_key_from_row(row)
        stats[key] = stats.get(key, 0) + 1
        used += 1
    total = sum(stats.values())
    return {
        "observed_combo_counts": stats,
        "total_orders_sampled": used,
        "unique_order_ids": len(seen_ids) if dedupe_order_id else used,
        "combo_distribution_pct": {
            k: round(100.0 * v / total, 2) if total else 0.0 for k, v in sorted(stats.items(), key=lambda x: -x[1])
        },
    }


def load_orders_from_old_results(
    path: str,
    symbol: str,
    *,
    limit: int = 500,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Extrae orderType/timeInForce desde bybit_raw.order_realtime en old_results.json."""
    meta: Dict[str, Any] = {
        "source": "old_results_json",
        "path": path,
        "symbol": symbol,
        "requested": limit,
        "errors": [],
    }
    if not os.path.isfile(path):
        meta["errors"].append(f"archivo no encontrado: {path}")
        return [], meta

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    trades = (payload.get("trades") or {}).get(symbol) or []
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for trade in trades:
        if not isinstance(trade, dict):
            continue
        raw = trade.get("bybit_raw") if isinstance(trade.get("bybit_raw"), dict) else {}
        ort = raw.get("order_realtime") if isinstance(raw.get("order_realtime"), dict) else {}
        if not ort.get("orderType"):
            continue
        oid = str(ort.get("orderId") or trade.get("order_id") or "").strip()
        if oid and oid in seen:
            continue
        if oid:
            seen.add(oid)
        rows.append(
            {
                "orderId": oid or None,
                "orderType": ort.get("orderType"),
                "timeInForce": ort.get("timeInForce") or ("IOC" if ort.get("orderType") == "Market" else "GTC"),
                "orderFilter": ort.get("orderFilter") or "Order",
                "symbol": symbol,
                "side": ort.get("side") or trade.get("action"),
                "orderStatus": ort.get("orderStatus"),
                "source": "old_results_json",
            }
        )
        if len(rows) >= limit:
            break

    meta["fetched"] = len(rows)
    meta["unique_order_ids"] = len(seen)
    return rows, meta


async def fetch_paginated_order_history(
    client: Any,
    *,
    category: str,
    symbol: str,
    target: int = 500,
    page_size: int = 50,
    max_windows: int = 12,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Historial de órdenes vía API privada Bybit (no SQLite/JSONL local).
    Pagina cursor + ventanas de 7 días hasta `target` órdenes únicas.
    """
    target = max(1, int(target))
    page_size = max(1, min(50, int(page_size)))
    all_rows: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    meta: Dict[str, Any] = {
        "source": "exchange_api_order_history",
        "category": category,
        "symbol": symbol,
        "requested": target,
        "pages": 0,
        "windows": 0,
        "errors": [],
    }

    end_time_ms: Optional[int] = None

    for window_idx in range(max_windows):
        if len(all_rows) >= target:
            break
        cursor: Optional[str] = None
        window_oldest_ms: Optional[int] = None
        meta["windows"] = window_idx + 1

        while len(all_rows) < target:
            kwargs: Dict[str, Any] = {
                "category": category,
                "symbol": symbol,
                "limit": page_size,
            }
            if cursor:
                kwargs["cursor"] = cursor
            if end_time_ms is not None:
                kwargs["end_time_ms"] = end_time_ms
                kwargs["start_time_ms"] = end_time_ms - MS_7D

            res = await client.order_history(**kwargs)
            meta["pages"] += 1

            if res.get("retCode") != 0:
                meta["errors"].append(
                    f"order_history retCode={res.get('retCode')} retMsg={res.get('retMsg')}"
                )
                break

            lst = (res.get("result") or {}).get("list") or []
            if not lst:
                break

            for row in lst:
                if not isinstance(row, dict):
                    continue
                oid = str(row.get("orderId") or "").strip()
                if oid and oid in seen_ids:
                    continue
                if oid:
                    seen_ids.add(oid)
                all_rows.append(row)
                try:
                    ct = int(row.get("createdTime") or 0)
                    if ct > 0 and (window_oldest_ms is None or ct < window_oldest_ms):
                        window_oldest_ms = ct
                except (TypeError, ValueError):
                    pass
                if len(all_rows) >= target:
                    break

            cursor = (res.get("result") or {}).get("nextPageCursor") or ""
            if not cursor:
                break

        if len(all_rows) >= target:
            break
        if window_oldest_ms is None:
            break
        end_time_ms = window_oldest_ms - 1

    meta["fetched"] = len(all_rows)
    meta["unique_order_ids"] = len(seen_ids)
    return all_rows[:target], meta


async def fetch_paginated_executions(
    client: Any,
    *,
    category: str,
    symbol: str,
    target: int = 500,
    page_size: int = 50,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Últimas ejecuciones (fills) spot — complemento al historial de órdenes."""
    target = max(1, int(target))
    page_size = max(1, min(100, int(page_size)))
    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    meta: Dict[str, Any] = {
        "source": "exchange_api_executions",
        "requested": target,
        "pages": 0,
        "errors": [],
    }

    while len(rows) < target:
        res = await client.execution_list(
            category=category,
            symbol=symbol,
            limit=page_size,
            cursor=cursor,
        )
        meta["pages"] += 1
        if res.get("retCode") != 0:
            meta["errors"].append(
                f"execution_list retCode={res.get('retCode')} retMsg={res.get('retMsg')}"
            )
            break
        lst = (res.get("result") or {}).get("list") or []
        if not lst:
            break
        rows.extend(lst)
        cursor = (res.get("result") or {}).get("nextPageCursor") or ""
        if not cursor:
            break

    meta["fetched"] = len(rows[:target])
    return rows[:target], meta


def resolve_order_stats(
    order_history_rows: List[Dict[str, Any]],
    *,
    stats_source: str,
    pagination_meta: Dict[str, Any],
) -> Dict[str, Any]:
    summary = count_combo_stats(order_history_rows, dedupe_order_id=True)
    summary["stats_source"] = stats_source
    summary["local_db_used"] = False
    summary["pagination"] = pagination_meta
    return summary