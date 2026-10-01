"""Catálogo live desde Bybit exchange — instruments-info, órdenes paginadas (hasta 500)."""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from src_dev.bybit.rest import BybitRestClient
from src_dev.config import DevSettings, private_rest_base_url
from src_dev.orders.exchange_order_stats import (
    fetch_paginated_executions,
    fetch_paginated_order_history,
    load_orders_from_old_results,
    resolve_order_stats,
)
from src_dev.orders.exchange_schema import SpotInstrumentConstraints

try:
    from bybit_v5 import BybitV5Client
except ImportError:
    BybitV5Client = None  # type: ignore


async def fetch_instrument_constraints(
    symbol: str,
    settings: Optional[DevSettings] = None,
) -> SpotInstrumentConstraints:
    cfg = settings or DevSettings.from_env()
    async with BybitRestClient(cfg) as client:
        rules = await client.instrument_rules(symbol, category="spot")
        payload = await client.get(
            "/v5/market/instruments-info",
            {"category": "spot", "symbol": symbol},
        )
    row: Dict[str, Any] = {}
    if payload.get("retCode") == 0:
        lst = (payload.get("result") or {}).get("list") or []
        row = lst[0] if lst else {}

    lf = row.get("lotSizeFilter") or {}
    return SpotInstrumentConstraints(
        symbol=symbol,
        tick_size=float(rules.get("tick_size") or 0.01),
        qty_step=float(rules.get("qty_step") or 0.0001),
        min_qty=float(rules.get("min_qty") or 0.0001),
        min_notional=float(rules.get("min_notional") or 1.0),
        max_order_qty=float(lf.get("maxOrderQty") or lf.get("postOnlyMaxOrderQty") or 0.0),
        max_mkt_order_qty=float(lf.get("maxMktOrderQty") or 0.0),
        status=str(row.get("status") or "Trading"),
        base_coin=str(row.get("baseCoin") or ""),
        quote_coin=str(row.get("quoteCoin") or ""),
    )


async def fetch_exchange_orders(
    symbol: str,
    *,
    category: str = "spot",
    settings: Optional[DevSettings] = None,
) -> Dict[str, Any]:
    """
    Órdenes/ejecuciones reales vía API Bybit (paginado hasta DEV_LAB_ORDER_HISTORY_LIMIT).
    Sin SQLite, JSONL ni trading.db local para estadísticas de scoring.
    """
    cfg = settings or DevSettings.from_env()
    target = max(1, int(cfg.lab_order_history_limit))
    stats_source = str(cfg.lab_order_stats_source or "exchange").strip().lower()

    out: Dict[str, Any] = {
        "ts": time.time(),
        "symbol": symbol,
        "category": category,
        "authenticated": False,
        "local_db_used": False,
        "stats_source_mode": stats_source,
        "open_orders": [],
        "order_history": [],
        "executions": [],
        "order_stats": {},
        "history_meta": {},
        "executions_meta": {},
        "errors": [],
    }

    order_rows: List[Dict[str, Any]] = []
    hist_meta: Dict[str, Any] = {"source": "none", "fetched": 0}
    exe_rows: List[Dict[str, Any]] = []
    exe_meta: Dict[str, Any] = {"source": "none", "fetched": 0}

    if stats_source == "old_results":
        order_rows, hist_meta = load_orders_from_old_results(
            cfg.lab_old_results_path, symbol, limit=target
        )
        out["errors"].extend(hist_meta.get("errors") or [])
        out["authenticated"] = bool(order_rows)
    else:
        if BybitV5Client is None:
            out["errors"].append("bybit_v5 no disponible")
            return out
        key = os.getenv("BYBIT_API_KEY", "")
        secret = os.getenv("BYBIT_API_SECRET", "")
        if not key or not secret:
            out["errors"].append("BYBIT_API_KEY/SECRET no configuradas")
            return out

        client = BybitV5Client(key, secret, base_url=private_rest_base_url())
        auth_ok = False
        try:
            st = await client.get_server_time()
            if st.get("retCode") == 0:
                auth_ok = True
            else:
                out["errors"].append(f"server_time retCode={st.get('retCode')} retMsg={st.get('retMsg')}")

            for order_filter in ("Order", "tpslOrder", "StopOrder"):
                try:
                    res = await client.order_realtime(
                        category=category,
                        symbol=symbol,
                        order_filter=order_filter,
                        limit=50,
                    )
                    if res.get("retCode") == 0:
                        out["open_orders"].extend((res.get("result") or {}).get("list") or [])
                except Exception as e:
                    out["errors"].append(f"open {order_filter}: {e}")

            order_rows, hist_meta = await fetch_paginated_order_history(
                client,
                category=category,
                symbol=symbol,
                target=target,
            )
            out["errors"].extend(hist_meta.get("errors") or [])

            exe_rows, exe_meta = await fetch_paginated_executions(
                client,
                category=category,
                symbol=symbol,
                target=target,
            )
            out["errors"].extend(exe_meta.get("errors") or [])

            if stats_source == "hybrid" and len(order_rows) < cfg.lab_min_order_stats_samples:
                old_rows, old_meta = load_orders_from_old_results(
                    cfg.lab_old_results_path, symbol, limit=target
                )
                seen = {str(r.get("orderId") or "") for r in order_rows}
                added = 0
                for row in old_rows:
                    oid = str(row.get("orderId") or "").strip()
                    if oid and oid in seen:
                        continue
                    if oid:
                        seen.add(oid)
                    order_rows.append(row)
                    added += 1
                    if len(order_rows) >= target:
                        break
                hist_meta["supplemented_from"] = "old_results_json"
                hist_meta["supplement_added"] = added
                hist_meta["old_results_meta"] = old_meta

            out["authenticated"] = auth_ok
        finally:
            await client.aclose()

    out["order_history"] = order_rows
    out["executions"] = exe_rows
    out["history_meta"] = hist_meta
    out["executions_meta"] = exe_meta
    out["order_stats"] = resolve_order_stats(
        order_rows,
        stats_source=str(hist_meta.get("source") or stats_source),
        pagination_meta=hist_meta,
    )
    return out


def summarize_exchange_orders(orders_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Compat — usa order_stats precalculado del fetch paginado."""
    pre = orders_payload.get("order_stats")
    if isinstance(pre, dict) and pre.get("observed_combo_counts") is not None:
        return pre
    from src_dev.orders.exchange_order_stats import count_combo_stats, resolve_order_stats

    rows = orders_payload.get("order_history") or []
    return resolve_order_stats(
        rows,
        stats_source=str(orders_payload.get("stats_source_mode") or "exchange_api_order_history"),
        pagination_meta=orders_payload.get("history_meta") or {},
    )