#!/usr/bin/env python3
"""
Verifica TP/SL colgados en panel Bybit: Market vs Limit con tops del laboratorio.

Monitorea orderFilter Order / tpslOrder / StopOrder directamente en el exchange.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src_dev.bybit.rest import BybitRestClient
from src_dev.config import OUTPUT_DIR, DevSettings, load_trading_thresholds, private_rest_base_url
from src_dev.orders.combinator import build_order_body, qty_for_notional, resolve_limit_price
from src_dev.orders.exchange_catalog import fetch_instrument_constraints
from src_dev.orders.exchange_schema import OrderCombination
from src_dev.orders.lab import _round_to_tick, _trigger_from_tick

try:
    from bybit_v5 import BybitV5Client
except ImportError:
    BybitV5Client = None  # type: ignore

ORDER_FILTERS = ("Order", "tpslOrder", "StopOrder", "OcoOrder", "BidirectionalTpslOrder")


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _fetch_panel(client: Any, symbol: str) -> Dict[str, Any]:
    """Todas las órdenes abiertas por filtro del panel."""
    by_filter: Dict[str, List[Dict[str, Any]]] = {}
    all_rows: Dict[str, Dict[str, Any]] = {}
    for flt in ORDER_FILTERS:
        res = await client.order_realtime(category="spot", symbol=symbol, order_filter=flt, limit=50)
        rows = (res.get("result") or {}).get("list") or [] if res.get("retCode") == 0 else []
        by_filter[flt] = rows
        for row in rows:
            oid = str(row.get("orderId") or "")
            if oid:
                row = dict(row)
                row["_panel_filter"] = flt
                all_rows[oid] = row
    merged = await client.get_open_orders_merged(category="spot", symbol=symbol, limit=50)
    return {
        "ts": _utc(),
        "by_filter_counts": {k: len(v) for k, v in by_filter.items()},
        "by_filter": by_filter,
        "merged_count": len((merged.get("result") or {}).get("list") or []),
        "all_open": list(all_rows.values()),
    }


def _tpsl_sane(side: str, entry: float, tp: Optional[float], sl: Optional[float]) -> Dict[str, Any]:
    """Detecta TP/SL contrarios al lado de la operación."""
    issues: List[str] = []
    if entry <= 0:
        return {"sane": None, "issues": ["no_entry"]}
    if tp is not None:
        if side == "Buy" and tp <= entry:
            issues.append(f"TP({tp}) <= entry({entry}) en Buy")
        if side == "Sell" and tp >= entry:
            issues.append(f"TP({tp}) >= entry({entry}) en Sell")
    if sl is not None:
        if side == "Buy" and sl >= entry:
            issues.append(f"SL({sl}) >= entry({entry}) en Buy")
        if side == "Sell" and sl <= entry:
            issues.append(f"SL({sl}) <= entry({entry}) en Sell")
    return {"sane": len(issues) == 0, "issues": issues}


def _classify_hanging(row: Dict[str, Any]) -> bool:
    flt = str(row.get("orderFilter") or row.get("_panel_filter") or "").lower()
    status = str(row.get("orderStatus") or "").lower().replace(" ", "")
    if status in ("filled", "cancelled", "canceled", "rejected", "deactivated", "expired"):
        return False
    return flt in ("tpslorder", "stoporder", "ocoorder", "bidirectionaltpslorder") or bool(
        row.get("takeProfit") or row.get("stopLoss") or row.get("stopOrderType")
    )


async def _monitor_after(
    client: Any,
    symbol: str,
    *,
    label: str,
    order_id: str,
    link_id: str,
    waits: List[float],
) -> List[Dict[str, Any]]:
    snapshots: List[Dict[str, Any]] = []
    for w in waits:
        await asyncio.sleep(w)
        panel = await _fetch_panel(client, symbol)
        parent = None
        if order_id:
            rt = await client.order_realtime(category="spot", symbol=symbol, order_id=order_id)
            lst = (rt.get("result") or {}).get("list") or []
            parent = lst[0] if lst else None

        hanging = [r for r in panel["all_open"] if _classify_hanging(r)]
        ours = [
            r for r in panel["all_open"]
            if str(r.get("orderLinkId") or "") == link_id
            or str(r.get("orderId") or "") == order_id
        ]
        related = [
            r for r in panel["all_open"]
            if link_id and link_id in str(r.get("orderLinkId") or "")
        ]
        snapshots.append({
            "wait_s": w,
            "panel": {
                "counts": panel["by_filter_counts"],
                "merged": panel["merged_count"],
                "hanging_tpsl_count": len(hanging),
                "hanging_tpsl": [
                    {
                        "orderId": r.get("orderId"),
                        "orderFilter": r.get("orderFilter"),
                        "orderType": r.get("orderType"),
                        "side": r.get("side"),
                        "orderStatus": r.get("orderStatus"),
                        "takeProfit": r.get("takeProfit"),
                        "stopLoss": r.get("stopLoss"),
                        "stopOrderType": r.get("stopOrderType"),
                        "triggerPrice": r.get("triggerPrice"),
                    }
                    for r in hanging
                ],
            },
            "parent_order": {
                "orderId": (parent or {}).get("orderId"),
                "orderStatus": (parent or {}).get("orderStatus"),
                "avgPrice": (parent or {}).get("avgPrice"),
                "takeProfit": (parent or {}).get("takeProfit"),
                "stopLoss": (parent or {}).get("stopLoss"),
            } if parent else None,
            "our_open_orders": len(ours) + len(related),
        })
    return snapshots


async def run_verify(symbol: str, test_notional: float) -> Dict[str, Any]:
    if BybitV5Client is None:
        raise RuntimeError("bybit_v5 no disponible")

    cfg = DevSettings.from_env()
    th = load_trading_thresholds()
    constraints = await fetch_instrument_constraints(symbol, cfg)

    async with BybitRestClient(cfg) as rest:
        snap = await rest.fetch_market_snapshot(symbol)
    ob = snap.get("orderbook") or {}
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    bid = float(bids[0][0]) if bids else 0.0
    ask = float(asks[0][0]) if asks else 0.0
    last = float((snap.get("ticker") or {}).get("last_price") or (bid + ask) / 2)

    tp_buy = last * (1 + float(th["tp_pct"]) / 100.0)
    sl_buy = last * (1 - float(th["sl_pct"]) / 100.0)
    tp_sell = last * (1 - float(th["tp_pct"]) / 100.0)
    sl_sell = last * (1 + float(th["sl_pct"]) / 100.0)
    # TP/SL invertidos (simula bug "contrarios")
    tp_wrong_buy = sl_buy
    sl_wrong_buy = tp_buy

    key = os.getenv("BYBIT_API_KEY", "")
    secret = os.getenv("BYBIT_API_SECRET", "")
    client = BybitV5Client(key, secret, base_url=private_rest_base_url())

    cases: List[Dict[str, Any]] = [
        {
            "id": "top_market_ioc_clean",
            "label": "TOP #1 Market+IOC sin TP/SL (lab correcto)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Market", time_in_force="IOC", market_unit="baseCoin",
                order_filter="Order", is_leverage=0, price_anchor=None,
                tp_sl_mode="none", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": False,
        },
        {
            "id": "nertzh_market_with_tpsl",
            "label": "Market+IOC CON TP/SL (patrón _replace_order_with_market)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Market", time_in_force="IOC", market_unit="baseCoin",
                order_filter="Order", is_leverage=0, price_anchor=None,
                tp_sl_mode="bracket_on_limit", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": True,
            "tp": tp_buy,
            "sl": sl_buy,
            "nertzh_style": True,
        },
        {
            "id": "market_wrong_tpsl",
            "label": "Market+IOC TP/SL CONTRARIOS (invertidos)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Market", time_in_force="IOC", market_unit="baseCoin",
                order_filter="Order", is_leverage=0, price_anchor=None,
                tp_sl_mode="bracket_on_limit", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": True,
            "tp": tp_wrong_buy,
            "sl": sl_wrong_buy,
        },
        {
            "id": "top_limit_bracket",
            "label": "Limit+GTC bracket_on_limit (TP/SL nativo Limit)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Limit", time_in_force="GTC", market_unit=None,
                order_filter="Order", is_leverage=0, price_anchor="best_bid",
                tp_sl_mode="bracket_on_limit", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": True,
            "tp": tp_buy,
            "sl": sl_buy,
            "limit_aggressive": False,
        },
        {
            "id": "limit_gtc_clean",
            "label": "Limit+GTC sin TP/SL (Nertzh _place_order actual)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Limit", time_in_force="GTC", market_unit=None,
                order_filter="Order", is_leverage=0, price_anchor="best_bid",
                tp_sl_mode="none", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": False,
        },
        {
            "id": "limit_postonly_clean",
            "label": "Limit+PostOnly sin TP/SL (mejor slippage live)",
            "side": "Buy",
            "combo": OrderCombination(
                order_type="Limit", time_in_force="PostOnly", market_unit=None,
                order_filter="Order", is_leverage=0, price_anchor="best_bid",
                tp_sl_mode="none", slippage_type=None, slippage_value=None,
                side_hint="Buy", valid=True, invalid_reason="",
            ),
            "attach_tpsl": False,
            "limit_aggressive": False,
        },
    ]

    report: Dict[str, Any] = {
        "generated_at": _utc(),
        "symbol": symbol,
        "bybit_env": os.getenv("BYBIT_ENV", "demo"),
        "last_price": last,
        "test_notional_usdt": test_notional,
        "thresholds": {"tp_pct": th["tp_pct"], "sl_pct": th["sl_pct"]},
        "panel_before": None,
        "cases": [],
        "cleanup": [],
        "conclusions": [],
    }

    try:
        report["panel_before"] = await _fetch_panel(client, symbol)
        ob_stats = {"best_bid": bid, "best_ask": ask, "mid": last, "tick_size": constraints.tick_size}

        for case in cases:
            side = case["side"]
            combo = case["combo"]
            link = f"tpslverify-{uuid.uuid4().hex[:12]}"
            qty = qty_for_notional(test_notional, ask if side == "Buy" else bid, constraints)

            price_s = None
            if combo.order_type == "Limit":
                anchor = combo.price_anchor or "mid"
                px = resolve_limit_price(anchor, ob_stats, 1.0)
                if case.get("limit_aggressive", True) and side == "Buy":
                    px = ask  # fill rápido
                price_s = str(_round_to_tick(px, constraints.tick_size))

            tp_v = case.get("tp")
            sl_v = case.get("sl")
            body = build_order_body(
                combo, symbol=symbol, side=side, qty=qty, price=price_s,
                trigger_price=_trigger_from_tick(last, constraints.tick_size, side),
                take_profit=str(_round_to_tick(tp_v, constraints.tick_size)) if tp_v else None,
                stop_loss=str(_round_to_tick(sl_v, constraints.tick_size)) if sl_v else None,
            )
            body["orderLinkId"] = link

            # Patrón Nertzh: fuerza tp/sl en Market aunque combo diga bracket_on_limit inválido para Market
            if case.get("nertzh_style") and combo.order_type == "Market" and tp_v and sl_v:
                body["takeProfit"] = str(_round_to_tick(tp_v, constraints.tick_size))
                body["stopLoss"] = str(_round_to_tick(sl_v, constraints.tick_size))
                body["tpOrderType"] = "Market"
                body["slOrderType"] = "Market"

            tpsl_check = _tpsl_sane(side, last, tp_v, sl_v)

            create_res = await client.create_order(body)
            order_id = str((create_res.get("result") or {}).get("orderId") or "")

            # Reintento sin TP/SL si falla (como Nertzh)
            fallback_res = None
            if create_res.get("retCode") != 0 and ("takeProfit" in body or "stopLoss" in body):
                body2 = dict(body)
                for k in ("takeProfit", "stopLoss", "tpOrderType", "slOrderType"):
                    body2.pop(k, None)
                fallback_res = await client.create_order(body2)
                if fallback_res.get("retCode") == 0:
                    order_id = str((fallback_res.get("result") or {}).get("orderId") or "")
                    create_res = fallback_res

            snapshots = await _monitor_after(
                client, symbol, label=case["label"], order_id=order_id, link_id=link, waits=[1.0, 4.0, 8.0],
            )

            final_panel = await _fetch_panel(client, symbol)
            hanging = [r for r in final_panel["all_open"] if _classify_hanging(r)]

            case_result = {
                "id": case["id"],
                "label": case["label"],
                "side": side,
                "order_type": combo.order_type,
                "time_in_force": combo.time_in_force,
                "tp_sl_mode": combo.tp_sl_mode,
                "attach_tpsl": case.get("attach_tpsl", False),
                "tpsl_sanity": tpsl_check,
                "body_sent": body,
                "create_retCode": create_res.get("retCode"),
                "create_retMsg": create_res.get("retMsg"),
                "order_id": order_id,
                "fallback_used": fallback_res is not None,
                "monitor_snapshots": snapshots,
                "final_hanging_in_panel": [
                    {
                        "orderId": r.get("orderId"),
                        "orderFilter": r.get("orderFilter"),
                        "orderStatus": r.get("orderStatus"),
                        "side": r.get("side"),
                        "takeProfit": r.get("takeProfit"),
                        "stopLoss": r.get("stopLoss"),
                        "stopOrderType": r.get("stopOrderType"),
                        "orderLinkId": r.get("orderLinkId"),
                    }
                    for r in hanging
                ],
                "hanging_count_after": len(hanging),
            }
            report["cases"].append(case_result)
            await asyncio.sleep(1.0)

        # Limpieza: cancelar todo lo abierto de nuestras pruebas + tpsl colgados
        final = await _fetch_panel(client, symbol)
        for row in final["all_open"]:
            link = str(row.get("orderLinkId") or "")
            if not link.startswith("tpslverify-"):
                continue
            oid = str(row.get("orderId") or "")
            if not oid:
                continue
            cr = await client.cancel_order({"category": "spot", "symbol": symbol, "orderId": oid})
            report["cleanup"].append({"orderId": oid, "orderFilter": row.get("orderFilter"), "cancel": cr.get("retCode")})

        # Cancelar tpsl huérfanos del panel (todas las pruebas)
        post = await _fetch_panel(client, symbol)
        for row in post["all_open"]:
            if not _classify_hanging(row):
                continue
            oid = str(row.get("orderId") or "")
            if oid:
                cr = await client.cancel_order({"category": "spot", "symbol": symbol, "orderId": oid})
                report["cleanup"].append({"orderId": oid, "type": "hanging_tpsl", "cancel": cr.get("retCode")})

        report["panel_after"] = await _fetch_panel(client, symbol)

        # Conclusiones automáticas
        for c in report["cases"]:
            hang = c["hanging_count_after"]
            if c["order_type"] == "Market" and c.get("attach_tpsl"):
                if hang > 0:
                    report["conclusions"].append(
                        f"CONFIRMADO: {c['id']} — Market con TP/SL dejó {hang} orden(es) colgada(s) en panel"
                    )
                elif c["create_retCode"] != 0:
                    report["conclusions"].append(
                        f"{c['id']}: Market+TP/SL rechazado por exchange (retCode={c['create_retCode']})"
                    )
            if c["order_type"] == "Limit" and c.get("attach_tpsl") and hang == 0:
                report["conclusions"].append(
                    f"OK: {c['id']} — Limit bracket TP/SL sin colgados tras fill/cancel"
                )
            if c["order_type"] == "Market" and not c.get("attach_tpsl") and hang == 0:
                report["conclusions"].append(
                    f"OK: {c['id']} — Market limpio sin TP/SL, panel vacío"
                )

    finally:
        await client.aclose()

    out = os.path.join(OUTPUT_DIR, "tpsl_panel_verify.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    report["output_file"] = out
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--notional", type=float, default=12.0)
    args = parser.parse_args()
    r = asyncio.run(run_verify(args.symbol, args.notional))
    print("\n=== TP/SL PANEL VERIFY ===")
    for c in r["cases"]:
        print(
            f"  {c['id']}: create={c['create_retCode']} hanging={c['hanging_count_after']} "
            f"tpsl_mode={c['tp_sl_mode']}"
        )
    print("\nConclusiones:")
    for line in r.get("conclusions") or []:
        print(f"  - {line}")
    print(f"\nGuardado: {r['output_file']}\n")


if __name__ == "__main__":
    main()
