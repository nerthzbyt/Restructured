"""TP/SL panel: market fill+amend, limit bracket, scan panel actual."""
import asyncio
import json
import os
import sys
import uuid

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "src"))

from bybit_v5 import BybitV5Client
from src_dev.config import DevSettings, OUTPUT_DIR, load_trading_thresholds, private_rest_base_url
from src_dev.bybit.rest import BybitRestClient
from src_dev.orders.exchange_catalog import fetch_instrument_constraints
from src_dev.orders.combinator import qty_for_notional

SYMBOL = "BTCUSDT"

async def scan_panel(client, sym):
    data = {}
    for flt in ("Order", "tpslOrder", "StopOrder", "OcoOrder"):
        r = await client.order_realtime(category="spot", symbol=sym, order_filter=flt, limit=50)
        rows = (r.get("result") or {}).get("list") or []
        data[flt] = [{
            "orderId": x.get("orderId"), "status": x.get("orderStatus"), "side": x.get("side"),
            "orderType": x.get("orderType"), "tp": x.get("takeProfit"), "sl": x.get("stopLoss"),
            "trigger": x.get("triggerPrice"), "stopOrderType": x.get("stopOrderType"),
            "filter": x.get("orderFilter"), "link": x.get("orderLinkId"), "created": x.get("createdTime"),
        } for x in rows]
    return data

async def main():
    cfg = DevSettings.from_env()
    th = load_trading_thresholds()
    c = await fetch_instrument_constraints(SYMBOL, cfg)
    async with BybitRestClient(cfg) as rest:
        snap = await rest.fetch_market_snapshot(SYMBOL)
    last = float((snap.get("ticker") or {}).get("last_price") or 0)
    ask = float(((snap.get("orderbook") or {}).get("asks") or [[last]])[0][0])
    bid = float(((snap.get("orderbook") or {}).get("bids") or [[last]])[0][0])
    tp_b = round(last * (1 + th["tp_pct"]/100), 1)
    sl_b = round(last * (1 - th["sl_pct"]/100), 1)
    tp_s = round(last * (1 - th["tp_pct"]/100), 1)
    sl_s = round(last * (1 + th["sl_pct"]/100), 1)
    qty = qty_for_notional(12.0, ask, c)
    client = BybitV5Client(os.getenv("BYBIT_API_KEY"), os.getenv("BYBIT_API_SECRET"), base_url=private_rest_base_url())
    report = {"last_price": last, "panel_initial": None, "tests": [], "panel_final": None, "cleanup": []}
    try:
        report["panel_initial"] = await scan_panel(client, SYMBOL)

        # 1) Market limpio -> fill -> amend TP/SL (simula AUTO_TPSL post-fill en Market)
        link1 = f"q2-mkt-{uuid.uuid4().hex[:8]}"
        cr1 = await client.create_order({
            "category": "spot", "symbol": SYMBOL, "side": "Buy", "orderType": "Market",
            "qty": qty, "timeInForce": "IOC", "marketUnit": "baseCoin", "orderLinkId": link1,
        })
        oid1 = (cr1.get("result") or {}).get("orderId")
        await asyncio.sleep(2)
        am1 = await client.amend_order({
            "category": "spot", "symbol": SYMBOL, "orderId": str(oid1),
            "takeProfit": str(tp_b), "stopLoss": str(sl_b),
        }) if oid1 else {"retCode": -1, "retMsg": "no_oid"}
        p1 = await scan_panel(client, SYMBOL)
        report["tests"].append({
            "name": "market_fill_then_amend_tpsl",
            "create": cr1.get("retCode"), "amend": am1.get("retCode"), "amend_msg": am1.get("retMsg"),
            "order_id": oid1, "panel": {k: len(v) for k, v in p1.items()},
            "panel_detail": p1,
        })

        # 2) Limit bracket TP/SL agresivo (fill inmediato)
        link2 = f"q2-lim-{uuid.uuid4().hex[:8]}"
        cr2 = await client.create_order({
            "category": "spot", "symbol": SYMBOL, "side": "Buy", "orderType": "Limit",
            "qty": qty, "timeInForce": "GTC", "price": str(ask),
            "takeProfit": str(tp_b), "stopLoss": str(sl_b),
            "tpOrderType": "Market", "slOrderType": "Market", "orderLinkId": link2,
        })
        oid2 = (cr2.get("result") or {}).get("orderId")
        await asyncio.sleep(3)
        rt2 = await client.order_realtime(category="spot", symbol=SYMBOL, order_id=str(oid2)) if oid2 else {}
        row2 = ((rt2.get("result") or {}).get("list") or [{}])[0]
        p2 = await scan_panel(client, SYMBOL)
        report["tests"].append({
            "name": "limit_bracket_at_ask",
            "create": cr2.get("retCode"), "order_id": oid2,
            "status": row2.get("orderStatus"), "tp": row2.get("takeProfit"), "sl": row2.get("stopLoss"),
            "panel": {k: len(v) for k, v in p2.items()}, "panel_detail": p2,
        })

        # 3) Market Sell con TP/SL invertidos (contrarios) — patrón bug
        link3 = f"q2-bug-{uuid.uuid4().hex[:8]}"
        cr3 = await client.create_order({
            "category": "spot", "symbol": SYMBOL, "side": "Sell", "orderType": "Market",
            "qty": qty, "timeInForce": "IOC", "marketUnit": "baseCoin", "orderLinkId": link3,
            "takeProfit": str(sl_s), "stopLoss": str(tp_s),  # invertidos para Sell
            "tpOrderType": "Market", "slOrderType": "Market",
        })
        if cr3.get("retCode") != 0:
            cr3b = await client.create_order({
                "category": "spot", "symbol": SYMBOL, "side": "Sell", "orderType": "Market",
                "qty": qty, "timeInForce": "IOC", "marketUnit": "baseCoin", "orderLinkId": link3 + "b",
            })
            cr3 = {"primary": cr3, "fallback": cr3b}
        await asyncio.sleep(3)
        p3 = await scan_panel(client, SYMBOL)
        report["tests"].append({
            "name": "market_sell_wrong_tpsl_or_fallback",
            "result": cr3, "panel": {k: len(v) for k, v in p3.items()}, "panel_detail": p3,
        })

        await asyncio.sleep(5)
        report["panel_before_cleanup"] = await scan_panel(client, SYMBOL)

        # Cleanup todo lo abierto
        for flt, rows in (report["panel_before_cleanup"] or {}).items():
            for row in rows:
                oid = row.get("orderId")
                if oid:
                    cx = await client.cancel_order({"category": "spot", "symbol": SYMBOL, "orderId": oid})
                    report["cleanup"].append({"orderId": oid, "filter": flt, "retCode": cx.get("retCode")})

        report["panel_final"] = await scan_panel(client, SYMBOL)
    finally:
        await client.aclose()

    path = os.path.join(OUTPUT_DIR, "tpsl_panel_verify_full.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2, default=str)[:8000])
    print("saved", path)

asyncio.run(main())