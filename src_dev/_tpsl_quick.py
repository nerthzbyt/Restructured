"""Verificación rápida TP/SL panel — 3 casos críticos."""
import asyncio
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src_dev.config import OUTPUT_DIR, private_rest_base_url, load_trading_thresholds
from src_dev.bybit.rest import BybitRestClient
from src_dev.config import DevSettings
from src_dev.orders.exchange_catalog import fetch_instrument_constraints
from src_dev.orders.combinator import qty_for_notional
from bybit_v5 import BybitV5Client

SYMBOL = "BTCUSDT"
NOTIONAL = 12.0

async def panel(client, sym):
    out = {}
    for flt in ("Order", "tpslOrder", "StopOrder"):
        r = await client.order_realtime(category="spot", symbol=sym, order_filter=flt, limit=50)
        rows = (r.get("result") or {}).get("list") or []
        out[flt] = [{"id": x.get("orderId"), "status": x.get("orderStatus"), "side": x.get("side"),
                      "tp": x.get("takeProfit"), "sl": x.get("stopLoss"), "filter": x.get("orderFilter"),
                      "link": x.get("orderLinkId")} for x in rows]
    return out


async def main():
    cfg = DevSettings.from_env()
    th = load_trading_thresholds()
    c = await fetch_instrument_constraints(SYMBOL, cfg)
    async with BybitRestClient(cfg) as rest:
        snap = await rest.fetch_market_snapshot(SYMBOL)
    last = float((snap.get("ticker") or {}).get("last_price") or 0)
    ask = float(((snap.get("orderbook") or {}).get("asks") or [[last]])[0][0])
    tp = round(last * 1.003, 1)
    sl = round(last * 0.998, 1)
    qty = qty_for_notional(NOTIONAL, ask, c)
    key, sec = os.getenv("BYBIT_API_KEY"), os.getenv("BYBIT_API_SECRET")
    client = BybitV5Client(key, sec, base_url=private_rest_base_url())
    results = []
    try:
        tests = [
            ("market_clean", {"category": "spot", "symbol": SYMBOL, "side": "Buy", "orderType": "Market",
                              "qty": qty, "timeInForce": "IOC", "marketUnit": "baseCoin"}),
            ("market_tpsl_nertzh", {"category": "spot", "symbol": SYMBOL, "side": "Buy", "orderType": "Market",
                                    "qty": qty, "timeInForce": "IOC", "marketUnit": "baseCoin",
                                    "takeProfit": str(tp), "stopLoss": str(sl),
                                    "tpOrderType": "Market", "slOrderType": "Market"}),
            ("limit_tpsl", {"category": "spot", "symbol": SYMBOL, "side": "Buy", "orderType": "Limit",
                            "qty": qty, "timeInForce": "GTC", "price": str(ask),
                            "takeProfit": str(tp), "stopLoss": str(sl),
                            "tpOrderType": "Market", "slOrderType": "Market"}),
        ]
        for name, body in tests:
            body = dict(body)
            body["orderLinkId"] = f"q-{name}-{uuid.uuid4().hex[:8]}"
            before = await panel(client, SYMBOL)
            cr = await client.create_order(body)
            oid = (cr.get("result") or {}).get("orderId")
            await asyncio.sleep(3)
            after = await panel(client, SYMBOL)
            rt = await client.order_realtime(category="spot", symbol=SYMBOL, order_id=str(oid)) if oid else {}
            parent = ((rt.get("result") or {}).get("list") or [{}])[0]
            results.append({
                "test": name, "retCode": cr.get("retCode"), "retMsg": cr.get("retMsg"),
                "order_id": oid, "parent_status": parent.get("orderStatus"),
                "parent_tp": parent.get("takeProfit"), "parent_sl": parent.get("stopLoss"),
                "panel_before": {k: len(v) for k, v in before.items()},
                "panel_after": {k: len(v) for k, v in after.items()},
                "panel_after_detail": after,
                "hanging_tpsl": sum(len(after.get(f) or []) for f in ("tpslOrder", "StopOrder")),
            })
            # cleanup opens from this test
            for flt in ("Order", "tpslOrder", "StopOrder"):
                for row in after.get(flt) or []:
                    if str(row.get("link") or "").startswith("q-"):
                        await client.cancel_order({"category": "spot", "symbol": SYMBOL, "orderId": row["id"]})
            await asyncio.sleep(0.5)
    finally:
        await client.aclose()
    path = os.path.join(OUTPUT_DIR, "tpsl_panel_verify.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_price": last, "tp": tp, "sl": sl, "results": results}, f, indent=2)
    print(json.dumps(results, indent=2))
    print("saved", path)

asyncio.run(main())