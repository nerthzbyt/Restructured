import asyncio, json, os, sys
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _root); sys.path.insert(0, os.path.join(_root, "src"))
from bybit_v5 import BybitV5Client
from src_dev.config import private_rest_base_url, OUTPUT_DIR

async def main():
    c = BybitV5Client(os.getenv("BYBIT_API_KEY"), os.getenv("BYBIT_API_SECRET"), base_url=private_rest_base_url())
    sym = "BTCUSDT"
    out = {"open": {}, "history_tpsl_sample": []}
    try:
        for flt in ("Order", "tpslOrder", "StopOrder", "OcoOrder"):
            r = await c.order_realtime(category="spot", symbol=sym, order_filter=flt, limit=50)
            out["open"][flt] = (r.get("result") or {}).get("list") or []
        h = await c.order_history(category="spot", symbol=sym, limit=50)
        for row in (h.get("result") or {}).get("list") or []:
            flt = str(row.get("orderFilter") or "Order")
            if flt.lower() != "order" or row.get("takeProfit") or row.get("stopLoss"):
                out["history_tpsl_sample"].append({
                    "id": row.get("orderId"), "type": row.get("orderType"), "tif": row.get("timeInForce"),
                    "status": row.get("orderStatus"), "filter": row.get("orderFilter"),
                    "tp": row.get("takeProfit"), "sl": row.get("stopLoss"), "side": row.get("side"),
                    "link": row.get("orderLinkId"),
                })
    finally:
        await c.aclose()
    p = os.path.join(OUTPUT_DIR, "exchange_panel_scan.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2)[:6000])

asyncio.run(main())