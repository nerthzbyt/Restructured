"""Ejecuta 4 perfiles diversos en demo — sin WS largo."""
import asyncio
import json
import os
import uuid

from src_dev.config import DevSettings, OUTPUT_DIR, load_trading_thresholds, private_rest_base_url
from src_dev.orders.combinator import build_order_body, qty_for_notional
from src_dev.orders.exchange_catalog import fetch_instrument_constraints
from src_dev.orders.lab import _round_to_tick
from src_dev.bybit.rest import BybitRestClient

try:
    from bybit_v5 import BybitV5Client
except ImportError:
    BybitV5Client = None


PROBES = [
    {"order_type": "Market", "time_in_force": "IOC", "market_unit": "baseCoin", "label": "Market+IOC+baseCoin"},
    {"order_type": "Market", "time_in_force": "IOC", "market_unit": "quoteCoin", "label": "Market+IOC+quoteCoin"},
    {"order_type": "Limit", "time_in_force": "GTC", "market_unit": None, "label": "Limit+GTC", "aggressive": True},
    {"order_type": "Limit", "time_in_force": "PostOnly", "market_unit": None, "label": "Limit+PostOnly", "aggressive": False},
]


async def main():
    cfg = DevSettings.from_env()
    sym = cfg.symbol
    constraints = await fetch_instrument_constraints(sym, cfg)
    async with BybitRestClient(cfg) as rest:
        snap = await rest.fetch_market_snapshot(sym)
    ob = snap.get("orderbook") or {}
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    bid = float(bids[0][0]) if bids else 0.0
    ask = float(asks[0][0]) if asks else float((snap.get("ticker") or {}).get("last_price") or 0)
    last = float((snap.get("ticker") or {}).get("last_price") or ask)
    test_usdt = 12.0

    key, secret = os.getenv("BYBIT_API_KEY", ""), os.getenv("BYBIT_API_SECRET", "")
    client = BybitV5Client(key, secret, base_url=private_rest_base_url())
    results = []
    try:
        for p in PROBES:
            from src_dev.orders.exchange_schema import OrderCombination

            combo = OrderCombination(
                order_type=p["order_type"],
                time_in_force=p["time_in_force"],
                market_unit=p.get("market_unit"),
                order_filter="Order",
                is_leverage=0,
                price_anchor="mid" if p["order_type"] == "Limit" else None,
                tp_sl_mode="none",
                slippage_type=None,
                slippage_value=None,
                side_hint="Buy",
                valid=True,
                invalid_reason="",
            )
            if p["order_type"] == "Market" and p.get("market_unit") == "quoteCoin":
                qty = f"{test_usdt:.2f}"
            else:
                px = ask if p["order_type"] == "Limit" else last
                qty = qty_for_notional(test_usdt, px, constraints)

            price_s = None
            if p["order_type"] == "Limit":
                price_s = str(_round_to_tick(ask if p.get("aggressive") else bid, constraints.tick_size))

            body = build_order_body(combo, symbol=sym, side="Buy", qty=qty, price=price_s)
            body["orderLinkId"] = f"qprobe-{uuid.uuid4().hex[:10]}"

            res = await client.create_order(body)
            oid = str((res.get("result") or {}).get("orderId") or "")
            await asyncio.sleep(1.5)
            status, avg_px, exec_q = "?", 0.0, 0.0
            cancel = None
            if oid:
                rt = await client.order_realtime(category="spot", symbol=sym, order_id=oid)
                row = ((rt.get("result") or {}).get("list") or [{}])[0]
                status = str(row.get("orderStatus") or "?")
                avg_px = float(row.get("avgPrice") or 0)
                exec_q = float(row.get("cumExecQty") or 0)
                if status not in ("Filled", "PartiallyFilled"):
                    cancel = await client.cancel_order({"category": "spot", "symbol": sym, "orderId": oid})

            slip = ((avg_px - last) / last * 10000.0) if avg_px and last else 0.0
            results.append({
                "label": p["label"],
                "create_ok": res.get("retCode") == 0,
                "retMsg": res.get("retMsg"),
                "order_id": oid,
                "status": status,
                "avg_price": avg_px,
                "exec_qty": exec_q,
                "slippage_bps": round(slip, 4),
                "filled": status in ("Filled", "PartiallyFilled"),
                "cancelled": cancel is not None and cancel.get("retCode") == 0,
            })
            await asyncio.sleep(0.3)
    finally:
        await client.aclose()

    ranked = sorted(results, key=lambda r: (1 if r["filled"] else 0, -r["slippage_bps"]), reverse=True)
    out = {"symbol": sym, "last_price": last, "probes": results, "true_top": ranked}
    path = os.path.join(OUTPUT_DIR, "live_probes_quick.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
    print("saved", path)


if __name__ == "__main__":
    asyncio.run(main())