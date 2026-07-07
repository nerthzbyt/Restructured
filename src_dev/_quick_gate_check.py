"""Quick gate comparison — no WS probe."""
import asyncio
import copy
import json
import os

from src_dev.config import DevSettings, OUTPUT_DIR
from src_dev.orders.exchange_catalog import fetch_exchange_orders, summarize_exchange_orders

async def main():
    cfg = DevSettings.from_env()
    out = {}
    for src in ("old_results", "exchange"):
        c = copy.copy(cfg)
        c.lab_order_stats_source = src
        orders = await fetch_exchange_orders("BTCUSDT", settings=c)
        stats = summarize_exchange_orders(orders)
        out[src] = {
            "total": stats.get("total_orders_sampled"),
            "distribution": stats.get("combo_distribution_pct"),
            "pagination": stats.get("pagination"),
        }
        print(f"{src}: {out[src]['total']} orders", list((out[src]['distribution'] or {}).items())[:5])
    path = os.path.join(OUTPUT_DIR, "gate_quick_check.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print("saved", path)

if __name__ == "__main__":
    asyncio.run(main())