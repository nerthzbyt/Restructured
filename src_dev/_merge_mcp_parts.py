#!/usr/bin/env python3
import json
import os
from collections import Counter, defaultdict

from src_dev.config import OUTPUT_DIR

PARTS = os.path.join(OUTPUT_DIR, "bybit_mcp_parts")
OUT = os.path.join(OUTPUT_DIR, "bybit_mcp_validate.json")

parts = sorted(f for f in os.listdir(PARTS) if f.startswith("part_") and f.endswith(".json"))
listing = json.load(open(os.path.join(PARTS, "list.json"), encoding="utf-8"))
all_results = []
for p in parts:
    data = json.load(open(os.path.join(PARTS, p), encoding="utf-8"))
    all_results.extend(data.get("results") or [])

status_counts = Counter(r.get("status", "unknown") for r in all_results)
cat_counts = Counter(r.get("category", "other") for r in all_results)
by_status = defaultdict(list)
for r in all_results:
    by_status[r.get("status", "unknown")].append(r.get("tool", ""))

nertzh = set()
for tools in (listing.get("nertzh_endpoints_used") or {}).values():
    nertzh.update(tools)
mcp_all = set(listing.get("tool_names") or [])

merged = {k: v for k, v in listing.items() if k not in ("mode",)}
merged.update(
    {
        "mode": "full_validate_batched",
        "tool_count": len(all_results),
        "status_counts": dict(status_counts),
        "category_counts": dict(cat_counts),
        "by_status": {k: v for k, v in by_status.items()},
        "results": all_results,
        "coverage": {
            "nertzh_tool_names": sorted(nertzh),
            "mcp_missing_nertzh_tools": sorted(nertzh - mcp_all),
            "mcp_extra_tools_count": len(mcp_all - nertzh),
            "nertzh_endpoint_count": len(listing.get("nertzh_endpoints_used") or {}),
            "mcp_tool_count": len(mcp_all),
        },
    }
)
json.dump(merged, open(OUT, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print(f"merged {len(all_results)} tools -> {OUT}")
for k, v in sorted(status_counts.items()):
    print(f"  {k}: {v}")