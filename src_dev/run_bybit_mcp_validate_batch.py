#!/usr/bin/env python3
"""Ejecuta validación MCP Bybit en lotes y fusiona resultados."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List

from src_dev.config import OUTPUT_DIR, PROJECT_ROOT

BATCH = 75
OUT = os.path.join(OUTPUT_DIR, "bybit_mcp_validate.json")
PARTS_DIR = os.path.join(OUTPUT_DIR, "bybit_mcp_parts")


def main() -> None:
    os.makedirs(PARTS_DIR, exist_ok=True)
    list_path = os.path.join(PARTS_DIR, "list.json")
    subprocess.run(
        [sys.executable, "-m", "src_dev.run_bybit_mcp_validate", "--list-only", "--out", list_path],
        cwd=PROJECT_ROOT,
        check=True,
    )
    with open(list_path, encoding="utf-8") as f:
        listing = json.load(f)
    total = int(listing.get("tool_count") or 0)

    all_results: List[Dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    cat_counts: Counter[str] = Counter()

    for offset in range(0, total, BATCH):
        size = min(BATCH, total - offset)
        part = os.path.join(PARTS_DIR, f"part_{offset:04d}.json")
        print(f"batch offset={offset} size={size}")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "src_dev.run_bybit_mcp_validate",
                "--offset",
                str(offset),
                "--max-tools",
                str(size),
                "--delay-ms",
                "40",
                "--out",
                part,
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )
        with open(part, encoding="utf-8") as f:
            chunk = json.load(f)
        for r in chunk.get("results") or []:
            all_results.append(r)
            status_counts[r.get("status", "unknown")] += 1
            cat_counts[r.get("category", "other")] += 1

    by_status: Dict[str, List[str]] = defaultdict(list)
    for r in all_results:
        by_status[r.get("status", "unknown")].append(r.get("tool", ""))

    merged = {k: v for k, v in listing.items() if k not in ("mode", "tool_names")}
    merged.update(
        {
            "mode": "full_validate_batched",
            "tool_count": len(all_results),
            "batch_size": BATCH,
            "status_counts": dict(status_counts),
            "category_counts": dict(cat_counts),
            "by_status": {k: v for k, v in by_status.items()},
            "results": all_results,
            "coverage": listing.get("coverage") or {},
        }
    )
    nertzh_tools = set()
    for tools in (listing.get("nertzh_endpoints_used") or {}).values():
        nertzh_tools.update(tools)
    mcp_names = {r.get("tool") for r in all_results}
    merged["coverage"] = {
        "nertzh_tool_names": sorted(nertzh_tools),
        "mcp_missing_nertzh_tools": sorted(nertzh_tools - mcp_names),
        "mcp_extra_tools_count": max(0, total - len(nertzh_tools)),
        "nertzh_endpoint_count": len(listing.get("nertzh_endpoints_used") or {}),
        "mcp_tool_count": total,
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"merged -> {OUT}")
    for k, v in sorted(status_counts.items()):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()