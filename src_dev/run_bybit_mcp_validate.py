#!/usr/bin/env python3
"""
Validación exhaustiva del MCP oficial de Bybit (bybit-official-trading-server).

Lista y prueba TODAS las herramientas expuestas por el servidor MCP — no solo las
que usa Nertzh/src. Clasifica resultados: ok, auth_required, schema_ok, api_error, etc.

Uso:
  python -m src_dev.run_bybit_mcp_validate
  python -m src_dev.run_bybit_mcp_validate --symbol ETHUSDT --max-tools 50
  python -m src_dev.run_bybit_mcp_validate --list-only
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

from dotenv import load_dotenv

from src_dev.config import OUTPUT_DIR, PROJECT_ROOT, private_rest_base_url
from src_dev.mcp_bybit.client import McpBybitClient
from src_dev.mcp_bybit.probes import (
    NERTZH_USED_ENDPOINTS,
    build_probe_args,
    classify_result,
    is_mutation_tool,
    map_tool_to_nertzh_usage,
    tool_category,
)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_mcp_command() -> List[str]:
    local_js = os.path.join(PROJECT_ROOT, ".vendor", "trading-mcp", "dist", "index.js")
    if os.path.isfile(local_js):
        node = shutil.which("node") or "node"
        return [node, local_js]
    return ["npx", "-y", "bybit-official-trading-server@latest"]


def _mcp_env() -> Dict[str, str]:
    env = {
        "BYBIT_API_KEY": os.getenv("BYBIT_API_KEY", ""),
        "BYBIT_API_SECRET": os.getenv("BYBIT_API_SECRET", ""),
        "BYBIT_TESTNET": os.getenv("BYBIT_TESTNET", "false"),
    }
    bybit_env = str(os.getenv("BYBIT_ENV", "mainnet") or "mainnet").strip().lower()
    if bybit_env == "demo":
        env["BYBIT_ENV_NOTE"] = "demo_keys_on_mainnet_mcp"
    return {k: v for k, v in env.items() if v}


def _extract_text(result: Dict[str, Any]) -> tuple[bool, str]:
    if result.get("isError"):
        content = result.get("content") or []
        parts = [c.get("text", "") for c in content if isinstance(c, dict)]
        return True, "\n".join(parts) or str(result)
    content = result.get("content") or []
    parts = [c.get("text", "") for c in content if isinstance(c, dict)]
    return False, "\n".join(parts) or json.dumps(result)


def _category_from_vendor_index() -> Dict[str, List[str]]:
    vendor = os.path.join(PROJECT_ROOT, ".vendor", "trading-mcp", "src", "tools")
    out: Dict[str, List[str]] = {}
    if not os.path.isdir(vendor):
        return out
    for name in os.listdir(vendor):
        idx = os.path.join(vendor, name, "index.ts")
        if not os.path.isfile(idx):
            continue
        tools: List[str] = []
        with open(idx, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("import {") and " from " in line:
                    fn = line.split("{", 1)[1].split("}", 1)[0].strip()
                    if fn:
                        tools.append(fn)
        if tools:
            out[name] = tools
    return out


def run(args: argparse.Namespace) -> Dict[str, Any]:
    symbol = args.symbol.upper()
    mcp_env = _mcp_env()
    client = McpBybitClient(command=_default_mcp_command(), env=mcp_env)
    report: Dict[str, Any] = {
        "generated_at": _utc_now(),
        "symbol": symbol,
        "mcp_command": _default_mcp_command(),
        "mcp_package": "bybit-official-trading-server@2.1.15",
        "project_bybit_env": os.getenv("BYBIT_ENV", "mainnet"),
        "mcp_base_url": "https://api-testnet.bybit.com"
        if str(os.getenv("BYBIT_TESTNET", "false")).lower() == "true"
        else "https://api.bybit.com",
        "nertzh_private_base": private_rest_base_url(),
        "mcp_demo_gap": str(os.getenv("BYBIT_ENV", "")).lower() == "demo",
        "has_api_credentials": bool(os.getenv("BYBIT_API_KEY") and os.getenv("BYBIT_API_SECRET")),
        "vendor_categories": _category_from_vendor_index(),
        "nertzh_endpoints_used": NERTZH_USED_ENDPOINTS,
    }

    try:
        client.start()
        tools = client.list_tools()
        tools = sorted(tools, key=lambda t: t.get("name", ""))
        report["total_tools_available"] = len(tools)
        if args.offset > 0:
            tools = tools[int(args.offset):]
        if args.max_tools and args.max_tools > 0:
            tools = tools[: int(args.max_tools)]

        report["tool_count"] = len(tools)
        report["tool_names"] = [t.get("name") for t in tools]

        if args.list_only:
            report["mode"] = "list_only"
            return report

        results: List[Dict[str, Any]] = []
        status_counts: Counter[str] = Counter()
        cat_counts: Counter[str] = Counter()

        for i, tool in enumerate(tools):
            name = str(tool.get("name") or "")
            cat = tool_category(name)
            cat_counts[cat] += 1
            probe_args = build_probe_args(tool, symbol=symbol)
            mode = "mutation_schema_probe" if is_mutation_tool(name) and not args.include_mutations else "live_call"
            if is_mutation_tool(name) and not args.include_mutations:
                probe_args = {}

            t0 = time.perf_counter()
            entry: Dict[str, Any] = {
                "tool": name,
                "category": cat,
                "mode": mode,
                "args": probe_args,
                "nertzh_uses": map_tool_to_nertzh_usage(name),
            }
            try:
                timeout = 45.0 if cat == "websocket" else 25.0
                raw = client.call_tool(name, probe_args, timeout_s=timeout)
                is_err, text = _extract_text(raw)
                status, detail = classify_result(
                    tool_name=name, is_error=is_err, text=text, category=cat
                )
                entry["status"] = status
                entry["detail"] = detail
                entry["duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)
                if status == "ok" and len(text) > 500:
                    entry["sample"] = text[:500]
            except Exception as exc:
                entry["status"] = "exception"
                entry["detail"] = str(exc)[:400]
                entry["duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)

            status_counts[entry["status"]] += 1
            results.append(entry)

            if args.delay_ms > 0 and i + 1 < len(tools):
                time.sleep(args.delay_ms / 1000.0)

        by_status: Dict[str, List[str]] = defaultdict(list)
        for r in results:
            by_status[r["status"]].append(r["tool"])

        nertzh_tools = {t for tools in NERTZH_USED_ENDPOINTS.values() for t in tools}
        mcp_names = {t.get("name") for t in tools}
        report.update(
            {
                "mode": "full_validate",
                "category_counts": dict(cat_counts),
                "status_counts": dict(status_counts),
                "results": results,
                "by_status": {k: v for k, v in by_status.items()},
                "coverage": {
                    "nertzh_tool_names": sorted(nertzh_tools),
                    "mcp_missing_nertzh_tools": sorted(nertzh_tools - mcp_names),
                    "mcp_extra_tools_count": len(mcp_names - nertzh_tools),
                    "nertzh_endpoint_count": len(NERTZH_USED_ENDPOINTS),
                    "mcp_tool_count": len(mcp_names),
                },
            }
        )
        return report
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validar todas las herramientas del MCP Bybit oficial")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--max-tools", type=int, default=0, help="0 = todas")
    parser.add_argument("--offset", type=int, default=0, help="Saltar N herramientas (batching)")
    parser.add_argument("--list-only", action="store_true", help="Solo listar herramientas, sin llamar")
    parser.add_argument("--include-mutations", action="store_true", help="Llamar mutaciones con args mínimos (riesgoso)")
    parser.add_argument("--delay-ms", type=int, default=80, help="Pausa entre llamadas")
    parser.add_argument("--out", default="", help="Ruta JSON de salida")
    args = parser.parse_args()

    report = run(args)
    out_path = args.out or os.path.join(OUTPUT_DIR, "bybit_mcp_validate.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Bybit MCP validation -> {out_path}")
    print(f"  tools: {report.get('tool_count', 0)}")
    if report.get("status_counts"):
        for k, v in sorted(report["status_counts"].items()):
            print(f"  {k}: {v}")
    if report.get("coverage"):
        missing = report["coverage"].get("mcp_missing_nertzh_tools") or []
        if missing:
            print(f"  WARNING missing nertzh tools in MCP: {missing}")
        else:
            print(f"  Nertzh endpoints cubiertos por MCP (+{report['coverage'].get('mcp_extra_tools_count', 0)} extra)")


if __name__ == "__main__":
    main()