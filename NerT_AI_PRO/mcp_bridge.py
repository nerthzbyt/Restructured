"""Puente MCP stdio → agente NerT (Bybit official trading server)."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DEV = BASE_DIR / "src_dev"
if str(SRC_DEV) not in sys.path:
    sys.path.insert(0, str(SRC_DEV))

from mcp_bybit.client import McpBybitClient  # noqa: E402
from mcp_bybit.probes import is_mutation_tool  # noqa: E402

_client: Optional[McpBybitClient] = None
_client_lock = asyncio.Lock()

MCP_SERVERS = ("bybit", "github", "playwright", "context7", "pycharm")


def bybit_env_info() -> Dict[str, Any]:
    env = str(os.getenv("BYBIT_ENV", "mainnet") or "mainnet").strip().lower()
    return {
        "bybit_env": env,
        "is_demo": env == "demo",
        "private_api": "https://api-demo.bybit.com" if env == "demo" else "https://api.bybit.com",
        "public_api": "https://api.bybit.com",
        "mcp_private_limitation": (
            "El MCP oficial Bybit usa api.bybit.com para endpoints privados. "
            "Con claves DEMO usar nertzh_api.balance / orders_status en lugar de mcp_bybit.getWalletBalance."
            if env == "demo"
            else None
        ),
    }


def _mcp_command() -> List[str]:
    local_js = BASE_DIR / ".vendor" / "trading-mcp" / "dist" / "index.js"
    if local_js.is_file():
        node = shutil.which("node") or "node"
        return [node, str(local_js)]
    return ["npx", "-y", "bybit-official-trading-server@latest"]


def _mcp_env() -> Dict[str, str]:
    """Env para MCP Bybit. Nota: servidor oficial no soporta api-demo; solo mainnet/testnet."""
    env = {
        "BYBIT_API_KEY": os.getenv("BYBIT_API_KEY", ""),
        "BYBIT_API_SECRET": os.getenv("BYBIT_API_SECRET", ""),
        "BYBIT_TESTNET": os.getenv("BYBIT_TESTNET", "false"),
        "BYBIT_ENV": os.getenv("BYBIT_ENV", "mainnet"),
    }
    return {k: v for k, v in env.items() if v}


def load_mcp_server_catalog(
    *,
    server: str = "bybit",
    read_only: bool = True,
    max_tools: int = 0,
    executable: bool = True,
) -> List[Dict[str, Any]]:
    """Carga herramientas desde mcps/{server}/tools/*.json."""
    srv = str(server or "bybit").strip().lower()
    tools_dir = BASE_DIR / "mcps" / srv / "tools"
    if not tools_dir.is_dir():
        return []
    prefix = f"mcp_{srv}"
    out: List[Dict[str, Any]] = []
    for path in sorted(tools_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        name = str(raw.get("name") or path.stem)
        if read_only and is_mutation_tool(name):
            continue
        desc = str(raw.get("description") or "").strip()
        if len(desc) > 300:
            desc = desc[:297] + "..."
        schema = raw.get("inputSchema") if isinstance(raw.get("inputSchema"), dict) else {}
        kind = prefix if executable and srv == "bybit" else f"{prefix}_catalog"
        out.append(
            {
                "name": f"{prefix}.{name}",
                "kind": kind,
                "mcp_server": srv,
                "mcp_tool": name,
                "executable": bool(executable and srv == "bybit"),
                "description": desc or f"MCP {srv} tool {name}",
                "input_schema": schema,
            }
        )
        if max_tools > 0 and len(out) >= max_tools:
            break
    return out


def load_bybit_tool_catalog(*, max_tools: int = 0, read_only: bool = True) -> List[Dict[str, Any]]:
    return load_mcp_server_catalog(
        server="bybit",
        read_only=read_only,
        max_tools=max_tools,
        executable=True,
    )


async def _get_client() -> McpBybitClient:
    global _client
    async with _client_lock:
        if _client is None:
            _client = McpBybitClient(command=_mcp_command(), env=_mcp_env())
            await asyncio.to_thread(_client.start)
        return _client


def _is_demo_private_mcp_tool(name: str) -> bool:
    """Herramientas privadas que fallan con claves demo en MCP mainnet."""
    low = name.lower()
    private_hints = (
        "wallet",
        "balance",
        "order",
        "position",
        "execution",
        "closedpnl",
        "account",
        "transfer",
        "withdraw",
        "deposit",
        "borrow",
        "repay",
        "fee",
        "apikey",
        "member",
    )
    if low.startswith("get") or low.startswith("query") or low.startswith("list"):
        return any(h in low for h in private_hints)
    return False


async def call_bybit_mcp(
    tool_name: str,
    arguments: Dict[str, Any],
    *,
    allow_mutations: bool = False,
    timeout_s: float = 45.0,
) -> Dict[str, Any]:
    name = str(tool_name or "").strip()
    if name.startswith("mcp_bybit."):
        name = name[len("mcp_bybit.") :]
    if not name:
        return {"ok": False, "error": "missing_tool_name"}

    env_info = bybit_env_info()
    if env_info.get("is_demo") and _is_demo_private_mcp_tool(name):
        return {
            "ok": False,
            "error": "demo_use_nertzh_api",
            "message": (
                f"MCP privado {name} no compatible con BYBIT_ENV=demo (MCP usa api.bybit.com). "
                "Usa: nertzh_api.balance, nertzh_api.orders_status, nertzh_api.open_orders"
            ),
            "suggested_tools": [
                "nertzh_api.balance",
                "nertzh_api.orders_status",
                "nertzh_api.open_orders",
            ],
        }

    if is_mutation_tool(name) and not allow_mutations:
        return {
            "ok": False,
            "error": "mutation_blocked",
            "message": f"Tool {name} requiere allow_mutations=true y apply=true en la petición",
        }
    try:
        client = await _get_client()
        result = await asyncio.to_thread(
            client.call_tool, name, dict(arguments or {}), timeout_s=timeout_s
        )
        content = result.get("content") if isinstance(result, dict) else None
        texts: List[str] = []
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and isinstance(c.get("text"), str):
                    texts.append(c["text"])
        text = "\n".join(texts).strip()
        if not text:
            text = json.dumps(result, ensure_ascii=False)[:8000]
        return {
            "ok": not bool(result.get("isError")),
            "tool": name,
            "text": text[:12000],
            "raw": result,
            "network": "mcp_mainnet_public_or_private",
        }
    except Exception as e:
        return {"ok": False, "error": "mcp_exception", "tool": name, "message": str(e)}


async def mcp_status() -> Dict[str, Any]:
    stats: Dict[str, Any] = {"servers": {}}
    for srv in MCP_SERVERS:
        cat = load_mcp_server_catalog(server=srv, read_only=False, max_tools=0)
        stats["servers"][srv] = {
            "tools": len(cat),
            "executable": srv == "bybit",
        }
    stats.update(bybit_env_info())
    stats["client_started"] = _client is not None
    stats["has_api_key"] = bool(os.getenv("BYBIT_API_KEY"))
    return stats