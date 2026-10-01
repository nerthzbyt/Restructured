"""Registro unificado de herramientas: nativas, Nertzh API, MCP, proyecto /src."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp_bridge import load_mcp_server_catalog

BASE_DIR = Path(__file__).resolve().parent.parent

# Endpoints reales montados en /api (src/Nertzh.py)
NERTZH_API_ROUTES: List[Dict[str, Any]] = [
    {"name": "nertzh_api.settings", "method": "GET", "path": "/settings", "desc": "Configuración activa del bot"},
    {"name": "nertzh_api.config", "method": "GET", "path": "/config", "desc": "Config runtime (thresholds, capital, BYBIT_ENV)"},
    {"name": "nertzh_api.status", "method": "GET", "path": "/status", "desc": "Estado del bot (running, ws, symbols)"},
    {"name": "nertzh_api.health", "method": "GET", "path": "/health", "desc": "Health check motor"},
    {"name": "nertzh_api.balance", "method": "GET", "path": "/balance", "desc": "Balance demo/mainnet vía motor (USAR en demo)"},
    {"name": "nertzh_api.profit", "method": "GET", "path": "/profit", "desc": "PnL agregado"},
    {"name": "nertzh_api.validation", "method": "GET", "path": "/validation", "desc": "Validación sistema"},
    {"name": "nertzh_api.ml_status", "method": "GET", "path": "/ml/status", "desc": "Estado ML"},
    {"name": "nertzh_api.agent_status", "method": "GET", "path": "/admin/agent/status", "desc": "Agent tick interno"},
    {"name": "nertzh_api.tpsl_status", "method": "GET", "path": "/admin/tpsl/status", "desc": "Estado TP/SL automático"},
    {"name": "nertzh_api.orders_status", "method": "GET", "path": "/orders/status", "desc": "Órdenes activas (demo OK)"},
    {"name": "nertzh_api.operations", "method": "GET", "path": "/operations/status", "desc": "Operaciones en curso"},
    {"name": "nertzh_api.storage", "method": "GET", "path": "/storage/status", "desc": "DuckDB/SQLite storage"},
    {"name": "nertzh_api.storage_recent", "method": "GET", "path": "/storage/recent/{symbol}", "desc": "Últimos ticks/orderbook/métricas DuckDB+SQLite espejo"},
    {"name": "nertzh_api.mode", "method": "GET", "path": "/mode/status", "desc": "Modo bot (full/hft/etc)"},
    {"name": "nertzh_api.ticker", "method": "GET", "path": "/ticker/{symbol}", "desc": "Ticker live del bot"},
    {"name": "nertzh_api.metrics", "method": "GET", "path": "/metrics/{symbol}", "desc": "Métricas recalculadas (velas memoria/DB). Secundario a bot_live_state/decisions."},
    {"name": "nertzh_api.combined", "method": "GET", "path": "/combined/{symbol}", "desc": "Score combinado recalculado. Secundario a bot_live_state/decisions."},
    {"name": "nertzh_api.decisions", "method": "GET", "path": "/decisions/{symbol}", "desc": "Última decisión del loop + blockers + métricas _last_metrics_by_symbol"},
    {"name": "nertzh_api.orderbook", "method": "GET", "path": "/orderbook/{symbol}", "desc": "Orderbook live bot"},
    {"name": "nertzh_api.candles", "method": "GET", "path": "/candles/{symbol}/{limit}", "desc": "Velas (memoria del loop o DB; usar limit=50 para retornos)"},
    {"name": "nertzh_api.trades", "method": "GET", "path": "/trades/{symbol}", "desc": "Trades DB"},
    {"name": "nertzh_api.last_trade", "method": "GET", "path": "/last_trade/{symbol}", "desc": "Último trade"},
    {"name": "nertzh_api.pio", "method": "GET", "path": "/pio/{symbol}", "desc": "Métrica PIO"},
    {"name": "nertzh_api.egm", "method": "GET", "path": "/egm/{symbol}", "desc": "Métrica EGM"},
    {"name": "nertzh_api.ild", "method": "GET", "path": "/ild/{symbol}", "desc": "Métrica ILD"},
    {"name": "nertzh_api.rol", "method": "GET", "path": "/rol/{symbol}", "desc": "Métrica ROL"},
    {"name": "nertzh_api.ogm", "method": "GET", "path": "/ogm/{symbol}", "desc": "Métrica OGM"},
    {"name": "nertzh_api.open_orders", "method": "GET", "path": "/exchange/open_orders/{symbol}", "desc": "Órdenes exchange demo"},
    {"name": "nertzh_api.market_data", "method": "GET", "path": "/market_data/{symbol}", "desc": "Market data agregado"},
    {"name": "nertzh_api.discovery", "method": "GET", "path": "/discovery/metrics/{symbol}", "desc": "Métricas discovery"},
]

NATIVE_AGENT_TOOLS: List[Dict[str, Any]] = [
    {"name": "market_ticker", "kind": "native", "description": "Ticker público Bybit REST mainnet.", "args": {"symbol": "BTCUSDT"}},
    {"name": "market_orderbook", "kind": "native", "description": "Orderbook público (depth 1-200).", "args": {"symbol": "BTCUSDT", "depth": 50}},
    {"name": "optimize", "kind": "native", "description": "Optimiza thresholds+pesos desde trades.", "args": {"symbol": "BTCUSDT", "limit": 2000, "iterations": 900, "apply": False}},
    {"name": "train_ml", "kind": "native", "description": "Entrena ML desde trades.", "args": {"symbol": "BTCUSDT"}},
    {"name": "autoevolve", "kind": "native", "description": "Evolución cuant multi-ronda.", "args": {"symbol": "BTCUSDT", "rounds": 2, "apply": False}},
    {"name": "tool_search", "kind": "meta", "description": "Busca herramientas por keyword en catálogo completo.", "args": {"query": "wallet balance", "limit": 15}},
    {"name": "project_context", "kind": "meta", "description": "Snapshot contexto proyecto, BYBIT_ENV, rutas /src.", "args": {}},
    {"name": "bot_live_state", "kind": "meta", "description": "Estado live del motor embebido: métricas loop, decision_detail, velas en memoria, jsonl mom.", "args": {"symbol": "BTCUSDT"}},
    {"name": "src_list", "kind": "project", "description": "Árbol de archivos bajo src/ o subpath.", "args": {"subpath": "src", "max_depth": 2}},
    {"name": "src_read", "kind": "project", "description": "Lee archivo del proyecto (max 400 líneas; JSON >512KB bloqueado).", "args": {"path": "src/Nertzh.py", "offset": 1, "limit": 80}},
    {"name": "json_file_info", "kind": "project", "description": "Metadatos de logs/*.json o data/*.jsonl sin cargar contenido.", "args": {"path": "logs/results.json"}},
    {"name": "analyze_trading_data", "kind": "native", "description": "Análisis matemático/admin completo de results.json + metrics_snapshots.jsonl (streaming).", "args": {"results_path": "logs/results.json", "jsonl_path": "data/metrics_snapshots.jsonl"}},
    {"name": "src_grep", "kind": "project", "description": "Busca regex en código fuente.", "args": {"pattern": "agent_tick", "subpath": "src"}},
    {"name": "src_outline", "kind": "project", "description": "Outline de símbolos de un módulo src/.", "args": {"module": "Nertzh.py"}},
]

PROMPT_CORE_TOOLS = [
    "tool_search",
    "project_context",
    "bot_live_state",
    "nertzh_api.decisions",
    "nertzh_api.status",
    "nertzh_api.config",
    "nertzh_api.candles",
    "nertzh_api.balance",
    "nertzh_api.orders_status",
    "nertzh_api.metrics",
    "nertzh_api.combined",
    "market_ticker",
    "market_orderbook",
    "nertzh_api.trades",
    "src_read",
    "src_grep",
    "mcp_bybit.getTickers",
    "mcp_bybit.getOrderbook",
    "mcp_bybit.getMarketKline",
    "mcp_bybit.getOpenInterest",
    "optimize",
]


def _nertzh_api_tools() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in NERTZH_API_ROUTES:
        out.append(
            {
                "name": r["name"],
                "kind": "nertzh_api",
                "method": r["method"],
                "path": r["path"],
                "description": r["desc"],
                "args": {"symbol": "BTCUSDT"} if "{symbol}" in r["path"] else {},
            }
        )
    return out


def build_full_catalog(
    *,
    include_mcp: bool = True,
    include_mcp_mutations: bool = False,
    include_mcp_catalog_only: bool = True,
) -> List[Dict[str, Any]]:
    catalog: List[Dict[str, Any]] = []
    catalog.extend(NATIVE_AGENT_TOOLS)
    catalog.extend(_nertzh_api_tools())
    if include_mcp:
        catalog.extend(
            load_mcp_server_catalog(
                server="bybit",
                read_only=not include_mcp_mutations,
                max_tools=0,
            )
        )
    if include_mcp_catalog_only:
        for srv in ("github", "playwright", "context7", "pycharm"):
            catalog.extend(
                load_mcp_server_catalog(
                    server=srv,
                    read_only=True,
                    max_tools=0,
                    executable=False,
                )
            )
    return catalog


def build_prompt_catalog(
    *,
    include_mcp: bool = True,
    extra_names: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Catálogo reducido para el prompt + búsqueda dinámica vía tool_search."""
    full = {t["name"]: t for t in build_full_catalog(include_mcp=include_mcp)}
    names = list(PROMPT_CORE_TOOLS)
    if extra_names:
        names.extend(extra_names)
    out: List[Dict[str, Any]] = []
    seen = set()
    for n in names:
        if n in seen:
            continue
        t = full.get(n)
        if t:
            out.append(t)
            seen.add(n)
    out.append(full.get("tool_search") or NATIVE_AGENT_TOOLS[5])
    out.append(full.get("project_context") or NATIVE_AGENT_TOOLS[6])
    out.append(full.get("bot_live_state") or NATIVE_AGENT_TOOLS[7])
    return out


def search_tools(query: str, *, limit: int = 20) -> List[Dict[str, Any]]:
    q = str(query or "").strip().lower()
    if not q:
        return []
    full = build_full_catalog(include_mcp=True, include_mcp_mutations=True)
    scored: List[tuple[int, Dict[str, Any]]] = []
    for t in full:
        name = str(t.get("name") or "").lower()
        desc = str(t.get("description") or "").lower()
        score = 0
        for token in q.split():
            if token in name:
                score += 3
            if token in desc:
                score += 1
        if score > 0:
            scored.append((score, t))
    scored.sort(key=lambda x: -x[0])
    return [t for _, t in scored[: max(1, min(50, int(limit)))]]


def resolve_nertzh_path(tool_name: str, args: Dict[str, Any]) -> Optional[str]:
    """Convierte nertzh_api.X o path directo a ruta /api."""
    if tool_name.startswith("nertzh_api."):
        key = tool_name.split(".", 1)[1]
        for r in NERTZH_API_ROUTES:
            if r["name"].endswith("." + key) or r["path"].strip("/").replace("/", "_") == key:
                path = r["path"]
                sym = str(args.get("symbol") or "BTCUSDT").upper()
                path = path.replace("{symbol}", sym)
                if "{limit}" in path:
                    path = path.replace("{limit}", str(int(args.get("limit") or 50)))
                return path
    if tool_name == "nertzh_api" and isinstance(args.get("path"), str):
        return str(args["path"])
    return None


def registry_stats() -> Dict[str, Any]:
    full = build_full_catalog(include_mcp=True, include_mcp_mutations=True)
    by_kind: Dict[str, int] = {}
    for t in full:
        k = str(t.get("kind") or "other")
        by_kind[k] = by_kind.get(k, 0) + 1
    return {
        "total": len(full),
        "by_kind": by_kind,
        "bybit_env": os.getenv("BYBIT_ENV", "mainnet"),
        "prompt_core": len(PROMPT_CORE_TOOLS),
    }