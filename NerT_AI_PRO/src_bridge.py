"""Acceso seguro al código fuente y contexto del proyecto (/src, config, data)."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
NERT_PRO = BASE_DIR / "NerT_AI_PRO"

_ALLOWED_ROOTS = (
    SRC_DIR.resolve(),
    CONFIG_DIR.resolve(),
    DATA_DIR.resolve(),
    LOGS_DIR.resolve(),
    NERT_PRO.resolve(),
    (BASE_DIR / "tests").resolve(),
    (BASE_DIR / "src_dev").resolve(),
)

# Archivos JSON/JSONL grandes: src_read por líneas no sirve (árbol anidado o 1 línea/record).
_LARGE_JSON_HINT = (
    "Archivo grande o JSON anidado: usa herramienta analyze_trading_data "
    "(scripts/analyze_trading_data.py) en lugar de src_read."
)


def _safe_path(rel: str) -> Optional[Path]:
    raw = str(rel or "").strip().replace("\\", "/").lstrip("/")
    if not raw or ".." in raw.split("/"):
        return None
    candidate = (BASE_DIR / raw).resolve()
    for root in _ALLOWED_ROOTS:
        try:
            candidate.relative_to(root)
            return candidate
        except ValueError:
            continue
    return None


def list_src_tree(*, subpath: str = "src", max_depth: int = 3) -> Dict[str, Any]:
    base = _safe_path(subpath)
    if base is None or not base.is_dir():
        return {"ok": False, "error": "invalid_path", "subpath": subpath}

    def _walk(p: Path, depth: int) -> List[Dict[str, Any]]:
        if depth > max_depth:
            return []
        out: List[Dict[str, Any]] = []
        try:
            entries = sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        except OSError as e:
            return [{"error": str(e)}]
        for e in entries:
            if e.name.startswith(".") or e.name in {"__pycache__", "node_modules", ".venv"}:
                continue
            rel = str(e.relative_to(BASE_DIR)).replace("\\", "/")
            node: Dict[str, Any] = {"path": rel, "type": "dir" if e.is_dir() else "file"}
            if e.is_dir() and depth < max_depth:
                node["children"] = _walk(e, depth + 1)
            out.append(node)
        return out

    return {
        "ok": True,
        "root": str(base.relative_to(BASE_DIR)).replace("\\", "/"),
        "tree": _walk(base, 0),
    }


def json_file_info(path: str) -> Dict[str, Any]:
    """Metadatos de JSON/JSONL sin parsear el árbol completo (evita OOM en IDE/agente)."""
    p = _safe_path(path)
    if p is None:
        return {"ok": False, "error": "invalid_path", "path": path}
    if not p.is_file():
        return {"ok": False, "error": "not_file", "path": path}
    size = p.stat().st_size
    suffix = p.suffix.lower()
    line_count = 0
    if suffix == ".jsonl":
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            for line_count, _ in enumerate(fh, 1):
                pass
    else:
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            line_count = sum(1 for _ in fh)
    return {
        "ok": True,
        "path": str(p.relative_to(BASE_DIR)).replace("\\", "/"),
        "size_bytes": size,
        "size_mb": round(size / (1024 * 1024), 3),
        "format": "jsonl" if suffix == ".jsonl" else "json",
        "line_count": line_count,
        "hint": _LARGE_JSON_HINT if size > 512_000 else None,
    }


def read_project_file(path: str, *, offset: int = 1, limit: int = 120) -> Dict[str, Any]:
    p = _safe_path(path)
    if p is None:
        return {"ok": False, "error": "invalid_path", "path": path}
    if not p.is_file():
        return {"ok": False, "error": "not_file", "path": path}

    size = p.stat().st_size
    suffix = p.suffix.lower()
    if size > 512_000 and suffix in {".json", ".jsonl"}:
        info = json_file_info(path)
        info["ok"] = False
        info["error"] = "file_too_large_for_src_read"
        info["max_safe_bytes"] = 512_000
        return info

    try:
        text = p.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return {"ok": False, "error": "read_error", "message": str(e)}
    lines = text.splitlines()
    start = max(0, int(offset) - 1)
    end = start + max(1, min(400, int(limit)))
    chunk = lines[start:end]
    return {
        "ok": True,
        "path": str(p.relative_to(BASE_DIR)).replace("\\", "/"),
        "total_lines": len(lines),
        "offset": int(offset),
        "limit": int(limit),
        "content": "\n".join(chunk),
    }


def grep_project(
    pattern: str,
    *,
    subpath: str = "src",
    glob: str = "*.py",
    head_limit: int = 30,
) -> Dict[str, Any]:
    base = _safe_path(subpath)
    if base is None:
        return {"ok": False, "error": "invalid_path"}
    try:
        rx = re.compile(str(pattern), re.IGNORECASE)
    except re.error as e:
        return {"ok": False, "error": "bad_regex", "message": str(e)}

    matches: List[Dict[str, Any]] = []
    for fp in sorted(base.rglob(glob)):
        if "__pycache__" in fp.parts:
            continue
        try:
            lines = fp.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for i, line in enumerate(lines, 1):
            if rx.search(line):
                matches.append(
                    {
                        "file": str(fp.relative_to(BASE_DIR)).replace("\\", "/"),
                        "line": i,
                        "text": line.strip()[:200],
                    }
                )
                if len(matches) >= head_limit:
                    break
        if len(matches) >= head_limit:
            break
    return {"ok": True, "pattern": pattern, "matches": matches, "count": len(matches)}


def project_context_snapshot() -> Dict[str, Any]:
    """Resumen real del proyecto para inyectar en el agente (sin inventar)."""
    src_files: List[str] = []
    if SRC_DIR.is_dir():
        for fp in sorted(SRC_DIR.glob("*.py")):
            src_files.append(fp.name)

    env_demo = str(os.getenv("BYBIT_ENV", "mainnet")).strip().lower() == "demo"
    ctx: Dict[str, Any] = {
        "project_root": str(BASE_DIR),
        "bybit_env": os.getenv("BYBIT_ENV", "mainnet"),
        "bybit_demo_trading": env_demo,
        "demo_note": (
            "Claves demo → API privada en https://api-demo.bybit.com. "
            "Datos públicos en api.bybit.com. "
            "MCP oficial Bybit usa api.bybit.com: para wallet/órdenes privadas en demo "
            "usa herramientas nertzh_api (balance, orders/status) NO mcp_bybit privadas."
            if env_demo
            else "Modo mainnet: MCP privado y nertzh_api usan la misma red autenticada."
        ),
        "live_trading_enabled": os.getenv("LIVE_TRADING_ENABLED", "false"),
        "llm_backend": os.getenv("LLM_BACKEND", "disabled"),
        "llm_model": os.getenv("LLM_MODEL", ""),
        "src_modules": src_files,
        "key_paths": {
            "motor": "src/Nertzh.py",
            "bybit_client": "src/bybit_v5.py",
            "metrics_utils": "src/utils.py",
            "settings": "src/settings.py",
            "optimizer": "src/optimizer.py",
            "agent_host": "NerT_AI_PRO/main.py",
            "react_agent": "NerT_AI_PRO/react_agent.py",
            "mcp_bridge": "NerT_AI_PRO/mcp_bridge.py",
            "data_db": "data/trading.db",
            "duckdb": "data/nertz.duckdb",
        },
        "nertzh_api_base": "/api",
        "agent_api_base": "/agent",
        "entry_point": "python NerT_AI_PRO/main.py (puerto 8787)",
        "architecture_note": (
            "Un solo proceso: main.py monta nertzh.app en /api e inicia el bot en lifespan. "
            "Para análisis en vivo el agente debe usar bot_live_state o nertzh_api.decisions "
            "(métricas de _last_metrics_by_symbol), no solo metrics/combined."
        ),
        "metrics_history_jsonl": "data/metrics_snapshots.jsonl",
    }
    return {"ok": True, "context": ctx}


def src_module_outline(module: str = "Nertzh.py") -> Dict[str, Any]:
    p = _safe_path(f"src/{module}")
    if p is None or not p.is_file():
        return {"ok": False, "error": "not_found", "module": module}
    try:
        text = p.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return {"ok": False, "error": str(e)}
    symbols: List[str] = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("class ") or s.startswith("async def ") or s.startswith("def "):
            if s.startswith("class "):
                symbols.append(s.split("(")[0].split(":")[0].strip())
            elif "app." in s or s.startswith("async def ") or s.startswith("def "):
                name = s.split("(")[0].replace("async def ", "def ").replace("def ", "").strip()
                if not name.startswith("_") or name in {"__init__"}:
                    symbols.append(name)
        if len(symbols) >= 80:
            break
    return {
        "ok": True,
        "module": f"src/{module}",
        "lines": len(text.splitlines()),
        "symbols_sample": symbols[:60],
    }