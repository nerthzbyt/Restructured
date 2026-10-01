from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import sqlite3
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Optional

import aiohttp
import numpy as np
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)

SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import Nertzh as nertzh  # noqa: E402
from mcp_bridge import bybit_env_info, call_bybit_mcp, mcp_status  # noqa: E402
from qwen_desktop import (  # noqa: E402
    normalize_model as _qwen_normalize_model,
    qwen_desktop_chat,
    qwen_desktop_status,
)
from bot_state_bridge import bot_live_state_snapshot  # noqa: E402
from react_agent import run_react_agent, tool_search_for_agent  # noqa: E402
from data_analyzer import analyze_trading_data  # noqa: E402
from src_bridge import (  # noqa: E402
    grep_project,
    json_file_info,
    list_src_tree,
    project_context_snapshot,
    read_project_file,
    src_module_outline,
)
from tool_registry import (  # noqa: E402
    build_full_catalog,
    build_prompt_catalog,
    registry_stats,
    resolve_nertzh_path,
    search_tools,
)
from intelligence_catalog import (  # noqa: E402
    compute_prediction_level,
    full_catalog as intelligence_full_catalog,
    ORDER_PROFILES_VALIDATED,
)
from optimizer import optimize_system_from_trades  # noqa: E402
from signal_engine import (  # noqa: E402
    DEFAULT_COMBINED_WEIGHTS,
    CombinedWeights,
    Thresholds,
    symmetrize_threshold_values,
)

MEMORY_DB_PATH = os.path.join(os.path.dirname(__file__), "agent_memory.sqlite")


class AgentMemoryStore:
    def __init__(self, path: str) -> None:
        self.path = str(path)
        self._lock = Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, check_same_thread=False)

    def _init_db(self) -> None:
        with self._lock:
            con = self._connect()
            try:
                # Crear tabla principal de eventos
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_ms INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        payload_json TEXT NOT NULL
                    )
                    """
                )
                # Crear índices para mejorar rendimiento de consultas
                con.execute(
                    "CREATE INDEX IF NOT EXISTS idx_agent_events_ts ON agent_events(ts_ms)"
                )
                con.execute(
                    "CREATE INDEX IF NOT EXISTS idx_agent_events_kind ON agent_events(kind)"
                )
                con.commit()
            except sqlite3.Error as e:
                nertzh.logger.error(f"Error creando tabla de eventos: {e}")
                raise
            finally:
                con.close()

    def add_event(self, kind: str, payload: Dict[str, Any]) -> None:
        k = str(kind or "").strip() or "event"
        body = payload if isinstance(payload, dict) else {"payload": payload}
        with self._lock:
            con = self._connect()
            try:
                con.execute(
                    "INSERT INTO agent_events(ts_ms, kind, payload_json) VALUES(?,?,?)",
                    (int(time.time() * 1000), k, json.dumps(body, ensure_ascii=False)),
                )
                con.commit()
            finally:
                con.close()

    def recent(
        self, limit: int = 50, kind: Optional[str] = None
    ) -> list[Dict[str, Any]]:
        lim = int(limit)
        lim = 50 if lim <= 0 else lim
        lim = 500 if lim > 500 else lim
        k = str(kind).strip() if isinstance(kind, str) and kind.strip() else None
        with self._lock:
            con = self._connect()
            try:
                if k is None:
                    rows = con.execute(
                        "SELECT id, ts_ms, kind, payload_json FROM agent_events ORDER BY ts_ms DESC LIMIT ?",
                        (lim,),
                    ).fetchall()
                else:
                    rows = con.execute(
                        "SELECT id, ts_ms, kind, payload_json FROM agent_events WHERE kind = ? ORDER BY ts_ms DESC LIMIT ?",
                        (k, lim),
                    ).fetchall()
            finally:
                con.close()

        out: list[Dict[str, Any]] = []
        for rid, ts_ms, rk, payload_json in rows or []:
            try:
                payload = (
                    json.loads(payload_json) if isinstance(payload_json, str) else {}
                )
            except Exception:
                payload = {"raw": payload_json}
            out.append(
                {
                    "id": int(rid),
                    "ts_ms": int(ts_ms),
                    "kind": str(rk),
                    "payload": payload,
                }
            )
        return out

    def clear(self) -> int:
        with self._lock:
            con = self._connect()
            try:
                cur = con.execute("DELETE FROM agent_events")
                con.commit()
                return int(cur.rowcount or 0)
            finally:
                con.close()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            con = self._connect()
            try:
                row = con.execute(
                    "SELECT COUNT(1), MIN(ts_ms), MAX(ts_ms) FROM agent_events"
                ).fetchone()
            finally:
                con.close()
        total = int(row[0] or 0) if row else 0
        min_ts = int(row[1]) if row and row[1] is not None else None
        max_ts = int(row[2]) if row and row[2] is not None else None
        return {"events": total, "min_ts_ms": min_ts, "max_ts_ms": max_ts}

    def chat_turns(self, limit: int = 30) -> list[Dict[str, Any]]:
        """Empareja chat_in + chat_out por session_id para restaurar el feed de la UI."""
        lim = int(limit)
        lim = 1 if lim <= 0 else lim
        lim = 100 if lim > 100 else lim
        outs = self.recent(limit=lim, kind="chat_out")
        if not outs:
            return []
        needed_sids = {
            str((ev.get("payload") or {}).get("session_id") or "").strip()
            for ev in outs
        }
        needed_sids.discard("")
        ins_by_sid: Dict[str, Dict[str, Any]] = {}
        if needed_sids:
            with self._lock:
                con = self._connect()
                try:
                    rows = con.execute(
                        "SELECT payload_json FROM agent_events WHERE kind = 'chat_in' ORDER BY ts_ms DESC LIMIT ?",
                        (max(lim * 4, 50),),
                    ).fetchall()
                finally:
                    con.close()
            for (payload_json,) in rows or []:
                try:
                    payload = (
                        json.loads(payload_json) if isinstance(payload_json, str) else {}
                    )
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                sid = str(payload.get("session_id") or "").strip()
                if sid and sid in needed_sids and sid not in ins_by_sid:
                    ins_by_sid[sid] = payload
        turns: list[Dict[str, Any]] = []
        for ev in reversed(outs):
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
            sid = str(payload.get("session_id") or "").strip()
            out = payload.get("out") if isinstance(payload.get("out"), dict) else {}
            chin = ins_by_sid.get(sid, {})
            turns.append(
                {
                    "id": int(ev.get("id") or 0),
                    "session_id": sid,
                    "ts_ms": int(ev.get("ts_ms") or 0),
                    "message": str(chin.get("message") or "").strip(),
                    "symbol": chin.get("symbol"),
                    "intent": out.get("intent"),
                    "response": out,
                }
            )
        return turns


agent_memory = AgentMemoryStore(path=MEMORY_DB_PATH)

_http_session: Optional[aiohttp.ClientSession] = None


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return float(v) if bool(np.isfinite(v)) else float(default)


def _env_str(name: str, default: str) -> str:
    v = os.getenv(str(name), None)
    if v is None:
        return str(default)
    s = str(v).strip()
    return s if s else str(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(str(name), str(default)))
    except Exception:
        return float(default)


@dataclass(frozen=True)
class LLMConfig:
    backend: str
    base_url: str
    model: str
    temperature: float
    timeout_s: float
    api_key: Optional[str]


def _llm_config() -> LLMConfig:
    backend = _env_str("LLM_BACKEND", "disabled").lower()
    base_url = _env_str("LLM_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
    model_env = os.getenv("LLM_MODEL", None)
    if model_env is not None and str(model_env).strip():
        model = str(model_env).strip()
    else:
        # Modelos por defecto según el backend:
        # - openai_compat (DashScope): qwen-plus-latest (Qwen 3.7 Plus)
        # - ollama (local): qwen2.5-coder:latest (fallback local)
        # Nota: Mantener Lingma como plugin IDE (no actualizar a Qoder CN)
        if backend == "ollama":
            model = "qwen2.5-coder:latest"
        else:
            model = "qwen-plus-latest"
    temperature = float(max(0.0, min(2.0, _env_float("LLM_TEMPERATURE", 0.1))))
    timeout_s = float(max(3.0, min(120.0, _env_float("LLM_TIMEOUT_S", 30.0))))
    api_key = os.getenv("LLM_API_KEY", None)
    api_key = (
        str(api_key).strip() if api_key is not None and str(api_key).strip() else None
    )
    return LLMConfig(
        backend=str(backend),
        base_url=str(base_url),
        model=str(model),
        temperature=float(temperature),
        timeout_s=float(timeout_s),
        api_key=api_key,
    )


def _extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    s = str(text or "").strip()
    if not s:
        return None
    try:
        j = json.loads(s)
        return j if isinstance(j, dict) else None
    except Exception:
        pass
    lb = s.find("{")
    rb = s.rfind("}")
    if lb < 0 or rb <= lb:
        return None
    try:
        j = json.loads(s[lb : rb + 1])
        return j if isinstance(j, dict) else None
    except Exception:
        return None


async def _with_http_session(fn):
    s = _http_session
    if s is not None and not s.closed:
        return await fn(s)
    async with aiohttp.ClientSession() as session:
        return await fn(session)


async def _ollama_chat(
    *,
    cfg: LLMConfig,
    messages: list[Dict[str, str]],
) -> Dict[str, Any]:
    url = f"{cfg.base_url}/api/chat"
    payload: Dict[str, Any] = {
        "model": cfg.model,
        "stream": False,
        "messages": messages,
        "options": {"temperature": float(cfg.temperature)},
    }

    async def _do(session: aiohttp.ClientSession) -> Dict[str, Any]:
        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=float(cfg.timeout_s))
        ) as resp:
            raw = await resp.json(content_type=None)
            if not isinstance(raw, dict):
                return {"ok": False, "error": "bad_response", "raw": raw}
            if int(getattr(resp, "status", 0) or 0) >= 400:
                return {
                    "ok": False,
                    "error": "http_error",
                    "status": int(resp.status),
                    "raw": raw,
                }
            msg = raw.get("message")
            content = msg.get("content") if isinstance(msg, dict) else None
            return {"ok": True, "content": content, "raw": raw}

    return await _with_http_session(_do)


async def _openai_compat_chat(
    *,
    cfg: LLMConfig,
    messages: list[Dict[str, str]],
) -> Dict[str, Any]:
    url = f"{cfg.base_url}/v1/chat/completions"
    payload: Dict[str, Any] = {
        "model": cfg.model,
        "messages": messages,
        "temperature": float(cfg.temperature),
    }
    headers: Dict[str, str] = {"content-type": "application/json"}
    if isinstance(cfg.api_key, str) and cfg.api_key:
        headers["authorization"] = f"Bearer {cfg.api_key}"

    async def _do(session: aiohttp.ClientSession) -> Dict[str, Any]:
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=float(cfg.timeout_s)),
        ) as resp:
            raw = await resp.json(content_type=None)
            if not isinstance(raw, dict):
                return {"ok": False, "error": "bad_response", "raw": raw}
            if int(getattr(resp, "status", 0) or 0) >= 400:
                return {
                    "ok": False,
                    "error": "http_error",
                    "status": int(resp.status),
                    "raw": raw,
                }
            choices = raw.get("choices")
            c0 = (choices[0] if isinstance(choices, list) and choices else None) or {}
            msg = c0.get("message") if isinstance(c0, dict) else None
            content = msg.get("content") if isinstance(msg, dict) else None
            return {"ok": True, "content": content, "raw": raw}

    return await _with_http_session(_do)


def _llm_with_model(cfg: LLMConfig, model: str) -> LLMConfig:
    return LLMConfig(
        backend=str(cfg.backend),
        base_url=str(cfg.base_url),
        model=str(model),
        temperature=float(cfg.temperature),
        timeout_s=float(cfg.timeout_s),
        api_key=cfg.api_key,
    )


def _looks_like_model_not_found(res: Dict[str, Any]) -> bool:
    try:
        status = int(res.get("status") or 0)
    except Exception:
        status = 0
    raw = res.get("raw")
    if status in {404, 400} and isinstance(raw, dict):
        err = raw.get("error")
        if isinstance(err, str) and (
            "not found" in err.lower() or "model" in err.lower()
        ):
            return True
        msg = raw.get("message")
        if isinstance(msg, str) and (
            "not found" in msg.lower() or "model" in msg.lower()
        ):
            return True
    err2 = res.get("error")
    return bool(isinstance(err2, str) and "not found" in err2.lower())


async def llm_chat(messages: list[Dict[str, str]], *, model: Optional[str] = None) -> Dict[str, Any]:
    cfg0 = _llm_config()
    if cfg0.backend in {"off", "none", "disabled"}:
        return {
            "ok": False,
            "error": "llm_disabled",
            "backend": cfg0.backend,
            "model": cfg0.model,
        }

    cfg = _llm_with_model(cfg0, str(model)) if isinstance(model, str) and model.strip() else cfg0
    try:
        if cfg.backend == "ollama":
            res = await _ollama_chat(cfg=cfg, messages=messages)
        elif cfg.backend in {"openai", "openai_compat", "openai-compatible", "openai_compatible"}:
            res = await _openai_compat_chat(cfg=cfg, messages=messages)
        elif cfg.backend in {"qwen_desktop", "qwen-desktop", "qwen_studio", "qwen-studio"}:
            res = await qwen_desktop_chat(
                messages=messages,
                model=cfg.model,
                timeout_s=cfg.timeout_s,
            )
        else:
            return {
                "ok": False,
                "error": "unsupported_backend",
                "backend": cfg.backend,
                "model": cfg.model,
            }
    except Exception as e:
        return {
            "ok": False,
            "error": "exception",
            "backend": cfg.backend,
            "model": cfg.model,
            "message": str(e),
        }

    if isinstance(res, dict):
        res = dict(res)
    else:
        res = {"ok": False, "error": "bad_response", "raw": res}

    if not bool(res.get("ok")) and _looks_like_model_not_found(res) and cfg.backend == "ollama":
        alt_model: Optional[str] = None
        m = str(cfg.model or "").strip()
        if m.endswith(":latest"):
            alt_model = m[: -len(":latest")] or None
        elif m and ":" not in m:
            alt_model = f"{m}:latest"
        if alt_model and alt_model != m:
            cfg_alt = _llm_with_model(cfg, alt_model)
            try:
                res_alt = await _ollama_chat(cfg=cfg_alt, messages=messages)
            except Exception as e:
                res_alt = {"ok": False, "error": "exception", "message": str(e)}
            if isinstance(res_alt, dict) and bool(res_alt.get("ok")):
                res = dict(res_alt)
                cfg = cfg_alt

    res["backend"] = cfg.backend
    res["model"] = cfg.model
    return res

def _weights_from_ticker_data(symbol: Optional[str]) -> CombinedWeights:
    if not isinstance(symbol, str) or not symbol:
        return DEFAULT_COMBINED_WEIGHTS
    td = nertzh.bot.ticker_data.get(symbol)
    cw = td.get("combined_weights") if isinstance(td, dict) else None
    return CombinedWeights.from_dict(cw if isinstance(cw, dict) else None)


def _live_metrics_for_symbol(symbol: str) -> Dict[str, Any]:
    """Métricas del loop del motor (_last_metrics_by_symbol), no ticker_data.metrics."""
    sym = str(symbol or "BTCUSDT").strip().upper()
    bot = getattr(nertzh, "bot", None)
    if bot is None:
        return {}
    metrics = dict(getattr(bot, "_last_metrics_by_symbol", {}).get(sym) or {})
    if metrics:
        return metrics
    if hasattr(bot, "ticker_data"):
        td = bot.ticker_data.get(sym) or {}
        fallback = td.get("metrics") if isinstance(td.get("metrics"), dict) else {}
        return dict(fallback)
    return {}


def _start_thresholds() -> Thresholds:
    return Thresholds(
        combined_buy_threshold=float(
            getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0) or 8.0
        ),
        combined_sell_threshold=float(
            getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0) or -8.0
        ),
        combined_hold_band=float(
            getattr(nertzh.config, "COMBINED_HOLD_BAND", 2.0) or 2.0
        ),
    )


async def _fetch_bybit_public_json(
    session: aiohttp.ClientSession, path: str, params: Dict[str, Any]
) -> Dict[str, Any]:
    base = "https://api.bybit.com"
    url = f"{base}{path}"
    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            data = await resp.json(content_type=None)
            if not isinstance(data, dict):
                return {"ok": False, "error": "bad_response", "url": url, "raw": data}
            ret_code = data.get("retCode")
            if isinstance(ret_code, (int, float)) and int(ret_code) != 0:
                return {
                    "ok": False,
                    "error": "bybit_error",
                    "url": url,
                    "retCode": int(ret_code),
                    "retMsg": data.get("retMsg"),
                    "raw": data,
                }
            return {"ok": True, "url": url, "raw": data}
    except Exception as e:
        return {"ok": False, "error": "network_error", "url": url, "message": str(e)}


async def _get_public_ticker(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing_symbol"}
    category = (
        str(getattr(nertzh.config, "BYBIT_CATEGORY", "linear") or "linear")
        .strip()
        .lower()
    )

    async def _do(session: aiohttp.ClientSession) -> Dict[str, Any]:
        return await _fetch_bybit_public_json(
            session,
            "/v5/market/tickers",
            params={"category": category, "symbol": sym},
        )

    payload = await _with_http_session(_do)
    if not bool(payload.get("ok")):
        return {"ok": False, "symbol": sym, **payload, "ts": int(time.time() * 1000)}
    raw = payload.get("raw") if isinstance(payload, dict) else None
    result = raw.get("result") if isinstance(raw, dict) else None
    lst = result.get("list") if isinstance(result, dict) else None
    row = (lst[0] if isinstance(lst, list) and lst else None) or {}
    return {
        "ok": True,
        "symbol": sym,
        "lastPrice": row.get("lastPrice"),
        "bid1Price": row.get("bid1Price"),
        "ask1Price": row.get("ask1Price"),
        "highPrice24h": row.get("highPrice24h"),
        "lowPrice24h": row.get("lowPrice24h"),
        "volume24h": row.get("volume24h"),
        "turnover24h": row.get("turnover24h"),
        "ts": int(time.time() * 1000),
        "raw": row,
    }


async def _get_public_orderbook(symbol: str, depth: int = 50) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing_symbol"}
    d = int(depth) if int(depth) > 0 else 50
    if d > 200:
        d = 200
    category = (
        str(getattr(nertzh.config, "BYBIT_CATEGORY", "linear") or "linear")
        .strip()
        .lower()
    )

    async def _do(session: aiohttp.ClientSession) -> Dict[str, Any]:
        return await _fetch_bybit_public_json(
            session,
            "/v5/market/orderbook",
            params={"category": category, "symbol": sym, "limit": str(d)},
        )

    payload = await _with_http_session(_do)
    if not bool(payload.get("ok")):
        return {
            "ok": False,
            "symbol": sym,
            "depth": d,
            **payload,
            "ts": int(time.time() * 1000),
        }
    raw = payload.get("raw") if isinstance(payload, dict) else None
    result = raw.get("result") if isinstance(raw, dict) else None
    bids = result.get("b") if isinstance(result, dict) else None
    asks = result.get("a") if isinstance(result, dict) else None
    return {
        "ok": True,
        "symbol": sym,
        "depth": d,
        "bids": bids if isinstance(bids, list) else [],
        "asks": asks if isinstance(asks, list) else [],
        "ts": int(time.time() * 1000),
        "raw": result if isinstance(result, dict) else {},
    }


def _chat_html() -> str:
    return """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>NerT AI PRO — Quant Agent</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
  <style>
    :root{
      --bg:#000000;--surface:#0a0a0a;--surface2:#111111;--border:rgba(255,255,255,.08);
      --text:#f5f5f5;--muted:#8a8a8a;--accent:#f7a600;--accent2:#20b26c;
      --warn:#f7a600;--danger:#ef454a;--ok:#20b26c;--glow:rgba(247,166,0,.12);
    }
    *{box-sizing:border-box}
    body{margin:0;font-family:Inter,system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
    .app{display:grid;grid-template-rows:auto 1fr;min-height:100vh}
    header{display:flex;align-items:center;justify-content:space-between;padding:14px 22px;
      background:linear-gradient(180deg,#111,#000);border-bottom:1px solid var(--border)}
    .brand{display:flex;align-items:center;gap:12px}
    .logo{width:36px;height:36px;border-radius:8px;background:var(--accent);
      display:grid;place-items:center;font-weight:700;font-size:14px;color:#000}
    .level-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:8px;
      font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:700;border:1px solid var(--border)}
    .level-badge.L0{color:var(--muted)} .level-badge.L1{color:#9ca3af}
    .level-badge.L2{color:var(--warn);border-color:rgba(247,166,0,.35)}
    .level-badge.L3{color:var(--ok);border-color:rgba(32,178,108,.35)}
    .level-badge.L4{color:#fff;background:linear-gradient(135deg,rgba(247,166,0,.25),rgba(32,178,108,.2));border-color:var(--accent)}
    .conf-bar{height:4px;border-radius:2px;background:#1a1a1a;margin-top:8px;overflow:hidden}
    .conf-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--ok));transition:width .4s}
    .profile-chip{font-size:10px;padding:4px 8px;border-radius:6px;background:#141414;border:1px solid var(--border);
      color:var(--muted);margin:4px 4px 0 0;display:inline-block;font-family:'JetBrains Mono',monospace}
    .brand h1{margin:0;font-size:17px;font-weight:700;letter-spacing:.02em}
    .brand span{display:block;font-size:11px;color:var(--muted);font-weight:500}
    .status-pills{display:flex;gap:8px;flex-wrap:wrap}
    .pill{font-size:11px;padding:5px 10px;border-radius:999px;border:1px solid var(--border);
      background:var(--surface);color:var(--muted)}
    .pill.ok{color:var(--ok);border-color:rgba(45,212,160,.35)}
    .pill.warn{color:var(--warn)}
    main{display:grid;grid-template-columns:1fr 340px;gap:14px;padding:14px 18px 18px;min-height:0}
    @media(max-width:960px){main{grid-template-columns:1fr}}
    .panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;display:flex;flex-direction:column;min-height:0}
    .panel-h{padding:12px 14px;border-bottom:1px solid var(--border);font-size:12px;font-weight:600;
      letter-spacing:.06em;text-transform:uppercase;color:var(--muted);display:flex;justify-content:space-between;align-items:center}
    .chat-feed{flex:1;overflow:auto;padding:14px;display:flex;flex-direction:column;gap:12px;min-height:320px;max-height:calc(100vh - 320px)}
    .bubble{max-width:92%;padding:12px 14px;border-radius:12px;line-height:1.55;font-size:14px;white-space:pre-wrap}
    .bubble.user{align-self:flex-end;background:linear-gradient(135deg,rgba(61,139,253,.25),rgba(0,212,170,.12));border:1px solid rgba(61,139,253,.3)}
    .bubble.agent{align-self:flex-start;background:var(--surface2);border:1px solid var(--border)}
    .bubble.system{align-self:center;font-size:12px;color:var(--muted);background:transparent;border:none;padding:4px}
    .bubble .ts{display:block;font-size:10px;color:var(--muted);margin-bottom:6px;font-family:'JetBrains Mono',monospace}
    .bubble h3{margin:0 0 8px;font-size:13px;color:var(--accent2)}
    .steps{display:flex;flex-direction:column;gap:6px;margin:10px 0 0}
    .step{font-family:'JetBrains Mono',monospace;font-size:11px;padding:6px 8px;border-radius:8px;background:#0a1020;border:1px solid var(--border)}
    .step.ok{border-color:rgba(45,212,160,.3);color:var(--ok)}
    .step.fail{border-color:rgba(255,92,106,.35);color:var(--danger)}
    .composer{padding:12px 14px;border-top:1px solid var(--border);background:var(--surface2)}
    .controls{display:grid;grid-template-columns:1fr 90px 90px auto;gap:8px;margin-bottom:8px}
    @media(max-width:700px){.controls{grid-template-columns:1fr 1fr}}
    input,textarea,button{font-family:inherit}
    input,textarea{background:#0a1020;border:1px solid var(--border);color:var(--text);border-radius:10px;padding:9px 11px;font-size:13px}
    input:focus,textarea:focus{outline:none;border-color:rgba(61,139,253,.55);box-shadow:0 0 0 3px var(--glow)}
    textarea{width:100%;min-height:72px;resize:vertical;margin-bottom:8px}
    .btn-row{display:flex;gap:8px;flex-wrap:wrap}
    button{border:1px solid var(--border);background:var(--surface);color:var(--text);border-radius:10px;
      padding:9px 14px;font-size:13px;font-weight:600;cursor:pointer;transition:.15s}
    button:hover{border-color:rgba(61,139,253,.45);background:#121c34}
    button.primary{background:linear-gradient(135deg,var(--accent),#2b6fd4);border-color:transparent;color:#fff}
    button.primary:hover{filter:brightness(1.08)}
    button:disabled{opacity:.5;cursor:not-allowed}
    .chk{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--muted);padding:0 4px}
    .side .card{padding:12px 14px;border-bottom:1px solid var(--border)}
    .metric{display:flex;justify-content:space-between;align-items:baseline;margin:6px 0}
    .metric .k{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
    .metric .v{font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:600}
    .metric .v.up{color:var(--ok)} .metric .v.down{color:var(--danger)}
    .raw-toggle{font-size:11px;color:var(--accent);cursor:pointer;margin-top:8px}
    pre.raw{font-family:'JetBrains Mono',monospace;font-size:10px;background:#0a1020;border:1px solid var(--border);
      border-radius:10px;padding:10px;overflow:auto;max-height:200px;display:none;margin-top:6px}
    .loading{display:inline-block;width:14px;height:14px;border:2px solid var(--border);border-top-color:var(--accent);
      border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:6px}
    @keyframes spin{to{transform:rotate(360deg)}}
    footer{padding:8px 18px 14px;font-size:11px;color:var(--muted)}
  </style>
</head>
<body>
<div class="app">
  <header>
    <div class="brand">
      <div class="logo">NT</div>
      <div><h1>NerT AI PRO</h1><span>Quant Agent v5 · Bybit Spot · Qwen</span></div>
    </div>
    <div class="status-pills">
      <div class="pill" id="pill-level">L? …</div>
      <div class="pill" id="pill-llm">LLM …</div>
      <div class="pill" id="pill-bot">Bot …</div>
      <div class="pill" id="pill-ml">ML …</div>
    </div>
  </header>
  <main>
    <section class="panel">
      <div class="panel-h"><span>Agent Console</span><span style="display:flex;gap:10px;align-items:center;font-weight:400;text-transform:none">
        <span id="history-tag" style="font-size:11px;color:var(--muted)"></span>
        <button type="button" onclick="loadChatHistory(true)" style="padding:4px 8px;font-size:10px;border-radius:6px">Historial</button>
        <span id="session-tag">—</span></span></div>
      <div class="chat-feed" id="feed">
        <div class="bubble system">Agente listo. Pide análisis de mercado, optimización o diagnóstico del sistema.</div>
      </div>
      <div class="composer">
        <div class="controls">
          <input id="symbol" value="BTCUSDT" placeholder="Symbol"/>
          <input id="limit" value="2000" type="number" title="Limit trades"/>
          <input id="iters" value="900" type="number" title="Iterations"/>
          <label class="chk"><input id="apply" type="checkbox"/> Aplicar</label>
        </div>
        <textarea id="msg" placeholder="Ej: analiza el estado completo del sistema para BTCUSDT">analiza el estado completo del sistema</textarea>
        <div class="btn-row">
          <button class="primary" id="btn-send" onclick="send()">Enviar</button>
        <button onclick="status()">Status ML</button>
        <button onclick="validate()">Validar</button>
        <button onclick="optimize()">Optimizar</button>
        <button onclick="refreshMarket()">Ticker</button>
        </div>
      </div>
    </section>
    <aside class="panel side">
      <div class="panel-h">Market Live</div>
      <div class="card" id="market-card">
        <div class="metric"><span class="k">Symbol</span><span class="v" id="m-sym">—</span></div>
        <div class="metric"><span class="k">Last</span><span class="v" id="m-last">—</span></div>
        <div class="metric"><span class="k">24h %</span><span class="v" id="m-pct">—</span></div>
        <div class="metric"><span class="k">Bid / Ask</span><span class="v" id="m-ba">—</span></div>
        <div class="metric"><span class="k">Volume 24h</span><span class="v" id="m-vol">—</span></div>
        <div class="metric"><span class="k">Funding</span><span class="v" id="m-fund">—</span></div>
      </div>
      <div class="panel-h">Nivel de Predicción</div>
      <div class="card" id="pred-card">
        <div class="level-badge L0" id="pred-badge">L0 — …</div>
        <div class="conf-bar"><div class="conf-fill" id="pred-conf" style="width:0%"></div></div>
        <div class="metric" style="margin-top:10px"><span class="k">Confianza</span><span class="v" id="pred-pct">—</span></div>
        <div class="metric"><span class="k">Acción</span><span class="v" id="pred-action" style="font-size:11px;max-width:180px;text-align:right">—</span></div>
        <div style="margin-top:8px"><span class="k" style="font-size:10px;color:var(--muted)">Perfiles</span>
          <div id="pred-profiles"></div></div>
      </div>
      <div class="panel-h">Señal Live (motor)</div>
      <div class="card" id="signal-card">
        <div class="metric"><span class="k">Decision</span><span class="v" id="sig-dec">—</span></div>
        <div class="metric"><span class="k">Combined</span><span class="v" id="sig-comb">—</span></div>
        <div class="metric"><span class="k">Mom / TFI</span><span class="v" id="sig-mom">—</span></div>
        <div class="metric"><span class="k">EGM / PIO</span><span class="v" id="sig-egm">—</span></div>
        <div class="metric"><span class="k">Motor</span><span class="v" id="sig-bot">—</span></div>
        <div class="metric"><span class="k">Bloqueo</span><span class="v" id="sig-block" style="font-size:11px">—</span></div>
      </div>
      <div class="panel-h">System</div>
      <div class="card" id="sys-card">
        <div class="metric"><span class="k">Backend</span><span class="v" id="s-backend">—</span></div>
        <div class="metric"><span class="k">Model</span><span class="v" id="s-model">—</span></div>
        <div class="metric"><span class="k">Buy TH</span><span class="v" id="s-buy">—</span></div>
        <div class="metric"><span class="k">Sell TH</span><span class="v" id="s-sell">—</span></div>
      </div>
    </aside>
  </main>
  <footer>API v5 · <a href="/project-docs/" target="_blank" rel="noopener" style="color:var(--accent)">Documentación</a> · <code>/agent/catalog</code> · <code>/docs</code></footer>
</div>
<script>
const feed = document.getElementById('feed');
let busy = false;

function fmtTs(ms) {
  if (!ms) return '';
  try {
    return new Date(ms).toLocaleString('es-MX', {hour:'2-digit',minute:'2-digit',day:'2-digit',month:'short'});
  } catch (_) { return ''; }
}

function addBubble(cls, html, tsMs) {
  const d = document.createElement('div');
  d.className = 'bubble ' + cls;
  const stamp = tsMs ? `<span class="ts">${fmtTs(tsMs)}</span>` : '';
  d.innerHTML = stamp + html;
  feed.appendChild(d);
  feed.scrollTop = feed.scrollHeight;
  return d;
}

async function loadChatHistory(force) {
  try {
    const r = await fetch('/agent/chat/history?limit=30');
    const data = await r.json();
    const turns = Array.isArray(data.turns) ? data.turns : [];
    const tag = document.getElementById('history-tag');
    if (!turns.length) {
      if (tag) tag.textContent = '';
      return;
    }
    if (force) feed.innerHTML = '';
    else if (feed.querySelector('[data-restored]')) return;
    feed.innerHTML = '';
    for (const t of turns) {
      const msg = (t.message || '').replace(/\\n/g, '<br/>');
      if (msg) addBubble('user', msg, t.ts_ms);
      addBubble('agent', renderResponse(t.response || {}), t.ts_ms);
      if (t.session_id) document.getElementById('session-tag').textContent = t.session_id.slice(0,8);
    }
    const restored = document.createElement('div');
    restored.className = 'bubble system';
    restored.setAttribute('data-restored', '1');
    restored.textContent = turns.length + ' interacción(es) restauradas · agent_memory.sqlite';
    feed.insertBefore(restored, feed.firstChild);
    if (tag) tag.textContent = turns.length + ' msgs';
  } catch (_) {}
}

function fmtAnalysis(text) {
  return '<h3>Análisis del Agente</h3>' + (text || '').replace(/\\n/g, '<br/>');
}

function renderPlanSteps(results) {
  if (!Array.isArray(results)) return '';
  return '<div class="steps">' + results.map(r => {
    const ok = !!r.ok;
    const tool = r.tool || '?';
    const detail = ok ? 'OK' : ((r.result && (r.result.message || r.result.error)) || 'FAIL');
    return `<div class="step ${ok?'ok':'fail'}">${ok?'✓':'✗'} ${tool} — ${detail}</div>`;
  }).join('') + '</div>';
}

function renderResponse(data) {
  let html = '';
  if (data.intent === 'react' && data.agent) {
    const ag = data.agent;
    if (ag.answer) html += fmtAnalysis(ag.answer);
    else html += '<h3>Agente ReAct</h3>Sin respuesta final.';
    if (Array.isArray(ag.trace) && ag.trace.length) {
      html += '<div class="steps">' + ag.trace.map(s => {
        if (s.type === 'final') return `<div class="step ok">✓ final — ${(s.thought||'').slice(0,80)}</div>`;
        const ok = !!(s.observation && s.observation.ok !== false);
        return `<div class="step ${ok?'ok':'fail'}">${ok?'✓':'✗'} ${s.tool||'?'} — ${(s.thought||'').slice(0,60)}</div>`;
      }).join('') + '</div>';
      html += `<div style="margin-top:8px;font-size:11px;color:var(--muted)">Modo ReAct · ${ag.steps_used||0} pasos · ${ag.tools_total||'?'} tools total · ${ag.bybit_env||'?'} · ${ag.backend||'llm'}</div>`;
    }
    const rawId = 'raw-' + Date.now();
    html += `<div class="raw-toggle" onclick="document.getElementById('${rawId}').style.display=document.getElementById('${rawId}').style.display==='block'?'none':'block'">Ver JSON completo</div>`;
    html += `<pre class="raw" id="${rawId}">${JSON.stringify(data, null, 2)}</pre>`;
    return html;
  }
  const syn = data.synthesis;
  if (syn && syn.analysis) {
    html += fmtAnalysis(syn.analysis);
  } else if (data.intent === 'status') {
    html += '<h3>Status</h3>' + JSON.stringify(data, null, 2);
  } else {
    const results = data.execution && data.execution.results;
    const llmStep = Array.isArray(results) && results.find(r => r.tool === 'llm_chat' && r.ok);
    if (llmStep) {
      const content = (((llmStep.result||{}).result||{}).content) || '';
      if (content) html += fmtAnalysis(content);
    }
    if (!html) html += '<h3>Orquestación completada</h3>Revisa pasos ejecutados abajo.';
    html += renderPlanSteps(results);
  }
  const rawId = 'raw-' + Date.now();
  html += `<div class="raw-toggle" onclick="document.getElementById('${rawId}').style.display=document.getElementById('${rawId}').style.display==='block'?'none':'block'">Ver JSON completo</div>`;
  html += `<pre class="raw" id="${rawId}">${JSON.stringify(data, null, 2)}</pre>`;
  return html;
}

async function send() {
  if (busy) return;
  const msg = document.getElementById('msg').value.trim();
  if (!msg) return;
  busy = true;
  document.getElementById('btn-send').disabled = true;
  addBubble('user', msg.replace(/\\n/g, '<br/>'));
  const loading = addBubble('agent', '<span class="loading"></span> Ejecutando plan autónomo…');
  try {
    const body = {
      message: msg,
      symbol: document.getElementById('symbol').value,
      limit: parseInt(document.getElementById('limit').value||'2000',10),
      iterations: parseInt(document.getElementById('iters').value||'900',10),
      apply: document.getElementById('apply').checked
    };
    const r = await fetch('/agent/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
    const data = await r.json();
    if (data.session_id) document.getElementById('session-tag').textContent = data.session_id.slice(0,8);
    loading.innerHTML = renderResponse(data);
    await refreshMarket();
  } catch (e) {
    loading.innerHTML = '<h3>Error</h3>' + String(e);
    loading.querySelector('h3') && (loading.className = 'bubble agent');
  } finally {
    busy = false;
    document.getElementById('btn-send').disabled = false;
  }
}

async function status() {
  addBubble('system', 'Consultando Status ML…');
  const r = await fetch('/api/ml/status');
  const data = await r.json();
  addBubble('agent', '<h3>ML Status</h3><pre class="raw" style="display:block;max-height:160px">'+JSON.stringify(data,null,2)+'</pre>');
}

async function validate() {
  const loading = addBubble('agent', '<span class="loading"></span> Validando cableado…');
  const r = await fetch('/agent/validate', {method:'POST'});
  const data = await r.json();
  const rows = (data.checks||[]).map(c => `<div class="step ${c.ok?'ok':'fail'}">${c.ok?'✓':'✗'} ${c.tool}</div>`).join('');
  loading.innerHTML = `<h3>Validación ${data.passed}/${data.total}</h3><div class="steps">${rows}</div><pre class="raw" style="display:block;max-height:120px">${JSON.stringify(data.bybit_env,null,2)}</pre>`;
}

async function optimize() {
  if (busy) return;
  busy = true;
  document.getElementById('btn-send').disabled = true;
  const loading = addBubble('agent', '<span class="loading"></span> Optimizando…');
  try {
    const body = {
      symbol: document.getElementById('symbol').value,
      limit: parseInt(document.getElementById('limit').value||'2000',10),
      iterations: parseInt(document.getElementById('iters').value||'900',10),
      apply: document.getElementById('apply').checked
    };
    const r = await fetch('/agent/optimize',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
    const data = await r.json();
    loading.innerHTML = '<h3>Optimización</h3><pre class="raw" style="display:block;max-height:200px">'+JSON.stringify(data,null,2)+'</pre>';
  } finally {
    busy = false;
    document.getElementById('btn-send').disabled = false;
  }
}

async function refreshMarket() {
  const sym = document.getElementById('symbol').value || 'BTCUSDT';
  try {
    const r = await fetch('/market/ticker/' + encodeURIComponent(sym));
    const d = await r.json();
    if (!d.ok) return;
    document.getElementById('m-sym').textContent = d.symbol || sym;
    document.getElementById('m-last').textContent = d.lastPrice || '—';
    const raw = d.raw || {};
    const pct = parseFloat(raw.price24hPcnt || 0) * 100;
    const pctEl = document.getElementById('m-pct');
    pctEl.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
    pctEl.className = 'v ' + (pct >= 0 ? 'up' : 'down');
    document.getElementById('m-ba').textContent = (d.bid1Price||'—') + ' / ' + (d.ask1Price||'—');
    document.getElementById('m-vol').textContent = d.volume24h || '—';
    document.getElementById('m-fund').textContent = raw.fundingRate != null ? (parseFloat(raw.fundingRate)*100).toFixed(4)+'%' : '—';
  } catch (_) {}
}

async function refreshSignals() {
  const sym = document.getElementById('symbol').value || 'BTCUSDT';
  try {
    const r = await fetch('/api/decisions/' + encodeURIComponent(sym));
    const d = await r.json();
    const det = d.decision_detail || {};
    const dec = (det.decision || '—').toUpperCase();
    const decEl = document.getElementById('sig-dec');
    decEl.textContent = dec;
    decEl.className = 'v ' + (dec === 'BUY' ? 'up' : dec === 'SELL' ? 'down' : '');
    document.getElementById('sig-comb').textContent = det.combined != null ? Number(det.combined).toFixed(3) : '—';
    document.getElementById('sig-mom').textContent = det.mom != null ? Number(det.mom).toFixed(4) : '—';
    const egm = det.egm != null ? Number(det.egm).toFixed(3) : '—';
    const pio = det.pio != null ? Number(det.pio).toFixed(3) : '—';
    document.getElementById('sig-egm').textContent = egm + ' / ' + pio;
    const gates = d.execution_gates || {};
    const blocked = gates.blocked_by || (Array.isArray(det.blockers_if_not_trading) && det.blockers_if_not_trading[0]) || '—';
    document.getElementById('sig-block').textContent = blocked;
    const live = await fetch('/agent/context?symbol=' + encodeURIComponent(sym)).then(x=>x.json()).catch(()=>({}));
    const st = live.bot_live_state || {};
    const motor = st.start_task_active ? 'LOOP ON' : (st.bot_running ? 'WS' : 'OFF');
    document.getElementById('sig-bot').textContent = motor;
    const pillBot = document.getElementById('pill-bot');
    pillBot.textContent = motor === 'LOOP ON' ? 'Motor ON' : (motor === 'WS' ? 'Motor WS' : 'Motor OFF');
    pillBot.className = 'pill ' + (motor === 'LOOP ON' ? 'ok' : motor === 'WS' ? 'warn' : '');
    const pred = live.prediction_level || {};
    const lvl = pred.level || 'L0';
    const badge = document.getElementById('pred-badge');
    badge.textContent = lvl + ' — ' + (pred.name || 'Sin señal');
    badge.className = 'level-badge ' + lvl;
    const conf = pred.confidence_pct || 0;
    document.getElementById('pred-pct').textContent = conf + '%';
    document.getElementById('pred-conf').style.width = conf + '%';
    document.getElementById('pred-action').textContent = (pred.action || '—').slice(0, 80);
    const profs = (pred.recommended_profiles || []).map(p => '<span class="profile-chip">'+p+'</span>').join('');
    document.getElementById('pred-profiles').innerHTML = profs || '<span class="profile-chip">—</span>';
    const pillLvl = document.getElementById('pill-level');
    pillLvl.textContent = lvl + ' ' + conf + '%';
    pillLvl.className = 'pill ' + (lvl === 'L4' || lvl === 'L3' ? 'ok' : lvl === 'L2' ? 'warn' : '');
    const tfi = det.tfi != null ? Number(det.tfi).toFixed(3) : '—';
    const mom = det.mom != null ? Number(det.mom).toFixed(4) : '—';
    document.getElementById('sig-mom').textContent = mom + ' / ' + tfi;
    const cfgTh = st.thresholds || {};
    const predTh = pred.thresholds || {};
    const buyTh = cfgTh.combined_buy ?? predTh.buy;
    const sellTh = cfgTh.combined_sell ?? predTh.sell;
    document.getElementById('s-buy').textContent = buyTh != null ? Number(buyTh).toFixed(2) : '—';
    document.getElementById('s-sell').textContent = sellTh != null ? Number(sellTh).toFixed(2) : '—';
  } catch (_) {}
}

async function refreshStatus() {
  try {
    const [llm, health] = await Promise.all([
      fetch('/agent/llm/status').then(r=>r.json()),
      fetch('/health').then(r=>r.json())
    ]);
    const pillLlm = document.getElementById('pill-llm');
    pillLlm.textContent = 'LLM ' + (llm.backend || '—');
    pillLlm.className = 'pill ' + (llm.ok ? 'ok' : 'warn');
    document.getElementById('s-backend').textContent = llm.backend || '—';
    document.getElementById('s-model').textContent = llm.model || '—';
    const pillBot = document.getElementById('pill-bot');
    pillBot.textContent = health.ok ? 'API Online' : 'API Down';
    pillBot.className = 'pill ' + (health.ok ? 'ok' : 'warn');
  } catch (_) {}
  try {
    const r = await fetch('/api/ml/status');
    const ml = await r.json();
    const pill = document.getElementById('pill-ml');
    const enabled = !!(ml.enabled || ml.ml_enabled);
    pill.textContent = 'ML ' + (enabled ? 'ON' : 'OFF');
    pill.className = 'pill ' + (enabled ? 'ok' : '');
  } catch (_) {}
}

document.getElementById('msg').addEventListener('keydown', e => {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) send();
});

loadChatHistory(false);
refreshStatus();
refreshMarket();
refreshSignals();
setInterval(refreshMarket, 15000);
setInterval(refreshSignals, 5000);
setInterval(refreshStatus, 30000);
</script>
</body>
</html>"""


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    symbol: Optional[str] = None
    limit: int = Field(default=2000, ge=50, le=200000)
    iterations: int = Field(default=900, ge=50, le=50000)
    seed: Optional[int] = None
    apply: bool = False
    use_react: bool = True
    max_steps: int = Field(default=8, ge=1, le=20)
    include_mcp: bool = True


class OptimizeRequest(BaseModel):
    symbol: Optional[str] = None
    limit: int = Field(default=2000, ge=50, le=200000)
    iterations: int = Field(default=900, ge=50, le=50000)
    seed: Optional[int] = None
    apply: bool = False


class TrainMLRequest(BaseModel):
    symbol: Optional[str] = None
    min_samples: Optional[int] = Field(default=None, ge=10, le=50000)


class ApiAutogenRequest(BaseModel):
    goal: str = Field(min_length=1, max_length=2000)


class RepairStrategyRequest(BaseModel):
    symbol: Optional[str] = None
    limit: int = Field(default=5000, ge=50, le=200000)
    iterations: int = Field(default=1200, ge=50, le=50000)
    seed: Optional[int] = None
    apply: bool = False
    strategy: Dict[str, Any] = Field(default_factory=dict)


class LLMChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=12000)
    system: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class AutoEvolveRequest(BaseModel):
    symbol: Optional[str] = None
    limit: int = Field(default=2000, ge=50, le=200000)
    iterations: int = Field(default=900, ge=50, le=50000)
    rounds: int = Field(default=2, ge=1, le=10)
    seed: Optional[int] = None
    apply: bool = False
    use_llm: bool = True


class AgentPlanRequest(BaseModel):
    goal: str = Field(min_length=1, max_length=12000)
    symbol: Optional[str] = None
    limit: int = Field(default=2000, ge=50, le=200000)
    iterations: int = Field(default=900, ge=50, le=50000)
    rounds: int = Field(default=2, ge=1, le=10)
    seed: Optional[int] = None
    apply: bool = False
    use_llm: bool = True
    session_id: Optional[str] = None


class AgentExecuteRequest(BaseModel):
    plan: list[Dict[str, Any]] = Field(default_factory=list)
    session_id: Optional[str] = None
    stop_on_error: bool = True


_monitor_task: Optional[asyncio.Task] = None


async def _monitor_loop() -> None:
    while True:
        try:
            bot = nertzh.bot
            start_task = bot.start_task
            support_task = bot.support_task
            ws = getattr(bot, "ws", None)
            ws_open = bool(ws is not None and not getattr(ws, "closed", False))
            payload = {
                "running": bool(bot.running),
                "start_task_running": bool(
                    start_task is not None and not start_task.done()
                ),
                "support_task_running": bool(
                    support_task is not None and not support_task.done()
                ),
                "websocket_open": bool(ws_open),
                "symbols": list(getattr(bot, "symbols", []) or []),
                "mode": str(getattr(bot, "mode", "") or ""),
                "timestamp_ms": int(time.time() * 1000),
            }
            nertzh.logger.info(f"MONITOR_5M {json.dumps(payload, ensure_ascii=False)}")
        except Exception as e:
            try:
                nertzh.logger.error(f"MONITOR_5M_ERROR {str(e)}")
            except Exception:
                pass
        await asyncio.sleep(300)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _monitor_task, _http_session

    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()

    preflight: Dict[str, Any]
    try:
        maybe_preflight = nertzh.bot.preflight()
        preflight = (
            await maybe_preflight
            if inspect.isawaitable(maybe_preflight)
            else maybe_preflight
        )
    except Exception as e:
        preflight = {"success": False, "message": str(e)}

    if bool(preflight.get("success")):
        # FIX #4: Restaurar thresholds calibrados desde DB al arrancar.
        # Sin esto, cada reinicio vuelve al .env ignorando el historial
        # acumulado por el optimizador y el agent_tick.
        try:
            with nertzh.SessionLocal() as _db:
                last_th = (
                    _db.query(nertzh.ThresholdSnapshot)
                    .order_by(nertzh.ThresholdSnapshot.timestamp.desc())
                    .first()
                )
                if last_th is not None:
                    nertzh.config.COMBINED_BUY_THRESHOLD = float(
                        last_th.combined_buy_threshold
                    )
                    nertzh.config.COMBINED_SELL_THRESHOLD = float(
                        last_th.combined_sell_threshold
                    )
                    nertzh.logger.info(
                        f"Thresholds restaurados desde DB -> "
                        f"buy={last_th.combined_buy_threshold:.4f}  "
                        f"sell={last_th.combined_sell_threshold:.4f}"
                    )
                    # Restaurar pesos si fueron guardados en stats
                    stats = last_th.stats if isinstance(last_th.stats, dict) else {}
                    cw = stats.get("combined_weights")
                    if isinstance(cw, dict):
                        for sym in nertzh.bot.symbols:
                            nertzh.bot.ticker_data.setdefault(sym, {})["combined_weights"] = dict(cw)
                        nertzh.logger.info("Pesos combinados restaurados desde DB.")
                else:
                    nertzh.logger.info(
                        "Sin historial de thresholds en DB, usando valores del .env"
                    )
        except Exception as _e:
            nertzh.logger.warning(f"No se pudo restaurar thresholds desde DB: {_e}")

        _llm_cfg = _llm_config()
        llm_auth = "SET" if _llm_cfg.api_key else "NOT SET"
        if _llm_cfg.backend in {
            "qwen_desktop",
            "qwen-desktop",
            "qwen_studio",
            "qwen-studio",
        }:
            from qwen_desktop import read_desktop_jwt  # noqa: WPS433

            if read_desktop_jwt():
                llm_auth = "JWT session (Firefox/Desktop)"
        nertzh.logger.info(
            f"LLM backend='{_llm_cfg.backend}'  model='{_llm_cfg.model}'  "
            f"base_url='{_llm_cfg.base_url}'  auth={llm_auth}"
        )
        if _llm_cfg.backend in {"off", "none", "disabled"}:
            nertzh.logger.warning(
                "⚠️ Agente INCOMPLETO: LLM deshabilitado — caerá en modo plan heurístico (no ReAct pleno)."
            )
        else:
            nertzh.logger.info("✅ Agente ReAct anti-fable habilitado (tools obligatorias para datos live).")

        await nertzh.bot.start_storage()
        nertzh.bot.schedule_start()
        nertzh.bot.start_support_loop(interval_s=nertzh.bot.support_interval_s)
        if _monitor_task is None or _monitor_task.done():
            _monitor_task = asyncio.create_task(_monitor_loop())

    else:
        try:
            nertzh.logger.error(
                f"❌ Preflight falló en NerT_AI_PRO startup: {preflight.get('message') or 'error'}"
            )
        except Exception:
            pass

    try:
        yield
    finally:
        t = _monitor_task
        _monitor_task = None
        if t is not None and not t.done():
            try:
                t.cancel()
                await t
            except Exception:
                pass

        s = _http_session
        _http_session = None
        if s is not None and not s.closed:
            try:
                await s.close()
            except Exception:
                pass
        try:
            await nertzh.bot.stop_storage()
        except Exception:
            pass
        try:
            maybe_stop = nertzh.bot.stop()
            if inspect.isawaitable(maybe_stop):
                await maybe_stop
        except Exception:
            pass


app = FastAPI(title="NerT AI PRO", version="1.0.0", lifespan=lifespan)
app.mount("/api", nertzh.app)
_DOCS_STATIC = os.path.join(BASE_DIR, "docs")
if os.path.isdir(_DOCS_STATIC):
    app.mount(
        "/project-docs",
        StaticFiles(directory=_DOCS_STATIC, html=True),
        name="project-docs",
    )


ALLOWED_TOOLS = {
    "market_ticker",
    "market_orderbook",
    "optimize",
    "train_ml",
    "autoevolve",
    "llm_chat",
}


def _new_session_id() -> str:
    return uuid.uuid4().hex


def _normalize_tool_args(tool: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza aliases que el LLM planner suele inventar (prompt→message, limit→depth)."""
    out = dict(args)
    if tool == "llm_chat":
        msg = str(out.get("message") or out.get("prompt") or out.get("query") or "").strip()
        if not msg and isinstance(out.get("content"), str):
            msg = str(out.get("content") or "").strip()
        if msg:
            out["message"] = msg
        out.pop("prompt", None)
        out.pop("query", None)
    elif tool == "market_orderbook":
        depth = out.get("depth")
        if depth is None and out.get("limit") is not None:
            depth = out.get("limit")
        try:
            d = int(depth) if depth is not None else 50
        except Exception:
            d = 50
        out["depth"] = max(1, min(200, d))
        out.pop("limit", None)
    elif tool == "market_ticker":
        sym = str(out.get("symbol") or out.get("pair") or "").strip().upper()
        if sym:
            out["symbol"] = sym
    return out


def _normalize_plan_step(step: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(step, dict):
        return None
    tool = step.get("tool")
    args = step.get("args")
    if not isinstance(tool, str):
        return None
    t = str(tool).strip()
    if t not in ALLOWED_TOOLS:
        return None
    if not isinstance(args, dict):
        args = {}
    return {"tool": t, "args": _normalize_tool_args(t, dict(args))}


def _is_analysis_goal(goal: str) -> bool:
    g = str(goal or "").lower()
    keys = (
        "analiz",
        "estado",
        "sistema",
        "diagn",
        "revis",
        "reporte",
        "resumen",
        "overview",
        "status",
        "health",
    )
    return any(k in g for k in keys)


def _orderbook_summary(bids: list, asks: list) -> Dict[str, Any]:
    def _sum_side(levels: list) -> float:
        total = 0.0
        for row in levels or []:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                total += _safe_float(row[1], 0.0)
        return float(total)

    bid_vol = _sum_side(bids)
    ask_vol = _sum_side(asks)
    denom = bid_vol + ask_vol
    imbalance = (bid_vol - ask_vol) / denom if denom > 0 else 0.0
    return {
        "bid_volume_top": round(bid_vol, 4),
        "ask_volume_top": round(ask_vol, 4),
        "imbalance": round(imbalance, 4),
        "spread_hint": "bid_heavy" if imbalance > 0.15 else ("ask_heavy" if imbalance < -0.15 else "balanced"),
    }


def _collect_plan_context(results: list[Dict[str, Any]]) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"steps": []}
    for r in results or []:
        if not isinstance(r, dict):
            continue
        tool = str(r.get("tool") or "")
        res = r.get("result") if isinstance(r.get("result"), dict) else {}
        step_ctx: Dict[str, Any] = {"tool": tool, "ok": bool(r.get("ok"))}
        if tool == "market_ticker" and bool(res.get("ok")):
            step_ctx["ticker"] = {
                "symbol": res.get("symbol"),
                "lastPrice": res.get("lastPrice"),
                "bid1Price": res.get("bid1Price"),
                "ask1Price": res.get("ask1Price"),
                "highPrice24h": res.get("highPrice24h"),
                "lowPrice24h": res.get("lowPrice24h"),
                "volume24h": res.get("volume24h"),
            }
            raw = res.get("raw") if isinstance(res.get("raw"), dict) else {}
            if raw:
                step_ctx["ticker"]["fundingRate"] = raw.get("fundingRate")
                step_ctx["ticker"]["openInterest"] = raw.get("openInterest")
                step_ctx["ticker"]["price24hPcnt"] = raw.get("price24hPcnt")
        elif tool == "market_orderbook" and bool(res.get("ok")):
            bids = res.get("bids") if isinstance(res.get("bids"), list) else []
            asks = res.get("asks") if isinstance(res.get("asks"), list) else []
            step_ctx["orderbook"] = {
                "symbol": res.get("symbol"),
                "depth": res.get("depth"),
                "levels_bids": len(bids),
                "levels_asks": len(asks),
                **_orderbook_summary(bids, asks),
            }
        elif tool == "optimize":
            step_ctx["optimize"] = {
                "success": res.get("success"),
                "before": res.get("before"),
                "best": (res.get("result") or {}).get("best") if isinstance(res.get("result"), dict) else res.get("best"),
            }
        elif tool == "llm_chat" and bool(res.get("ok")):
            inner = res.get("result") if isinstance(res.get("result"), dict) else {}
            content = inner.get("content")
            if isinstance(content, str) and content.strip():
                step_ctx["llm_reply"] = content.strip()[:4000]
        ctx["steps"].append(step_ctx)
    return ctx


async def _auto_synthesize_analysis(
    *,
    goal: str,
    symbol: Optional[str],
    results: list[Dict[str, Any]],
    session_id: str,
) -> Dict[str, Any]:
    """Síntesis autónoma cuando el plan falla o el usuario pide análisis del sistema."""
    ctx = _collect_plan_context(results)
    ctx["goal"] = goal
    ctx["symbol"] = symbol
    try:
        cfg = _llm_config()
        bot = nertzh.bot
        ctx["system"] = {
            "running": bool(getattr(bot, "running", False)),
            "symbols": list(getattr(bot, "symbols", []) or []),
            "mode": str(getattr(bot, "mode", "") or ""),
            "ml_enabled": bool(getattr(nertzh.config, "ML_ENABLED", False)),
            "thresholds": {
                "buy": float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0)),
                "sell": float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0)),
                "hold_band": float(getattr(nertzh.config, "COMBINED_HOLD_BAND", 2.0)),
            },
        }
    except Exception as e:
        ctx["system"] = {"error": str(e)}

    system = (
        "Eres NerT AI PRO, agente cuantitativo autónomo. "
        "Analiza el estado del sistema y del mercado con los datos provistos. "
        "Responde en español, estructurado con secciones claras: "
        "Mercado, Orderbook, Motor/ML, Riesgos, Recomendaciones. "
        "Sé conciso pero accionable. No inventes datos que no estén en el contexto."
    )
    user = (
        f"Objetivo del usuario: {goal}\n\n"
        f"Contexto recopilado:\n{json.dumps(ctx, ensure_ascii=False, indent=2)}\n\n"
        "Genera el análisis completo del estado del sistema."
    )
    res = await llm_chat(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
    )
    out = {
        "ok": bool(res.get("ok")),
        "synthesizer": "auto",
        "backend": res.get("backend"),
        "model": res.get("model"),
        "analysis": res.get("content") if bool(res.get("ok")) else None,
        "error": res.get("error") if not bool(res.get("ok")) else None,
        "context_summary": ctx,
    }
    agent_memory.add_event("synthesis", {"session_id": session_id, "out": out})
    return out


def _heuristic_plan(req: AgentPlanRequest) -> Dict[str, Any]:
    msg = str(req.goal or "").lower()
    plan: list[Dict[str, Any]] = []
    sym = str(req.symbol or "").strip() or None
    sym_default = sym or "BTCUSDT"
    wants_analysis = _is_analysis_goal(req.goal)
    if wants_analysis:
        plan.append({"tool": "bot_live_state", "args": {"symbol": sym_default}})
        plan.append({"tool": "nertzh_api.decisions", "args": {"symbol": sym_default}})
    if wants_analysis or ("ticker" in msg) or ("precio" in msg) or ("price" in msg) or ("mercado" in msg):
        plan.append({"tool": "market_ticker", "args": {"symbol": sym_default}})
    if wants_analysis or ("orderbook" in msg) or ("libro" in msg) or ("profund" in msg):
        plan.append(
            {
                "tool": "market_orderbook",
                "args": {"symbol": sym_default, "depth": 50},
            }
        )
    if ("entren" in msg or "train" in msg) and ("ml" in msg or "modelo" in msg):
        plan.append({"tool": "train_ml", "args": {"symbol": sym, "min_samples": None}})
    if "autoevol" in msg or "evoluc" in msg or "quant" in msg or "cuant" in msg:
        plan.append(
            {
                "tool": "autoevolve",
                "args": {
                    "symbol": sym,
                    "limit": int(req.limit),
                    "iterations": int(req.iterations),
                    "rounds": int(req.rounds),
                    "seed": req.seed,
                    "apply": bool(req.apply),
                    "use_llm": bool(req.use_llm),
                },
            }
        )
    if ("optimiz" in msg) or ("mejor" in msg) or ("arregl" in msg) or ("correg" in msg):
        plan.append(
            {
                "tool": "optimize",
                "args": {
                    "symbol": sym,
                    "limit": int(req.limit),
                    "iterations": int(req.iterations),
                    "seed": req.seed,
                    "apply": bool(req.apply),
                },
            }
        )
    if wants_analysis and plan:
        plan.append(
            {
                "tool": "llm_chat",
                "args": {
                    "message": (
                        f"Analiza el estado completo del sistema para {sym_default} "
                        "usando los datos de ticker y orderbook recopilados en el contexto."
                    ),
                },
            }
        )
    elif not plan:
        plan.append({"tool": "llm_chat", "args": {"message": req.goal}})
    return {"ok": True, "planner": "heuristic", "plan": plan}


async def _llm_plan(req: AgentPlanRequest) -> Dict[str, Any]:
    cfg = _llm_config()
    if cfg.backend in {"off", "none", "disabled"}:
        return {
            "ok": False,
            "error": "llm_disabled",
            "backend": cfg.backend,
            "model": cfg.model,
        }
    system = (
        "Eres un planificador de agente cuantitativo. Devuelve SOLO JSON válido sin markdown. "
        'Salida: {"plan":[{"tool":"...","args":{...}}]}. '
        f"tools permitidas: {sorted(ALLOWED_TOOLS)}. "
        "Esquemas EXACTOS de args (no uses otras claves): "
        'market_ticker: {"symbol":"BTCUSDT"}; '
        'market_orderbook: {"symbol":"BTCUSDT","depth":50} (depth 1-200, NO uses "limit"); '
        'optimize: {"symbol":null,"limit":2000,"iterations":900,"seed":null,"apply":false}; '
        'train_ml: {"symbol":null,"min_samples":null}; '
        'autoevolve: {"symbol":null,"limit":2000,"iterations":900,"rounds":2,"seed":null,"apply":false,"use_llm":true}; '
        'llm_chat: {"message":"texto del análisis o pregunta"} (usa "message", NUNCA "prompt"). '
        "Para análisis de mercado/sistema: ticker + orderbook + llm_chat con message descriptivo. "
        "Reglas: tool debe ser una de las permitidas. args debe ser objeto. No inventes claves fuera de plan."
    )
    ctx = {
        "goal": req.goal,
        "symbol": req.symbol,
        "limit": int(req.limit),
        "iterations": int(req.iterations),
        "rounds": int(req.rounds),
        "seed": req.seed,
        "apply": bool(req.apply),
    }
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(ctx, ensure_ascii=False)},
        {"role": "user", "content": "Genera un plan mínimo y ejecutable."},
    ]
    res = await llm_chat(messages)
    if not bool(res.get("ok")):
        return {
            "ok": False,
            "error": res.get("error"),
            "backend": res.get("backend"),
            "model": res.get("model"),
            "raw": res,
        }
    j = _extract_first_json_object(str(res.get("content") or ""))
    if not isinstance(j, dict):
        return {
            "ok": False,
            "error": "invalid_json",
            "backend": res.get("backend"),
            "model": res.get("model"),
            "content": res.get("content"),
        }
    steps = j.get("plan")
    if not isinstance(steps, list):
        return {
            "ok": False,
            "error": "missing_plan",
            "backend": res.get("backend"),
            "model": res.get("model"),
            "raw": j,
        }
    norm: list[Dict[str, Any]] = []
    for s in steps:
        ns = _normalize_plan_step(s)
        if ns is None:
            continue
        norm.append(ns)
    if not norm:
        return {
            "ok": False,
            "error": "empty_plan",
            "backend": res.get("backend"),
            "model": res.get("model"),
            "raw": j,
        }
    return {
        "ok": True,
        "planner": "llm",
        "backend": res.get("backend"),
        "model": res.get("model"),
        "plan": norm,
    }


async def _run_plan(
    plan: list[Dict[str, Any]], *, session_id: str, stop_on_error: bool
) -> Dict[str, Any]:
    results: list[Dict[str, Any]] = []
    for idx, step in enumerate(plan):
        s = _normalize_plan_step(step)
        if s is None:
            r = {
                "ok": False,
                "error": "invalid_step",
                "step_index": int(idx),
                "step": step,
            }
            results.append(r)
            if bool(stop_on_error):
                break
            continue
        tool = s["tool"]
        args = _normalize_tool_args(tool, s["args"])
        out: Dict[str, Any]
        try:
            if tool == "market_ticker":
                out = await _get_public_ticker(str(args.get("symbol") or "BTCUSDT"))
            elif tool == "market_orderbook":
                out = await _get_public_orderbook(
                    str(args.get("symbol") or "BTCUSDT"),
                    depth=int(args.get("depth") or 50),
                )
            elif tool == "train_ml":
                req2 = TrainMLRequest(
                    symbol=(
                        args.get("symbol")
                        if isinstance(args.get("symbol"), str)
                        else None
                    ),
                    min_samples=args.get("min_samples"),
                )
                out = await agent_train_ml(req2)
            elif tool == "optimize":
                req2 = OptimizeRequest(
                    symbol=(
                        args.get("symbol")
                        if isinstance(args.get("symbol"), str)
                        else None
                    ),
                    limit=int(args.get("limit") or 2000),
                    iterations=int(args.get("iterations") or 900),
                    seed=(
                        args.get("seed") if isinstance(args.get("seed"), int) else None
                    ),
                    apply=bool(args.get("apply")),
                )
                out = await agent_optimize(req2)
            elif tool == "autoevolve":
                req2 = AutoEvolveRequest(
                    symbol=(
                        args.get("symbol")
                        if isinstance(args.get("symbol"), str)
                        else None
                    ),
                    limit=int(args.get("limit") or 2000),
                    iterations=int(args.get("iterations") or 900),
                    rounds=int(args.get("rounds") or 2),
                    seed=(
                        args.get("seed") if isinstance(args.get("seed"), int) else None
                    ),
                    apply=bool(args.get("apply")),
                    use_llm=bool(args.get("use_llm", True)),
                )
                out = await agent_autoevolve(req2)
            elif tool == "llm_chat":
                msg = str(args.get("message") or "").strip()
                sys_prompt = args.get("system")
                ctx = args.get("context")
                if not isinstance(ctx, dict) or not ctx:
                    ctx = _collect_plan_context(results)
                if not msg:
                    msg = (
                        "Analiza el estado del sistema y del mercado usando el contexto "
                        "de los pasos anteriores. Responde en español con recomendaciones."
                    )
                req2 = LLMChatRequest(
                    message=msg,
                    system=(str(sys_prompt) if isinstance(sys_prompt, str) else None),
                    context=(ctx if isinstance(ctx, dict) else {}),
                )
                out = await agent_llm_chat(req2)
            else:
                out = {"ok": False, "error": "unknown_tool", "tool": tool}
        except Exception as e:
            out = {"ok": False, "error": "exception", "tool": tool, "message": str(e)}
        step_result = {
            "ok": bool(out.get("ok", True)),
            "tool": tool,
            "args": args,
            "result": out,
        }
        results.append(step_result)
        agent_memory.add_event(
            "plan_step",
            {
                "session_id": session_id,
                "i": int(idx),
                "tool": tool,
                "args": args,
                "out": out,
            },
        )
        if not bool(step_result["ok"]) and bool(stop_on_error):
            break
    return {
        "ok": True,
        "session_id": session_id,
        "results": results,
        "timestamp": int(time.time() * 1000),
    }

@app.post("/agent/session/new")
async def agent_session_new():
    sid = _new_session_id()
    agent_memory.add_event("session_new", {"session_id": sid})
    return {"ok": True, "session_id": sid, "timestamp": int(time.time() * 1000)}


@app.post("/agent/plan")
async def agent_plan(req: AgentPlanRequest):
    sid = (
        str(req.session_id).strip()
        if isinstance(req.session_id, str) and str(req.session_id).strip()
        else _new_session_id()
    )
    planner = (
        await _llm_plan(req)
        if bool(req.use_llm)
        else {"ok": False, "error": "llm_disabled_by_request"}
    )
    if not bool(planner.get("ok")):
        planner = _heuristic_plan(req)
    out = {
        "ok": True,
        "session_id": sid,
        "planner": planner,
        "timestamp": int(time.time() * 1000),
    }
    agent_memory.add_event(
        "plan", {"session_id": sid, "req": req.model_dump(), "planner": planner}
    )
    return out


@app.post("/agent/execute")
async def agent_execute(req: AgentExecuteRequest):
    sid = (
        str(req.session_id).strip()
        if isinstance(req.session_id, str) and str(req.session_id).strip()
        else _new_session_id()
    )
    plan: list[Dict[str, Any]] = []
    for s in req.plan or []:
        ns = _normalize_plan_step(s)
        if ns is None:
            plan.append({"tool": "llm_chat", "args": {"message": "invalid_step"}})
        else:
            plan.append(ns)
    agent_memory.add_event(
        "execute_in",
        {"session_id": sid, "plan": plan, "stop_on_error": bool(req.stop_on_error)},
    )
    out = await _run_plan(plan, session_id=sid, stop_on_error=bool(req.stop_on_error))
    agent_memory.add_event("execute_out", {"session_id": sid, "out": out})
    return out


@app.get("/", response_class=HTMLResponse)
async def ui_chat():
    return HTMLResponse(_chat_html())


@app.get("/health")
async def health():
    return {"ok": True, "name": "NerT_AI_PRO", "timestamp": int(time.time() * 1000)}


@app.get("/agent/readiness")
async def agent_readiness():
    """Comprueba si el agente está completo (LLM + tools + motor) y no degradado."""
    cfg = _llm_config()
    llm_ok = cfg.backend not in {"off", "none", "disabled", ""}
    qwen_detail: Dict[str, Any] = {}
    if cfg.backend in {"qwen_desktop", "qwen-desktop", "qwen_studio", "qwen-studio"}:
        qwen_detail = await _qwen_desktop_status()
        llm_ok = llm_ok and bool(qwen_detail.get("session_found"))
    elif cfg.backend == "ollama":
        llm_ok = llm_ok and bool(cfg.base_url)
    elif cfg.backend in {"openai", "openai_compat", "openai-compatible", "openai_compatible"}:
        llm_ok = llm_ok and bool(cfg.api_key)

    stats = registry_stats()
    bot_running = bool(getattr(nertzh.bot, "running", False))
    executable_tools = int(stats.get("by_kind", {}).get("nertzh_api", 0) or 0) + int(
        stats.get("by_kind", {}).get("mcp_bybit", 0) or 0
    )

    blockers: List[str] = []
    if not llm_ok:
        blockers.append("llm_not_ready")
    if executable_tools < 5:
        blockers.append("few_executable_tools")
    if not bot_running:
        blockers.append("bot_not_running")

    return {
        "ok": len(blockers) == 0,
        "agent_complete": len(blockers) == 0,
        "anti_fable_mode": True,
        "llm": {
            "backend": cfg.backend,
            "model": _qwen_normalize_model(cfg.model)
            if cfg.backend in {"qwen_desktop", "qwen-desktop", "qwen_studio", "qwen-studio"}
            else cfg.model,
            "ready": llm_ok,
            "qwen_desktop": qwen_detail or None,
        },
        "motor": {"running": bot_running, "symbols": list(getattr(nertzh.bot, "symbols", []) or [])},
        "tools": stats,
        "blockers": blockers,
        "hint": (
            "Agente completo: LLM activo, bot corriendo, herramientas ejecutables. "
            "ReAct exige tools antes de conclusiones live (anti-fable)."
        ),
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/llm/status")
async def agent_llm_status():
    cfg = _llm_config()
    out: Dict[str, Any] = {
        "ok": True,
        "backend": cfg.backend,
        "base_url": cfg.base_url,
        "model": cfg.model,
        "temperature": cfg.temperature,
        "timeout_s": cfg.timeout_s,
        "has_api_key": bool(cfg.api_key),
        "timestamp": int(time.time() * 1000),
    }
    if cfg.backend in {"qwen_desktop", "qwen-desktop", "qwen_studio", "qwen-studio"}:
        out["model"] = _qwen_normalize_model(cfg.model)
        out["qwen_desktop"] = await qwen_desktop_status()
    return out


@app.post("/agent/llm/chat")
async def agent_llm_chat(req: LLMChatRequest):
    sys_prompt = str(req.system or "").strip()
    ctx = req.context if isinstance(req.context, dict) else {}
    messages: list[Dict[str, str]] = []
    if sys_prompt:
        messages.append({"role": "system", "content": sys_prompt})
    if ctx:
        messages.append(
            {
                "role": "user",
                "content": f"Context JSON:\n{json.dumps(ctx, ensure_ascii=False)}",
            }
        )
    messages.append({"role": "user", "content": str(req.message)})
    res = await llm_chat(messages)
    agent_memory.add_event(
        "llm_chat",
        {"ok": bool(res.get("ok")), "backend": res.get("backend"), "res": res},
    )
    return {
        "ok": bool(res.get("ok")),
        "backend": res.get("backend"),
        "result": res,
        "timestamp": int(time.time() * 1000),
    }


@app.get("/market/ticker/{symbol}")
async def market_ticker(symbol: str):
    return await _get_public_ticker(symbol)


@app.websocket("/ws/ticker/{symbol}")
async def ws_ticker(ws: WebSocket, symbol: str):
    await ws.accept()
    try:
        while True:
            payload = await _get_public_ticker(symbol)
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            return


@app.get("/market/orderbook/{symbol}")
async def market_orderbook(symbol: str, depth: int = 50):
    return await _get_public_orderbook(symbol, depth=depth)


@app.websocket("/ws/orderbook/{symbol}")
async def ws_orderbook(ws: WebSocket, symbol: str):
    await ws.accept()
    try:
        while True:
            payload = await _get_public_orderbook(symbol, depth=50)
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(2.0)
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            return


@app.post("/predict/{symbol}")
async def predict(symbol: str, payload: Dict[str, Any]):
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        metrics = payload if isinstance(payload, dict) else {}
    buy_p = (
        nertzh.bot.ml_predict_proba(symbol=str(symbol), action="buy", metrics=metrics)
        if bool(getattr(nertzh.config, "ML_ENABLED", False))
        else None
    )
    sell_p = (
        nertzh.bot.ml_predict_proba(symbol=str(symbol), action="sell", metrics=metrics)
        if bool(getattr(nertzh.config, "ML_ENABLED", False))
        else None
    )
    decision = (
        nertzh.bot._determine_decision(str(symbol), metrics)
        if hasattr(nertzh.bot, "_determine_decision")
        else "hold"
    )
    return {
        "symbol": str(symbol),
        "decision_raw": decision,
        "ml_enabled": bool(getattr(nertzh.config, "ML_ENABLED", False)),
        "p_buy": buy_p,
        "p_sell": sell_p,
        "timestamp": int(time.time() * 1000),
    }


def _load_final_trades(symbol: Optional[str], limit: int) -> list[Any]:
    with nertzh.SessionLocal() as db:
        q = db.query(nertzh.Trade).filter(nertzh.Trade.outcome_status == "final")
        if isinstance(symbol, str) and symbol:
            q = q.filter(nertzh.Trade.symbol == symbol)
        return q.order_by(nertzh.Trade.timestamp.desc()).limit(int(limit)).all()


def _apply_optimization(symbol: Optional[str], best: Dict[str, Any]) -> Dict[str, Any]:
    applied: Dict[str, Any] = {"thresholds": False, "weights": False, "persisted": None}
    th = best.get("thresholds")
    if isinstance(th, dict):
        try:
            nertzh.config.COMBINED_BUY_THRESHOLD = float(
                th.get("combined_buy_threshold")
                or getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0)
            )
            nertzh.config.COMBINED_SELL_THRESHOLD = float(
                th.get("combined_sell_threshold")
                or getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0)
            )
            nertzh.config.COMBINED_HOLD_BAND = float(
                th.get("combined_hold_band")
                or getattr(nertzh.config, "COMBINED_HOLD_BAND", 2.0)
            )
            applied["thresholds"] = True
        except Exception:
            applied["thresholds"] = False

    w = best.get("weights")
    if isinstance(w, dict):
        if isinstance(symbol, str) and symbol:
            nertzh.bot.ticker_data.setdefault(symbol, {})["combined_weights"] = dict(w)
        else:
            for sym in nertzh.bot.symbols:
                nertzh.bot.ticker_data.setdefault(sym, {})["combined_weights"] = dict(w)
        applied["weights"] = True

        # FIX #5: Persistir thresholds + pesos a DB para sobrevivir reinicios.
        # Al arrancar (lifespan), se restauran desde el ultimo ThresholdSnapshot.
        try:
            import datetime as _dt
            with nertzh.SessionLocal() as _db:
                # Solo guardar si hay algún cambio significativo en los thresholds o pesos
                last_snap = _db.query(nertzh.ThresholdSnapshot).order_by(
                    nertzh.ThresholdSnapshot.timestamp.desc()
                ).first()
                
                current_buy_th = float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0))
                current_sell_th = float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0))
                
                # Comparar con el último snapshot para evitar duplicados innecesarios
                should_save = True
                if last_snap is not None:
                    # Si ambos thresholds son prácticamente iguales, no guardar
                    th_buy_equal = abs(last_snap.combined_buy_threshold - current_buy_th) < 0.001
                    th_sell_equal = abs(last_snap.combined_sell_threshold - current_sell_th) < 0.001
                    
                    # También comprobar si los pesos son iguales
                    last_weights = last_snap.stats.get("combined_weights") if isinstance(last_snap.stats, dict) else None
                    current_weights = dict(w)
                    
                    weights_equal = last_weights == current_weights
                    
                    if th_buy_equal and th_sell_equal and weights_equal:
                        should_save = False
                
                if should_save:
                    _snap = nertzh.ThresholdSnapshot(
                        timestamp=_dt.datetime.now(_dt.timezone.utc),
                        egm_buy_threshold=float(
                            getattr(nertzh.config, "EGM_BUY_THRESHOLD", 0.02)
                        ),
                        egm_sell_threshold=float(
                            getattr(nertzh.config, "EGM_SELL_THRESHOLD", -0.02)
                        ),
                        combined_buy_threshold=current_buy_th,
                        combined_sell_threshold=current_sell_th,
                        stats={"combined_weights": current_weights, "source": "agent_optimize"},
                    )
                    _db.add(_snap)
                    _db.commit()
                    applied["weights_persisted_to_db"] = True
                else:
                    applied["weights_persisted_to_db"] = True  # Considerar como guardado porque no era necesario
        except Exception as _e:
            applied["weights_persisted_to_db"] = False
            applied["weights_persist_error"] = str(_e)

    if bool(getattr(nertzh.config, "PERSIST_THRESHOLDS_TO_ENV", False)):
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        applied["persisted"] = nertzh._persist_thresholds_to_env(str(env_path))

    return applied



def _parse_strategy_thresholds(strategy: Dict[str, Any]) -> Thresholds:
    th = strategy.get("thresholds")
    if not isinstance(th, dict):
        return _start_thresholds()
    return Thresholds(
        combined_buy_threshold=_safe_float(
            th.get("combined_buy_threshold"),
            float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0) or 8.0),
        ),
        combined_sell_threshold=_safe_float(
            th.get("combined_sell_threshold"),
            float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0) or -8.0),
        ),
        combined_hold_band=_safe_float(
            th.get("combined_hold_band"),
            float(getattr(nertzh.config, "COMBINED_HOLD_BAND", 2.0) or 2.0),
        ),
    )


def _parse_strategy_weights(strategy: Dict[str, Any]) -> CombinedWeights:
    w = strategy.get("weights")
    return CombinedWeights.from_dict(w if isinstance(w, dict) else None)


def _clamp_thresholds(th: Dict[str, Any]) -> Thresholds:
    buy = _safe_float(
        th.get("combined_buy_threshold"),
        float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0) or 8.0),
    )
    sell = _safe_float(
        th.get("combined_sell_threshold"),
        float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0) or -8.0),
    )
    hold = _safe_float(
        th.get("combined_hold_band"),
        float(getattr(nertzh.config, "COMBINED_HOLD_BAND", 2.0) or 2.0),
    )
    mag = float(max(1.0, min(15.0, (abs(buy) + abs(sell)) / 2.0)))
    hold = float(max(0.5, min(6.0, hold)))
    return Thresholds(mag, -mag, hold)


def _clamp_weights(w: Dict[str, Any]) -> CombinedWeights:
    return CombinedWeights.normalize(
        pio=_safe_float(w.get("pio"), DEFAULT_COMBINED_WEIGHTS.pio),
        egm=_safe_float(w.get("egm"), DEFAULT_COMBINED_WEIGHTS.egm),
        ild=_safe_float(w.get("ild"), DEFAULT_COMBINED_WEIGHTS.ild),
        rol=_safe_float(w.get("rol"), DEFAULT_COMBINED_WEIGHTS.rol),
        ogm=_safe_float(w.get("ogm"), DEFAULT_COMBINED_WEIGHTS.ogm),
        mom=_safe_float(w.get("mom"), DEFAULT_COMBINED_WEIGHTS.mom),
        tfi=_safe_float(w.get("tfi"), DEFAULT_COMBINED_WEIGHTS.tfi),
        scale=float(max(1.0, min(50.0, _safe_float(w.get("scale"), DEFAULT_COMBINED_WEIGHTS.scale)))),
    )


async def _llm_propose_strategy(
    *,
    symbol: Optional[str],
    baseline: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    cfg = _llm_config()
    if cfg.backend in {"off", "none", "disabled"}:
        return {"ok": False, "error": "disabled"}
    system = (
        "Eres un agente cuantitativo. Devuelve SOLO un JSON válido (sin markdown) "
        "con claves: thresholds, weights. thresholds={combined_buy_threshold,combined_sell_threshold,combined_hold_band}. "
        "weights={pio,egm,ild,rol,ogm,mom,tfi,scale}. No inventes claves."
    )
    ctx = {"symbol": symbol, "baseline": baseline, "current": current}
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(ctx, ensure_ascii=False)},
        {
            "role": "user",
            "content": "Propón una estrategia inicial mejorada para la próxima optimización.",
        },
    ]
    res = await llm_chat(messages)
    if not bool(res.get("ok")):
        return {
            "ok": False,
            "error": res.get("error"),
            "backend": res.get("backend"),
            "raw": res,
        }
    content = res.get("content")
    j = _extract_first_json_object(str(content or ""))
    if not isinstance(j, dict):
        return {
            "ok": False,
            "error": "invalid_json",
            "backend": res.get("backend"),
            "content": content,
        }
    th = j.get("thresholds")
    w = j.get("weights")
    out: Dict[str, Any] = {"ok": True, "backend": res.get("backend"), "raw": j}
    if isinstance(th, dict):
        out["thresholds"] = _clamp_thresholds(th).__dict__
    if isinstance(w, dict):
        out["weights"] = _clamp_weights(w).as_dict()
    return out


@app.post("/agent/optimize")
async def agent_optimize(req: OptimizeRequest):
    trades = _load_final_trades(req.symbol, req.limit)
    start_th = _start_thresholds()
    start_w = _weights_from_ticker_data(req.symbol)
    before = {
        "thresholds": {
            "combined_buy_threshold": float(start_th.combined_buy_threshold),
            "combined_sell_threshold": float(start_th.combined_sell_threshold),
            "combined_hold_band": float(start_th.combined_hold_band),
        },
        "weights": start_w.as_dict(),
        "trades_used": len(trades),
    }

    res = optimize_system_from_trades(
        trades,
        start_thresholds=start_th,
        start_weights=start_w,
        iterations=int(req.iterations),
        seed=req.seed,
    )
    applied = None
    if bool(req.apply) and bool(res.success) and isinstance(res.best, dict):
        applied = _apply_optimization(req.symbol, res.best)
    agent_memory.add_event(
        "optimize",
        {
            "symbol": req.symbol,
            "limit": int(req.limit),
            "iterations": int(req.iterations),
            "seed": req.seed,
            "apply": bool(req.apply),
            "trades_used": len(trades),
            "before": before,
            "baseline": res.baseline,
            "best": res.best,
            "applied": applied,
        },
    )
    return {
        "success": bool(res.success),
        "symbol": req.symbol,
        "before": before,
        "result": {
            "baseline": res.baseline,
            "best": res.best,
            "searched": res.searched,
            "timestamp": res.timestamp,
        },
        "applied": applied,
        "timestamp": int(time.time() * 1000),
    }


@app.post("/agent/train_ml")
async def agent_train_ml(req: TrainMLRequest):
    with nertzh.SessionLocal() as db:
        res = nertzh.bot.train_ml_model_from_trades(
            db, symbol=req.symbol, min_samples=req.min_samples
        )
    agent_memory.add_event(
        "train_ml",
        {
            "symbol": req.symbol,
            "min_samples": req.min_samples,
            "success": bool(res.get("success")),
            "result": res,
        },
    )
    return {
        "success": bool(res.get("success")),
        "result": res,
        "timestamp": int(time.time() * 1000),
    }


@app.post("/agent/api_autogen")
async def agent_api_autogen(req: ApiAutogenRequest):
    goal = str(req.goal or "").strip()
    return {
        "ok": True,
        "goal": goal,
        "suggested_endpoints": [
            {
                "method": "POST",
                "path": "/agent/chat",
                "use": "Chat con agente optimizador",
            },
            {
                "method": "POST",
                "path": "/agent/repair",
                "use": "Reparar y mejorar estrategia enviada",
            },
            {
                "method": "POST",
                "path": "/agent/optimize",
                "use": "Optimizar desde trades finales",
            },
            {
                "method": "POST",
                "path": "/agent/train_ml",
                "use": "Entrenar ML con trades finales",
            },
            {
                "method": "POST",
                "path": "/predict/{symbol}",
                "use": "Predicción con ML + decisión heurística",
            },
            {
                "method": "GET",
                "path": "/market/ticker/{symbol}",
                "use": "Ticker público tiempo real (REST)",
            },
            {
                "method": "WS",
                "path": "/ws/ticker/{symbol}",
                "use": "Ticker tiempo real (WebSocket)",
            },
            {
                "method": "GET",
                "path": "/api/openapi.json",
                "use": "OpenAPI completo del sistema base",
            },
        ],
        "curl_examples": [
            'curl -X POST http://127.0.0.1:8787/agent/optimize -H "content-type: application/json" -d "{\\"symbol\\":\\"BTCUSDT\\",\\"apply\\":false}"',
            'curl -X POST http://127.0.0.1:8787/agent/repair -H "content-type: application/json" -d "{\\"symbol\\":\\"BTCUSDT\\",\\"strategy\\":{\\"thresholds\\":{\\"combined_buy_threshold\\":8,\\"combined_sell_threshold\\":-8,\\"combined_hold_band\\":2},\\"weights\\":{\\"pio\\":0.45,\\"egm\\":0.3,\\"ild\\":-0.15,\\"rol\\":0.1,\\"ogm\\":0.05,\\"scale\\":10}}}"',
        ],
        "timestamp": int(time.time() * 1000),
    }


@app.post("/agent/repair")
async def agent_repair(req: RepairStrategyRequest):
    strategy = req.strategy if isinstance(req.strategy, dict) else {}
    trades = _load_final_trades(req.symbol, req.limit)
    start_th = _parse_strategy_thresholds(strategy)
    start_w = _parse_strategy_weights(strategy)
    res = optimize_system_from_trades(
        trades,
        start_thresholds=start_th,
        start_weights=start_w,
        iterations=int(req.iterations),
        seed=req.seed,
    )
    applied = None
    if bool(req.apply) and bool(res.success) and isinstance(res.best, dict):
        applied = _apply_optimization(req.symbol, res.best)
    agent_memory.add_event(
        "repair",
        {
            "symbol": req.symbol,
            "limit": int(req.limit),
            "iterations": int(req.iterations),
            "seed": req.seed,
            "apply": bool(req.apply),
            "trades_used": len(trades),
            "input_strategy": {
                "thresholds": {
                    "combined_buy_threshold": start_th.combined_buy_threshold,
                    "combined_sell_threshold": start_th.combined_sell_threshold,
                    "combined_hold_band": start_th.combined_hold_band,
                },
                "weights": start_w.as_dict(),
            },
            "baseline": res.baseline,
            "best": res.best,
            "applied": applied,
        },
    )
    return {
        "success": bool(res.success),
        "symbol": req.symbol,
        "trades_used": len(trades),
        "input_strategy": {
            "thresholds": {
                "combined_buy_threshold": start_th.combined_buy_threshold,
                "combined_sell_threshold": start_th.combined_sell_threshold,
                "combined_hold_band": start_th.combined_hold_band,
            },
            "weights": start_w.as_dict(),
        },
        "result": {
            "baseline": res.baseline,
            "best": res.best,
            "searched": res.searched,
            "timestamp": res.timestamp,
        },
        "applied": applied,
        "timestamp": int(time.time() * 1000),
    }


@app.post("/agent/autoevolve")
async def agent_autoevolve(req: AutoEvolveRequest):
    trades = _load_final_trades(req.symbol, req.limit)
    if not trades:
        return {"ok": False, "error": "no_trades", "timestamp": int(time.time() * 1000)}

    history: list[Dict[str, Any]] = []
    cur_th = _start_thresholds()
    cur_w = _weights_from_ticker_data(req.symbol)

    rounds = min(max(int(req.rounds), 1), 10)
    for r in range(rounds):
        baseline = _evaluate_baseline_for_autoevolve(trades, cur_th, cur_w)
        proposal = None
        if bool(req.use_llm):
            proposal = await _llm_propose_strategy(
                symbol=req.symbol,
                baseline=baseline,
                current={
                    "thresholds": {
                        "combined_buy_threshold": cur_th.combined_buy_threshold,
                        "combined_sell_threshold": cur_th.combined_sell_threshold,
                        "combined_hold_band": cur_th.combined_hold_band,
                    },
                    "weights": cur_w.as_dict(),
                },
            )
            if isinstance(proposal, dict) and bool(proposal.get("ok")):
                pth = proposal.get("thresholds")
                pw = proposal.get("weights")
                if isinstance(pth, dict):
                    cur_th = _clamp_thresholds(pth)
                if isinstance(pw, dict):
                    cur_w = _clamp_weights(pw)

        res = optimize_system_from_trades(
            trades,
            start_thresholds=cur_th,
            start_weights=cur_w,
            iterations=int(req.iterations),
            seed=req.seed,
        )
        reflection = _reflection_from_opt(res.baseline, res.best)
        applied = None
        if (
            bool(req.apply)
            and bool(reflection.get("recommend_apply"))
            and bool(res.success)
            and isinstance(res.best, dict)
        ):
            applied = _apply_optimization(req.symbol, res.best)
        step = {
            "round": int(r + 1),
            "proposal": proposal,
            "result": {
                "baseline": res.baseline,
                "best": res.best,
                "searched": res.searched,
                "timestamp": res.timestamp,
            },
            "reflection": reflection,
            "applied": applied,
        }
        history.append(step)
        agent_memory.add_event(
            "autoevolve_round",
            {"symbol": req.symbol, "round": int(r + 1), "step": step},
        )

        best_th = (
            res.best.get("thresholds") if isinstance(res.best, dict) else None
        ) or {}
        best_w = (res.best.get("weights") if isinstance(res.best, dict) else None) or {}
        if isinstance(best_th, dict) and best_th:
            cur_th = _clamp_thresholds(best_th)
        if isinstance(best_w, dict) and best_w:
            cur_w = _clamp_weights(best_w)

    return {
        "ok": True,
        "symbol": req.symbol,
        "trades_used": len(trades),
        "history": history,
        "timestamp": int(time.time() * 1000),
    }


def _evaluate_baseline_for_autoevolve(
    trades: list[Any], th: Thresholds, w: CombinedWeights
) -> Dict[str, Any]:
    """Replica exacta de la lógica de _determine_decision() del motor real.

    CORRECCIONES aplicadas vs versión original:
    1. Se incluye el componente `mom` (momentum) en el cálculo de combined,
       igual que utils.py: combined_z = combined_z_micro + w_mom * mom_z
    2. Se agrega compuerta ok_v2 (ema_diff_rel, igd_n5_n20, cbd_n20)
       para que el evaluador del optimizer replique la misma lógica OR
       que usa _determine_decision() en Nertzh.py L1366/1372.
    """
    selected = 0
    wins = 0
    losses = 0
    net_profit = 0.0
    total = 0
    for t in trades:
        total += 1
        action = str(getattr(t, "action", "") or "").lower()
        if action not in {"buy", "sell"}:
            continue
        pl = _safe_float(getattr(t, "profit_loss", 0.0), 0.0)
        metrics = getattr(t, "bybit_raw", None)
        snap = metrics.get("metrics_snapshot") if isinstance(metrics, dict) else None
        m = snap.get("metrics") if isinstance(snap, dict) else None
        md = m if isinstance(m, dict) else {}
        pio = _safe_float(md.get("pio"), _safe_float(getattr(t, "pio", 0.0), 0.0))
        egm = _safe_float(md.get("egm"), _safe_float(getattr(t, "egm", 0.0), 0.0))
        ild = _safe_float(md.get("ild"), _safe_float(getattr(t, "ild", 0.0), 0.0))
        rol = _safe_float(md.get("rol"), _safe_float(getattr(t, "rol", 0.0), 0.0))
        ogm = _safe_float(md.get("ogm"), _safe_float(getattr(t, "ogm", 0.0), 0.0))
        # FIX #2: incluir mom igual que utils.py (combined_z_micro + w_mom*mom_z)
        mom = _safe_float(md.get("mom"), 0.0)
        ema_diff_rel = _safe_float(md.get("ema_diff_rel"), 0.0)
        igd_n5_n20 = _safe_float(md.get("igd_n5_n20"), 0.0)
        cbd_n20 = _safe_float(md.get("cbd_n20"), 0.0)

        combined = float(w.scale) * (
            float(w.pio) * pio
            + float(w.egm) * egm
            + float(w.ild) * ild
            + float(w.rol) * rol
            + float(w.ogm) * ogm
            + float(w.mom) * mom  # FIX #2: mom ahora incluido
        )
        pred = "hold"
        if abs(float(combined)) >= float(th.combined_hold_band):
            # FIX #3: ok_v2 — réplica exacta de Nertzh.py L1365-1368
            if float(combined) >= float(th.combined_buy_threshold):
                ok_v2_buy = (
                    ema_diff_rel >= 0.0
                    and igd_n5_n20 >= 0.0
                    and cbd_n20 >= 0.0
                )
                if (pio > 0 and egm > 0) or ok_v2_buy:
                    pred = "buy"
            elif float(combined) <= float(th.combined_sell_threshold):
                ok_v2_sell = (
                    ema_diff_rel <= 0.0
                    and igd_n5_n20 <= 0.0
                    and cbd_n20 >= 0.0
                )
                if (pio < 0 and egm < 0) or ok_v2_sell:
                    pred = "sell"
        if pred != action:
            continue
        selected += 1
        net_profit += float(pl)
        if pl > 0:
            wins += 1
        elif pl < 0:
            losses += 1
    win_rate = float(wins) / float(selected) if selected > 0 else 0.0
    return {
        "total_trades": int(total),
        "selected": int(selected),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": float(win_rate),
        "net_profit": float(net_profit),
    }


@app.get("/agent/memory/stats")
async def agent_memory_stats():
    return {
        "ok": True,
        "db_path": MEMORY_DB_PATH,
        "stats": agent_memory.stats(),
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/memory/recent")
async def agent_memory_recent(limit: int = 50, kind: Optional[str] = None):
    return {
        "ok": True,
        "events": agent_memory.recent(limit=limit, kind=kind),
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/chat/history")
async def agent_chat_history(limit: int = 30):
    turns = agent_memory.chat_turns(limit=limit)
    return {
        "ok": True,
        "db_path": MEMORY_DB_PATH,
        "turns": turns,
        "count": len(turns),
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/analyze")
async def agent_analyze_trading_data(
    results_path: str = "logs/results.json",
    jsonl_path: str = "data/metrics_snapshots.jsonl",
):
    """Análisis streaming de results.json + metrics_snapshots.jsonl (archivos grandes)."""
    return analyze_trading_data(results_path=results_path, jsonl_path=jsonl_path)


@app.post("/agent/memory/clear")
async def agent_memory_clear():
    deleted = agent_memory.clear()
    return {"ok": True, "deleted": int(deleted), "timestamp": int(time.time() * 1000)}


def _reflection_from_opt(baseline: Any, best: Any) -> Dict[str, Any]:
    b = baseline if isinstance(baseline, dict) else {}
    x = best if isinstance(best, dict) else {}
    b_np = _safe_float(b.get("net_profit"), 0.0)
    x_np = _safe_float(x.get("net_profit"), 0.0)
    b_wr = _safe_float(b.get("win_rate"), 0.0)
    x_wr = _safe_float(x.get("win_rate"), 0.0)
    b_sel = (
        int(b.get("selected") or 0)
        if isinstance(b.get("selected"), (int, float, str))
        else 0
    )
    x_sel = (
        int(x.get("selected") or 0)
        if isinstance(x.get("selected"), (int, float, str))
        else 0
    )
    delta_np = x_np - b_np
    delta_wr = x_wr - b_wr
    return {
        "delta_net_profit": float(delta_np),
        "delta_win_rate": float(delta_wr),
        "baseline": {
            "net_profit": float(b_np),
            "win_rate": float(b_wr),
            "selected": int(b_sel),
        },
        "best": {
            "net_profit": float(x_np),
            "win_rate": float(x_wr),
            "selected": int(x_sel),
        },
        "recommend_apply": bool(
            delta_np > 0.0 and x_sel >= max(10, int(0.25 * max(1, b_sel)))
        ),
    }


async def _fetch_nertzh_api(
    path: str,
    *,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
    query: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    async def _do(session: aiohttp.ClientSession) -> Dict[str, Any]:
        url = f"http://127.0.0.1:8787/api{path}"
        params = query if isinstance(query, dict) else None
        m = str(method or "GET").upper()
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            if m == "POST":
                async with session.post(url, json=body or {}, params=params, timeout=timeout) as resp:
                    data = await resp.json(content_type=None)
                    return {"ok": int(resp.status) < 400, "status": int(resp.status), "data": data}
            async with session.get(url, params=params, timeout=timeout) as resp:
                data = await resp.json(content_type=None)
                return {"ok": int(resp.status) < 400, "status": int(resp.status), "data": data}
        except Exception as e:
            return {"ok": False, "error": "api_error", "path": path, "message": str(e)}

    return await _with_http_session(_do)


async def _execute_agent_tool(
    tool: str,
    args: Dict[str, Any],
    ctx: Dict[str, Any],
) -> Dict[str, Any]:
    sym = str(args.get("symbol") or ctx.get("symbol") or "BTCUSDT").strip().upper()
    t = str(tool or "").strip()
    allow_mut = bool(ctx.get("allow_mutations")) or bool(args.get("allow_mutations"))
    try:
        if t == "tool_search":
            return tool_search_for_agent(
                str(args.get("query") or args.get("q") or ""),
                limit=int(args.get("limit") or 15),
            )
        if t == "project_context":
            return project_context_snapshot()
        if t == "bot_live_state":
            return bot_live_state_snapshot(symbol=sym)
        if t == "src_list":
            return list_src_tree(
                subpath=str(args.get("subpath") or "src"),
                max_depth=int(args.get("max_depth") or 2),
            )
        if t == "src_read":
            return read_project_file(
                str(args.get("path") or "src/Nertzh.py"),
                offset=int(args.get("offset") or 1),
                limit=int(args.get("limit") or 80),
            )
        if t == "json_file_info":
            return json_file_info(str(args.get("path") or "logs/results.json"))
        if t == "analyze_trading_data":
            return analyze_trading_data(
                results_path=str(args.get("results_path") or "logs/results.json"),
                jsonl_path=str(args.get("jsonl_path") or "data/metrics_snapshots.jsonl"),
            )
        if t == "src_grep":
            return grep_project(
                str(args.get("pattern") or ""),
                subpath=str(args.get("subpath") or "src"),
                glob=str(args.get("glob") or "*.py"),
                head_limit=int(args.get("head_limit") or 25),
            )
        if t == "src_outline":
            return src_module_outline(str(args.get("module") or "Nertzh.py"))
        if t == "market_ticker":
            return await _get_public_ticker(sym)
        if t == "market_orderbook":
            depth = int(args.get("depth") or args.get("limit") or 50)
            return await _get_public_orderbook(sym, depth=depth)
        if t.startswith("nertzh_api."):
            api_path = resolve_nertzh_path(t, {**args, "symbol": sym})
            if not api_path:
                return {"ok": False, "error": "unknown_nertzh_api", "tool": t}
            q = {k: v for k, v in args.items() if k not in {"symbol", "limit", "path"}}
            return await _fetch_nertzh_api(api_path, query=q if q else None)
        if t == "nertzh_api" and isinstance(args.get("path"), str):
            return await _fetch_nertzh_api(
                str(args["path"]),
                method=str(args.get("method") or "GET"),
                body=args.get("body") if isinstance(args.get("body"), dict) else None,
            )
        # aliases legacy
        legacy = {
            "nertzh_metrics": f"/metrics/{sym}",
            "nertzh_combined": f"/combined/{sym}",
            "nertzh_trades": f"/trades/{sym}",
            "nertzh_ml_status": "/ml/status",
            "nertzh_agent_status": "/admin/agent/status",
        }
        if t in legacy:
            return await _fetch_nertzh_api(legacy[t])
        if t == "optimize":
            req2 = OptimizeRequest(
                symbol=args.get("symbol") if isinstance(args.get("symbol"), str) else sym,
                limit=int(args.get("limit") or ctx.get("limit") or 2000),
                iterations=int(args.get("iterations") or ctx.get("iterations") or 900),
                seed=args.get("seed") if isinstance(args.get("seed"), int) else None,
                apply=bool(args.get("apply")),
            )
            return await agent_optimize(req2)
        if t == "train_ml":
            req2 = TrainMLRequest(
                symbol=args.get("symbol") if isinstance(args.get("symbol"), str) else sym,
                min_samples=args.get("min_samples"),
            )
            return await agent_train_ml(req2)
        if t == "autoevolve":
            req2 = AutoEvolveRequest(
                symbol=args.get("symbol") if isinstance(args.get("symbol"), str) else sym,
                limit=int(args.get("limit") or 2000),
                iterations=int(args.get("iterations") or 900),
                rounds=int(args.get("rounds") or 2),
                seed=args.get("seed") if isinstance(args.get("seed"), int) else None,
                apply=bool(args.get("apply")),
                use_llm=bool(args.get("use_llm", True)),
            )
            return await agent_autoevolve(req2)
        if t == "llm_chat":
            msg = str(args.get("message") or args.get("prompt") or "").strip()
            req2 = LLMChatRequest(message=msg or "Analiza el contexto.", context=args.get("context") or {})
            return await agent_llm_chat(req2)
        if t.startswith("mcp_bybit."):
            return await call_bybit_mcp(t, args, allow_mutations=allow_mut)
        if t.startswith("mcp_") and t.endswith("_catalog"):
            return {
                "ok": False,
                "error": "catalog_only",
                "message": f"{t} es referencia de schema MCP (IDE). Usa mcp_bybit.* ejecutable o nertzh_api.*",
            }
        return {
            "ok": False,
            "error": "unknown_tool",
            "tool": t,
            "hint": "Usa tool_search para encontrar herramientas en el catálogo completo.",
        }
    except Exception as e:
        return {"ok": False, "error": "exception", "tool": t, "message": str(e)}


@app.get("/agent/tools")
async def agent_tools(
    include_mcp: bool = True,
    full: bool = False,
    query: Optional[str] = None,
    limit: int = 50,
):
    if isinstance(query, str) and query.strip():
        hits = search_tools(query.strip(), limit=int(limit))
        return {
            "ok": True,
            "query": query.strip(),
            "count": len(hits),
            "tools": hits,
            "timestamp": int(time.time() * 1000),
        }
    catalog = (
        build_full_catalog(include_mcp=bool(include_mcp), include_mcp_mutations=full)
        if bool(full)
        else build_prompt_catalog(include_mcp=bool(include_mcp))
    )
    mcp_stat = await mcp_status()
    stats = registry_stats()
    return {
        "ok": True,
        "stats": stats,
        "prompt_core": len(build_prompt_catalog(include_mcp=True)),
        "returned": len(catalog),
        "tools": catalog,
        "mcp": mcp_stat,
        "bybit_env": bybit_env_info(),
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/context")
async def agent_context(symbol: Optional[str] = "BTCUSDT"):
    sym = str(symbol or "BTCUSDT").strip().upper()
    metrics: Dict[str, Any] = {}
    prediction: Dict[str, Any] = {}
    try:
        metrics = _live_metrics_for_symbol(sym)
        buy_th = float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 6.0) or 6.0)
        sell_th = float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -6.0) or -6.0)
        hold_band = float(getattr(nertzh.config, "COMBINED_HOLD_BAND", 3.0) or 3.0)
        prediction = compute_prediction_level(
            metrics,
            buy_th=buy_th,
            sell_th=sell_th,
            hold_band=hold_band,
        )
    except Exception:
        prediction = {"level": "L0", "name": "Sin señal", "confidence_pct": 0}
    return {
        **project_context_snapshot(),
        "bot_live_state": bot_live_state_snapshot(symbol=sym),
        "registry": registry_stats(),
        "metrics_live": metrics,
        "prediction_level": prediction,
        "order_profiles_validated": ORDER_PROFILES_VALIDATED,
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/catalog")
async def agent_catalog():
    return {
        "ok": True,
        "catalog": intelligence_full_catalog(),
        "docs_url": "/project-docs/",
        "docs_public_url": "https://nerthzbyt.github.io/Restructured/",
        "docs_note": "Usar docs_url en local; docs_public_url tras deploy GitHub Pages",
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/prediction-level/{symbol}")
async def agent_prediction_level(symbol: str):
    sym = str(symbol or "BTCUSDT").strip().upper()
    metrics = _live_metrics_for_symbol(sym)
    buy_th = float(getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 6.0) or 6.0)
    sell_th = float(getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -6.0) or -6.0)
    hold_band = float(getattr(nertzh.config, "COMBINED_HOLD_BAND", 3.0) or 3.0)
    ml_enabled = bool(getattr(nertzh.config, "ML_ENABLED", False))
    p_buy = (
        nertzh.bot.ml_predict_proba(symbol=sym, action="buy", metrics=metrics)
        if ml_enabled
        else None
    )
    p_sell = (
        nertzh.bot.ml_predict_proba(symbol=sym, action="sell", metrics=metrics)
        if ml_enabled
        else None
    )
    level = compute_prediction_level(
        metrics,
        buy_th=buy_th,
        sell_th=sell_th,
        hold_band=hold_band,
        ml_p_buy=p_buy,
        ml_p_sell=p_sell,
    )
    return {
        "ok": True,
        "symbol": sym,
        "metrics": metrics,
        "prediction": level,
        "ml": {"enabled": ml_enabled, "p_buy": p_buy, "p_sell": p_sell},
        "timestamp": int(time.time() * 1000),
    }


@app.get("/agent/order-profiles")
async def agent_order_profiles():
    sweep_cat = intelligence_full_catalog().get("validation_summary")
    return {
        "ok": True,
        "profiles": ORDER_PROFILES_VALIDATED,
        "validation_summary": sweep_cat,
        "bybit_docs": "https://bybit-exchange.github.io/docs/v5/order/create-order",
        "timestamp": int(time.time() * 1000),
    }


@app.post("/agent/validate")
async def agent_validate():
    """Valida cableado de herramientas críticas (demo-aware)."""
    sym = "BTCUSDT"
    checks: List[Dict[str, Any]] = []

    async def _chk(name: str, coro) -> None:
        try:
            res = await coro
            checks.append({"tool": name, "ok": bool(res.get("ok", True)), "sample": _sample(res)})
        except Exception as e:
            checks.append({"tool": name, "ok": False, "error": str(e)})

    await _chk("project_context", _execute_agent_tool("project_context", {}, {"symbol": sym}))
    await _chk("bot_live_state", _execute_agent_tool("bot_live_state", {"symbol": sym}, {"symbol": sym}))
    await _chk("nertzh_api.decisions", _execute_agent_tool("nertzh_api.decisions", {"symbol": sym}, {"symbol": sym}))
    await _chk("src_read", _execute_agent_tool("src_read", {"path": "src/Nertzh.py", "limit": 5}, {}))
    await _chk("market_ticker", _execute_agent_tool("market_ticker", {"symbol": sym}, {}))
    await _chk("nertzh_api.config", _execute_agent_tool("nertzh_api.config", {}, {}))
    await _chk("nertzh_api.storage_recent", _execute_agent_tool("nertzh_api.storage_recent", {"symbol": sym, "limit": 3}, {"symbol": sym}))
    await _chk("nertzh_api.metrics", _execute_agent_tool("nertzh_api.metrics", {"symbol": sym}, {}))
    await _chk("nertzh_api.balance", _execute_agent_tool("nertzh_api.balance", {}, {}))
    await _chk("mcp_bybit.getTickers", _execute_agent_tool("mcp_bybit.getTickers", {"category": "linear", "symbol": sym}, {}))
    demo_block = await _execute_agent_tool("mcp_bybit.getWalletBalance", {"accountType": "UNIFIED"}, {})
    checks.append({
        "tool": "mcp_bybit.getWalletBalance_demo_guard",
        "ok": demo_block.get("error") == "demo_use_nertzh_api" if bybit_env_info().get("is_demo") else bool(demo_block.get("ok")),
        "sample": _sample(demo_block),
    })
    await _chk("tool_search", _execute_agent_tool("tool_search", {"query": "orderbook kline"}, {}))

    passed = sum(1 for c in checks if c.get("ok"))
    return {
        "ok": passed == len(checks),
        "passed": passed,
        "total": len(checks),
        "bybit_env": bybit_env_info(),
        "registry": registry_stats(),
        "checks": checks,
        "timestamp": int(time.time() * 1000),
    }


def _sample(res: Dict[str, Any], max_len: int = 200) -> Any:
    if not isinstance(res, dict):
        return res
    s = json.dumps({k: res[k] for k in list(res.keys())[:6]}, ensure_ascii=False)
    return s[:max_len] + ("..." if len(s) > max_len else "")


@app.post("/agent/chat")
async def agent_chat(req: ChatRequest):
    session_id = _new_session_id()
    agent_memory.add_event(
        "chat_in",
        {
            "session_id": session_id,
            "message": req.message,
            "symbol": req.symbol,
            "limit": int(req.limit),
            "iterations": int(req.iterations),
            "seed": req.seed,
            "apply": bool(req.apply),
        },
    )
    msg = str(req.message or "").strip()
    msg_l = msg.lower()
    if msg_l in {"status", "help", "ayuda"}:
        out = {
            "ok": True,
            "intent": "status",
            "session_id": session_id,
            "api": {"openapi": "/openapi.json", "base_api": "/api", "agent_tools": "/agent/tools", "validate": "/agent/validate"},
            "registry": registry_stats(),
            "bybit_env": bybit_env_info(),
            "project": project_context_snapshot(),
            "hint": "Modo ReAct activo (use_react=true). Usa tool_search y src_read para explorar.",
            "timestamp": int(time.time() * 1000),
        }
        agent_memory.add_event("chat_out", {"session_id": session_id, "out": out})
        return out

    if "api" in msg_l and ("genera" in msg_l or "autogen" in msg_l or "auto" in msg_l):
        out = await agent_api_autogen(ApiAutogenRequest(goal=msg))
        out2 = {
            "ok": True,
            "intent": "api_autogen",
            "session_id": session_id,
            "result": out,
            "timestamp": int(time.time() * 1000),
        }
        agent_memory.add_event("chat_out", {"session_id": session_id, "out": out2})
        return out2

    if bool(req.use_react):
        react_out = await run_react_agent(
            goal=msg,
            symbol=req.symbol,
            execute_tool=_execute_agent_tool,
            llm_chat_fn=llm_chat,
            max_steps=int(req.max_steps),
            include_mcp=bool(req.include_mcp),
            allow_mutations=bool(req.apply),
            session_id=session_id,
        )
        out = {
            "ok": bool(react_out.get("ok")),
            "intent": "react",
            "session_id": session_id,
            "agent": react_out,
            "timestamp": int(time.time() * 1000),
        }
        agent_memory.add_event("chat_out", {"session_id": session_id, "out": out})
        return out

    req_plan = AgentPlanRequest(
        goal=msg,
        symbol=req.symbol,
        limit=req.limit,
        iterations=req.iterations,
        rounds=2,
        seed=req.seed,
        apply=req.apply,
        use_llm=True,
        session_id=session_id,
    )
    planner = await _llm_plan(req_plan)
    if not bool(planner.get("ok")):
        planner = _heuristic_plan(req_plan)
    plan = planner.get("plan") if isinstance(planner, dict) else None
    if not isinstance(plan, list):
        plan = [{"tool": "llm_chat", "args": {"message": msg}}]

    exec_out = await _run_plan(plan, session_id=session_id, stop_on_error=False)
    results = exec_out.get("results") if isinstance(exec_out.get("results"), list) else []
    llm_ok = any(
        isinstance(r, dict)
        and r.get("tool") == "llm_chat"
        and bool((r.get("result") or {}).get("ok"))
        for r in results
    )
    synthesis = None
    if not llm_ok:
        synthesis = await _auto_synthesize_analysis(
            goal=msg,
            symbol=req.symbol,
            results=results,
            session_id=session_id,
        )
    out = {
        "ok": True,
        "intent": "orchestrate",
        "session_id": session_id,
        "planner": planner,
        "execution": exec_out,
        "synthesis": synthesis,
        "timestamp": int(time.time() * 1000),
    }
    agent_memory.add_event("chat_out", {"session_id": session_id, "out": out})
    return out


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="NerT_AI_PRO")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Inicia el host local (API + chat)")
    run.add_argument("--host", "--BindHost", dest="host", default="127.0.0.1")
    run.add_argument("--port", type=int, default=8787)

    opt = sub.add_parser(
        "optimize", help="Optimiza thresholds+pesos desde trades finales"
    )
    opt.add_argument("--symbol", default="")
    opt.add_argument("--limit", type=int, default=2000)
    opt.add_argument("--iterations", type=int, default=900)
    opt.add_argument("--seed", type=int, default=0)
    opt.add_argument("--apply", action="store_true")

    return p


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()
    if args.cmd == "run":
        uvicorn.run(app, host=str(args.host), port=int(args.port), reload=False)
        return

    if args.cmd == "optimize":
        symbol = str(args.symbol or "").strip() or None
        trades = _load_final_trades(symbol, int(args.limit))
        start_th = _start_thresholds()
        start_w = _weights_from_ticker_data(symbol)
        res = optimize_system_from_trades(
            trades,
            start_thresholds=start_th,
            start_weights=start_w,
            iterations=int(args.iterations),
            seed=(int(args.seed) if int(args.seed) != 0 else None),
        )
        out = {
            "success": bool(res.success),
            "baseline": res.baseline,
            "best": res.best,
            "searched": res.searched,
            "timestamp": res.timestamp,
        }
        if bool(args.apply) and bool(res.success) and isinstance(res.best, dict):
            out["applied"] = _apply_optimization(symbol, res.best)
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return


if __name__ == "__main__":
    main()
