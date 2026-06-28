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
from typing import Any, Dict, Optional

import aiohttp
import numpy as np
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import Nertzh as nertzh  # noqa: E402
from optimizer import (  # noqa: E402
    DEFAULT_COMBINED_WEIGHTS,
    CombinedWeights,
    Thresholds,
    optimize_system_from_trades,
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
                con.execute(
                    "CREATE INDEX IF NOT EXISTS idx_agent_events_ts ON agent_events(ts_ms)"
                )
                con.execute(
                    "CREATE INDEX IF NOT EXISTS idx_agent_events_kind ON agent_events(kind)"
                )
                con.commit()
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
        model = "deepseek-r1:latest" if backend == "ollama" else "deepseek-r1"
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
    if not isinstance(cw, dict):
        return DEFAULT_COMBINED_WEIGHTS
    return CombinedWeights(
        pio=_safe_float(cw.get("pio"), DEFAULT_COMBINED_WEIGHTS.pio),
        egm=_safe_float(cw.get("egm"), DEFAULT_COMBINED_WEIGHTS.egm),
        ild=_safe_float(cw.get("ild"), DEFAULT_COMBINED_WEIGHTS.ild),
        rol=_safe_float(cw.get("rol"), DEFAULT_COMBINED_WEIGHTS.rol),
        ogm=_safe_float(cw.get("ogm"), DEFAULT_COMBINED_WEIGHTS.ogm),
        mom=_safe_float(cw.get("mom"), DEFAULT_COMBINED_WEIGHTS.mom),
        scale=_safe_float(cw.get("scale"), DEFAULT_COMBINED_WEIGHTS.scale),
    )


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
  <title>NerT AI PRO</title>
  <style>
    body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:#0b1020;color:#e7eaf3}
    .wrap{max-width:980px;margin:0 auto;padding:18px}
    .card{background:#121a33;border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:14px}
    .row{display:flex;gap:12px;flex-wrap:wrap}
    input,textarea,button{border-radius:10px;border:1px solid rgba(255,255,255,.12);background:#0f1630;color:#e7eaf3;padding:10px}
    textarea{width:100%;min-height:120px;resize:vertical}
    button{cursor:pointer}
    .log{white-space:pre-wrap;background:#0a0f24;border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:12px;min-height:220px}
    .muted{color:#aab3d6}
  </style>
</head>
<body>
  <div class="wrap">
    <h2>NerT AI PRO</h2>
    <div class="card">
      <div class="row">
        <input id="symbol" placeholder="SYMBOL (ej: BTCUSDT)" value="BTCUSDT"/>
        <input id="limit" placeholder="limit trades" value="2000"/>
        <input id="iters" placeholder="iterations" value="900"/>
        <label class="muted"><input id="apply" type="checkbox"/> aplicar cambios</label>
      </div>
      <div style="height:10px"></div>
      <textarea id="msg" placeholder="Dime qué quieres optimizar...">optimiza el sistema con datos reales y corrige lógica defectuosa</textarea>
      <div style="height:10px"></div>
      <div class="row">
        <button onclick="send()">Enviar</button>
        <button onclick="status()">Status ML</button>
        <button onclick="optimize()">Optimizar (directo)</button>
      </div>
      <div style="height:10px"></div>
      <div id="out" class="log"></div>
    </div>
    <p class="muted">API base disponible en /api (incluye endpoints de trading existentes).</p>
  </div>
<script>
const out = document.getElementById('out');
function log(x){ out.textContent = typeof x === 'string' ? x : JSON.stringify(x,null,2); }
async function send(){
  const body = {
    message: document.getElementById('msg').value,
    symbol: document.getElementById('symbol').value,
    limit: parseInt(document.getElementById('limit').value||'2000',10),
    iterations: parseInt(document.getElementById('iters').value||'900',10),
    apply: document.getElementById('apply').checked
  };
  const r = await fetch('/agent/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  log(await r.json());
}
async function status(){
  const r = await fetch('/api/ml/status');
  log(await r.json());
}
async function optimize(){
  const body = {
    symbol: document.getElementById('symbol').value,
    limit: parseInt(document.getElementById('limit').value||'2000',10),
    iterations: parseInt(document.getElementById('iters').value||'900',10),
    apply: document.getElementById('apply').checked
  };
  const r = await fetch('/agent/optimize',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  log(await r.json());
}
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

        # Log del estado del LLM para verificar la configuracion activa
        _llm_cfg = _llm_config()
        nertzh.logger.info(
            f"LLM backend='{_llm_cfg.backend}'  model='{_llm_cfg.model}'  "
            f"base_url='{_llm_cfg.base_url}'  api_key={'SET' if _llm_cfg.api_key else 'NOT SET'}"
        )

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
            maybe_stop = nertzh.bot.stop()
            if inspect.isawaitable(maybe_stop):
                await maybe_stop
        except Exception:
            pass


app = FastAPI(title="NerT AI PRO", version="1.0.0", lifespan=lifespan)
app.mount("/api", nertzh.app)


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
    return {"tool": t, "args": dict(args)}


def _heuristic_plan(req: AgentPlanRequest) -> Dict[str, Any]:
    msg = str(req.goal or "").lower()
    plan: list[Dict[str, Any]] = []
    sym = str(req.symbol or "").strip() or None
    if ("ticker" in msg) or ("precio" in msg) or ("price" in msg):
        plan.append({"tool": "market_ticker", "args": {"symbol": sym or "BTCUSDT"}})
    if ("orderbook" in msg) or ("libro" in msg) or ("profund" in msg):
        plan.append(
            {
                "tool": "market_orderbook",
                "args": {"symbol": sym or "BTCUSDT", "depth": 50},
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
    if not plan:
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
        "Eres un planificador de agente. Devuelve SOLO JSON válido sin markdown. "
        'Salida: {"plan":[{"tool":"...","args":{...}}]}. '
        f"tools permitidas: {sorted(ALLOWED_TOOLS)}. "
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
        args = s["args"]
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
                msg = str(args.get("message") or "")
                sys_prompt = args.get("system")
                ctx = args.get("context")
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


@app.get("/agent/llm/status")
async def agent_llm_status():
    cfg = _llm_config()
    return {
        "ok": True,
        "backend": cfg.backend,
        "base_url": cfg.base_url,
        "model": cfg.model,
        "temperature": cfg.temperature,
        "timeout_s": cfg.timeout_s,
        "has_api_key": bool(cfg.api_key),
        "timestamp": int(time.time() * 1000),
    }


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
                _snap = nertzh.ThresholdSnapshot(
                    timestamp=_dt.datetime.now(_dt.timezone.utc),
                    egm_buy_threshold=float(
                        getattr(nertzh.config, "EGM_BUY_THRESHOLD", 0.02)
                    ),
                    egm_sell_threshold=float(
                        getattr(nertzh.config, "EGM_SELL_THRESHOLD", -0.02)
                    ),
                    combined_buy_threshold=float(
                        getattr(nertzh.config, "COMBINED_BUY_THRESHOLD", 8.0)
                    ),
                    combined_sell_threshold=float(
                        getattr(nertzh.config, "COMBINED_SELL_THRESHOLD", -8.0)
                    ),
                    stats={"combined_weights": dict(w), "source": "agent_optimize"},
                )
                _db.add(_snap)
                _db.commit()
                applied["weights_persisted_to_db"] = True
        except Exception as _e:
            applied["weights_persisted_to_db"] = False
            applied["weights_persist_error"] = str(_e)

    if bool(getattr(nertzh.config, "PERSIST_THRESHOLDS_TO_ENV", False)):
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        applied["persisted"] = nertzh._persist_thresholds_to_env(env_path)

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
    if not isinstance(w, dict):
        return DEFAULT_COMBINED_WEIGHTS
    return CombinedWeights(
        pio=_safe_float(w.get("pio"), DEFAULT_COMBINED_WEIGHTS.pio),
        egm=_safe_float(w.get("egm"), DEFAULT_COMBINED_WEIGHTS.egm),
        ild=_safe_float(w.get("ild"), DEFAULT_COMBINED_WEIGHTS.ild),
        rol=_safe_float(w.get("rol"), DEFAULT_COMBINED_WEIGHTS.rol),
        ogm=_safe_float(w.get("ogm"), DEFAULT_COMBINED_WEIGHTS.ogm),
        mom=_safe_float(w.get("mom"), DEFAULT_COMBINED_WEIGHTS.mom),
        scale=_safe_float(w.get("scale"), DEFAULT_COMBINED_WEIGHTS.scale),
    )


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
    buy = float(max(1.0, min(15.0, buy)))
    sell = float(-max(1.0, min(15.0, abs(sell))))
    hold = float(max(0.5, min(6.0, hold)))
    return Thresholds(
        combined_buy_threshold=buy,
        combined_sell_threshold=sell,
        combined_hold_band=hold,
    )


def _clamp_weights(w: Dict[str, Any]) -> CombinedWeights:
    pio = _safe_float(w.get("pio"), DEFAULT_COMBINED_WEIGHTS.pio)
    egm = _safe_float(w.get("egm"), DEFAULT_COMBINED_WEIGHTS.egm)
    ild = _safe_float(w.get("ild"), DEFAULT_COMBINED_WEIGHTS.ild)
    rol = _safe_float(w.get("rol"), DEFAULT_COMBINED_WEIGHTS.rol)
    ogm = _safe_float(w.get("ogm"), DEFAULT_COMBINED_WEIGHTS.ogm)
    mom = _safe_float(w.get("mom"), DEFAULT_COMBINED_WEIGHTS.mom)
    scale = _safe_float(w.get("scale"), DEFAULT_COMBINED_WEIGHTS.scale)
    scale = float(max(1.0, min(50.0, scale)))
    return CombinedWeights(
        pio=pio, egm=egm, ild=ild, rol=rol, ogm=ogm, mom=mom, scale=scale
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
        "weights={pio,egm,ild,rol,ogm,mom,scale}. No inventes claves."
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

    for r in range(int(req.rounds)):
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
            "api": {"openapi": "/openapi.json", "base_api": "/api"},
            "tools": sorted(ALLOWED_TOOLS),
            "hint": "Usa /agent/plan y /agent/execute para multi-tarea; o escribe 'planifica ...'",
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

    exec_out = await _run_plan(plan, session_id=session_id, stop_on_error=True)
    out = {
        "ok": True,
        "intent": "orchestrate",
        "session_id": session_id,
        "planner": planner,
        "execution": exec_out,
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
