from __future__ import annotations

import base64
import json
import os
import random
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

BAXIA_VERSION = "2.5.36"
QWEN_BASE_URL = "https://chat.qwen.ai"
WEB_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Qwen/1.0 Chrome/120.0.0.0 Safari/537.36"
)
JWT_RE = re.compile(
    r"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
)

MODEL_ALIASES = {
    "qwen-plus-latest": "qwen3.7-plus",
    "qwen-max-latest": "qwen3.7-max",
    "qwen-turbo-latest": "qwen3.5-flash",
    "qwen-coder-plus-latest": "qwen3-coder-plus",
    "qwen3.5-plus": "qwen3.5-plus",
    "qwen3.7-plus": "qwen3.7-plus",
    "qwen3.7-max": "qwen3.7-max",
}

_baxia_cache: Optional[Dict[str, str]] = None
_baxia_cache_time: float = 0.0
_baxia_cache_ttl_s: float = 240.0


def default_leveldb_path() -> Path:
    override = os.getenv("LLM_QWEN_DESKTOP_LEVELDB", "").strip()
    if override:
        return Path(override)
    appdata = os.getenv("APPDATA", "")
    return Path(appdata) / "Qwen" / "Local Storage" / "leveldb"


def normalize_model(model: str) -> str:
    m = str(model or "").strip()
    if not m:
        return "qwen3.7-plus"
    return MODEL_ALIASES.get(m, m)


def read_desktop_jwt(*, leveldb_path: Optional[Path] = None) -> Optional[str]:
    token_override = os.getenv("LLM_API_KEY", "").strip()
    if token_override and token_override.startswith("eyJ"):
        return token_override

    path = leveldb_path or default_leveldb_path()
    if not path.exists():
        return None

    best: Optional[str] = None
    for f in sorted(path.glob("*")):
        if f.suffix not in {".log", ".ldb"}:
            continue
        try:
            text = f.read_bytes().decode("utf-8", "ignore")
        except OSError:
            continue
        for m in JWT_RE.finditer(text):
            tok = m.group(0)
            if best is None or len(tok) > len(best):
                best = tok
    return best


def _encode_baxia_token(data: Dict[str, Any]) -> str:
    encoded = base64.b64encode(
        json.dumps(data, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return f"{BAXIA_VERSION.replace('.', '')}!{encoded}"


async def _get_baxia_tokens(session: aiohttp.ClientSession) -> Dict[str, str]:
    global _baxia_cache, _baxia_cache_time
    now = time.time()
    if _baxia_cache and (now - _baxia_cache_time) < _baxia_cache_ttl_s:
        return dict(_baxia_cache)

    renderers = [
        "ANGLE (Intel, Intel(R) UHD Graphics 630, OpenGL 4.6)",
        "ANGLE (NVIDIA, NVIDIA GeForce GTX 1080, OpenGL 4.6)",
        "ANGLE (AMD, AMD Radeon RX 580, OpenGL 4.6)",
    ]
    import hashlib

    fp = {
        "p": "Win32",
        "l": "es-ES",
        "hc": random.randint(4, 15),
        "dm": random.choice([4, 8, 16, 32]),
        "to": 60,
        "sw": 1920 + random.randint(0, 200),
        "sh": 1080 + random.randint(0, 100),
        "cd": 24,
        "pr": random.choice([1, 1.25, 1.5, 2]),
        "wf": random.choice(renderers)[:20],
        "cf": base64.b64encode(hashlib.md5(secrets.token_bytes(32)).digest())
        .decode("ascii")[:32],
        "af": f"{124.04347527516074 + random.random() * 0.001:.14f}",
        "ts": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "r": random.random(),
    }
    bx_ua = _encode_baxia_token(fp)
    bx_umid = "T2gA" + secrets.token_urlsafe(30)
    try:
        async with session.get(
            "https://sg-wum.alibaba.com/w/wu.json",
            headers={"User-Agent": WEB_USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            etag = resp.headers.get("etag")
            if isinstance(etag, str) and etag.strip():
                bx_umid = etag.strip()
    except Exception:
        pass

    out = {"bx-ua": bx_ua, "bx-umidtoken": bx_umid, "bx-v": BAXIA_VERSION}
    _baxia_cache = dict(out)
    _baxia_cache_time = now
    return out


def _merge_messages(messages: List[Dict[str, str]]) -> str:
    parts: List[str] = []
    for msg in messages:
        role = str(msg.get("role") or "user").strip().lower()
        content = str(msg.get("content") or "").strip()
        if not content:
            continue
        if role == "assistant":
            label = "Assistant"
        elif role == "system":
            label = "System"
        else:
            label = "User"
        parts.append(f"[{label}]: {content}")
    return "\n\n".join(parts).strip()


def _parse_sse_payload(raw: str) -> str:
    chunks: List[str] = []
    for line in str(raw or "").splitlines():
        trimmed = line.lstrip()
        if not trimmed.startswith("data:"):
            continue
        data = trimmed[5:].strip()
        if not data or data == "[DONE]":
            continue
        try:
            parsed = json.loads(data)
        except Exception:
            continue
        choices = parsed.get("choices")
        if not isinstance(choices, list) or not choices:
            continue
        delta = choices[0].get("delta") if isinstance(choices[0], dict) else None
        if not isinstance(delta, dict):
            continue
        content = delta.get("content")
        if isinstance(content, str) and content:
            chunks.append(content)
    return "".join(chunks).strip()


async def _create_chat(
    *,
    session: aiohttp.ClientSession,
    jwt: str,
    model: str,
    baxia: Dict[str, str],
) -> str:
    payload = {
        "title": "Nertzh Agent",
        "models": [model],
        "chat_mode": "normal",
        "chat_type": "t2t",
        "timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "project_id": "",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt}",
        "User-Agent": WEB_USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin": QWEN_BASE_URL,
        "Referer": f"{QWEN_BASE_URL}/",
        "source": "web",
        "version": "0.2.9",
        "timezone": format_datetime(datetime.now(tz=timezone.utc), usegmt=True),
        "x-request-id": str(uuid.uuid4()),
        **baxia,
    }
    url = f"{QWEN_BASE_URL}/api/v2/chats/new"
    async with session.post(
        url,
        json=payload,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=30),
    ) as resp:
        raw = await resp.json(content_type=None)
        if int(resp.status) >= 400:
            return ""
        if not isinstance(raw, dict) or not raw.get("success"):
            return ""
        data = raw.get("data")
        if isinstance(data, dict) and isinstance(data.get("id"), str):
            return data["id"]
    return ""


async def qwen_desktop_chat(
    *,
    messages: List[Dict[str, str]],
    model: str,
    timeout_s: float,
) -> Dict[str, Any]:
    actual_model = normalize_model(model)
    jwt = read_desktop_jwt()
    if not jwt:
        return {
            "ok": False,
            "error": "qwen_desktop_token_missing",
            "message": (
                "No se encontró token JWT en Qwen Desktop. "
                "Abre Qwen Desktop e inicia sesión con tu cuenta Google."
            ),
        }

    content = _merge_messages(messages)
    if not content:
        return {"ok": False, "error": "empty_messages"}

    timeout = aiohttp.ClientTimeout(total=float(timeout_s))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        baxia = await _get_baxia_tokens(session)
        chat_id = await _create_chat(
            session=session,
            jwt=jwt,
            model=actual_model,
            baxia=baxia,
        )
        if not chat_id:
            return {
                "ok": False,
                "error": "qwen_desktop_chat_create_failed",
                "message": "No se pudo crear sesión de chat en chat.qwen.ai",
            }

        payload = {
            "stream": True,
            "version": "2.1",
            "incremental_output": True,
            "chat_id": chat_id,
            "chat_mode": "normal",
            "model": actual_model,
            "parent_id": None,
            "messages": [
                {
                    "fid": str(uuid.uuid4()),
                    "parentId": None,
                    "childrenIds": [str(uuid.uuid4())],
                    "role": "user",
                    "content": content,
                    "user_action": "chat",
                    "files": [],
                    "timestamp": int(
                        datetime.now(tz=timezone.utc).timestamp() * 1000
                    ),
                    "models": [actual_model],
                    "chat_type": "t2t",
                    "feature_config": {
                        "thinking_enabled": False,
                        "output_schema": "phase",
                        "research_mode": "normal",
                        "auto_thinking": False,
                        "thinking_format": "summary",
                        "auto_search": False,
                    },
                    "extra": {"meta": {"subChatType": "t2t"}},
                    "sub_chat_type": "t2t",
                    "parent_id": None,
                }
            ],
            "timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {jwt}",
            "User-Agent": WEB_USER_AGENT,
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Origin": QWEN_BASE_URL,
            "Referer": f"{QWEN_BASE_URL}/c/{chat_id}",
            "source": "web",
            "version": "0.2.9",
            "timezone": format_datetime(datetime.now(tz=timezone.utc), usegmt=True),
            "x-request-id": str(uuid.uuid4()),
            **baxia,
        }
        url = f"{QWEN_BASE_URL}/api/v2/chat/completions?chat_id={chat_id}"
        async with session.post(url, json=payload, headers=headers) as resp:
            if int(resp.status) >= 400:
                raw_err = await resp.text()
                return {
                    "ok": False,
                    "error": "http_error",
                    "status": int(resp.status),
                    "raw": raw_err[:500],
                }
            body = await resp.text()
            if body.lstrip().startswith("{"):
                try:
                    parsed = json.loads(body)
                except Exception:
                    parsed = {"raw": body[:500]}
                if isinstance(parsed, dict) and parsed.get("success") is False:
                    return {
                        "ok": False,
                        "error": "qwen_api_error",
                        "raw": parsed,
                    }
            text = _parse_sse_payload(body)
            if not text:
                return {
                    "ok": False,
                    "error": "empty_response",
                    "raw": body[:500],
                }
            return {
                "ok": True,
                "content": text,
                "raw": {"chat_id": chat_id, "model": actual_model},
            }


async def qwen_desktop_status() -> Dict[str, Any]:
    jwt = read_desktop_jwt()
    out: Dict[str, Any] = {
        "session_found": bool(jwt),
        "leveldb_path": str(default_leveldb_path()),
    }
    if not jwt:
        return out

    try:
        import base64 as b64

        payload = jwt.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(b64.urlsafe_b64decode(payload.encode("ascii")))
        if isinstance(data, dict):
            out["user_id"] = data.get("id")
            out["token_exp"] = data.get("exp")
    except Exception:
        pass

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        headers = {
            "Authorization": f"Bearer {jwt}",
            "User-Agent": WEB_USER_AGENT,
            "Accept": "application/json",
            "Referer": f"{QWEN_BASE_URL}/",
        }
        try:
            async with session.get(
                f"{QWEN_BASE_URL}/api/v2/chats?page=1&page_size=1",
                headers=headers,
            ) as resp:
                raw = await resp.json(content_type=None)
                out["api_ok"] = bool(
                    isinstance(raw, dict) and raw.get("success") is True
                )
        except Exception as e:
            out["api_ok"] = False
            out["api_error"] = str(e)
    return out