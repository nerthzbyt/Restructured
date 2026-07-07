from __future__ import annotations

import base64
import json
import os
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

_QWEN_ORIGIN_MARKERS = ("qwen", "chat.qwen.ai")
_MAX_JWT_SCAN_BYTES = 50_000_000


def _xdg_config_home() -> Path:
    return Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config")).expanduser()


def _xdg_data_home() -> Path:
    return Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share")).expanduser()


def _firefox_profiles_root() -> List[Path]:
    roots: List[Path] = []
    for base in (
        _xdg_config_home() / "mozilla" / "firefox",
        Path.home() / "snap" / "firefox" / "common" / ".mozilla" / "firefox",
    ):
        if base.is_dir():
            roots.append(base)
    return roots


def discover_jwt_storage_paths() -> List[Path]:
    """Rutas candidatas para extraer JWT (Windows, Linux Desktop, Firefox, Chromium)."""
    seen: set[str] = set()
    out: List[Path] = []

    def _add(path: Path) -> None:
        key = str(path.expanduser())
        if key in seen:
            return
        seen.add(key)
        out.append(path.expanduser())

    override = os.getenv("LLM_QWEN_DESKTOP_LEVELDB", "").strip()
    if override:
        _add(Path(override))

    appdata = os.getenv("APPDATA", "").strip()
    if appdata:
        _add(Path(appdata) / "Qwen" / "Local Storage" / "leveldb")

    config = _xdg_config_home()
    home = Path.home()

    for rel in (
        config / "Qwen" / "Local Storage" / "leveldb",
        home / "snap" / "qwen-desktop" / "current" / ".config" / "Qwen" / "Local Storage" / "leveldb",
        home / "snap" / "qwen-desktop" / "common" / ".config" / "Qwen" / "Local Storage" / "leveldb",
    ):
        _add(rel)

    for browser in (
        "chromium",
        "google-chrome",
        "BraveSoftware/Brave-Browser",
        "microsoft-edge",
        "vivaldi",
    ):
        _add(config / browser / "Default" / "Local Storage" / "leveldb")
        _add(config / browser / "Profile 1" / "Local Storage" / "leveldb")

    for ff_root in _firefox_profiles_root():
        for profile in sorted(ff_root.glob("*.default*")):
            if not profile.is_dir():
                continue
            storage = profile / "storage" / "default"
            if not storage.is_dir():
                continue
            _add(storage / "https+++chat.qwen.ai" / "ls")
            _add(storage / "https+++chat.qwen.ai")
            for origin_dir in storage.iterdir():
                name = origin_dir.name.lower()
                if any(marker in name for marker in _QWEN_ORIGIN_MARKERS):
                    _add(origin_dir / "ls")
                    _add(origin_dir)

    token_file = os.getenv("LLM_QWEN_TOKEN_FILE", "").strip()
    if token_file:
        _add(Path(token_file))

    return out


def default_leveldb_path() -> Path:
    override = os.getenv("LLM_QWEN_DESKTOP_LEVELDB", "").strip()
    if override:
        return Path(override).expanduser()
    for path in discover_jwt_storage_paths():
        if path.exists():
            return path
    if os.name == "nt" and os.getenv("APPDATA"):
        return Path(os.getenv("APPDATA", "")) / "Qwen" / "Local Storage" / "leveldb"
    return _xdg_config_home() / "Qwen" / "Local Storage" / "leveldb"


def _pick_best_jwt(candidates: List[Optional[str]]) -> Optional[str]:
    best: Optional[str] = None
    for tok in candidates:
        if not tok:
            continue
        if best is None or len(tok) > len(best):
            best = tok
    return best


def _extract_jwt_from_bytes(data: bytes) -> Optional[str]:
    text = data.decode("utf-8", "ignore")
    return _pick_best_jwt([m.group(0) for m in JWT_RE.finditer(text)])


def _scan_leveldb_dir(path: Path) -> Optional[str]:
    tokens: List[Optional[str]] = []
    for f in sorted(path.glob("*")):
        if f.suffix not in {".log", ".ldb"}:
            continue
        try:
            tokens.append(_extract_jwt_from_bytes(f.read_bytes()))
        except OSError:
            continue
    return _pick_best_jwt(tokens)


def _scan_sqlite_file(path: Path) -> Optional[str]:
    try:
        import sqlite3

        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            rows = conn.execute("SELECT key, value FROM data").fetchall()
        finally:
            conn.close()
    except Exception:
        try:
            return _extract_jwt_from_bytes(path.read_bytes())
        except OSError:
            return None

    tokens: List[Optional[str]] = []
    for key, value in rows:
        if isinstance(key, (bytes, bytearray)):
            tokens.append(_extract_jwt_from_bytes(bytes(key)))
        elif isinstance(key, str):
            tokens.append(_extract_jwt_from_bytes(key.encode("utf-8", "ignore")))
        if isinstance(value, (bytes, bytearray)):
            tokens.append(_extract_jwt_from_bytes(bytes(value)))
        elif isinstance(value, str):
            tokens.append(_extract_jwt_from_bytes(value.encode("utf-8", "ignore")))
    return _pick_best_jwt(tokens)


def _scan_storage_path(path: Path) -> Optional[str]:
    if path.is_file():
        if path.suffix.lower() in {".sqlite", ".sqlite-wal", ".log", ".ldb"}:
            if path.suffix.lower() == ".sqlite":
                return _scan_sqlite_file(path)
            try:
                return _extract_jwt_from_bytes(path.read_bytes())
            except OSError:
                return None
        return None

    if not path.is_dir():
        return None

    if path.name == "leveldb" or any(path.glob("*.ldb")):
        tok = _scan_leveldb_dir(path)
        if tok:
            return tok

    tokens: List[Optional[str]] = []
    for f in sorted(path.rglob("*")):
        if not f.is_file():
            continue
        try:
            if f.stat().st_size > _MAX_JWT_SCAN_BYTES:
                continue
        except OSError:
            continue
        if f.suffix.lower() == ".sqlite":
            tokens.append(_scan_sqlite_file(f))
        elif f.suffix.lower() in {".sqlite-wal", ".log", ".ldb"}:
            try:
                tokens.append(_extract_jwt_from_bytes(f.read_bytes()))
            except OSError:
                continue
    return _pick_best_jwt(tokens)


def normalize_model(model: str) -> str:
    m = str(model or "").strip()
    if not m:
        return "qwen3.7-plus"
    return MODEL_ALIASES.get(m, m)


def read_desktop_jwt(
    *,
    leveldb_path: Optional[Path] = None,
    _return_meta: bool = False,
) -> Optional[str] | tuple[Optional[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "platform": os.name,
        "source": None,
        "searched_paths": [],
    }

    token_override = os.getenv("LLM_API_KEY", "").strip()
    if token_override and token_override.startswith("eyJ"):
        meta["source"] = "env:LLM_API_KEY"
        return (token_override, meta) if _return_meta else token_override

    candidates = [leveldb_path] if leveldb_path else discover_jwt_storage_paths()
    for path in candidates:
        if path is None:
            continue
        expanded = path.expanduser()
        meta["searched_paths"].append(str(expanded))
        if not expanded.exists():
            continue
        tok = _scan_storage_path(expanded)
        if tok:
            meta["source"] = str(expanded)
            return (tok, meta) if _return_meta else tok

    return (None, meta) if _return_meta else None


def _jwt_missing_message(meta: Dict[str, Any]) -> str:
    if os.name == "nt":
        hint = (
            "Abre Qwen Desktop e inicia sesión, o define LLM_API_KEY=eyJ... "
            "con el JWT de chat.qwen.ai."
        )
    else:
        hint = (
            "En Linux: inicia sesión en chat.qwen.ai (Firefox/Chromium) o usa "
            "Qwen Desktop snap, luego define LLM_API_KEY=eyJ... copiado del "
            "header Authorization en DevTools → Red, o apunta "
            "LLM_QWEN_DESKTOP_LEVELDB a tu carpeta de almacenamiento."
        )
    paths = meta.get("searched_paths") or []
    if paths:
        hint += f" Rutas revisadas: {paths[0]}"
        if len(paths) > 1:
            hint += f" (+{len(paths) - 1} más)."
    return hint


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

    def _secret_randint(lo: int, hi: int) -> int:
        return lo + secrets.randbelow(hi - lo + 1)

    def _secret_choice(options: List[Any]) -> Any:
        return options[secrets.randbelow(len(options))]

    def _secret_unit_float() -> float:
        return secrets.randbits(53) / float(1 << 53)

    fp = {
        "p": "Win32",
        "l": "es-ES",
        "hc": _secret_randint(4, 15),
        "dm": _secret_choice([4, 8, 16, 32]),
        "to": 60,
        "sw": 1920 + _secret_randint(0, 200),
        "sh": 1080 + _secret_randint(0, 100),
        "cd": 24,
        "pr": _secret_choice([1, 1.25, 1.5, 2]),
        "wf": str(_secret_choice(renderers))[:20],
        "cf": hashlib.sha256(secrets.token_bytes(32)).hexdigest()[:32],
        "af": f"{124.04347527516074 + _secret_unit_float() * 0.001:.14f}",
        "ts": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "r": _secret_unit_float(),
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
    jwt, meta = read_desktop_jwt(_return_meta=True)
    if not jwt:
        return {
            "ok": False,
            "error": "qwen_desktop_token_missing",
            "message": _jwt_missing_message(meta),
            "platform": meta.get("platform"),
            "searched_paths": meta.get("searched_paths"),
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
    jwt, meta = read_desktop_jwt(_return_meta=True)
    candidates = discover_jwt_storage_paths()
    out: Dict[str, Any] = {
        "session_found": bool(jwt),
        "platform": os.name,
        "leveldb_path": str(default_leveldb_path()),
        "token_source": meta.get("source"),
        "searched_paths": meta.get("searched_paths") or [str(p) for p in candidates],
        "existing_paths": [str(p) for p in candidates if p.exists()],
        "firefox_profiles": [str(p) for p in _firefox_profiles_root()],
    }
    if not jwt:
        out["hint"] = _jwt_missing_message(meta)
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