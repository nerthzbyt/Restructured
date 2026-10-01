"""Validación de URLs locales para evitar SSRF en scripts de validación."""
from __future__ import annotations

import ipaddress
from urllib.parse import urlparse

_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})


def safe_local_base_url(base: str) -> str:
    raw = str(base or "").strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme or 'missing'}")
    if parsed.username or parsed.password:
        raise ValueError("credentials in URL are not allowed")

    host = (parsed.hostname or "").lower()
    if not host:
        raise ValueError("missing URL host")

    if host not in _ALLOWED_HOSTS:
        try:
            if not ipaddress.ip_address(host).is_loopback:
                raise ValueError(f"host not allowed: {host}")
        except ValueError as exc:
            if "not allowed" in str(exc):
                raise
            raise ValueError(f"host not allowed: {host}") from exc

    port_suffix = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port_suffix}"