"""Validación con credenciales reales — debug de cada conexión Bybit."""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from src_dev.bybit.rest import BybitRestClient
from src_dev.bybit.ws import BybitWsCollector
from src_dev.config import DevSettings, private_rest_base_url

try:
    from bybit_v5 import BybitV5Client
except ImportError:
    BybitV5Client = None  # type: ignore


def _mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return "(vacía o corta)"
    return f"{key[:4]}...{key[-4:]}"


async def validate_all_connections(
    symbol: str,
    settings: Optional[DevSettings] = None,
    *,
    ws_probe_s: float = 8.0,
) -> Dict[str, Any]:
    """Prueba REST público, WS público y REST privado (demo/mainnet según BYBIT_ENV)."""
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    key = str(os.getenv("BYBIT_API_KEY", "") or "").strip()
    secret = str(os.getenv("BYBIT_API_SECRET", "") or "").strip()
    bybit_env = str(os.getenv("BYBIT_ENV", "mainnet") or "mainnet").strip().lower()

    report: Dict[str, Any] = {
        "ts": time.time(),
        "symbol": sym,
        "bybit_env": bybit_env,
        "api_key_masked": _mask_key(key),
        "connections": {},
        "errors": [],
        "ok": False,
    }

    # --- REST público (market data mainnet) ---
    try:
        async with BybitRestClient(cfg) as pub:
            st = await pub.server_time()
            snap = await pub.fetch_market_snapshot(sym, include_oi=False)
        ok = st.get("retCode") == 0 and bool(snap.get("candles"))
        report["connections"]["rest_public"] = {
            "ok": ok,
            "base_url": cfg.endpoints.rest_base,
            "server_retCode": st.get("retCode"),
            "candles": len(snap.get("candles") or []),
            "orderbook_bids": len((snap.get("orderbook") or {}).get("bids") or []),
            "last_price": (snap.get("ticker") or {}).get("last_price"),
        }
        if not ok:
            report["errors"].append("rest_public: snapshot incompleto o server_time falló")
    except Exception as e:
        report["connections"]["rest_public"] = {"ok": False, "error": str(e)}
        report["errors"].append(f"rest_public: {e}")

    # --- WebSocket público spot ---
    try:
        ws = BybitWsCollector(cfg)
        ws.symbol = sym
        ws_data = await ws.collect(duration_s=ws_probe_s)
        ok = bool(ws_data.get("ready"))
        report["connections"]["ws_public_spot"] = {
            "ok": ok,
            "url": ws.ws_url,
            "duration_s": ws_probe_s,
            "messages": ws_data.get("message_count"),
            "last_price": (ws_data.get("ticker") or {}).get("last_price"),
        }
        if not ok:
            report["errors"].append("ws_public_spot: no listo tras probe")
    except Exception as e:
        report["connections"]["ws_public_spot"] = {"ok": False, "error": str(e)}
        report["errors"].append(f"ws_public_spot: {e}")

    # --- REST privado (credenciales) ---
    if not key or not secret:
        report["connections"]["rest_private"] = {
            "ok": False,
            "skipped": True,
            "reason": "BYBIT_API_KEY/BYBIT_API_SECRET no configuradas",
        }
        report["errors"].append("rest_private: credenciales ausentes")
    elif BybitV5Client is None:
        report["connections"]["rest_private"] = {"ok": False, "error": "bybit_v5 no importable"}
        report["errors"].append("rest_private: módulo bybit_v5 no disponible")
    else:
        base = private_rest_base_url()
        client = BybitV5Client(key, secret, base_url=base)
        priv: Dict[str, Any] = {"base_url": base, "checks": {}}
        try:
            st = await client.get_server_time()
            priv["checks"]["server_time"] = {
                "retCode": st.get("retCode"),
                "ok": st.get("retCode") == 0,
            }
            bal = await client.wallet_balance(account_type="UNIFIED", coin="USDT")
            priv["checks"]["wallet_balance"] = {
                "retCode": bal.get("retCode"),
                "retMsg": bal.get("retMsg"),
                "ok": bal.get("retCode") == 0,
            }
            for flt in ("Order", "tpslOrder", "StopOrder"):
                oo = await client.order_realtime(
                    category="spot", symbol=sym, order_filter=flt, limit=20
                )
                priv["checks"][f"open_orders_{flt}"] = {
                    "retCode": oo.get("retCode"),
                    "count": len((oo.get("result") or {}).get("list") or []),
                    "ok": oo.get("retCode") == 0,
                }
            hist = await client.order_history(category="spot", symbol=sym, limit=50)
            priv["checks"]["order_history"] = {
                "retCode": hist.get("retCode"),
                "count": len((hist.get("result") or {}).get("list") or []),
                "ok": hist.get("retCode") == 0,
            }
            priv["ok"] = all(
                c.get("ok") for c in priv["checks"].values() if isinstance(c, dict)
            )
            report["connections"]["rest_private"] = priv
            if not priv["ok"]:
                report["errors"].append(
                    "rest_private: algún check falló — revisar retCode/retMsg en debug"
                )
        except Exception as e:
            report["connections"]["rest_private"] = {"ok": False, "error": str(e), "base_url": base}
            report["errors"].append(f"rest_private: {e}")
        finally:
            await client.aclose()

    conn_ok = [c.get("ok") for c in report["connections"].values() if isinstance(c, dict)]
    report["ok"] = all(conn_ok) if conn_ok else False
    report["public_ok"] = (
        report["connections"].get("rest_public", {}).get("ok")
        and report["connections"].get("ws_public_spot", {}).get("ok")
    )
    report["private_ok"] = report["connections"].get("rest_private", {}).get("ok", False)
    return report