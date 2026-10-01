"""Validación en consola: demo trading Bybit (público mainnet + privado api-demo)."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import aiohttp
import Nertzh as nertzh
from bybit_v5 import BybitV5Client


def _ok(label: str, detail: dict) -> dict:
    return {"label": label, "ok": True, **detail}


def _fail(label: str, detail: dict) -> dict:
    return {"label": label, "ok": False, **detail}


async def _public_get(session: aiohttp.ClientSession, path: str, params: dict) -> dict:
    url = f"{nertzh.BASE_URL}{path}"
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json(content_type=None)
        return {"http": resp.status, "url": str(resp.url), "data": data}


async def _ws_private_demo_auth(client: BybitV5Client, timeout_s: float = 12.0) -> dict:
    import websockets
    from utils import generate_signature

    ws_url = "wss://stream-demo.bybit.com/v5/private"
    expires = int((time.time() + 1) * 1000)
    sign = generate_signature(client.api_secret, f"GET/realtime{expires}")
    messages: list[dict] = []
    try:
        async with websockets.connect(ws_url, open_timeout=10) as ws:
            await ws.send(json.dumps({"op": "auth", "args": [client.api_key, expires, sign]}))
            await ws.send(json.dumps({"op": "subscribe", "args": ["wallet"]}))
            t0 = time.time()
            while time.time() - t0 < timeout_s:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                payload = json.loads(raw)
                messages.append(payload)
                if payload.get("op") in {"auth", "subscribe"} and payload.get("success"):
                    continue
                if "wallet" in str(payload.get("topic", "")):
                    break
                if len(messages) >= 6:
                    break
    except Exception as e:
        return {"ok": False, "ws_url": ws_url, "error": str(e), "messages": messages[:3]}

    ok = any(m.get("op") == "auth" and m.get("success") for m in messages) or any(
        "wallet" in str(m.get("topic", "")) for m in messages
    )
    return {"ok": ok, "ws_url": ws_url, "messages": messages[:5]}


async def _ws_public_probe(symbol: str, timeout_s: float = 12.0) -> dict:
    import websockets

    ws_url = nertzh.WS_URL
    subs = {
        "op": "subscribe",
        "args": [f"kline.1.{symbol}", f"orderbook.50.{symbol}", f"tickers.{symbol}"],
    }
    messages: list[dict] = []
    t0 = time.time()
    try:
        async with websockets.connect(ws_url, open_timeout=10) as ws:
            await ws.send(json.dumps(subs))
            while time.time() - t0 < timeout_s:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                payload = json.loads(raw)
                if payload.get("op") == "subscribe" and payload.get("success"):
                    continue
                if "topic" in payload:
                    messages.append({"topic": payload.get("topic"), "type": payload.get("type")})
                if len(messages) >= 2:
                    break
    except Exception as e:
        return {"ok": False, "ws_url": ws_url, "error": str(e), "messages": messages}

    return {
        "ok": len(messages) >= 1,
        "ws_url": ws_url,
        "messages_received": len(messages),
        "topics": sorted({m["topic"] for m in messages}),
        "sample": messages[:3],
    }


async def main() -> int:
    cfg = nertzh.config
    symbol = cfg.SYMBOL[0] if isinstance(cfg.SYMBOL, list) else str(cfg.SYMBOL)
    results: list[dict] = []

    print("=== CONFIG ===")
    print(f"BYBIT_ENV={cfg.BYBIT_ENV}")
    print(f"LIVE_TRADING_ENABLED={cfg.LIVE_TRADING_ENABLED}")
    print(f"BASE_URL (public REST)={nertzh.BASE_URL}")
    print(f"WS_URL (public)={nertzh.WS_URL}")
    engine = nertzh.NertzMetalEngine()
    client = engine._bybit_client()
    print(f"Private REST (engine)={client.base_url if client else None}")
    print(f"SYMBOL={symbol}")
    print()

    async with aiohttp.ClientSession() as session:
        for name, path, params in [
            ("public_ticker", "/v5/market/tickers", {"category": "spot", "symbol": symbol}),
            ("public_orderbook", "/v5/market/orderbook", {"category": "spot", "symbol": symbol, "limit": 5}),
            ("public_kline", "/v5/market/kline", {"category": "spot", "symbol": symbol, "interval": "1", "limit": 2}),
        ]:
            try:
                r = await _public_get(session, path, params)
                rc = (r["data"] or {}).get("retCode")
                ok = r["http"] == 200 and rc == 0
                results.append(
                    _ok(name, {"http": r["http"], "retCode": rc, "url": r["url"]})
                    if ok
                    else _fail(name, {"http": r["http"], "retCode": rc, "retMsg": (r["data"] or {}).get("retMsg"), "url": r["url"]})
                )
            except Exception as e:
                results.append(_fail(name, {"error": str(e)}))

    try:
        pf = await engine.preflight()
        results.append(_ok("engine_preflight", pf) if pf.get("success") else _fail("engine_preflight", pf))
    except Exception as e:
        results.append(_fail("engine_preflight", {"error": str(e)}))

    if client is None:
        results.append(_fail("private_client", {"error": "LIVE_TRADING_ENABLED o credenciales"}))
    else:
        try:
            st = await client.get_server_time()
            ok = st.get("http_status") == 200 and st.get("retCode") == 0
            results.append(_ok("demo_server_time", {"base_url": client.base_url, "retCode": st.get("retCode")}) if ok else _fail("demo_server_time", st))
        except Exception as e:
            results.append(_fail("demo_server_time", {"error": str(e)}))

        try:
            bal = await client.wallet_balance(account_type="UNIFIED", coin="USDT")
            ok = bal.get("http_status") == 200 and bal.get("retCode") == 0
            equity = None
            if ok:
                lst = ((bal.get("result") or {}).get("list") or [])
                if lst:
                    equity = lst[0].get("totalEquity")
            results.append(
                _ok("demo_wallet_balance", {"base_url": client.base_url, "retCode": bal.get("retCode"), "totalEquity": equity})
                if ok
                else _fail("demo_wallet_balance", {"retCode": bal.get("retCode"), "retMsg": bal.get("retMsg")})
            )
        except Exception as e:
            results.append(_fail("demo_wallet_balance", {"error": str(e)}))

        wrong = BybitV5Client(cfg.BYBIT_API_KEY, cfg.BYBIT_API_SECRET, base_url="https://api.bybit.com")
        try:
            wb = await wrong.wallet_balance(account_type="UNIFIED", coin="USDT")
            rc = wb.get("retCode")
            results.append(
                _ok("demo_keys_reject_mainnet_private", {"retCode": rc})
                if rc != 0
                else _fail("demo_keys_reject_mainnet_private", {"retCode": rc, "note": "claves demo aceptadas en mainnet"})
            )
        except Exception as e:
            results.append(_ok("demo_keys_reject_mainnet_private", {"error": str(e)}))
        finally:
            await wrong.aclose()

        if cfg.BYBIT_ENV == "demo":
            try:
                pws = await _ws_private_demo_auth(client)
                results.append(_ok("demo_private_websocket", pws) if pws.get("ok") else _fail("demo_private_websocket", pws))
            except Exception as e:
                results.append(_fail("demo_private_websocket", {"error": str(e)}))

        await client.aclose()
        engine._bybit = None

    try:
        ws = await _ws_public_probe(symbol)
        results.append(_ok("public_websocket", ws) if ws.get("ok") else _fail("public_websocket", ws))
    except Exception as e:
        results.append(_fail("public_websocket", {"error": str(e)}))

    print("=== RESULTS ===")
    passed = sum(1 for r in results if r.get("ok"))
    for r in results:
        status = "PASS" if r.get("ok") else "FAIL"
        body = {k: v for k, v in r.items() if k not in ("label", "ok")}
        print(f"[{status}] {r.get('label')}: {json.dumps(body, ensure_ascii=False)}")
    print(f"\nTOTAL: {passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))