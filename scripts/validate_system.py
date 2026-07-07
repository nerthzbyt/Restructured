#!/usr/bin/env python3
"""Validación integral del sistema Restructured (motor + agente)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _get(url: str, timeout: float = 15.0) -> Tuple[bool, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return True, json.loads(resp.read())
    except Exception as exc:
        return False, str(exc)


def _post(url: str, payload: Dict[str, Any], timeout: float = 60.0) -> Tuple[bool, Any]:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return True, json.loads(resp.read())
    except Exception as exc:
        return False, str(exc)


def run_unit_tests() -> Tuple[int, int]:
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    passed = proc.stdout.count(" ... ok")
    failed = proc.stdout.count(" ... FAIL") + proc.stdout.count(" ... ERROR")
    if passed == 0 and "Ran " in proc.stdout and proc.returncode == 0:
        for line in proc.stdout.splitlines():
            if line.startswith("Ran ") and " tests" in line:
                try:
                    passed = int(line.split()[1])
                except (IndexError, ValueError):
                    pass
                break
    print(proc.stdout[-2000:] if len(proc.stdout) > 2000 else proc.stdout)
    if proc.returncode != 0 and proc.stderr:
        print(proc.stderr[-1000:])
    if proc.returncode != 0 and failed == 0:
        failed = 1
    return passed, failed


def validate_api(base: str) -> List[Tuple[str, bool, str]]:
    checks: List[Tuple[str, bool, str]] = []
    endpoints = [
        "/health",
        "/agent/llm/status",
        "/api/status",
        "/api/config",
        "/api/ticker/BTCUSDT",
        "/api/metrics/BTCUSDT",
        "/api/balance?account_type=UNIFIED&coin=USDT",
        "/api/orders/status",
        "/agent/catalog",
        "/agent/prediction-level/BTCUSDT",
        "/agent/context",
    ]
    for path in endpoints:
        ok, data = _get(f"{base}{path}")
        detail = ""
        if ok and isinstance(data, dict):
            if path == "/agent/llm/status":
                ok = bool(
                    data.get("qwen_desktop", {}).get("session_found")
                    or data.get("backend") in {"ollama", "openai_compat", "disabled"}
                )
                detail = f"backend={data.get('backend')} session={data.get('qwen_desktop', {}).get('session_found')}"
            elif path == "/api/ticker/BTCUSDT":
                ok = bool(data.get("last_price") or data.get("price"))
                detail = f"price={data.get('last_price') or data.get('price')}"
            elif path == "/api/metrics/BTCUSDT":
                metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
                detail = f"combined={metrics.get('combined')} calibrated={metrics.get('metrics_calibrated')}"
            else:
                detail = str(list(data.keys())[:5])
        else:
            detail = str(data)[:120]
        checks.append((path, ok, detail))

    ok, data = _post(
        f"{base}/agent/llm/chat",
        {"message": "Responde solo: VALIDADO", "system": "Respuesta breve"},
    )
    if ok and isinstance(data, dict):
        result = data.get("result") if isinstance(data.get("result"), dict) else {}
        content = str(result.get("content") or "")
        checks.append(("/agent/llm/chat", bool(data.get("ok")), content[:80]))
    else:
        checks.append(("/agent/llm/chat", False, str(data)[:120]))
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Validar Restructured end-to-end")
    parser.add_argument("--base", default="http://127.0.0.1:8787")
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--wait-metrics", type=int, default=0, help="Segundos extra antes de métricas")
    args = parser.parse_args()

    print("=== 1) Tests unitarios ===")
    if args.skip_tests:
        print("skipped")
    else:
        passed, failed = run_unit_tests()
        print(f"unit tests: {passed} ok, {failed} fail")
        if failed:
            return 1

    if args.wait_metrics > 0:
        print(f"Esperando {args.wait_metrics}s para calibración de métricas...")
        time.sleep(args.wait_metrics)

    print(f"\n=== 2) API en {args.base} ===")
    ok_health, health = _get(f"{args.base}/health")
    if not ok_health:
        print(f"Servidor no disponible: {health}")
        print("Arranca: python NerT_AI_PRO/main.py run --host 127.0.0.1 --port 8787")
        return 2

    checks = validate_api(args.base)
    passed = sum(1 for _, ok, _ in checks if ok)
    for path, ok, detail in checks:
        print(f"[{'OK' if ok else 'FAIL'}] {path} — {detail}")
    print(f"\nAPI: {passed}/{len(checks)} checks passed")
    return 0 if passed == len(checks) else 3


if __name__ == "__main__":
    raise SystemExit(main())