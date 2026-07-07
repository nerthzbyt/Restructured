#!/usr/bin/env python3
"""
Sesión de monitoreo 1h: signal lab + health bot + errores/incidencias.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from src_dev.config import DEFAULT_SETTINGS
from src_dev.run_signal_analysis import analyze, load_observations


def _fetch_json(url: str, timeout: float = 8.0) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        return {"error": str(exc)}


def _log_incident(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


async def monitor_session(
    *,
    duration_s: float,
    interval_s: float,
    symbol: str,
    settings,
) -> Dict[str, Any]:
    base = settings.live_api_url.rstrip("/")
    incidents_path = Path(settings.monitor_incidents_path)
    status_path = Path(settings.monitor_status_path)
    t_end = time.time() + duration_s
    ticks: List[Dict[str, Any]] = []
    incidents = 0

    print(f"Monitor {duration_s/60:.0f}min | {symbol} | cada {interval_s}s")
    print(f"  incidents → {incidents_path}")
    print(f"  status    → {status_path}\n")

    i = 0
    while time.time() < t_end:
        i += 1
        t0 = time.time()
        tick: Dict[str, Any] = {"ts": t0, "i": i}

        health = _fetch_json(f"{base}/health")
        status = _fetch_json(f"{base}/api/status")
        metrics = _fetch_json(f"{base}/api/metrics/{symbol}")
        pred = _fetch_json(f"{base}/agent/prediction-level/{symbol}")
        balance = _fetch_json(f"{base}/api/balance?account_type=UNIFIED&coin=USDT")

        tick["health_ok"] = bool(health.get("status") == "healthy" or health.get("ok"))
        tick["bot_running"] = bool(status.get("running"))
        mm = (metrics.get("metrics") or {}) if isinstance(metrics, dict) else {}
        tick["combined"] = mm.get("combined")
        tick["calibrated"] = mm.get("metrics_calibrated")
        tick["decision"] = (pred.get("prediction") or pred).get("decision") if isinstance(pred, dict) else None
        tick["level"] = (pred.get("prediction") or pred).get("level") if isinstance(pred, dict) else None

        bal = balance.get("balance") if isinstance(balance, dict) else {}
        tick["equity"] = (bal or {}).get("total_equity")

        issues: List[str] = []
        if not tick["health_ok"]:
            issues.append("health_down")
        if not tick["bot_running"]:
            issues.append("bot_not_running")
        if mm and not mm.get("data_ok", True):
            issues.append("metrics_data_not_ok")
        if isinstance(balance, dict) and not balance.get("success"):
            issues.append("balance_fetch_failed")
        if isinstance(bal, dict):
            try:
                eq = float(bal.get("total_equity") or 0)
                if 0 < eq < 5000:
                    issues.append("equity_looks_simulated")
            except (TypeError, ValueError):
                pass

        tick["issues"] = issues
        ticks.append(tick)

        if issues:
            incidents += 1
            _log_incident(
                incidents_path,
                {"ts": t0, "issues": issues, "tick": tick, "health": health, "balance": balance},
            )

        print(
            f"[{i}] eq={tick.get('equity')} comb={tick.get('combined')} "
            f"dec={tick.get('decision')} lvl={tick.get('level')} "
            f"issues={issues or 'ok'}"
        )

        sleep_s = max(0.5, interval_s - (time.time() - t0))
        if time.time() + sleep_s < t_end:
            await asyncio.sleep(sleep_s)

    # Análisis signal lab si existe
    lab_report = analyze(load_observations(Path(settings.signal_lab_log_path)))

    summary = {
        "duration_s": duration_s,
        "ticks": len(ticks),
        "incidents": incidents,
        "last_equity": ticks[-1].get("equity") if ticks else None,
        "decision_distribution": _count(t.get("decision") for t in ticks),
        "level_distribution": _count(t.get("level") for t in ticks),
        "issue_counts": _count(issue for t in ticks for issue in (t.get("issues") or [])),
        "signal_lab_analysis": lab_report,
        "ended_at": time.time(),
    }
    status_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def _count(items) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for x in items:
        if x is None:
            continue
        k = str(x)
        out[k] = out.get(k, 0) + 1
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-min", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--symbol", default=None)
    args = parser.parse_args()

    settings = DEFAULT_SETTINGS
    summary = asyncio.run(
        monitor_session(
            duration_s=max(60.0, args.duration_min * 60.0),
            interval_s=max(5.0, args.interval),
            symbol=args.symbol or settings.symbol,
            settings=settings,
        )
    )
    print("\n=== RESUMEN MONITOR ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()