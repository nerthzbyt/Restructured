#!/usr/bin/env python3
"""Repara capital_inicial hardcodeado (CAPITAL_USDT) en results.json cuando hay balance live."""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from utils import load_results_json, patch_results  # noqa: E402


def _first_live_balance(events: list) -> dict | None:
    for ev in events:
        if not isinstance(ev, dict) or ev.get("type") != "balance":
            continue
        mode = str(ev.get("mode") or "").lower()
        if mode in {"disabled", "simulated"}:
            continue
        if ev.get("retCode") not in (None, 0, "0"):
            continue
        try:
            te = float(ev.get("total_equity") or 0.0)
        except (TypeError, ValueError):
            te = 0.0
        if te > 0:
            return ev
    return None


def repair(log_dir: str | None = None) -> dict:
    log_dir = log_dir or os.path.join(ROOT, "logs")
    data = load_results_json(log_dir=log_dir)
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    events = data.get("events") if isinstance(data.get("events"), list) else []
    last_balance = data.get("last_balance") if isinstance(data.get("last_balance"), dict) else {}

    live_ev = _first_live_balance(events)
    live_equity = None
    if live_ev:
        live_equity = float(live_ev.get("total_equity") or 0.0)
    elif last_balance.get("retCode") in (None, 0, "0"):
        try:
            live_equity = float(last_balance.get("total_equity") or 0.0)
        except (TypeError, ValueError):
            live_equity = None

    if not live_equity or live_equity <= 0:
        return {"ok": False, "message": "no_live_balance_found"}

    prev_initial = meta.get("capital_inicial")
    prev_source = str(meta.get("capital_source") or "")
    changes = {}

    try:
        prev_f = float(prev_initial)
    except (TypeError, ValueError):
        prev_f = 0.0

    cfg_capital = 2000.0
    try:
        from settings import ConfigSettings

        cfg_capital = float(ConfigSettings().CAPITAL_USDT)
    except Exception:
        pass

    should_fix = (
        prev_source != "bybit_wallet_balance"
        or (cfg_capital > 0 and abs(prev_f - cfg_capital) < 1e-6 and live_equity > cfg_capital * 1.5)
        or prev_f <= 0
    )

    if should_fix:
        new_initial = float(live_ev.get("total_equity") or live_equity) if live_ev else live_equity
        changes["capital_inicial"] = {"from": prev_initial, "to": new_initial}
        meta["capital_inicial"] = round(new_initial, 6)
        meta["capital_source"] = "bybit_wallet_balance"
        meta["capital_inicial_repaired_at"] = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()
        meta["capital_inicial_repair_note"] = "baseline desde primer balance live (no CAPITAL_USDT)"

    capital_actual = float(meta.get("capital_actual") or last_balance.get("total_equity") or live_equity)
    meta["capital_actual"] = round(capital_actual, 6)
    meta["capital_final"] = round(capital_actual, 6)
    meta["capital_pnl"] = round(capital_actual - float(meta.get("capital_inicial") or capital_actual), 6)

    patch_results({"metadata": meta, "last_balance": last_balance or None}, log_dir=log_dir)

    return {
        "ok": True,
        "changes": changes,
        "capital_inicial": meta.get("capital_inicial"),
        "capital_actual": meta.get("capital_actual"),
        "capital_pnl": meta.get("capital_pnl"),
    }


if __name__ == "__main__":
    result = repair()
    print(json.dumps(result, indent=2, ensure_ascii=False))