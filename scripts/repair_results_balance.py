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

from utils import load_results_json, patch_results, save_results  # noqa: E402


def _cfg_capital() -> float:
    try:
        from settings import ConfigSettings

        return float(ConfigSettings().CAPITAL_USDT)
    except Exception:
        return 2000.0


def _is_legacy_balance_event(ev: dict, cfg_capital: float) -> bool:
    if not isinstance(ev, dict) or ev.get("type") != "balance":
        return False
    mode = str(ev.get("mode") or "").lower()
    if mode in {"disabled", "simulated"}:
        return True
    if ev.get("retCode") in (0, "0") or ev.get("http_status") == 200:
        return False
    try:
        te = float(ev.get("total_equity") or 0.0)
    except (TypeError, ValueError):
        te = 0.0
    if cfg_capital > 0 and abs(te - cfg_capital) < 1e-6:
        return True
    return False


def _annotate_legacy(ev: dict) -> dict:
    out = dict(ev)
    out["legacy_simulated"] = True
    out["do_not_use_for_pnl"] = True
    out["note"] = "placeholder CAPITAL_USDT pre-live; archivado en events_legacy"
    return out


def sanitize_balance_events(
    data: dict,
    *,
    cfg_capital: float | None = None,
) -> tuple[dict, dict]:
    """Mueve balances fake (2000/disabled) a events_legacy; deja metrics + wallet live."""
    cfg = float(cfg_capital if cfg_capital is not None else _cfg_capital())
    events = data.get("events") if isinstance(data.get("events"), list) else []
    legacy_prev = data.get("events_legacy") if isinstance(data.get("events_legacy"), list) else []

    kept: list = []
    archived: list = []
    for ev in events:
        if _is_legacy_balance_event(ev, cfg):
            archived.append(_annotate_legacy(ev))
        else:
            kept.append(ev)

    live_ev = _first_live_balance(kept) or _first_live_balance(events)
    if live_ev and not any(e.get("type") == "balance_baseline" for e in kept[:3]):
        baseline = {
            "type": "balance_baseline",
            "source": "bybit_wallet_balance",
            "total_equity": float(live_ev.get("total_equity") or 0.0),
            "available_balance": float(live_ev.get("available_balance") or 0.0),
            "account_type": live_ev.get("account_type"),
            "coin": live_ev.get("coin"),
            "timestamp": live_ev.get("timestamp"),
            "note": "Baseline oficial wallet; ver metadata.capital_inicial",
        }
        kept.insert(0, baseline)

    out = dict(data)
    out["events"] = kept
    out["events_legacy"] = legacy_prev + archived

    meta = out.get("metadata") if isinstance(out.get("metadata"), dict) else {}
    meta = dict(meta)
    meta["legacy_balance_events_archived"] = len(archived)
    meta["events_sanitized_at"] = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ).isoformat()
    meta["events_sanitized_note"] = (
        "Balances mode=disabled/simulated (CAPITAL_USDT) movidos a events_legacy"
    )
    out["metadata"] = meta

    stats = {
        "archived": len(archived),
        "kept": len(kept),
        "events_legacy_total": len(out["events_legacy"]),
        "first_event_type": kept[0].get("type") if kept else None,
    }
    return out, stats


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


def repair(log_dir: str | None = None, *, sanitize_events: bool = True) -> dict:
    log_dir = log_dir or os.path.join(ROOT, "logs")
    data = load_results_json(log_dir=log_dir)
    sanitize_stats: dict = {}
    if sanitize_events and data:
        data, sanitize_stats = sanitize_balance_events(data)

    meta = dict(data.get("metadata") or {}) if isinstance(data.get("metadata"), dict) else {}
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

    cfg_capital = _cfg_capital()

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

    data["metadata"] = meta
    if last_balance:
        data["last_balance"] = last_balance
    save_results(data, log_dir=log_dir)

    return {
        "ok": True,
        "changes": changes,
        "sanitize": sanitize_stats,
        "capital_inicial": meta.get("capital_inicial"),
        "capital_actual": meta.get("capital_actual"),
        "capital_pnl": meta.get("capital_pnl"),
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--no-sanitize-events", action="store_true")
    args = ap.parse_args()
    result = repair(sanitize_events=not args.no_sanitize_events)
    print(json.dumps(result, indent=2, ensure_ascii=False))