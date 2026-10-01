#!/usr/bin/env python3
"""
Barrida completa: todas las combinaciones de órdenes × perfiles de parámetros del sistema.

Produce JSON completo sin truncar (ranked_all) para filtrar top real y planificar
factorización de Nertzh (~70% reducción usando módulos src_dev).
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import time
from datetime import datetime, timezone
from itertools import product
from typing import Any, Dict, List, Optional, Tuple

from src_dev.config import OUTPUT_DIR, DevSettings, load_trading_thresholds
from src_dev.orders.lab import _round_to_tick, _scored_to_dict, _trigger_from_tick
from src_dev.collectors.multi_connection import build_multi_connection_context
from src_dev.orders.combinator import build_order_body, iter_spot_combinations, qty_for_notional, resolve_limit_price
from src_dev.orders.exchange_catalog import fetch_exchange_orders, summarize_exchange_orders
from src_dev.orders.scorer import rank_all, score_combination

# Perfiles de umbrales (optimizer + .env actual + variantes)
THRESHOLD_PROFILES: List[Dict[str, float]] = [
    {"combined_buy": 1.0, "combined_sell": -1.0, "combined_hold_band": 0.3},
    {"combined_buy": 1.25, "combined_sell": -1.25, "combined_hold_band": 0.4},
    {"combined_buy": 1.5, "combined_sell": -1.5, "combined_hold_band": 0.5},  # .env actual
    {"combined_buy": 2.0, "combined_sell": -2.0, "combined_hold_band": 0.75},
    {"combined_buy": 3.0, "combined_sell": -3.0, "combined_hold_band": 1.0},
    {"combined_buy": 6.0, "combined_sell": -6.0, "combined_hold_band": 3.0},  # settings default
    {"combined_buy": 8.0, "combined_sell": -8.0, "combined_hold_band": 2.0},  # Nertzh fallback
    {"combined_buy": 1.5, "combined_sell": -1.5, "combined_hold_band": 1.5},  # hold amplio
]

TP_SL_PROFILES: List[Dict[str, float]] = [
    {"tp_pct": 0.2, "sl_pct": 0.15},
    {"tp_pct": 0.3, "sl_pct": 0.2},   # .env actual
    {"tp_pct": 0.5, "sl_pct": 0.3},
    {"tp_pct": 0.3, "sl_pct": 0.5},
    {"tp_pct": 1.5, "sl_pct": 0.5},   # settings default
    {"tp_pct": 1.5, "sl_pct": 1.0},
]

NERTZH_FACTORIZATION_MODULES = [
    {
        "module": "nertz_engine.orders.placement",
        "source_lines": "Nertzh.py:_place_order (~3660-3740)",
        "replace_with": "src_dev/orders/combinator.build_order_body + exchange_schema",
        "estimated_reduction_pct": 12,
    },
    {
        "module": "nertz_engine.signals.decision",
        "source_lines": "Nertzh.py:_determine_decision (~1504-1544)",
        "replace_with": "src/optimizer thresholds + utils metrics (already external)",
        "estimated_reduction_pct": 8,
    },
    {
        "module": "nertz_engine.orders.tpsl",
        "source_lines": "Nertzh.py:AUTO_TPSL block (~2580-2900)",
        "replace_with": "dedicated TPSL service using lab tp_sl_mode profiles",
        "estimated_reduction_pct": 15,
    },
    {
        "module": "nertz_engine.exchange.client",
        "source_lines": "Nertzh.py:_bybit_client, instrument rules scattered",
        "replace_with": "bybit_v5.BybitV5Client + src_dev/orders/exchange_catalog",
        "estimated_reduction_pct": 10,
    },
    {
        "module": "nertz_engine.metrics.loop",
        "source_lines": "Nertzh.py:main cycle, save_results, thresholds persist",
        "replace_with": "src_dev/collectors/multi_connection + nertz_engine/storage",
        "estimated_reduction_pct": 20,
    },
    {
        "module": "nertz_engine.admin.api",
        "source_lines": "Nertzh.py:FastAPI routes tail",
        "replace_with": "nertz_engine/api/ router modules",
        "estimated_reduction_pct": 15,
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_live_execution_bonus() -> Dict[str, Dict[str, Any]]:
    """Bonus de ejecución real desde probes del laboratorio."""
    bonus: Dict[str, Dict[str, Any]] = {}
    for fname in ("live_probes_quick.json", "live_exchange_verify.json"):
        path = os.path.join(OUTPUT_DIR, fname)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = data.get("probes") or data.get("live_probe_results") or data.get("true_top_by_execution") or []
        for row in rows:
            if not row.get("filled"):
                continue
            label = row.get("label") or ""
            ot = row.get("order_type") or ""
            tif = row.get("time_in_force") or ""
            mu = row.get("market_unit") or row.get("body_sent", {}).get("marketUnit")
            key = f"{ot}|{tif}|{mu or '-'}"
            if label:
                key = label.replace("+", "|")
            slip = float(row.get("slippage_bps") or 0)
            exec_score = max(0.0, min(100.0, 90.0 - abs(slip) * 2.0))
            bonus[key] = {
                "execution_score": round(exec_score, 2),
                "slippage_bps": slip,
                "filled": True,
                "source_file": fname,
            }
    return bonus


def _nertzh_production_fit(combo_row: Dict[str, Any], current_env: Dict[str, str]) -> float:
    """
    Qué tan implementable es el combo con Nertzh actual (solo ORDER_TYPE/TIF/marketUnit fijos).
    1.0 = coincide con producción; 0.5 = viable con cambio mínimo .env; 0.2 = requiere refactor.
    """
    ot = combo_row.get("order_type") or ""
    tif = combo_row.get("time_in_force") or ""
    mu = combo_row.get("market_unit") or "-"
    flt = combo_row.get("order_filter") or "Order"
    tp_sl = combo_row.get("tp_sl_mode") or "none"
    slip = (combo_row.get("slippage") or {}).get("type")
    env_ot = current_env.get("ORDER_TYPE", "Limit")
    env_tif = current_env.get("TIME_IN_FORCE", "GTC")

    score = 0.2
    if ot == env_ot and tif == env_tif:
        score = 0.7
    elif ot == "Market" and tif == "IOC":
        score = 0.6
    if flt != "Order":
        score *= 0.5
    if tp_sl not in ("none",):
        score *= 0.6  # Nertzh usa TPSL virtual, no bracket nativo
    if slip:
        score *= 0.4
    if ot == "Market" and mu == "baseCoin":
        score = min(1.0, score + 0.25)
    if combo_row.get("price_anchor") in ("best_bid", "best_ask"):
        score = min(1.0, score + 0.15)
    return round(score, 3)


def _system_profile_id(tp_sl: Dict[str, float], th: Dict[str, float]) -> str:
    return (
        f"buy{tp_sl.get('tp_pct')}_sl{tp_sl.get('sl_pct')}"
        f"_cb{th['combined_buy']}_cs{th['combined_sell']}_hb{th['combined_hold_band']}"
    )


def _combo_execution_key(row: Dict[str, Any]) -> str:
    mu = row.get("market_unit") or "-"
    return f"{row.get('order_type')}|{row.get('time_in_force')}|{mu}"


async def run_full_sweep(
    symbol: str,
    *,
    include_slippage: bool = True,
    score_both_sides: bool = True,
    ws_duration_s: float = 4.0,
) -> Dict[str, Any]:
    t0 = time.time()
    cfg = DevSettings.from_env()
    cfg.lab_order_stats_source = str(os.getenv("DEV_LAB_ORDER_STATS_SOURCE", "exchange") or "exchange")
    base_thresholds = load_trading_thresholds()
    notional = float(base_thresholds["capital_usdt"])

    ctx = await build_multi_connection_context(symbol, cfg, ws_duration_s=ws_duration_s)
    exchange_orders = await fetch_exchange_orders(symbol, settings=cfg)
    order_stats = summarize_exchange_orders(exchange_orders)
    observed = order_stats.get("observed_combo_counts") or {}

    metrics = ctx.get("metrics") or {}
    ob_stats = dict(ctx.get("orderbook_stats") or {})
    constraints = ctx["constraints"]
    ob_stats["tick_size"] = constraints.tick_size
    last_price = float(metrics.get("last_price") or ob_stats.get("mid") or 0.0)
    metric_history_len = int(ctx.get("metric_history_len") or 0)
    combined = float(metrics.get("combined") or 0.0)

    exec_bonus = _load_live_execution_bonus()
    current_env = {
        "ORDER_TYPE": str(os.getenv("ORDER_TYPE", "Limit")),
        "TIME_IN_FORCE": str(os.getenv("TIME_IN_FORCE", "GTC")),
    }

    all_entries: List[Dict[str, Any]] = []
    system_profiles = [
        {**tp, **th, "profile_id": _system_profile_id(tp, th)}
        for tp, th in product(TP_SL_PROFILES, THRESHOLD_PROFILES)
    ]

    sides = ["Buy", "Sell"] if score_both_sides else [
        "Buy" if combined >= float(base_thresholds["combined_buy"])
        else "Sell" if combined <= float(base_thresholds["combined_sell"])
        else ("Buy" if combined >= 0 else "Sell")
    ]

    valid_combos = list(iter_spot_combinations(side_hint="Buy", include_slippage=include_slippage))
    valid_count = len(valid_combos)

    for sys_prof in system_profiles:
        thresholds = dict(base_thresholds)
        thresholds["combined_buy"] = float(sys_prof["combined_buy"])
        thresholds["combined_sell"] = float(sys_prof["combined_sell"])
        thresholds["combined_hold_band"] = float(sys_prof["combined_hold_band"])
        thresholds["tp_pct"] = float(sys_prof["tp_pct"])
        thresholds["sl_pct"] = float(sys_prof["sl_pct"])

        for side in sides:
            for combo in iter_spot_combinations(side_hint=side, include_slippage=include_slippage):
                price_f = (
                    resolve_limit_price(combo.price_anchor or "mid", ob_stats, combined)
                    if combo.order_type == "Limit"
                    else last_price
                )
                qty = qty_for_notional(notional, price_f or last_price, constraints)
                tp = last_price * (1 + float(thresholds["tp_pct"]) / 100.0)
                sl = last_price * (1 - float(thresholds["sl_pct"]) / 100.0)
                trigger = _trigger_from_tick(last_price, constraints.tick_size, side)
                body = build_order_body(
                    combo,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    price=str(_round_to_tick(price_f, constraints.tick_size))
                    if combo.order_type == "Limit"
                    else None,
                    trigger_price=trigger,
                    take_profit=str(_round_to_tick(tp, constraints.tick_size)),
                    stop_loss=str(_round_to_tick(sl, constraints.tick_size)),
                )
                scored = score_combination(
                    combo,
                    metrics,
                    ob_stats,
                    constraints,
                    body_preview=body,
                    thresholds=thresholds,
                    observed_combo_counts=observed,
                    min_calibration_samples=cfg.lab_min_calibration_samples,
                    metric_history_len=metric_history_len,
                )
                row = _scored_to_dict(scored, 0)
                exec_key = _combo_execution_key(row)
                exec_data = exec_bonus.get(exec_key) or exec_bonus.get(
                    f"{row.get('order_type')}|{row.get('time_in_force')}|{row.get('market_unit') or '-'}"
                )
                execution_score = float((exec_data or {}).get("execution_score") or 0.0)
                nertzh_fit = _nertzh_production_fit(row, current_env)

                composite = round(
                    scored.score * 0.75
                    + execution_score * 0.15 * (1.0 if execution_score else 0.0)
                    + nertzh_fit * 100.0 * 0.10,
                    4,
                )
                if execution_score:
                    composite = round(scored.score * 0.65 + execution_score * 0.25 + nertzh_fit * 100.0 * 0.10, 4)

                entry = {
                    "rank": 0,
                    "composite_score": composite,
                    "lab_score": scored.score,
                    "execution_score": execution_score or None,
                    "nertzh_production_fit": nertzh_fit,
                    "system_profile_id": sys_prof["profile_id"],
                    "system_params": {
                        "combined_buy": thresholds["combined_buy"],
                        "combined_sell": thresholds["combined_sell"],
                        "combined_hold_band": thresholds["combined_hold_band"],
                        "tp_pct": thresholds["tp_pct"],
                        "sl_pct": thresholds["sl_pct"],
                        "risk_reward": round(thresholds["tp_pct"] / max(thresholds["sl_pct"], 1e-9), 3),
                    },
                    "combo_id": row["combo_id"],
                    "full_id": f"{sys_prof['profile_id']}::{row['combo_id']}",
                    "order_type": row["order_type"],
                    "time_in_force": row["time_in_force"],
                    "market_unit": row["market_unit"],
                    "order_filter": row["order_filter"],
                    "price_anchor": row["price_anchor"],
                    "tp_sl_mode": row["tp_sl_mode"],
                    "slippage": row["slippage"],
                    "side_hint": row["side_hint"],
                    "rank_factors": row["rank_factors"],
                    "rationale": row["rationale"],
                    "body_preview": row["body_preview"],
                    "live_execution": exec_data,
                    "recommended_env": {
                        "ORDER_TYPE": row["order_type"],
                        "TIME_IN_FORCE": row["time_in_force"],
                        "TP_PERCENTAGE": thresholds["tp_pct"],
                        "SL_PERCENTAGE": thresholds["sl_pct"],
                        "COMBINED_BUY_THRESHOLD": thresholds["combined_buy"],
                        "COMBINED_SELL_THRESHOLD": thresholds["combined_sell"],
                        "COMBINED_HOLD_BAND": thresholds["combined_hold_band"],
                    },
                }
                all_entries.append(entry)

    all_entries.sort(key=lambda x: x["composite_score"], reverse=True)
    for i, e in enumerate(all_entries):
        e["rank"] = i + 1

    # Agrupaciones útiles
    by_order_combo: Dict[str, Dict[str, Any]] = {}
    for e in all_entries:
        cid = e["combo_id"]
        if cid not in by_order_combo or e["composite_score"] > by_order_combo[cid]["best_composite"]:
            by_order_combo[cid] = {
                "combo_id": cid,
                "best_composite": e["composite_score"],
                "best_system_profile": e["system_profile_id"],
                "best_rank": e["rank"],
                "order_type": e["order_type"],
                "time_in_force": e["time_in_force"],
            }

    best_order_combos = sorted(by_order_combo.values(), key=lambda x: x["best_composite"], reverse=True)

    unique_full_ids = len({e["full_id"] for e in all_entries})
    report: Dict[str, Any] = {
        "generated_at": _utc_now(),
        "symbol": symbol,
        "sweep_version": "1.0.0",
        "duration_s": round(time.time() - t0, 2),
        "data_source": "bybit_exchange_only",
        "stats_source": order_stats.get("stats_source"),
        "live_metrics": {
            "combined": combined,
            "last_price": last_price,
            "metrics_calibrated": metrics.get("metrics_calibrated"),
            "pio": metrics.get("pio"),
            "egm": metrics.get("egm"),
            "volatility": metrics.get("volatility"),
        },
        "exchange_order_stats": order_stats,
        "sweep_dimensions": {
            "valid_order_profiles": valid_count,
            "threshold_profiles": len(THRESHOLD_PROFILES),
            "tp_sl_profiles": len(TP_SL_PROFILES),
            "system_param_combinations": len(system_profiles),
            "sides_scored": len(sides),
            "include_slippage": include_slippage,
            "total_scored": len(all_entries),
            "unique_full_ids": unique_full_ids,
        },
        "production_env_current": {
            "ORDER_TYPE": current_env["ORDER_TYPE"],
            "TIME_IN_FORCE": current_env["TIME_IN_FORCE"],
            **{k: base_thresholds.get(k) for k in (
                "combined_buy", "combined_sell", "combined_hold_band", "tp_pct", "sl_pct", "capital_usdt"
            )},
        },
        "nertzh_factorization_plan": {
            "target_reduction_pct": 70,
            "modules": NERTZH_FACTORIZATION_MODULES,
            "estimated_total_reduction_pct": sum(m["estimated_reduction_pct"] for m in NERTZH_FACTORIZATION_MODULES),
            "reuse_from_src_dev": [
                "orders/combinator.py",
                "orders/scorer.py",
                "orders/exchange_schema.py",
                "orders/exchange_catalog.py",
                "collectors/multi_connection.py",
                "config.load_trading_thresholds",
            ],
            "nertzh_lines_approx": 5513,
            "keep_in_nertzh": ["orchestration loop", "symbol registry", "FastAPI app shell"],
        },
        "top_recommendations": {
            "best_overall": all_entries[:50],
            "best_order_combo_unique": best_order_combos[:100],
            "best_nertzh_ready": [e for e in all_entries if e["nertzh_production_fit"] >= 0.85][:50],
            "best_with_live_execution": [e for e in all_entries if e.get("execution_score")][:30],
            "best_market_ioc": [e for e in all_entries if e["order_type"] == "Market"][:30],
            "best_limit": [e for e in all_entries if e["order_type"] == "Limit"][:30],
        },
        "ranked_all": all_entries,
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Barrida completa sistema × órdenes")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--no-slippage", action="store_true")
    parser.add_argument("--single-side", action="store_true")
    parser.add_argument("--ws-s", type=float, default=4.0)
    parser.add_argument("--json-stdout", action="store_true", help="Dump completo a stdout (muy grande)")
    args = parser.parse_args()

    cfg = DevSettings.from_env()
    symbol = args.symbol or cfg.symbol

    report = asyncio.run(
        run_full_sweep(
            symbol,
            include_slippage=not args.no_slippage,
            score_both_sides=not args.single_side,
            ws_duration_s=args.ws_s,
        )
    )

    out_path = os.path.join(OUTPUT_DIR, "full_system_sweep.json")
    summary_path = os.path.join(OUTPUT_DIR, "full_system_sweep_summary.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    summary = {
        k: report[k]
        for k in report
        if k != "ranked_all"
    }
    summary["ranked_all_count"] = len(report["ranked_all"])
    summary["output_full_path"] = out_path
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    dims = report["sweep_dimensions"]
    print("\n=== FULL SYSTEM SWEEP ===")
    print(f"symbol={symbol} total_scored={dims['total_scored']} duration={report['duration_s']}s")
    print(f"system_profiles={dims['system_param_combinations']} order_profiles={dims['valid_order_profiles']}")
    best = report["top_recommendations"]["best_overall"][:5]
    print("\nTOP 5 composite:")
    for row in best:
        print(
            f"  #{row['rank']} composite={row['composite_score']} lab={row['lab_score']} "
            f"{row['order_type']}+{row['time_in_force']} | sys={row['system_profile_id']}"
        )
    print(f"\nFull JSON: {out_path} ({os.path.getsize(out_path) / 1024 / 1024:.1f} MB)")
    print(f"Summary:   {summary_path}\n")

    if args.json_stdout:
        print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()