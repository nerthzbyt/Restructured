#!/usr/bin/env python3
"""
Verificación live: ranking old_results vs exchange API + operaciones reales demo.

Compara el impacto del gate de estadísticas (old_results histórico vs historial
paginado del exchange) y ejecuta perfiles diversos en Bybit demo para medir fills reales.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from src_dev.config import OUTPUT_DIR, DevSettings, load_trading_thresholds, private_rest_base_url
from src_dev.orders.lab import _round_to_tick, _scored_to_dict, _trigger_from_tick
from src_dev.collectors.multi_connection import build_multi_connection_context
from src_dev.orders.combinator import build_order_body, iter_spot_combinations, qty_for_notional, resolve_limit_price
from src_dev.orders.exchange_catalog import fetch_exchange_orders, summarize_exchange_orders
from src_dev.orders.scorer import rank_top_n, score_combination

try:
    from bybit_v5 import BybitV5Client
except ImportError:
    BybitV5Client = None  # type: ignore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _wallet_usdt_async(client: Any) -> Dict[str, Any]:
    bal = await client.wallet_balance("UNIFIED", "USDT")
    out: Dict[str, Any] = {"retCode": bal.get("retCode"), "retMsg": bal.get("retMsg")}
    if bal.get("retCode") != 0:
        return out
    lst = (bal.get("result") or {}).get("list") or []
    if not lst:
        return out
    coins = (lst[0].get("coin") or [])
    for c in coins:
        if str(c.get("coin") or "").upper() == "USDT":
            out["equity"] = float(c.get("equity") or 0)
            out["available"] = float(c.get("availableToWithdraw") or c.get("walletBalance") or 0)
            out["wallet_balance"] = float(c.get("walletBalance") or 0)
            break
    out["total_equity"] = float(lst[0].get("totalEquity") or 0)
    return out


async def _order_stats_for_source(
    stats_source: str,
    *,
    symbol: str,
    cfg: DevSettings,
) -> Dict[str, Any]:
    cfg_copy = copy.copy(cfg)
    cfg_copy.lab_order_stats_source = stats_source
    exchange_orders = await fetch_exchange_orders(symbol, settings=cfg_copy)
    return summarize_exchange_orders(exchange_orders)


async def _score_rank_with_context(
    *,
    symbol: str,
    cfg: DevSettings,
    ctx: Dict[str, Any],
    order_stats: Dict[str, Any],
    stats_source: str,
    top_n: int,
    notional_usdt: float,
) -> Dict[str, Any]:
    thresholds = load_trading_thresholds()
    metrics = ctx.get("metrics") or {}
    ob_stats = dict(ctx.get("orderbook_stats") or {})
    constraints = ctx["constraints"]
    ob_stats["tick_size"] = constraints.tick_size
    last_price = float(metrics.get("last_price") or ob_stats.get("mid") or 0.0)
    observed = order_stats.get("observed_combo_counts") or {}
    metric_history_len = int(ctx.get("metric_history_len") or 0)
    combined = float(metrics.get("combined") or 0.0)

    scored = []
    for side in ("Buy", "Sell"):
        for combo in iter_spot_combinations(side_hint=side, include_slippage=True):
            price_f = (
                resolve_limit_price(combo.price_anchor or "mid", ob_stats, combined)
                if combo.order_type == "Limit"
                else last_price
            )
            qty = qty_for_notional(notional_usdt, price_f or last_price, constraints)
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
            scored.append(
                score_combination(
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
            )

    top = rank_top_n(scored, n=top_n)
    return {
        "stats_source": stats_source,
        "order_stats": order_stats,
        "live_metrics": {
            "combined": metrics.get("combined"),
            "last_price": last_price,
            "metrics_calibrated": metrics.get("metrics_calibrated"),
        },
        "top": [_scored_to_dict(t, i + 1) for i, t in enumerate(top)],
        "ranked_count": len(scored),
    }


def _diverse_profiles(ranked: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen_keys: set[str] = set()
    picked: List[Dict[str, Any]] = []
    for row in ranked:
        key = f"{row.get('order_type')}|{row.get('time_in_force')}|{row.get('order_filter')}|{row.get('tp_sl_mode')}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        picked.append(row)
        if len(picked) >= limit:
            break
    return picked


def _test_qty(constraints: Any, last_price: float, test_notional: float) -> str:
    return qty_for_notional(max(test_notional, float(constraints.min_notional or 5.0)), last_price, constraints)


async def _execute_live_probes(
    profiles: List[Dict[str, Any]],
    *,
    symbol: str,
    ctx: Dict[str, Any],
    test_notional_usdt: float,
    wait_fill_s: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    if BybitV5Client is None:
        raise RuntimeError("bybit_v5 no disponible")

    key = os.getenv("BYBIT_API_KEY", "")
    secret = os.getenv("BYBIT_API_SECRET", "")
    if not key or not secret:
        raise RuntimeError("BYBIT_API_KEY/SECRET no configuradas")

    constraints = ctx["constraints"]
    ob_stats = dict(ctx.get("orderbook_stats") or {})
    ob_stats["tick_size"] = constraints.tick_size
    metrics = ctx.get("metrics") or {}
    thresholds = load_trading_thresholds()
    last_price = float(metrics.get("last_price") or ob_stats.get("mid") or 0.0)
    combined = float(metrics.get("combined") or 0.0)

    client = BybitV5Client(key, secret, base_url=private_rest_base_url())
    wallet_before = await _wallet_usdt_async(client)
    results: List[Dict[str, Any]] = []

    try:
        from src_dev.orders.exchange_schema import OrderCombination

        for prof in profiles:
            side = str(prof.get("side_hint") or "Buy")
            order_type = str(prof.get("order_type") or "Limit")
            tif = str(prof.get("time_in_force") or "GTC")
            anchor = prof.get("price_anchor")
            tp_sl = str(prof.get("tp_sl_mode") or "none")

            combo = OrderCombination(
                order_type=order_type,
                time_in_force=tif,
                market_unit=prof.get("market_unit"),
                order_filter=str(prof.get("order_filter") or "Order"),
                is_leverage=0,
                price_anchor=anchor,
                tp_sl_mode=tp_sl,
                slippage_type=(prof.get("slippage") or {}).get("type"),
                slippage_value=(prof.get("slippage") or {}).get("value"),
                side_hint=side,
                valid=True,
                invalid_reason="",
            )

            qty = _test_qty(constraints, last_price, test_notional_usdt)
            if order_type == "Limit":
                price_f = resolve_limit_price(anchor or "mid", ob_stats, combined)
                if side == "Buy":
                    ask = float(ob_stats.get("best_ask") or last_price)
                    price_f = max(price_f, ask)
                else:
                    bid = float(ob_stats.get("best_bid") or last_price)
                    price_f = min(price_f, bid)
                price_s = str(_round_to_tick(price_f, constraints.tick_size))
            else:
                price_s = None

            tp = last_price * (1 + float(thresholds["tp_pct"]) / 100.0)
            sl = last_price * (1 - float(thresholds["sl_pct"]) / 100.0)
            trigger = _trigger_from_tick(last_price, constraints.tick_size, side)
            body = build_order_body(
                combo,
                symbol=symbol,
                side=side,
                qty=qty,
                price=price_s,
                trigger_price=trigger,
                take_profit=str(_round_to_tick(tp, constraints.tick_size)),
                stop_loss=str(_round_to_tick(sl, constraints.tick_size)),
            )
            body["orderLinkId"] = f"liveverify-{uuid.uuid4().hex[:12]}"

            t0 = time.time()
            create_res = await client.create_order(body)
            order_id = str((create_res.get("result") or {}).get("orderId") or "")
            status = "unknown"
            avg_price = 0.0
            exec_qty = 0.0
            fee = 0.0
            cancel_res = None

            await asyncio.sleep(max(0.5, wait_fill_s))

            if order_id:
                rt = await client.order_realtime(category="spot", symbol=symbol, order_id=order_id)
                row = ((rt.get("result") or {}).get("list") or [{}])[0]
                status = str(row.get("orderStatus") or "unknown")
                try:
                    avg_price = float(row.get("avgPrice") or 0)
                    exec_qty = float(row.get("cumExecQty") or 0)
                except (TypeError, ValueError):
                    pass
                fee_detail = row.get("cumFeeDetail") or {}
                if isinstance(fee_detail, dict):
                    for v in fee_detail.values():
                        try:
                            fee += abs(float(v))
                        except (TypeError, ValueError):
                            pass
                if status not in ("Filled", "PartiallyFilled"):
                    cancel_res = await client.cancel_order(
                        {"category": "spot", "symbol": symbol, "orderId": order_id}
                    )

            latency_ms = round((time.time() - t0) * 1000.0, 1)
            slippage_bps = 0.0
            if avg_price > 0 and last_price > 0:
                if side == "Buy":
                    slippage_bps = (avg_price - last_price) / last_price * 10000.0
                else:
                    slippage_bps = (last_price - avg_price) / last_price * 10000.0

            results.append(
                {
                    "combo_id": prof.get("combo_id"),
                    "rank": prof.get("rank"),
                    "lab_score": prof.get("score"),
                    "order_type": order_type,
                    "time_in_force": tif,
                    "tp_sl_mode": tp_sl,
                    "side": side,
                    "test_notional_usdt": test_notional_usdt,
                    "qty": qty,
                    "create_retCode": create_res.get("retCode"),
                    "create_retMsg": create_res.get("retMsg"),
                    "order_id": order_id,
                    "final_status": status,
                    "avg_price": avg_price,
                    "cum_exec_qty": exec_qty,
                    "fee": fee,
                    "latency_ms": latency_ms,
                    "slippage_bps": round(slippage_bps, 4),
                    "filled": status in ("Filled", "PartiallyFilled"),
                    "cancel_retCode": (cancel_res or {}).get("retCode") if cancel_res else None,
                }
            )
            await asyncio.sleep(0.2)
    finally:
        wallet_after = await _wallet_usdt_async(client)
        await client.aclose()

    return results, wallet_before, wallet_after


async def run_verify(
    *,
    symbol: str,
    top_n: int,
    execute_n: int,
    test_notional: float,
    wait_fill_s: float,
) -> Dict[str, Any]:
    cfg = DevSettings.from_env()
    notional = float(load_trading_thresholds()["capital_usdt"])
    ws_s = min(4.0, float(cfg.lab_ws_probe_s))

    ctx = await build_multi_connection_context(symbol, cfg, ws_duration_s=ws_s)
    old_stats, exch_stats = await asyncio.gather(
        _order_stats_for_source("old_results", symbol=symbol, cfg=cfg),
        _order_stats_for_source("exchange", symbol=symbol, cfg=cfg),
    )

    old_rank, exch_rank = await asyncio.gather(
        _score_rank_with_context(
            symbol=symbol, cfg=cfg, ctx=ctx, order_stats=old_stats,
            stats_source="old_results", top_n=top_n, notional_usdt=notional,
        ),
        _score_rank_with_context(
            symbol=symbol, cfg=cfg, ctx=ctx, order_stats=exch_stats,
            stats_source="exchange", top_n=top_n, notional_usdt=notional,
        ),
    )

    diverse = _diverse_profiles(exch_rank["top"], execute_n) or _diverse_profiles(old_rank["top"], execute_n)

    live_results: List[Dict[str, Any]] = []
    wallet_before: Dict[str, Any] = {}
    wallet_after: Dict[str, Any] = {}
    live_error = ""

    try:
        live_results, wallet_before, wallet_after = await _execute_live_probes(
            diverse, symbol=symbol, ctx=ctx, test_notional_usdt=test_notional, wait_fill_s=wait_fill_s,
        )
    except Exception as e:
        live_error = str(e)

    exch_stats_after = await _order_stats_for_source("exchange", symbol=symbol, cfg=cfg)
    exch_rank_after = await _score_rank_with_context(
        symbol=symbol, cfg=cfg, ctx=ctx, order_stats=exch_stats_after,
        stats_source="exchange", top_n=top_n, notional_usdt=notional,
    )

    live_ranked = sorted(
        live_results,
        key=lambda r: (
            1 if r.get("filled") else 0,
            -float(r.get("lab_score") or 0),
            -abs(float(r.get("slippage_bps") or 0)),
        ),
        reverse=True,
    )
    for i, row in enumerate(live_ranked):
        row["live_rank"] = i + 1

    report: Dict[str, Any] = {
        "generated_at": _utc_now(),
        "symbol": symbol,
        "bybit_env": os.getenv("BYBIT_ENV", "mainnet"),
        "notional_lab_usdt": notional,
        "test_notional_per_probe_usdt": test_notional,
        "gate_comparison": {
            "old_results": {
                "orders_sampled": old_stats.get("total_orders_sampled"),
                "distribution": old_stats.get("combo_distribution_pct"),
                "top_n": old_rank["top"],
            },
            "exchange_before": {
                "orders_sampled": exch_stats.get("total_orders_sampled"),
                "distribution": exch_stats.get("combo_distribution_pct"),
                "top_n": exch_rank["top"],
            },
            "exchange_after_live_ops": {
                "orders_sampled": exch_stats_after.get("total_orders_sampled"),
                "distribution": exch_stats_after.get("combo_distribution_pct"),
                "top_n": exch_rank_after["top"],
            },
        },
        "live_metrics": exch_rank.get("live_metrics"),
        "wallet": {"before": wallet_before, "after": wallet_after},
        "live_probes_executed": len(live_results),
        "live_probe_results": live_results,
        "true_top_by_execution": live_ranked[:top_n],
        "live_error": live_error or None,
    }

    out_path = os.path.join(OUTPUT_DIR, "live_exchange_verify.json")
    md_path = os.path.join(OUTPUT_DIR, "LIVE_VERIFY_RESUMEN.md")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    _write_md(md_path, report)
    report["output_files"] = {"json": out_path, "summary": md_path}
    return report


def _write_md(path: str, report: Dict[str, Any]) -> None:
    gc = report.get("gate_comparison") or {}
    lines = [
        "# Verificación live — gate old_results vs exchange",
        "",
        f"Generado: {report.get('generated_at')}",
        f"Símbolo: **{report.get('symbol')}** | Env: **{report.get('bybit_env')}**",
        "",
        "## Gate de estadísticas",
        "",
    ]
    for label, key in (
        ("old_results (histórico)", "old_results"),
        ("exchange API (antes)", "exchange_before"),
        ("exchange API (después ops live)", "exchange_after_live_ops"),
    ):
        block = gc.get(key) or {}
        lines.append(f"### {label}")
        lines.append(f"- órdenes muestreadas: **{block.get('orders_sampled')}**")
        for k, pct in list((block.get("distribution") or {}).items())[:8]:
            lines.append(f"  - `{k}`: {pct}%")
        lines.append("")

    lines.extend(["## Top por fuente (score laboratorio)", ""])
    for label, key in (("old_results", "old_results"), ("exchange (post-ops)", "exchange_after_live_ops")):
        block = gc.get(key) or {}
        lines.append(f"### {label}")
        for row in (block.get("top_n") or [])[:5]:
            lines.append(
                f"- #{row.get('rank')} score={row.get('score')} | "
                f"{row.get('order_type')}+{row.get('time_in_force')} | "
                f"obs={row.get('rank_factors', {}).get('exchange_observed')}"
            )
        lines.append("")

    lines.extend(["## True top por ejecución real (demo)", ""])
    for row in report.get("true_top_by_execution") or []:
        lines.append(
            f"- live #{row.get('live_rank')} | lab #{row.get('rank')} score={row.get('lab_score')} | "
            f"{row.get('order_type')}+{row.get('time_in_force')} | "
            f"filled={row.get('filled')} slippage_bps={row.get('slippage_bps')} latency_ms={row.get('latency_ms')}"
        )

    wb = (report.get("wallet") or {}).get("before") or {}
    wa = (report.get("wallet") or {}).get("after") or {}
    lines.extend([
        "",
        "## Balance demo",
        f"- antes: equity={wb.get('equity')} available={wb.get('available')}",
        f"- después: equity={wa.get('equity')} available={wa.get('available')}",
        "",
    ])
    if report.get("live_error"):
        lines.append(f"**Error live:** {report['live_error']}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verificación live exchange vs old_results")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--execute", type=int, default=6)
    parser.add_argument("--test-notional", type=float, default=12.0)
    parser.add_argument("--wait-fill", type=float, default=2.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    cfg = DevSettings.from_env()
    symbol = args.symbol or cfg.symbol
    report = asyncio.run(
        run_verify(
            symbol=symbol,
            top_n=max(1, args.top),
            execute_n=max(1, args.execute),
            test_notional=max(5.0, args.test_notional),
            wait_fill_s=max(0.5, args.wait_fill),
        )
    )
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        gc = report["gate_comparison"]
        print("\n=== LIVE EXCHANGE VERIFY ===")
        print(f"symbol={symbol} env={report.get('bybit_env')}")
        print(
            f"gate old_results: {gc['old_results']['orders_sampled']} | "
            f"exchange before: {gc['exchange_before']['orders_sampled']} | "
            f"exchange after: {gc['exchange_after_live_ops']['orders_sampled']}"
        )
        for row in report.get("true_top_by_execution") or []:
            print(
                f"  live#{row.get('live_rank')} {row.get('order_type')}+{row.get('time_in_force')} "
                f"filled={row.get('filled')} slip_bps={row.get('slippage_bps')}"
            )
        print(f"\nGuardado: {report['output_files']['json']}\n")


if __name__ == "__main__":
    main()