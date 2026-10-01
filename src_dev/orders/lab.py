"""Orquestador del laboratorio de órdenes spot — exchange-only, política lab."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src_dev.collectors.multi_connection import build_multi_connection_context
from src_dev.config import OUTPUT_DIR, DevSettings, load_trading_thresholds
from src_dev.lab_policy import FORBIDDEN_IN_RANKING, LAB_POLICY_VERSION
from src_dev.orders.combinator import (
    build_order_body,
    iter_spot_combinations,
    qty_for_notional,
    resolve_limit_price,
)
from src_dev.orders.exchange_catalog import summarize_exchange_orders
from src_dev.orders.exchange_schema import OPTIONAL_SPOT_KEYS, REQUIRED_CREATE_KEYS
from src_dev.orders.scorer import ScoredCombo, rank_all, rank_top_n, score_combination


def _output_path(name: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, name)


def _round_to_tick(value: float, tick_size: float) -> float:
    tick = max(float(tick_size or 0.01), 1e-12)
    return round(round(value / tick) * tick, 8)


def _trigger_from_tick(last_price: float, tick_size: float, side: str) -> str:
    """Precio trigger alineado a tick_size del instrumento (sin factor 0.999 fijo)."""
    tick = max(float(tick_size or 0.01), 1e-12)
    if side == "Buy":
        raw = last_price - tick
    else:
        raw = last_price + tick
    return str(_round_to_tick(raw, tick))


def _scored_to_dict(item: ScoredCombo, rank: int) -> Dict[str, Any]:
    return {
        "rank": rank,
        "score": item.score,
        "combo_id": item.combo.combo_id(),
        "order_type": item.combo.order_type,
        "time_in_force": item.combo.time_in_force,
        "market_unit": item.combo.market_unit,
        "order_filter": item.combo.order_filter,
        "price_anchor": item.combo.price_anchor,
        "tp_sl_mode": item.combo.tp_sl_mode,
        "slippage": {
            "type": item.combo.slippage_type,
            "value": item.combo.slippage_value,
        },
        "side_hint": item.combo.side_hint,
        "rank_factors": item.rank_factors,
        "rationale": item.rationale,
        "body_preview": item.body_preview,
    }


def _write_lab_rules(path: str) -> None:
    lines = [
        "# Reglas del laboratorio src_dev",
        "",
        f"Versión política: **{LAB_POLICY_VERSION}**",
        "",
        "1. Toda configuración sale de .env / ConfigSettings (producción).",
        "2. Datos solo del exchange (REST público, WS, API privada con credenciales).",
        "3. Credenciales obligatorias para ranking final (DEV_LAB_REQUIRE_CREDENTIALS=true).",
        "4. Top N y notional desde env, no constantes en código.",
        "5. Scoring derivado de métricas live + frecuencia real de órdenes en el exchange.",
        "6. output/ guarda ranking completo + debug de conexiones + errores.",
        "",
        "## Prohibido en ranking",
        "",
    ]
    for rule in FORBIDDEN_IN_RANKING:
        lines.append(f"- {rule}")
    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _write_summary_md(path: str, report: Dict[str, Any], top_n: int) -> None:
    lines = [
        "# Laboratorio de órdenes spot — resumen (exchange only)",
        "",
        f"Generado: {report.get('generated_at')}",
        f"Símbolo: **{report.get('symbol')}**",
        f"Fuente: Bybit REST/WS/Private API — **sin DB local**",
        f"Docs API: {report.get('api_docs')}",
        f"Estado: **{'OK' if report.get('ok') else 'FALLIDO'}**",
        "",
    ]
    if report.get("errors"):
        lines.append("## Errores")
        lines.append("")
        for err in report["errors"]:
            lines.append(f"- {err}")
        lines.append("")

    lines.extend([
        "## Métricas live (laboratorio utils)",
        "",
    ])
    lm = report.get("live_metrics") or {}
    lines.append(f"- combined: `{lm.get('combined')}`")
    lines.append(f"- calibrated: `{lm.get('metrics_calibrated')}`")
    lines.append(f"- last_price: `{lm.get('last_price')}`")
    lines.append(f"- muestras historial: `{report.get('metric_history_samples')}`")
    lines.append(f"- notional (CAPITAL_USDT): `{report.get('notional_usdt')}`")
    eos = report.get("exchange_order_stats") or {}
    lines.append(f"- stats órdenes fuente: `{eos.get('stats_source')}`")
    lines.append(f"- órdenes muestreadas (exchange): `{eos.get('total_orders_sampled')}`")
    dist = eos.get("combo_distribution_pct") or {}
    if dist:
        lines.append("- distribución tipos (exchange):")
        for k, pct in list(dist.items())[:6]:
            lines.append(f"  - `{k}`: {pct}%")
    lines.append("")
    lines.append(f"## Top {top_n} perfiles de orden")
    lines.append("")
    for row in report.get("top_order_profiles") or []:
        lines.append(f"### #{row.get('rank')} — score {row.get('score')}")
        lines.append(f"- **Tipo:** `{row.get('order_type')}` + `{row.get('time_in_force')}`")
        lines.append(f"- **Filter:** `{row.get('order_filter')}` | TP/SL: `{row.get('tp_sl_mode')}`")
        if row.get("price_anchor"):
            lines.append(f"- **Precio:** anchor `{row.get('price_anchor')}`")
        if row.get("market_unit"):
            lines.append(f"- **marketUnit:** `{row.get('market_unit')}`")
        lines.append(f"- **Side hint:** `{row.get('side_hint')}`")
        lines.append(f"- **ID:** `{row.get('combo_id')}`")
        lines.append("")

    top_file = report.get("output_files", {}).get("top") or f"order_lab_top{top_n}.json"
    lines.extend([
        "## Archivos en output/",
        f"- `{top_file}` — reporte top {top_n}",
        "- `order_lab_ranked.json` — ranking completo",
        "- `order_lab_debug.json` — debug conexiones + errores",
        "- `exchange_catalog.json` — instrument rules + órdenes exchange",
        "- `LAB_RULES.md` — política del laboratorio",
        "",
    ])
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


async def run_order_lab(
    symbol: Optional[str] = None,
    settings: Optional[DevSettings] = None,
    *,
    top_n: Optional[int] = None,
    notional_usdt: Optional[float] = None,
    include_slippage: bool = True,
    score_both_sides: bool = True,
) -> Dict[str, Any]:
    cfg = settings or DevSettings.from_env()
    sym = symbol or cfg.symbol
    thresholds = load_trading_thresholds()
    effective_top_n = max(1, int(top_n if top_n is not None else cfg.lab_top_n))
    effective_notional = float(
        notional_usdt if notional_usdt is not None else thresholds["capital_usdt"]
    )

    ctx = await build_multi_connection_context(
        sym,
        cfg,
        ws_duration_s=cfg.lab_ws_probe_s,
    )

    connection_debug = ctx.get("connection_debug") or {}
    errors: List[str] = list(connection_debug.get("errors") or [])
    constraints = ctx["constraints"]
    exchange_orders = ctx["exchange_orders"]
    order_stats = summarize_exchange_orders(exchange_orders)
    observed_counts = (order_stats.get("observed_combo_counts") or {})

    credentials_ok = bool(connection_debug.get("private_ok"))
    if cfg.lab_require_credentials and not credentials_ok:
        errors.append(
            "Credenciales requeridas (DEV_LAB_REQUIRE_CREDENTIALS=true) "
            "pero REST privado no validó — ranking omitido"
        )

    metrics = ctx.get("metrics") or {}
    ob_stats = dict(ctx.get("orderbook_stats") or {})
    ob_stats["tick_size"] = constraints.tick_size
    last_price = float(metrics.get("last_price") or ob_stats.get("mid") or 0.0)
    metric_history_len = int(ctx.get("metric_history_len") or 0)

    combined = float(metrics.get("combined") or 0.0)
    buy_th = float(thresholds["combined_buy"])
    sell_th = float(thresholds["combined_sell"])
    if combined >= buy_th:
        primary_side = "Buy"
    elif combined <= sell_th:
        primary_side = "Sell"
    else:
        primary_side = "Buy" if combined >= 0 else "Sell"

    all_combos: List[ScoredCombo] = []
    ranked_all: List[ScoredCombo] = []
    top: List[ScoredCombo] = []

    if not (cfg.lab_require_credentials and not credentials_ok):
        sides = ["Buy", "Sell"] if score_both_sides else [primary_side]
        for side in sides:
            for combo in iter_spot_combinations(side_hint=side, include_slippage=include_slippage):
                price_f = (
                    resolve_limit_price(combo.price_anchor or "mid", ob_stats, combined)
                    if combo.order_type == "Limit"
                    else last_price
                )
                qty = qty_for_notional(effective_notional, price_f or last_price, constraints)
                tp = last_price * (1 + float(thresholds["tp_pct"]) / 100.0)
                sl = last_price * (1 - float(thresholds["sl_pct"]) / 100.0)
                trigger = _trigger_from_tick(last_price, constraints.tick_size, side)

                body = build_order_body(
                    combo,
                    symbol=sym,
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
                    observed_combo_counts=observed_counts,
                    min_calibration_samples=cfg.lab_min_calibration_samples,
                    metric_history_len=metric_history_len,
                )
                all_combos.append(scored)

        ranked_all = rank_all(all_combos)
        top = rank_top_n(all_combos, n=effective_top_n)

    valid_count = sum(1 for _ in iter_spot_combinations(include_slippage=include_slippage))
    ok = not (cfg.lab_require_credentials and not credentials_ok)

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "api_docs": "https://bybit-exchange.github.io/docs/v5/order/create-order",
        "symbol": sym,
        "data_source": "bybit_exchange_only",
        "local_db_used": False,
        "lab_policy_version": LAB_POLICY_VERSION,
        "ok": ok,
        "errors": errors,
        "credentials_required": cfg.lab_require_credentials,
        "credentials_ok": credentials_ok,
        "top_n": effective_top_n,
        "notional_usdt": effective_notional,
        "thresholds": thresholds,
        "market_snapshot_ts": ctx.get("ts"),
        "metric_history_samples": metric_history_len,
        "min_calibration_samples": cfg.lab_min_calibration_samples,
        "live_metrics": {
            "combined": metrics.get("combined"),
            "pio": metrics.get("pio"),
            "egm": metrics.get("egm"),
            "volatility": metrics.get("volatility"),
            "metrics_calibrated": metrics.get("metrics_calibrated"),
            "last_price": last_price,
        },
        "instrument_constraints": {
            "tick_size": constraints.tick_size,
            "qty_step": constraints.qty_step,
            "min_qty": constraints.min_qty,
            "min_notional": constraints.min_notional,
            "max_order_qty": constraints.max_order_qty,
            "max_mkt_order_qty": constraints.max_mkt_order_qty,
            "status": constraints.status,
        },
        "exchange_order_stats": order_stats,
        "schema": {
            "required_keys": sorted(REQUIRED_CREATE_KEYS),
            "optional_spot_keys": list(OPTIONAL_SPOT_KEYS),
        },
        "combinator": {
            "valid_profiles_count": valid_count,
            "scored_count": len(all_combos),
            "ranked_count": len(ranked_all),
            "include_slippage": include_slippage,
        },
        "top_order_profiles": [_scored_to_dict(item, i + 1) for i, item in enumerate(top)],
        "ranked_all": [_scored_to_dict(item, i + 1) for i, item in enumerate(ranked_all)],
    }

    debug_path = cfg.debug_output_path()
    ranked_path = cfg.ranked_output_path()
    top_path = cfg.top_output_path() if effective_top_n == cfg.lab_top_n else os.path.join(
        OUTPUT_DIR, f"order_lab_top{effective_top_n}.json"
    )
    catalog_path = _output_path("exchange_catalog.json")
    summary_path = _output_path("ORDENES_RESUMEN.md")
    rules_path = _output_path("LAB_RULES.md")

    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "connection_debug": connection_debug,
                "errors": errors,
                "sources": ctx.get("sources"),
                "credentials_required": cfg.lab_require_credentials,
                "credentials_ok": credentials_ok,
            },
            f,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

    with open(ranked_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": report["generated_at"],
                "symbol": sym,
                "ok": ok,
                "ranked_count": len(ranked_all),
                "ranked_all": report["ranked_all"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    with open(top_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "constraints": report["instrument_constraints"],
                "exchange_orders": exchange_orders,
                "order_stats": order_stats,
                "schema": report["schema"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    _write_summary_md(summary_path, report, effective_top_n)
    _write_lab_rules(rules_path)

    report["output_files"] = {
        "debug": debug_path,
        "ranked": ranked_path,
        "top": top_path,
        "catalog": catalog_path,
        "summary": summary_path,
        "rules": rules_path,
    }
    return report