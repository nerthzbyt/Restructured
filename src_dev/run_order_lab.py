#!/usr/bin/env python3
"""Laboratorio de órdenes spot — combinaciones Bybit + scoring métricas live."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from src_dev.config import DEFAULT_SETTINGS, DevSettings, load_trading_thresholds
from src_dev.orders.lab import run_order_lab


def _print_top(report: dict) -> None:
    top_n = report.get("top_n") or DEFAULT_SETTINGS.lab_top_n
    print("\n" + "=" * 72)
    print(
        f"ORDER LAB | {report.get('symbol')} | exchange-only | "
        f"{report.get('generated_at')} | ok={report.get('ok')}"
    )
    print("=" * 72)
    lm = report.get("live_metrics") or {}
    print(
        f"  combined={lm.get('combined')}  calibrated={lm.get('metrics_calibrated')}  "
        f"price={lm.get('last_price')}  notional={report.get('notional_usdt')}"
    )
    print(f"  Perfiles válidos: {report.get('combinator', {}).get('valid_profiles_count')}")
    print(f"  Credenciales: required={report.get('credentials_required')} ok={report.get('credentials_ok')}")

    errors = report.get("errors") or []
    if errors:
        print("\n  ERRORES DE CONEXIÓN / POLÍTICA:")
        for err in errors:
            print(f"    - {err}")

    print(f"\n  TOP {top_n}:")
    for row in report.get("top_order_profiles") or []:
        print(
            f"  #{row.get('rank')} score={row.get('score')} | "
            f"{row.get('order_type')}+{row.get('time_in_force')} | "
            f"filter={row.get('order_filter')} | anchor={row.get('price_anchor')} | "
            f"tp_sl={row.get('tp_sl_mode')} | side={row.get('side_hint')}"
        )
    files = report.get("output_files") or {}
    print("\n  Guardado en:")
    for k, p in files.items():
        print(f"    {k}: {p}")
    print("=" * 72 + "\n")


async def main_async(args: argparse.Namespace) -> int:
    cfg = DevSettings.from_env()
    thresholds = load_trading_thresholds()

    top_n = int(args.top) if args.top is not None else cfg.lab_top_n
    notional = float(args.notional) if args.notional is not None else float(thresholds["capital_usdt"])

    report = await run_order_lab(
        symbol=args.symbol or cfg.symbol,
        settings=cfg,
        top_n=top_n,
        notional_usdt=notional,
        include_slippage=not args.no_slippage,
        score_both_sides=not args.single_side,
    )
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        _print_top(report)

    if report.get("credentials_required") and not report.get("credentials_ok"):
        return 1
    if not report.get("ok", True):
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Laboratorio órdenes spot Bybit (src_dev)")
    parser.add_argument("--symbol", default=None)
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        help=f"Override DEV_ORDER_LAB_TOP_N (default env: {DEFAULT_SETTINGS.lab_top_n})",
    )
    parser.add_argument(
        "--notional",
        type=float,
        default=None,
        help="Override CAPITAL_USDT para calcular qty",
    )
    parser.add_argument("--no-slippage", action="store_true", help="Excluir combos slippageTolerance")
    parser.add_argument("--single-side", action="store_true", help="Solo puntuar side del combined")
    parser.add_argument("--json", action="store_true", help="Dump JSON completo a stdout")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()