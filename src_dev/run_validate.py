#!/usr/bin/env python3
"""CLI: validación puntual de métricas utils vs referencia + JSONL del bot."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from src_dev.collectors.snapshot_builder import (
    build_snapshot_from_db_hybrid,
    build_snapshot_from_rest,
    build_snapshot_from_ws,
)
from src_dev.config import DEFAULT_SETTINGS, OUTPUT_DIR
from src_dev.validators.metrics_validator import validate_snapshot


def _print_report(report) -> None:
    d = report.to_dict()
    print("\n" + "=" * 72)
    print(f"VALIDACIÓN {report.symbol} | fuente={report.source} | passed={report.passed}")
    print("=" * 72)
    um = report.utils_metrics
    print(f"  data_ok={report.data_ok}  metrics_calibrated={report.metrics_calibrated}")
    print(f"  combined={um.get('combined')}  pio={um.get('pio')}  egm={um.get('egm')}")
    print(f"  pio_raw={um.get('pio_raw')}  ild_raw={um.get('ild_raw')}  ogm_raw={um.get('ogm_raw')}")
    print("\n  RAW cross-check (utils vs referencia independiente):")
    for key, chk in report.raw_checks.items():
        status = "OK" if chk.get("ok") else "FAIL"
        print(
            f"    [{status}] {key}: utils={chk.get('utils'):.6g} "
            f"ref={chk.get('reference'):.6g} err={chk.get('rel_error_pct')}%"
        )
    if report.orderbook_stats and report.orderbook_stats.get("valid"):
        ob = report.orderbook_stats
        print(
            f"\n  Orderbook: mid={ob.get('mid'):.2f} spread_bps={ob.get('spread_bps'):.4f} "
            f"imbalance={ob.get('depth_imbalance_qty'):.4f}"
        )
    if report.jsonl_compare:
        jc = report.jsonl_compare
        print(f"\n  JSONL bot (age={jc.get('age_s')}s decision={jc.get('jsonl_decision')}):")
        for key, chk in (jc.get("checks") or {}).items():
            status = "OK" if chk.get("ok") else "DIFF"
            print(f"    [{status}] {key}: live={chk.get('utils')} jsonl={chk.get('jsonl')}")
    if report.notes:
        print("\n  Notas:")
        for n in report.notes:
            print(f"    - {n}")
    print("=" * 72 + "\n")


async def main_async(args: argparse.Namespace) -> int:
    settings = DEFAULT_SETTINGS
    symbol = args.symbol or settings.symbol

    notes: list[str] = []
    if args.source == "rest":
        snapshot = await build_snapshot_from_rest(symbol, settings)
    elif args.source == "ws":
        snapshot = await build_snapshot_from_ws(
            duration_s=float(args.ws_seconds),
            symbol=symbol,
            settings=settings,
        )
    elif args.source == "db":
        snapshot, notes = await build_snapshot_from_db_hybrid(symbol, settings)
    else:
        print(f"Fuente desconocida: {args.source}", file=sys.stderr)
        return 2

    report = validate_snapshot(snapshot, settings, compare_jsonl=not args.no_jsonl)
    report.notes.extend(notes)
    _print_report(report)

    if args.save:
        out_path = settings.validation_log_path
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")
        print(f"Guardado en {out_path}")

    return 0 if report.passed else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Validar métricas Nertz (src_dev)")
    parser.add_argument("--symbol", default=None, help="Ej: BTCUSDT")
    parser.add_argument(
        "--source",
        choices=["rest", "ws", "db"],
        default="rest",
        help="rest=Bybit HTTP, ws=WebSocket+REST fallback, db=SQLite+REST",
    )
    parser.add_argument("--ws-seconds", type=float, default=12.0, help="Duración WS si source=ws")
    parser.add_argument("--no-jsonl", action="store_true", help="No comparar con metrics_snapshots.jsonl")
    parser.add_argument("--save", action="store_true", help="Append a src_dev/output/validation_log.jsonl")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()