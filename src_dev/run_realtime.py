#!/usr/bin/env python3
"""Loop realtime: valida métricas cada N segundos contra Bybit REST."""
from __future__ import annotations

import argparse
import asyncio
import json
import time

from src_dev.collectors.snapshot_builder import build_snapshot_from_rest
from src_dev.config import DEFAULT_SETTINGS
from src_dev.validators.metrics_validator import validate_snapshot


async def loop_validate(
    symbol: str,
    interval_s: float,
    iterations: int,
    *,
    save: bool,
) -> None:
    settings = DEFAULT_SETTINGS
    print(f"Realtime validation {symbol} cada {interval_s}s ({iterations} iteraciones)\n")

    for i in range(iterations):
        t0 = time.time()
        snapshot = await build_snapshot_from_rest(symbol, settings)
        report = validate_snapshot(snapshot, settings)
        um = report.utils_metrics
        status = "PASS" if report.passed else "FAIL"
        print(
            f"[{i + 1}/{iterations}] {status} | "
            f"combined={float(um.get('combined') or 0):+.3f} "
            f"pio_raw={float(um.get('pio_raw') or 0):+.3f} "
            f"calibrated={report.metrics_calibrated} "
            f"({time.time() - t0:.1f}s)"
        )
        if save:
            with open(settings.validation_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")
        if i < iterations - 1:
            await asyncio.sleep(max(0.5, interval_s))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validación realtime src_dev")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    symbol = args.symbol or DEFAULT_SETTINGS.symbol
    asyncio.run(loop_validate(symbol, args.interval, args.iterations, save=args.save))


if __name__ == "__main__":
    main()