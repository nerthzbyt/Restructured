#!/usr/bin/env python3
"""
Laboratorio de señales src_dev:
- Multi-horizonte (depth, TFI, velas, ventana calibración) desde env
- Señal completa + niveles L0-L4 + blockers
- Comparación con bot live :8787
- ML dev con etiquetas forward (sin tocar src/)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from src_dev.collectors.snapshot_builder import build_snapshot_from_rest
from src_dev.config import DEFAULT_SETTINGS, DevSettings, load_trading_thresholds
from src_dev.ml.dev_model import DevMLModel
from src_dev.observe.signal_observer import (
    append_observation,
    build_observation,
    fetch_live_bot_state,
)


def _label_forward(
    price_then: float,
    price_now: float,
    *,
    min_bps: float,
) -> tuple[Optional[int], float]:
    if price_then <= 0 or price_now <= 0:
        return None, 0.0
    ret_bps = ((price_now - price_then) / price_then) * 10000.0
    if abs(ret_bps) < float(min_bps):
        return None, ret_bps
    return (1 if ret_bps > 0 else 0), ret_bps


async def run_lab(
    *,
    symbol: str,
    settings: DevSettings,
    iterations: int,
    interval_s: float,
    compare_live: bool,
    train_ml: bool,
    ml_min_samples: int,
    forward_min_bps: float,
) -> Dict[str, Any]:
    thresholds = load_trading_thresholds()
    thresholds = {
        "combined_buy": thresholds["combined_buy"],
        "combined_sell": thresholds["combined_sell"],
        "combined_hold_band": thresholds["combined_hold_band"],
    }

    history_buffer: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []
    observations: List[Dict[str, Any]] = []

    model = DevMLModel()
    model_path = Path(settings.signal_lab_model_path)
    if settings.dev_ml_enabled and model_path.is_file():
        model.load(model_path)

    log_path = settings.signal_lab_log_path
    print(f"Signal Lab | {symbol} | iter={iterations} | interval={interval_s}s")
    print(f"  log → {log_path}")
    print(f"  thresholds → {thresholds}")
    print(f"  compare_live={compare_live} ml={settings.dev_ml_enabled}\n")

    for i in range(iterations):
        t0 = time.time()
        snapshot = await build_snapshot_from_rest(symbol, settings)

        # Etiquetar observaciones pendientes con precio actual
        price_now = snapshot.last_price
        for item in pending:
            label, ret_bps = _label_forward(
                float(item["price"]),
                price_now,
                min_bps=forward_min_bps,
            )
            if label is not None:
                item["obs"]["forward_label"] = label
                item["obs"]["forward_return_bps"] = ret_bps

        live_state = None
        if compare_live:
            live_state = fetch_live_bot_state(symbol, settings.live_api_url)

        ml_proba = None
        if settings.dev_ml_enabled and model.w is not None:
            # predict on previous complete obs if exists
            if observations:
                ml_proba = model.predict_proba(observations[-1])

        obs = build_observation(
            snapshot,
            settings,
            history_buffer=history_buffer,
            thresholds=thresholds,
            live_state=live_state,
            ml_proba=ml_proba,
        )
        observations.append(obs)
        append_observation(log_path, obs)

        pending.append({"price": price_now, "obs": obs, "ts": obs["ts"]})
        # mantener solo los que aún no tienen label (últimas 3 ventanas)
        pending = [p for p in pending if "forward_label" not in p["obs"]][-5:]

        prod = obs["production"]
        sig = prod.get("signal") or {}
        lvl = prod.get("prediction_level") or {}
        cmp_ = obs.get("comparison") or {}
        line = (
            f"[{i + 1}/{iterations}] "
            f"px={price_now:.1f} "
            f"dec={sig.get('decision')} "
            f"lvl={lvl.get('level')} "
            f"comb={float(sig.get('combined') or 0):+.2f} "
            f"blockers={sig.get('blockers') or []}"
        )
        if cmp_:
            line += (
                f" | live={cmp_.get('live_decision')}"
                f"/{cmp_.get('live_level')}"
                f" match={cmp_.get('decision_match')}"
            )
        if ml_proba is not None:
            line += f" | ml_p={ml_proba:.3f}"
        print(line)

        if i < iterations - 1:
            await asyncio.sleep(max(0.5, interval_s - (time.time() - t0)))

    train_result: Optional[Dict[str, Any]] = None
    labeled = [o for o in observations if o.get("forward_label") in (0, 1)]
    if train_ml and settings.dev_ml_enabled and labeled:
        train_result = model.train_from_labeled_observations(
            labeled,
            min_samples=ml_min_samples,
        )
        if train_result.get("success"):
            model.save(model_path)
        print(f"\nML train: {json.dumps(train_result, ensure_ascii=False)}")

    summary = _build_summary(observations)
    summary_path = Path(settings.signal_lab_summary_path)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nResumen → {summary_path}")
    return summary


def _build_summary(observations: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not observations:
        return {"observations": 0}

    decisions: Dict[str, int] = {}
    levels: Dict[str, int] = {}
    blockers: Dict[str, int] = {}
    comparisons: List[Dict[str, Any]] = []
    horizon_buy_votes: Dict[str, int] = {}

    for obs in observations:
        prod = obs.get("production") or {}
        sig = prod.get("signal") or {}
        lvl = prod.get("prediction_level") or {}
        d = str(sig.get("decision") or "hold")
        decisions[d] = decisions.get(d, 0) + 1
        lv = str(lvl.get("level") or "L0")
        levels[lv] = levels.get(lv, 0) + 1
        for b in sig.get("blockers") or []:
            blockers[str(b)] = blockers.get(str(b), 0) + 1
        if obs.get("comparison"):
            comparisons.append(obs["comparison"])
        for name, row in (obs.get("horizons") or {}).items():
            if str(row.get("decision")) == "buy":
                horizon_buy_votes[name] = horizon_buy_votes.get(name, 0) + 1

    cmp_summary = {}
    if comparisons:
        cmp_summary = {
            "n": len(comparisons),
            "decision_match_rate": sum(1 for c in comparisons if c.get("decision_match")) / len(comparisons),
            "level_match_rate": sum(1 for c in comparisons if c.get("level_match")) / len(comparisons),
            "interpretations": _count_keys(c.get("interpretation") for c in comparisons),
        }

    return {
        "observations": len(observations),
        "decisions": decisions,
        "levels": levels,
        "top_blockers": sorted(blockers.items(), key=lambda x: -x[1])[:12],
        "live_comparison": cmp_summary,
        "horizon_buy_votes": sorted(horizon_buy_votes.items(), key=lambda x: -x[1])[:10],
        "labeled_samples": sum(1 for o in observations if o.get("forward_label") in (0, 1)),
    }


def _count_keys(items) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for k in items:
        if not k:
            continue
        out[str(k)] = out.get(str(k), 0) + 1
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Signal Lab src_dev")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--no-live", action="store_true")
    parser.add_argument("--train-ml", action="store_true")
    parser.add_argument("--ml-min-samples", type=int, default=None)
    parser.add_argument("--forward-min-bps", type=float, default=None)
    args = parser.parse_args()

    settings = DEFAULT_SETTINGS
    symbol = args.symbol or settings.symbol
    iterations = args.iterations if args.iterations is not None else settings.dev_signal_lab_iterations
    interval_s = args.interval if args.interval is not None else settings.dev_signal_lab_interval_s
    ml_min = args.ml_min_samples if args.ml_min_samples is not None else settings.dev_ml_min_samples
    forward_bps = (
        args.forward_min_bps if args.forward_min_bps is not None else settings.dev_ml_forward_min_bps
    )

    summary = asyncio.run(
        run_lab(
            symbol=symbol,
            settings=settings,
            iterations=iterations,
            interval_s=interval_s,
            compare_live=not args.no_live,
            train_ml=args.train_ml or settings.dev_ml_auto_train,
            ml_min_samples=ml_min,
            forward_min_bps=forward_bps,
        )
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()