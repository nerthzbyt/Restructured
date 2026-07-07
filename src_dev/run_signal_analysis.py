#!/usr/bin/env python3
"""Analiza signal_lab.jsonl: blockers, niveles, alineación live, potencial oculto."""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from src_dev.config import DEFAULT_SETTINGS, PROJECT_ROOT

_SCRIPTS = PROJECT_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from path_safety import safe_lines, safe_path_under_project, safe_write_text  # noqa: E402


def load_observations(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    safe = safe_path_under_project(path, must_exist=False)
    if not safe.is_file():
        return rows
    for line in safe_lines(safe):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def analyze(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"error": "no_observations"}

    missed_signals: List[Dict[str, Any]] = []
    horizon_splits: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for obs in rows:
        prod = obs.get("production") or {}
        sig = prod.get("signal") or {}
        prod_dec = str(sig.get("decision") or "hold")
        prod_lvl = str((prod.get("prediction_level") or {}).get("level") or "L0")
        prod_blockers = sig.get("blockers") or []

        for name, h in (obs.get("horizons") or {}).items():
            if name == "production":
                continue
            horizon_splits[name][str(h.get("decision") or "hold")] += 1

        # Señal oculta: algún horizonte dice buy/sell pero producción hold
        alt_decisions = set()
        for name, h in (obs.get("horizons") or {}).items():
            if name == "production":
                continue
            d = str(h.get("decision") or "hold")
            if d in {"buy", "sell"}:
                alt_decisions.add(d)
        if prod_dec == "hold" and alt_decisions:
            missed_signals.append(
                {
                    "ts": obs.get("ts"),
                    "price": obs.get("price"),
                    "prod_level": prod_lvl,
                    "prod_blockers": prod_blockers,
                    "alt_decisions": sorted(alt_decisions),
                    "horizons_buy_sell": {
                        k: v.get("decision")
                        for k, v in (obs.get("horizons") or {}).items()
                        if str(v.get("decision")) in {"buy", "sell"}
                    },
                }
            )

    cmp_rows = [o.get("comparison") for o in rows if o.get("comparison")]
    cmp_stats = {}
    if cmp_rows:
        cmp_stats = {
            "n": len(cmp_rows),
            "decision_match_rate": sum(1 for c in cmp_rows if c.get("decision_match")) / len(cmp_rows),
            "level_match_rate": sum(1 for c in cmp_rows if c.get("level_match")) / len(cmp_rows),
        }

    return {
        "observations": len(rows),
        "missed_signal_candidates": len(missed_signals),
        "missed_samples": missed_signals[:15],
        "horizon_decision_matrix": {k: dict(v) for k, v in horizon_splits.items()},
        "live_comparison": cmp_stats,
        "insight": _insight_text(missed_signals, cmp_stats),
    }


def _insight_text(missed: List[Dict[str, Any]], cmp_stats: Dict[str, Any]) -> str:
    parts: List[str] = []
    if missed:
        parts.append(
            f"Hay {len(missed)} ticks donde algún horizonte alternativo generó buy/sell "
            "pero producción quedó en hold — revisar blockers y calibración."
        )
    if cmp_stats:
        dm = cmp_stats.get("decision_match_rate", 0.0)
        if dm < 0.7:
            parts.append(
                "Baja alineación con bot live: probable diferencia de metric_history o timing."
            )
        else:
            parts.append("Buena alineación dev vs live en decisiones.")
    if not parts:
        parts.append("Sin divergencias destacables en esta muestra.")
    return " ".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default=None, help="Ruta a signal_lab.jsonl")
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()

    log_path = safe_path_under_project(Path(args.log or DEFAULT_SETTINGS.signal_lab_log_path))
    report = analyze(load_observations(log_path))
    text = json.dumps(report, indent=2, ensure_ascii=False)
    print(text)
    if args.save:
        out = safe_write_text(DEFAULT_SETTINGS.signal_lab_analysis_path, text)
        print(f"\nGuardado → {out}")


if __name__ == "__main__":
    main()