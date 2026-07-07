"""Valida calculate_metrics de utils contra referencia independiente y JSONL."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from src_dev.analysis.orderbook_stats import analyze_orderbook
from src_dev.analysis.reference_metrics import compute_reference_raw
from src_dev.collectors.db_sources import load_jsonl_tail
from src_dev.config import DevSettings
from src_dev.models.market import MarketSnapshot, MetricValidationReport

from utils import calculate_metrics


RAW_KEYS = ("pio_raw", "ild_raw", "ogm_raw", "rol_raw", "weighted_liquidity")


def _rel_err(a: float, b: float) -> float:
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def _check_raw(
    utils_val: float,
    ref_val: float,
    *,
    tolerance_pct: float,
    key: str,
) -> Dict[str, Any]:
    err = _rel_err(utils_val, ref_val)
    ok = err <= (tolerance_pct / 100.0)
    if key == "rol_raw" and ref_val == 0.0 and utils_val == 0.0:
        ok = True
    return {
        "ok": ok,
        "utils": utils_val,
        "reference": ref_val,
        "rel_error_pct": round(err * 100.0, 4),
        "tolerance_pct": tolerance_pct,
    }


def _compare_jsonl(
    utils_metrics: Dict[str, Any],
    jsonl_row: Dict[str, Any],
    *,
    z_tol: float,
) -> Dict[str, Any]:
    jm = jsonl_row.get("metrics") or {}
    checks = {}
    for key in ("combined", "pio_raw", "ild_raw", "egm_raw"):
        u = float(utils_metrics.get(key) or 0.0)
        j = float(jm.get(key) or 0.0)
        if key in ("combined",):
            diff = abs(u - j)
            checks[key] = {"utils": u, "jsonl": j, "abs_diff": diff, "ok": diff <= z_tol * 10}
        else:
            err = _rel_err(u, j)
            checks[key] = {"utils": u, "jsonl": j, "rel_error_pct": err * 100, "ok": err <= 0.02}
    age_s = time.time() - float(jsonl_row.get("ts") or 0.0)
    return {
        "jsonl_ts": jsonl_row.get("ts"),
        "jsonl_decision": jsonl_row.get("decision"),
        "age_s": round(age_s, 1),
        "checks": checks,
        "all_ok": all(c.get("ok") for c in checks.values()),
    }


def validate_snapshot(
    snapshot: MarketSnapshot,
    settings: Optional[DevSettings] = None,
    *,
    compare_jsonl: bool = True,
) -> MetricValidationReport:
    cfg = settings or DevSettings.from_env()
    notes: List[str] = []

    if not snapshot.data_ready:
        notes.append("Snapshot incompleto: faltan velas/orderbook/precio")

    ticker_payload = snapshot.to_ticker_payload(cfg)
    utils_metrics = calculate_metrics(
        snapshot.candles,
        snapshot.orderbook,
        ticker_payload,
        depth=int(cfg.orderbook_depth),
        recent_trades=snapshot.recent_trades,
    )

    reference = compute_reference_raw(
        snapshot.candles,
        snapshot.orderbook,
        ticker_payload,
        depth=int(cfg.orderbook_depth),
    )

    raw_checks = {}
    for key in RAW_KEYS:
        if key not in reference:
            continue
        raw_checks[key] = _check_raw(
            float(utils_metrics.get(key) or 0.0),
            float(reference.get(key) or 0.0),
            tolerance_pct=cfg.raw_tolerance_pct,
            key=key,
        )

    ob_stats = analyze_orderbook(
        snapshot.orderbook,
        depth=int(cfg.orderbook_depth),
        last_price=snapshot.last_price,
    )

    jsonl_cmp = None
    if compare_jsonl:
        tail = load_jsonl_tail(snapshot.symbol, limit=1, settings=cfg)
        if tail:
            jsonl_cmp = _compare_jsonl(utils_metrics, tail[-1], z_tol=cfg.z_tolerance)
            if jsonl_cmp.get("age_s", 999) > 120:
                notes.append(f"JSONL antiguo ({jsonl_cmp['age_s']}s) — comparación orientativa")
        else:
            notes.append("Sin JSONL del bot para comparar")

    data_ok = bool(utils_metrics.get("data_ok"))
    calibrated = bool(utils_metrics.get("metrics_calibrated", False))
    raw_pass = all(c.get("ok") for c in raw_checks.values()) if raw_checks else False

    if not calibrated:
        notes.append(
            f"metrics_calibrated=False (historial={len(snapshot.metric_history)} muestras, se requieren ≥4)"
        )

    passed = data_ok and raw_pass

    return MetricValidationReport(
        symbol=snapshot.symbol,
        source=snapshot.source,
        ts=time.time(),
        data_ok=data_ok,
        metrics_calibrated=calibrated,
        utils_metrics=utils_metrics,
        reference_raw=reference,
        raw_checks=raw_checks,
        jsonl_compare=jsonl_cmp,
        orderbook_stats=ob_stats,
        passed=passed,
        notes=notes,
    )