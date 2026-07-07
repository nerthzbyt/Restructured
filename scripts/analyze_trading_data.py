#!/usr/bin/env python3
"""Análisis matemático/administrativo/algorítmico de results.json y metrics_snapshots.jsonl.

Streaming: no requiere cargar JSONL completo en memoria estructurada.
Salida: resumen JSON en stdout o archivo (--out).
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from utils import load_results_json as _store_load_results  # noqa: E402

from path_safety import safe_path_under_project  # noqa: E402

DEFAULT_RESULTS = ROOT / "logs" / "results.json"
DEFAULT_JSONL = ROOT / "data" / "metrics_snapshots.jsonl"

CORE_INDICATORS = ("combined", "pio", "egm", "ild", "rol", "ogm", "mom", "mom_raw")
MICRO_INDICATORS = (
    "volatility", "spread_bps", "obi", "tfi", "imbalance20", "ret5m", "ret20m",
    "recent_trades_imbalance_qty_pct", "weighted_liquidity", "asymmetry",
)


@dataclass
class OnlineStats:
    n: int = 0
    mn: float = 0.0
    m2: float = 0.0
    mn_min: float = math.inf
    mx: float = -math.inf
    pos: int = 0
    neg: int = 0

    def push(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.n += 1
        d = x - self.mn
        self.mn += d / self.n
        d2 = x - self.mn
        self.m2 += d * d2
        self.mn_min = min(self.mn_min, x)
        self.mx = max(self.mx, x)
        if x > 0:
            self.pos += 1
        elif x < 0:
            self.neg += 1

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0

    def summary(self) -> Dict[str, Any]:
        if self.n == 0:
            return {"n": 0}
        return {
            "n": self.n,
            "mean": round(self.mn, 6),
            "std": round(self.std, 6),
            "min": round(self.mn_min, 6),
            "max": round(self.mx, 6),
            "pct_positive": round(100 * self.pos / self.n, 2),
        }


def _f(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def audit_combined(metrics: Dict[str, Any], tol: float = 0.05) -> Tuple[bool, float]:
    """Verifica combined ≈ sum_z * scale (incluye TFI en componentes)."""
    w = metrics.get("combined_weights") or {}
    c = metrics.get("combined_components") or {}
    combined = _f(metrics.get("combined"))
    sum_z = _f(c.get("sum_z"))
    scale = _f(w.get("scale")) or 10.0
    if combined is None:
        return False, 0.0
    if sum_z is None:
        parts = ("pio", "egm", "ild", "rol", "ogm", "mom", "tfi")
        sum_z = sum(_f(c.get(k)) or 0.0 for k in parts)
    expected = sum_z * scale
    err = abs(combined - expected)
    ok = err <= max(tol, abs(combined) * 0.001)
    return ok, err


def stream_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    safe = safe_path_under_project(path, must_exist=True)
    with safe.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                yield {"_parse_error": str(e), "_line": i}


def analyze_jsonl(path: Path) -> Dict[str, Any]:
    by_symbol: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "records": 0,
        "decisions": Counter(),
        "indicators": defaultdict(OnlineStats),
        "combined_audit_ok": 0,
        "combined_audit_fail": 0,
        "combined_audit_max_err": 0.0,
        "threshold_crossings": {"buy_signal": 0, "sell_signal": 0, "hold_zone": 0},
        "hourly_decisions": defaultdict(Counter),
        "parse_errors": 0,
        "first_ts": None,
        "last_ts": None,
    })

    transition_counts: Counter = Counter()
    prev_decision: Dict[str, str] = {}

    for rec in stream_jsonl(path):
        sym = str(rec.get("symbol") or "UNKNOWN").upper()
        bucket = by_symbol[sym]
        if rec.get("_parse_error"):
            bucket["parse_errors"] += 1
            continue

        bucket["records"] += 1
        ts = rec.get("timestamp") or ""
        if bucket["first_ts"] is None:
            bucket["first_ts"] = ts
        bucket["last_ts"] = ts

        decision = str(rec.get("decision") or "unknown").lower()
        bucket["decisions"][decision] += 1
        if len(ts) >= 13:
            hour = ts[11:13]
            bucket["hourly_decisions"][hour][decision] += 1

        prev = prev_decision.get(sym)
        if prev:
            transition_counts[f"{prev}->{decision}"] += 1
        prev_decision[sym] = decision

        metrics = rec.get("metrics") or {}
        if isinstance(metrics, dict):
            for key in CORE_INDICATORS + MICRO_INDICATORS:
                v = _f(metrics.get(key))
                if v is not None:
                    bucket["indicators"][key].push(v)

            ok, err = audit_combined(metrics)
            if ok:
                bucket["combined_audit_ok"] += 1
            else:
                bucket["combined_audit_fail"] += 1
                bucket["combined_audit_max_err"] = max(bucket["combined_audit_max_err"], err)

            combined = _f(metrics.get("combined"))
            thr = rec.get("thresholds") or {}
            buy_th = _f(thr.get("combined_buy_threshold")) or 1.5
            sell_th = _f(thr.get("combined_sell_threshold")) or -1.5
            hold_band = _f(thr.get("combined_hold_band")) or 0.5
            if combined is not None:
                if combined >= buy_th:
                    bucket["threshold_crossings"]["buy_signal"] += 1
                elif combined <= sell_th:
                    bucket["threshold_crossings"]["sell_signal"] += 1
                elif abs(combined) <= hold_band:
                    bucket["threshold_crossings"]["hold_zone"] += 1

    out_symbols = {}
    for sym, b in by_symbol.items():
        ind_sum = {k: v.summary() for k, v in b["indicators"].items()}
        out_symbols[sym] = {
            "records": b["records"],
            "time_range": {"first": b["first_ts"], "last": b["last_ts"]},
            "decisions": dict(b["decisions"]),
            "decision_transitions_top": [],
            "hourly_decisions": {h: dict(c) for h, c in sorted(b["hourly_decisions"].items())},
            "indicators": ind_sum,
            "combined_formula_audit": {
                "ok": b["combined_audit_ok"],
                "fail": b["combined_audit_fail"],
                "max_abs_error": round(b["combined_audit_max_err"], 6),
            },
            "threshold_crossings": b["threshold_crossings"],
            "parse_errors": b["parse_errors"],
        }

    top_trans = transition_counts.most_common(15)
    return {
        "file": str(path),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "symbols": out_symbols,
        "global_transitions_top": [{"transition": k, "count": v} for k, v in top_trans],
    }


def _load_results_json(path: Path, retries: int = 3) -> Dict[str, Any]:
    safe = safe_path_under_project(path, must_exist=True)
    last_err: Optional[Exception] = None
    log_dir = str(safe.parent)
    for attempt in range(max(1, retries)):
        try:
            data = _store_load_results(log_dir)
            if isinstance(data, dict) and data:
                return data
            with safe.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError as e:
            last_err = e
            import time
            time.sleep(0.25 * (attempt + 1))
    raise last_err or ValueError("empty_or_invalid_results_json")


def analyze_results(path: Path) -> Dict[str, Any]:
    data = _load_results_json(path)

    meta = data.get("metadata") or {}
    summary = data.get("summary") or {}
    by_symbol_meta = data.get("by_symbol") or {}
    trades_root = data.get("trades") or {}

    all_trades: List[Dict[str, Any]] = []
    for sym, rows in trades_root.items():
        for t in rows or []:
            if isinstance(t, dict):
                rec = dict(t)
                rec["symbol"] = sym
                all_trades.append(rec)

    closed = [t for t in all_trades if t.get("outcome_status") == "final"]
    open_other = [t for t in all_trades if t.get("outcome_status") != "final"]
    pnls = [_f(t.get("profit_loss")) or 0.0 for t in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    # Drawdown
    cum = peak = max_dd = 0.0
    for t in sorted(closed, key=lambda x: x.get("outcome_timestamp") or ""):
        cum += _f(t.get("profit_loss")) or 0.0
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    # Indicadores en entrada de trades cerrados
    entry_indicators: Dict[str, OnlineStats] = defaultdict(OnlineStats)
    win_combined: List[float] = []
    loss_combined: List[float] = []
    buy_pnl = sell_pnl = 0.0
    buy_n = sell_n = 0

    for t in closed:
        pnl = _f(t.get("profit_loss")) or 0.0
        action = str(t.get("action") or "").lower()
        if action == "buy":
            buy_pnl += pnl
            buy_n += 1
        elif action == "sell":
            sell_pnl += pnl
            sell_n += 1

        snap = t.get("metrics_snapshot") or {}
        m = snap.get("metrics") if isinstance(snap, dict) else {}
        if isinstance(m, dict):
            comb = _f(m.get("combined"))
            if comb is not None:
                if pnl > 0:
                    win_combined.append(comb)
                elif pnl < 0:
                    loss_combined.append(comb)
            for key in CORE_INDICATORS:
                v = _f(m.get(key))
                if v is not None:
                    entry_indicators[key].push(v)

    calc_net = sum(pnls)
    calc_profit = sum(wins)
    calc_loss = sum(losses)

    admin_checks = {
        "summary_net_matches_closed_sum": abs(calc_net - _f(summary.get("net_profit")) or 0) < 0.01,
        "summary_profit_matches": abs(calc_profit - (_f(summary.get("total_profit")) or 0)) < 0.01,
        "summary_loss_matches": abs(calc_loss - (_f(summary.get("total_loss")) or 0)) < 0.01,
        "metadata_total_trades_vs_closed": int(meta.get("total_trades") or 0) == len(closed),
        "capital_pnl_vs_trading_pnl_note": (
            "capital_pnl incluye wallet/demo funding; total_pnl es solo trades cerrados"
        ),
        "capital_pnl": _f(meta.get("capital_pnl")),
        "total_pnl_trades": _f(meta.get("total_pnl")),
        "delta_wallet_vs_trades": round(
            (_f(meta.get("capital_pnl")) or 0) - (_f(meta.get("total_pnl")) or 0), 2
        ),
    }

    by_sym_out = {}
    for sym in sorted(set(list(by_symbol_meta.keys()) + list(trades_root.keys()))):
        sym_trades = [t for t in closed if t.get("symbol") == sym]
        sym_pnls = [_f(t.get("profit_loss")) or 0.0 for t in sym_trades]
        meta_sym = by_symbol_meta.get(sym) or {}
        by_sym_out[sym] = {
            "trade_count_closed": len(sym_trades),
            "net_pnl": round(sum(sym_pnls), 6),
            "win_rate_pct": round(100 * sum(1 for p in sym_pnls if p > 0) / len(sym_pnls), 2) if sym_pnls else 0,
            "metadata_net": _f(meta_sym.get("net_profit")),
            "actions": dict(Counter(t.get("action") for t in sym_trades)),
        }

    patterns = {
        "all_exits_via_sl_approx": True,
        "tp_hit_count": 0,
        "sl_hit_count": 0,
        "avg_combined_winners": round(statistics.mean(win_combined), 4) if win_combined else None,
        "avg_combined_losers": round(statistics.mean(loss_combined), 4) if loss_combined else None,
        "buy_side": {"n": buy_n, "net_pnl": round(buy_pnl, 4)},
        "sell_side": {"n": sell_n, "net_pnl": round(sell_pnl, 4)},
        "entry_indicators": {k: v.summary() for k, v in entry_indicators.items()},
    }

    for t in closed:
        xp = _f(t.get("exit_price"))
        sl = _f(t.get("sl_price"))
        tp = _f(t.get("tp_price"))
        if xp is None or sl is None:
            continue
        if tp is not None and abs(xp - tp) < abs(xp - sl):
            patterns["tp_hit_count"] += 1
            patterns["all_exits_via_sl_approx"] = False
        else:
            patterns["sl_hit_count"] += 1

    return {
        "file": str(path),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "metadata": meta,
        "summary_file": summary,
        "admin_checks": admin_checks,
        "counts": {
            "total_records": len(all_trades),
            "closed": len(closed),
            "non_final": len(open_other),
            "wins": len(wins),
            "losses": len(losses),
        },
        "verified_math": {
            "net_pnl": round(calc_net, 6),
            "gross_profit": round(calc_profit, 6),
            "gross_loss": round(calc_loss, 6),
            "win_rate_pct": round(100 * len(wins) / len(closed), 2) if closed else 0,
            "avg_pnl": round(statistics.mean(pnls), 6) if pnls else 0,
            "median_pnl": round(statistics.median(pnls), 6) if pnls else 0,
            "max_drawdown": round(max_dd, 6),
            "best_trade": round(max(pnls), 6) if pnls else 0,
            "worst_trade": round(min(pnls), 6) if pnls else 0,
        },
        "by_symbol": by_sym_out,
        "patterns": patterns,
        "open_non_final": [
            {
                "trade_id": t.get("trade_id"),
                "symbol": t.get("symbol"),
                "action": t.get("action"),
                "status": t.get("outcome_status"),
                "entry_price": t.get("entry_price"),
                "timestamp": t.get("timestamp"),
            }
            for t in open_other
        ],
    }


def run_analysis(
    results_path: Path = DEFAULT_RESULTS,
    jsonl_path: Path = DEFAULT_JSONL,
) -> Dict[str, Any]:
    results_safe = safe_path_under_project(results_path)
    jsonl_safe = safe_path_under_project(jsonl_path)
    report: Dict[str, Any] = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "project_root": str(ROOT),
    }
    if results_safe.exists():
        report["results"] = analyze_results(results_safe)
    else:
        report["results"] = {"error": "missing_file", "path": str(results_safe)}

    if jsonl_safe.exists():
        report["metrics_snapshots"] = analyze_jsonl(jsonl_safe)
    else:
        report["metrics_snapshots"] = {"error": "missing_file", "path": str(jsonl_path)}

    # Cruce símbolos
    r_syms = set((report.get("results") or {}).get("by_symbol") or {})
    j_syms = set((report.get("metrics_snapshots") or {}).get("symbols") or {})
    report["cross_file"] = {
        "symbols_in_results": sorted(r_syms),
        "symbols_in_jsonl": sorted(j_syms),
        "symbols_only_results": sorted(r_syms - j_syms),
        "symbols_only_jsonl": sorted(j_syms - r_syms),
    }
    return report


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Analiza results.json + metrics_snapshots.jsonl")
    ap.add_argument("--results", default=str(DEFAULT_RESULTS))
    ap.add_argument("--jsonl", default=str(DEFAULT_JSONL))
    ap.add_argument("--out", default="", help="Ruta salida JSON (opcional)")
    args = ap.parse_args(argv)

    report = run_analysis(
        safe_path_under_project(Path(args.results)),
        safe_path_under_project(Path(args.jsonl)),
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)

    if args.out:
        out_path = safe_path_under_project(Path(args.out))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        print(f"Reporte guardado: {out_path}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())