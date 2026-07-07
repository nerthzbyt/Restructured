#!/usr/bin/env python3
"""
Monitor 15min: exchange Bybit (REST directo) vs motor NerT (:8787).
Al finalizar: precisión por decisión, calidad de holds, métrica más predictiva.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src_dev.config import DEFAULT_SETTINGS, PROJECT_ROOT

BYBIT_PUBLIC = "https://api.bybit.com"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

METRIC_KEYS = (
    "combined",
    "combined_z",
    "mom",
    "pio",
    "egm",
    "tfi",
    "ild",
    "rol",
    "ogm",
    "volatility",
    "rvol",
)


def _fetch_json(url: str, timeout: float = 10.0) -> Dict[str, Any]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NerT-dev-monitor/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
            return data if isinstance(data, dict) else {"raw": data}
    except Exception as exc:
        return {"error": str(exc)}


def fetch_exchange_ticker(symbol: str) -> Dict[str, Any]:
    q = urllib.parse.urlencode({"category": "spot", "symbol": symbol})
    raw = _fetch_json(f"{BYBIT_PUBLIC}/v5/market/tickers?{q}")
    if raw.get("retCode") != 0:
        return {"ok": False, "source": "bybit_rest_direct", "error": raw}
    lst = (raw.get("result") or {}).get("list") or []
    row = lst[0] if lst else {}
    last = float(row.get("lastPrice") or 0)
    bid = float(row.get("bid1Price") or 0)
    ask = float(row.get("ask1Price") or 0)
    return {
        "ok": True,
        "source": "bybit_rest_direct",
        "symbol": symbol,
        "last": last,
        "bid": bid,
        "ask": ask,
        "mid": (bid + ask) / 2 if bid and ask else last,
        "spread_bps": ((ask - bid) / last * 10000) if last and ask > bid else 0.0,
        "pct_24h": float(row.get("price24hPcnt") or 0) * 100,
        "volume_24h": float(row.get("volume24h") or 0),
        "ts_exchange": int(row.get("time") or raw.get("time") or time.time() * 1000),
    }


def fetch_motor_snapshot(symbol: str, base: str) -> Dict[str, Any]:
    base = base.rstrip("/")
    metrics_r = _fetch_json(f"{base}/api/metrics/{symbol}")
    dec_r = _fetch_json(f"{base}/api/decisions/{symbol}")
    pred_r = _fetch_json(f"{base}/agent/prediction-level/{symbol}")
    bot_ticker = _fetch_json(f"{base}/market/ticker/{symbol}")

    mm = (metrics_r.get("metrics") or {}) if isinstance(metrics_r, dict) else {}
    det = (dec_r.get("decision_detail") or {}) if isinstance(dec_r, dict) else {}
    pred = (pred_r.get("prediction") or {}) if isinstance(pred_r, dict) else {}

    metrics_pick = {k: mm.get(k) for k in METRIC_KEYS if k in mm}
    return {
        "metrics": metrics_pick,
        "metrics_calibrated": bool(mm.get("metrics_calibrated")),
        "motor_last_price": mm.get("last_price"),
        "motor_decision": str(det.get("decision") or "hold"),
        "motor_market_state": str(det.get("market_state") or ""),
        "motor_blockers": det.get("blockers_if_not_trading") or [],
        "pred_decision": str(pred.get("decision") or "hold"),
        "pred_level": str(pred.get("level") or "L0"),
        "pred_confidence_pct": pred.get("confidence_pct"),
        "pred_blockers": pred.get("blockers") or [],
        "pred_market_state": str(pred.get("market_state") or ""),
        "bot_ticker_ok": bool(bot_ticker.get("ok")),
        "bot_ticker_last": float(bot_ticker.get("lastPrice") or 0) if bot_ticker.get("ok") else None,
    }


async def run_session(
    *,
    duration_s: float,
    interval_s: float,
    symbol: str,
    base_url: str,
    log_path: Path,
) -> Dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    t_end = time.time() + duration_s
    ticks: List[Dict[str, Any]] = []
    i = 0

    print(f"Exchange vs Motor | {duration_s/60:.0f}min | {symbol} | cada {interval_s}s")
    print(f"  exchange → {BYBIT_PUBLIC}/v5/market/tickers")
    print(f"  motor    → {base_url}")
    print(f"  log      → {log_path}\n")

    while time.time() < t_end:
        i += 1
        t0 = time.time()
        ex = fetch_exchange_ticker(symbol)
        motor = fetch_motor_snapshot(symbol, base_url)

        tick: Dict[str, Any] = {
            "ts": t0,
            "i": i,
            "exchange": ex,
            "motor": motor,
        }
        if ex.get("ok") and motor.get("motor_last_price"):
            try:
                tick["price_delta_motor_vs_exchange"] = float(motor["motor_last_price"]) - float(
                    ex.get("last") or 0
                )
            except (TypeError, ValueError):
                tick["price_delta_motor_vs_exchange"] = None

        ticks.append(tick)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(tick, ensure_ascii=False) + "\n")

        ex_p = ex.get("last") if ex.get("ok") else "?"
        md = motor.get("motor_decision", "?")
        pl = motor.get("pred_level", "?")
        comb = (motor.get("metrics") or {}).get("combined")
        comb_s = f"{comb:.2f}" if isinstance(comb, (int, float)) else "?"
        match = "✓" if motor.get("motor_decision") == motor.get("pred_decision") else "≠"
        print(
            f"[{i:02d}] ex={ex_p} comb={comb_s} motor={md} pred={motor.get('pred_decision')} "
            f"lvl={pl} sync={match}"
        )

        sleep_s = max(0.5, interval_s - (time.time() - t0))
        if time.time() + sleep_s < t_end:
            await asyncio.sleep(sleep_s)

    analysis = analyze_ticks(ticks)
    summary_path = log_path.with_suffix(".analysis.json")
    summary = {
        "duration_s": duration_s,
        "interval_s": interval_s,
        "symbol": symbol,
        "ticks": len(ticks),
        "log_path": str(log_path),
        "analysis": analysis,
        "ended_at": time.time(),
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def _forward_return_bps(ticks: List[Dict[str, Any]], idx: int, horizon: int = 1) -> Optional[float]:
    if idx + horizon >= len(ticks):
        return None
    p0 = ticks[idx].get("exchange", {}).get("last")
    p1 = ticks[idx + horizon].get("exchange", {}).get("last")
    if not p0 or not p1:
        return None
    try:
        return (float(p1) - float(p0)) / float(p0) * 10000.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _decision_correct(decision: str, ret_bps: float, *, hold_band_bps: float = 3.0) -> Optional[bool]:
    d = str(decision or "hold").lower()
    if d == "buy":
        return ret_bps > 0
    if d == "sell":
        return ret_bps < 0
    if d == "hold":
        return abs(ret_bps) <= hold_band_bps
    return None


def _metric_correlation(ticks: List[Dict[str, Any]], key: str) -> Optional[float]:
    pairs: List[Tuple[float, float]] = []
    for i in range(len(ticks) - 1):
        ret = _forward_return_bps(ticks, i, 1)
        m = (ticks[i].get("motor") or {}).get("metrics") or {}
        v = m.get(key)
        if ret is not None and isinstance(v, (int, float)) and not math.isnan(float(v)):
            pairs.append((float(v), ret))
    if len(pairs) < 5:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in pairs)
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x < 1e-12 or den_y < 1e-12:
        return None
    return num / (den_x * den_y)


def analyze_ticks(ticks: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not ticks:
        return {"error": "no_ticks"}

    motor_decisions = [((t.get("motor") or {}).get("motor_decision") or "hold") for t in ticks]
    pred_decisions = [((t.get("motor") or {}).get("pred_decision") or "hold") for t in ticks]
    decision_sync = sum(1 for m, p in zip(motor_decisions, pred_decisions) if m == p)

    motor_precision: Dict[str, Dict[str, Any]] = {}
    pred_precision: Dict[str, Dict[str, Any]] = {}

    for label, key_fn in (
        ("motor", lambda t: (t.get("motor") or {}).get("motor_decision")),
        ("pred", lambda t: (t.get("motor") or {}).get("pred_decision")),
    ):
        by_dec: Dict[str, List[float]] = {"buy": [], "sell": [], "hold": []}
        correct: Dict[str, int] = {"buy": 0, "sell": 0, "hold": 0}
        total: Dict[str, int] = {"buy": 0, "sell": 0, "hold": 0}
        for i, t in enumerate(ticks):
            dec = str(key_fn(t) or "hold").lower()
            if dec not in by_dec:
                dec = "hold"
            ret = _forward_return_bps(ticks, i, 1)
            if ret is None:
                continue
            by_dec[dec].append(ret)
            total[dec] += 1
            ok = _decision_correct(dec, ret)
            if ok:
                correct[dec] += 1
        store = motor_precision if label == "motor" else pred_precision
        for dec in ("buy", "sell", "hold"):
            n = total[dec]
            store[dec] = {
                "n": n,
                "precision": (correct[dec] / n) if n else None,
                "avg_forward_bps": statistics.mean(by_dec[dec]) if by_dec[dec] else None,
            }

    hold_ticks: List[Dict[str, Any]] = []
    for i, t in enumerate(ticks):
        motor = t.get("motor") or {}
        if str(motor.get("motor_decision") or "hold").lower() != "hold":
            continue
        ret = _forward_return_bps(ticks, i, 1)
        if ret is None:
            continue
        metrics = motor.get("metrics") or {}
        hold_ticks.append(
            {
                "i": t.get("i"),
                "forward_bps": ret,
                "good_hold": abs(ret) <= 3.0,
                "avoided_loss": (ret < -3 and (metrics.get("combined") or 0) > 0)
                or (ret > 3 and (metrics.get("combined") or 0) < 0),
                "blockers": motor.get("motor_blockers"),
                "combined": metrics.get("combined"),
                "mom": metrics.get("mom"),
                "egm": metrics.get("egm"),
                "tfi": metrics.get("tfi"),
                "market_state": motor.get("motor_market_state"),
            }
        )

    good_holds = sum(1 for h in hold_ticks if h.get("good_hold"))
    correlations = {
        k: _metric_correlation(ticks, k)
        for k in METRIC_KEYS
        if _metric_correlation(ticks, k) is not None
    }
    best_metric = None
    if correlations:
        best_metric = max(correlations.items(), key=lambda x: abs(x[1]))

    price_deltas = [
        t.get("price_delta_motor_vs_exchange")
        for t in ticks
        if t.get("price_delta_motor_vs_exchange") is not None
    ]

    levels = {}
    for t in ticks:
        lvl = (t.get("motor") or {}).get("pred_level") or "L0"
        levels[lvl] = levels.get(lvl, 0) + 1

    blockers_hold = {}
    for h in hold_ticks:
        for b in h.get("blockers") or []:
            blockers_hold[b] = blockers_hold.get(b, 0) + 1

    return {
        "motor_pred_decision_sync_rate": decision_sync / len(ticks) if ticks else 0,
        "motor_precision_1tick": motor_precision,
        "pred_precision_1tick": pred_precision,
        "hold_analysis": {
            "n": len(hold_ticks),
            "good_hold_rate": good_holds / len(hold_ticks) if hold_ticks else None,
            "top_blockers_on_hold": sorted(blockers_hold.items(), key=lambda x: -x[1])[:8],
            "samples": hold_ticks[:10],
        },
        "metric_forward_correlation": correlations,
        "best_predictive_metric": (
            {"name": best_metric[0], "corr_with_1tick_return_bps": round(best_metric[1], 4)}
            if best_metric
            else None
        ),
        "exchange_vs_motor_price": {
            "avg_delta_usd": statistics.mean(price_deltas) if price_deltas else None,
            "max_abs_delta_usd": max(abs(x) for x in price_deltas) if price_deltas else None,
        },
        "level_distribution": levels,
        "insight": _build_insight(
            motor_precision, pred_precision, correlations, good_holds, len(hold_ticks), levels
        ),
    }


def _build_insight(
    motor_p: Dict[str, Any],
    pred_p: Dict[str, Any],
    corr: Dict[str, float],
    good_holds: int,
    n_holds: int,
    levels: Dict[str, int],
) -> str:
    parts: List[str] = []
    hp = motor_p.get("hold", {})
    if hp.get("precision") is not None:
        parts.append(f"Holds motor: {hp['precision']*100:.0f}% acertaron banda lateral (1 tick).")
    if n_holds:
        parts.append(f"Buenos holds: {good_holds}/{n_holds}.")
    if corr:
        top = max(corr.items(), key=lambda x: abs(x[1]))
        parts.append(f"Métrica más correlacionada con retorno 1-tick: {top[0]} (r={top[1]:.3f}).")
    if levels:
        parts.append(f"Niveles: {levels}.")
    mp = motor_p.get("buy", {})
    if mp.get("n"):
        parts.append(f"Buys motor: n={mp['n']} precision={mp.get('precision')}.")
    return " ".join(parts) if parts else "Sin datos suficientes."


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor exchange vs motor NerT")
    parser.add_argument("--duration-min", type=float, default=15.0)
    parser.add_argument("--interval", type=float, default=20.0)
    parser.add_argument("--symbol", default=None)
    args = parser.parse_args()

    settings = DEFAULT_SETTINGS
    symbol = args.symbol or settings.symbol
    ts_tag = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    log_path = OUTPUT_DIR / f"exchange_motor_compare_{ts_tag}.jsonl"

    summary = asyncio.run(
        run_session(
            duration_s=max(60.0, args.duration_min * 60.0),
            interval_s=max(5.0, args.interval),
            symbol=symbol,
            base_url=settings.live_api_url,
            log_path=log_path,
        )
    )
    print("\n=== ANÁLISIS EXCHANGE vs MOTOR ===")
    print(json.dumps(summary.get("analysis", {}), indent=2, ensure_ascii=False))
    print(f"\nGuardado: {log_path.with_suffix('.analysis.json')}")


if __name__ == "__main__":
    main()