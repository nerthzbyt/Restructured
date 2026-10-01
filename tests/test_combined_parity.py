"""Paridad runtime <-> optimizer/backtest en la composición del combined.

Los snapshots se generan con el ``utils.calculate_metrics`` real (simulación
secuencial con historia, igual que el ciclo del motor), así que ``combined``,
``combined_z`` y ``combined_components`` son exactamente los del runtime.
"""
import json
import os
import random
import sys
import unittest
from types import SimpleNamespace

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
for p in (SRC_DIR, BASE_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

import optimizer  # noqa: E402
import utils  # noqa: E402
from nertz_core.engine.reporting import ReportingMixin  # noqa: E402
from nertz_core.history import MetricHistory, raw_sample_from_metrics  # noqa: E402
from settings import DEFAULT_FORMULAS, ConfigSettings  # noqa: E402
from signal_engine import (  # noqa: E402
    COMBINED_INPUT_KEYS,
    RAW_DEFAULT_WEIGHTS,
    RUNTIME_COMBINED_WEIGHTS,
    CombinedWeights,
    Thresholds,
    compose_combined,
    evaluate_signal,
    raw_weights,
    recompute_combined,
    recompute_composition,
)

TOL = 1e-10
REAL_FIXTURE = os.path.join(BASE_DIR, "tests", "fixtures", "runtime_parity_20261001.json")
BUY_TH, SELL_TH, HOLD = 4.5, -4.5, 3.0
COMPONENTS = ("pio", "egm", "ild", "rol", "ogm", "mom", "tfi")
EXPECTED_RAW = {
    "pio": 0.25, "egm": 0.30, "ild": -0.15, "rol": 0.10,
    "ogm": 0.05, "mom": 0.16, "tfi": 0.25, "scale": 10.0,
}
CUSTOM_WEIGHTS = {
    "pio": 0.4, "egm": 0.1, "ild": -0.3, "rol": 0.2,
    "ogm": -0.05, "mom": 0.35, "tfi": 0.6, "scale": 13.0,
}


def simulate_runtime(seed, n=260, warmup=30, combined_weights=None):
    """Snapshots del runtime: calculate_metrics en bucle alimentando su historia."""
    rng = random.Random(seed)
    price = 100.0
    closes = [price * (1 + rng.gauss(0, 0.001)) for _ in range(30)]
    hist = MetricHistory()
    prev_liq = None
    trend = 0.0
    out = []
    for step in range(n):
        if step % 25 == 0:
            trend = rng.choice([-1, 0, 1]) * rng.uniform(0.0005, 0.003)
        price *= 1 + trend + rng.gauss(0, 0.0015)
        closes.append(price)
        candles = [{"open": c, "high": c * 1.001, "low": c * 0.999, "close": c, "volume": 10.0}
                   for c in closes[-60:][::-1]]
        bias = max(-0.9, min(0.9, trend * 400 + rng.gauss(0, 0.3)))
        ob = {"bids": [[str(price * (1 - 0.0001 * (i + 1))), str(rng.uniform(0.5, 2) * (1 + bias))]
                       for i in range(50)],
              "asks": [[str(price * (1 + 0.0001 * (i + 1))), str(rng.uniform(0.5, 2) * (1 - bias))]
                       for i in range(50)]}
        trades = [{"ts": 1e10, "qty": rng.uniform(0.1, 3), "price": price,
                   "side": "Buy" if rng.random() < 0.5 + bias / 2 else "Sell"} for _ in range(20)]
        td = {"last_price": price, "metric_history": hist, "formulas": dict(DEFAULT_FORMULAS),
              "prev_weighted_liquidity": prev_liq, "rol_dt_s": 1.0}
        if combined_weights is not None:
            td["combined_weights"] = dict(combined_weights)
        m = utils.calculate_metrics(candles, ob, td, depth=50, recent_trades=trades)
        hist.append(float(step), raw_sample_from_metrics(m))
        prev_liq = m.get("weighted_liquidity")
        if step >= warmup:
            out.append(m)
    return out


def _cross(combined, ev):
    th = ev["thresholds_effective"]
    if combined >= th["buy"]:
        return "BUY_CROSS"
    if combined <= th["sell"]:
        return "SELL_CROSS"
    return "NO_CROSS"


def _with_recomputed(m, w=None):
    comp = recompute_composition(m, w)
    return {**m, "combined": comp.combined, "combined_z": comp.combined_z}


def _legacy_optimizer_combined(m):
    """Camino anterior del optimizer (bug): pesos normalizados + TFI crudo."""
    w = CombinedWeights.normalize(**RAW_DEFAULT_WEIGHTS)
    z = sum(getattr(w, k) * m[k] for k in ("pio", "egm", "ild", "rol", "ogm", "mom"))
    return (z + w.tfi * m["tfi"]) * w.scale


class CombinedParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snaps = simulate_runtime(1) + simulate_runtime(2)
        cls.custom_snaps = simulate_runtime(3, n=120, combined_weights=CUSTOM_WEIGHTS)
        decisions = [evaluate_signal(m, buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)["decision"]
                     for m in cls.snaps]
        # El fixture debe cubrir los tres resultados para que la paridad signifique algo.
        assert {"buy", "sell", "hold"} <= set(decisions), decisions

    # A) runtime parity ----------------------------------------------------------
    def test_a_runtime_parity_combined(self):
        for i, m in enumerate(self.snaps + self.custom_snaps):
            comp = recompute_composition(m)
            with self.subTest(i=i):
                self.assertLessEqual(abs(comp.combined - m["combined"]), TOL)
                self.assertLessEqual(abs(comp.combined_z - m["combined_z"]), TOL)
                self.assertLessEqual(abs(comp.combined_z_micro - m["combined_z_micro"]), TOL)

    def test_a_runtime_parity_with_explicit_weights(self):
        for m in self.snaps:
            self.assertLessEqual(abs(recompute_combined(m, RUNTIME_COMBINED_WEIGHTS) - m["combined"]), TOL)
            self.assertLessEqual(abs(recompute_combined(m, dict(EXPECTED_RAW)) - m["combined"]), TOL)
        custom = CombinedWeights.from_raw(CUSTOM_WEIGHTS)
        for m in self.custom_snaps:
            self.assertLessEqual(abs(recompute_combined(m, custom) - m["combined"]), TOL)

    def test_a_runtime_uses_canonical_composition_bit_exact(self):
        # El runtime ES compose_combined: igualdad exacta, no solo dentro de tolerancia.
        for m in self.snaps:
            z = {k: m[src] for k, src in COMBINED_INPUT_KEYS.items()}
            comp = compose_combined(z, m["combined_weights"])
            self.assertEqual(comp.combined, m["combined"])
            self.assertEqual(comp.combined_z, m["combined_z"])
            self.assertEqual(comp.combined_z_micro, m["combined_z_micro"])

    # B) component parity --------------------------------------------------------
    def test_b_component_parity(self):
        for i, m in enumerate(self.snaps + self.custom_snaps):
            comp = recompute_composition(m)
            with self.subTest(i=i):
                self.assertEqual(set(comp.components), set(COMPONENTS))
                for k in COMPONENTS:
                    self.assertLessEqual(abs(comp.components[k] - m["combined_components"][k]), TOL, k)
                self.assertLessEqual(abs(sum(comp.components.values()) - m["combined_components"]["sum_z"]), TOL)

    # C) threshold parity --------------------------------------------------------
    def test_c_threshold_cross_parity(self):
        crosses = set()
        for m in self.snaps:
            ev_rt = evaluate_signal(m, buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)
            rec = _with_recomputed(m, RUNTIME_COMBINED_WEIGHTS)
            ev_opt = evaluate_signal(rec, buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)
            self.assertEqual(ev_rt["thresholds_effective"], ev_opt["thresholds_effective"])
            self.assertEqual(_cross(m["combined"], ev_rt), _cross(rec["combined"], ev_opt))
            crosses.add(_cross(m["combined"], ev_rt))
        self.assertEqual(crosses, {"BUY_CROSS", "SELL_CROSS", "NO_CROSS"})

    # D) decision parity ---------------------------------------------------------
    def test_d_decision_parity_evaluate_signal(self):
        for m in self.snaps + self.custom_snaps:
            ev_rt = evaluate_signal(m, buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)
            ev_opt = evaluate_signal(_with_recomputed(m), buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)
            self.assertEqual(ev_rt["decision"], ev_opt["decision"])
            self.assertEqual(ev_rt["market_state"], ev_opt["market_state"])
            self.assertEqual(ev_rt["blockers"], ev_opt["blockers"])

    def test_d_optimizer_selects_every_runtime_trade(self):
        # Trades tal como los persiste el motor (metrics_snapshot serializado).
        trades = []
        for m in self.snaps:
            decision = evaluate_signal(m, buy_th=BUY_TH, sell_th=SELL_TH, hold_band=HOLD)["decision"]
            if decision in {"buy", "sell"}:
                snap = {"metrics": ReportingMixin._serialize_metrics_for_storage(m)}
                trades.append(SimpleNamespace(action=decision, profit_loss=1.0,
                                              bybit_raw={"metrics_snapshot": snap}))
        self.assertGreater(len(trades), 50)
        th = Thresholds(BUY_TH, SELL_TH, HOLD)
        ev = optimizer._evaluate_system(trades, th, RUNTIME_COMBINED_WEIGHTS)
        self.assertEqual(ev["selected"], len(trades))
        res = optimizer.optimize_system_from_trades(trades, start_thresholds=th, iterations=0)
        self.assertEqual(res.baseline["selected"], len(trades))

    def test_legacy_path_diverges(self):
        # Documenta el bug corregido: el camino anterior no reproducía el runtime.
        diffs = [abs(_legacy_optimizer_combined(m) - m["combined"]) for m in self.snaps]
        self.assertGreater(max(diffs), 1.0)

    # E) TFI ---------------------------------------------------------------------
    def test_e_snapshot_tfi_is_raw_and_differs_from_tfi_z(self):
        self.assertTrue(any(abs(m["tfi"] - m["tfi_z"]) > 1e-6 for m in self.snaps))
        for m in self.snaps:
            self.assertEqual(m["tfi_raw"], m["recent_trades_imbalance_qty_pct"])

    def test_e_recompute_uses_tfi_z_not_raw_tfi(self):
        m = self.snaps[0]
        base = recompute_combined(m)
        for raw_key in ("tfi", "tfi_raw", "recent_trades_imbalance_qty_pct"):
            self.assertEqual(recompute_combined({**m, raw_key: 123.0}), base, raw_key)
        bumped = recompute_combined({**m, "tfi_z": m["tfi_z"] + 1.0})
        self.assertAlmostEqual(bumped - base, EXPECTED_RAW["tfi"] * EXPECTED_RAW["scale"], places=9)
        self.assertEqual(recompute_composition(m).components["tfi"], EXPECTED_RAW["tfi"] * m["tfi_z"])

    def test_e_missing_tfi_z_does_not_fall_back_to_raw(self):
        m = {k: 0.0 for k in COMPONENTS}
        m["tfi"] = 0.9
        m["recent_trades_imbalance_qty_pct"] = 0.9
        self.assertEqual(recompute_combined(m), 0.0)

    # F) weights -----------------------------------------------------------------
    def test_f_production_weights_are_raw(self):
        self.assertEqual(RAW_DEFAULT_WEIGHTS, EXPECTED_RAW)
        self.assertEqual(raw_weights(), EXPECTED_RAW)
        self.assertEqual(RUNTIME_COMBINED_WEIGHTS.as_dict(), EXPECTED_RAW)
        self.assertEqual(CombinedWeights.from_raw(ConfigSettings({}).COMBINED_WEIGHTS_JSON).as_dict(), EXPECTED_RAW)
        for m in self.snaps:
            self.assertEqual(m["combined_weights"], EXPECTED_RAW)

    def test_f_from_raw_does_not_normalize_or_clamp(self):
        w = CombinedWeights.from_raw({**EXPECTED_RAW, "scale": 30.0})
        self.assertEqual(w.as_dict(), {**EXPECTED_RAW, "scale": 30.0})
        self.assertEqual(CombinedWeights.from_raw({"tfi": 0.0}).tfi, 0.0)
        norm = CombinedWeights.normalize(**EXPECTED_RAW)
        self.assertNotAlmostEqual(norm.pio, EXPECTED_RAW["pio"])

    def test_f_optimizer_baseline_keeps_raw_weights(self):
        trades = [SimpleNamespace(action="buy", profit_loss=1.0,
                                  bybit_raw={"metrics_snapshot": {"metrics": dict(self.snaps[0])}})]
        th = Thresholds(BUY_TH, SELL_TH, HOLD)
        res = optimizer.optimize_system_from_trades(trades, start_thresholds=th, iterations=0)
        self.assertEqual(res.baseline["weights"], EXPECTED_RAW)
        custom = CombinedWeights.from_raw(CUSTOM_WEIGHTS)
        res = optimizer.optimize_system_from_trades(trades, start_thresholds=th, start_weights=custom, iterations=0)
        self.assertEqual(res.baseline["weights"], CUSTOM_WEIGHTS)

    def test_f_optimizer_candidates_evaluated_as_returned(self):
        # Lo que el optimizer devuelve (y se aplicaría) es exactamente lo evaluado.
        trades = []
        for m in self.snaps[:80]:
            snap = {"metrics": ReportingMixin._serialize_metrics_for_storage(m)}
            trades.append(SimpleNamespace(action="buy" if m["combined"] > 0 else "sell",
                                          profit_loss=1.0 if m["mom"] * m["combined"] > 0 else -1.0,
                                          bybit_raw={"metrics_snapshot": snap}))
        th = Thresholds(BUY_TH, SELL_TH, HOLD)
        res = optimizer.optimize_system_from_trades(trades, start_thresholds=th, iterations=60, seed=5)
        best_w = CombinedWeights.from_raw(res.best["weights"])
        best_th = Thresholds(res.best["thresholds"]["combined_buy_threshold"],
                             res.best["thresholds"]["combined_sell_threshold"],
                             res.best["thresholds"]["combined_hold_band"])
        again = optimizer._evaluate_system(trades, best_th, best_w)
        self.assertEqual(again["selected"], res.best["selected"])
        self.assertEqual(again["net_profit"], res.best["net_profit"])


class RealRuntimeSnapshotParityTests(unittest.TestCase):
    """Mismas comprobaciones sobre snapshots reales del runtime (logs/results.json)."""

    @classmethod
    def setUpClass(cls):
        with open(REAL_FIXTURE, encoding="utf-8") as fh:
            cls.rows = json.load(fh)["snapshots"]
        assert len(cls.rows) > 1000

    @staticmethod
    def _kw(row):
        th = row["thresholds"]
        return dict(buy_th=th["combined_buy_threshold"], sell_th=th["combined_sell_threshold"],
                    hold_band=th["combined_hold_band"])

    def test_a_real_runtime_parity(self):
        for i, r in enumerate(self.rows):
            m = r["metrics"]
            comp = recompute_composition(m)
            self.assertLessEqual(abs(comp.combined - m["combined"]), TOL, i)
            self.assertLessEqual(abs(comp.combined_z - m["combined_z"]), TOL, i)
            self.assertLessEqual(abs(comp.combined_z_micro - m["combined_z_micro"]), TOL, i)
            self.assertLessEqual(abs(recompute_combined(m, RUNTIME_COMBINED_WEIGHTS) - m["combined"]), TOL, i)

    def test_b_real_component_parity(self):
        checked = 0
        for i, r in enumerate(self.rows):
            m = r["metrics"]
            if "combined_components" not in m:
                continue
            comp = recompute_composition(m)
            for k in COMPONENTS:
                self.assertLessEqual(abs(comp.components[k] - m["combined_components"][k]), TOL, (i, k))
            checked += 1
        self.assertGreater(checked, 1000)

    def test_c_d_real_threshold_and_decision_parity(self):
        crosses = set()
        for i, r in enumerate(self.rows):
            m = r["metrics"]
            ev_rt = evaluate_signal(m, **self._kw(r))
            ev_opt = evaluate_signal(_with_recomputed(m), **self._kw(r))
            self.assertEqual(_cross(m["combined"], ev_rt), _cross(ev_opt["combined"], ev_opt), i)
            self.assertEqual(ev_rt["decision"], r["expected_decision"], i)
            self.assertEqual(ev_opt["decision"], r["expected_decision"], i)
            crosses.add(_cross(m["combined"], ev_rt))
        self.assertEqual(crosses, {"BUY_CROSS", "SELL_CROSS", "NO_CROSS"})

    def test_d_real_optimizer_selects_every_runtime_trade(self):
        by_th = {}
        for r in self.rows:
            if r["expected_decision"] in {"buy", "sell"}:
                key = tuple(sorted(self._kw(r).items()))
                by_th.setdefault(key, []).append(SimpleNamespace(
                    action=r["expected_decision"], profit_loss=1.0,
                    bybit_raw={"metrics_snapshot": {"metrics": r["metrics"]}}))
        self.assertTrue(by_th)
        for key, trades in by_th.items():
            kw = dict(key)
            th = Thresholds(kw["buy_th"], kw["sell_th"], kw["hold_band"])
            ev = optimizer._evaluate_system(trades, th, RUNTIME_COMBINED_WEIGHTS)
            self.assertEqual(ev["selected"], len(trades))

    def test_e_real_tfi_z_not_raw(self):
        differ = 0
        for r in self.rows:
            m = r["metrics"]
            base = recompute_combined(m)
            self.assertEqual(recompute_combined({**m, "tfi": -m.get("tfi", 0.0) + 7.0}), base)
            if abs(m.get("tfi", m["tfi_z"]) - m["tfi_z"]) > 1e-6:
                differ += 1
        self.assertGreater(differ, 100)

    def test_legacy_path_reproduces_reported_bug(self):
        diffs, changed = [], 0
        for r in self.rows:
            m = r["metrics"]
            if "tfi" not in m:
                continue
            legacy = _legacy_optimizer_combined(m)
            diffs.append(abs(legacy - m["combined"]))
            ev_legacy = evaluate_signal({**m, "combined": legacy}, **self._kw(r))
            changed += ev_legacy["decision"] != r["expected_decision"]
        self.assertGreater(max(diffs), 8.0)
        self.assertGreater(changed, 0)


if __name__ == "__main__":
    unittest.main()
