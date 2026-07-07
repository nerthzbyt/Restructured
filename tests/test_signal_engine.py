import os
import sys
import unittest

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from signal_engine import (  # noqa: E402
    DEFAULT_COMBINED_WEIGHTS,
    MarketState,
    check_execution_gates,
    determine_decision_from_metrics,
    evaluate_signal,
    is_spoof_trap,
    recompute_combined,
    symmetrize_threshold_values,
)


def _bullish(combined: float = 7.0) -> dict:
    return {
        "combined": combined,
        "combined_z": combined / 10.0,
        "pio": 1.0,
        "egm": 1.0,
        "mom": 0.2,
        "tfi": 0.9,
        "volatility": 0.003,
        "ema_diff_rel": 0.001,
        "igd_n5_n20": 0.001,
        "cbd_n20": 1.0,
        "rvol": 1e-5,
        "spread_bps": 1.0,
        "microprice_offset_bps": 0.001,
        "data_ok": True,
        "metrics_calibrated": True,
        "recent_trades_last_trade_age_s": 1.0,
    }


def _bearish(combined: float = -7.0) -> dict:
    m = _bullish(abs(combined))
    m["combined"] = combined
    m["combined_z"] = combined / 10.0
    m["pio"] = -1.0
    m["egm"] = -1.0
    m["mom"] = -0.2
    m["tfi"] = -0.9
    m["ema_diff_rel"] = -0.001
    m["igd_n5_n20"] = -0.001
    m["microprice_offset_bps"] = -0.001
    return m


class SignalEngineTests(unittest.TestCase):
    def test_symmetric_thresholds(self):
        th = symmetrize_threshold_values(3.5, -6.0, 2.0)
        self.assertAlmostEqual(th.combined_buy_threshold, 4.75)
        self.assertAlmostEqual(th.combined_sell_threshold, -4.75)

    def test_default_weights_include_tfi(self):
        w = DEFAULT_COMBINED_WEIGHTS
        d = w.as_dict()
        self.assertIn("tfi", d)
        self.assertGreater(d["pio"], 0.15)
        self.assertGreater(d["tfi"], 0.15)
        total = sum(abs(float(d[k])) for k in ("pio", "egm", "ild", "rol", "ogm", "mom", "tfi"))
        self.assertAlmostEqual(total, 1.0, places=2)

    def test_recompute_combined_uses_tfi(self):
        w = DEFAULT_COMBINED_WEIGHTS
        base = recompute_combined(
            {"pio": 1.0, "egm": 0.0, "ild": 0.0, "rol": 0.0, "ogm": 0.0, "mom": 0.0, "tfi": 0.0},
            w,
        )
        with_tfi = recompute_combined(
            {"pio": 1.0, "egm": 0.0, "ild": 0.0, "rol": 0.0, "ogm": 0.0, "mom": 0.0, "tfi": 2.0},
            w,
        )
        self.assertNotAlmostEqual(base, with_tfi)

    def test_spoof_trap_detected(self):
        sig = {
            "pio": 1.5,
            "tfi": -0.95,
            "combined": 10.0,
            "rvol": 1e-7,
        }
        self.assertTrue(is_spoof_trap(sig))

    def test_toxic_market_blocks_buy(self):
        m = _bullish(10.0)
        m["tfi"] = -0.95
        ev = evaluate_signal(m, buy_th=4.5, sell_th=-4.5, hold_band=3.0)
        self.assertEqual(ev["market_state"], MarketState.TOXIC.value)
        self.assertEqual(ev["decision"], "hold")

    def test_optimal_buy_signal(self):
        ev = evaluate_signal(_bullish(), buy_th=4.5, sell_th=-4.5, hold_band=3.0)
        self.assertEqual(ev["decision"], "buy")
        self.assertIn(ev["market_state"], {MarketState.OPTIMAL.value, MarketState.BREAKOUT.value, MarketState.NEUTRAL.value})

    def test_chop_hold(self):
        m = _bullish(1.0)
        m["volatility"] = 0.0002
        m["tfi"] = 0.1
        ev = evaluate_signal(m, buy_th=4.5, sell_th=-4.5, hold_band=3.0)
        self.assertEqual(ev["decision"], "hold")

    def test_execution_gate_stale_trades(self):
        m = _bullish()
        m["recent_trades_last_trade_age_s"] = 10.0
        ok, reason = check_execution_gates(m)
        self.assertFalse(ok)
        self.assertEqual(reason, "trade_age_stale")

    def test_execution_gate_allows_breakout_rvol(self):
        m = _bullish()
        m["rvol"] = 0.05
        ok, _ = check_execution_gates(m)
        self.assertTrue(ok)

    def test_determine_decision_hold_in_band(self):
        self.assertEqual(
            determine_decision_from_metrics({"combined": 1.0, "egm": 0.02}),
            "hold",
        )


if __name__ == "__main__":
    unittest.main()