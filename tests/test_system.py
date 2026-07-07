import asyncio
import json
import os
import sys
import tempfile
import unittest

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import Nertzh as nertzh
from optimizer import (
    Thresholds,
    determine_decision_from_metrics,
    _evaluate_system,
    optimize_system_from_trades,
)
from utils import (
    append_results_event,
    load_results_json,
    patch_results,
    save_results,
    update_last_balance,
    validate_results_file,
)


class DummyBybitClient:
    def __init__(self, api_key, api_secret, base_url, recv_window="5000", **kwargs):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.recv_window = recv_window


def _trade_metrics(action: str, combined: float) -> dict:
    bullish = {
        "combined": abs(combined),
        "combined_z": abs(combined) / 10.0,
        "pio": 1.0,
        "egm": 0.5,
        "mom": 0.2,
        "tfi": 0.9,
        "volatility": 0.003,
        "ema_diff_rel": 0.001,
        "igd_n5_n20": 0.001,
        "cbd_n20": 1.0,
        "rvol": 1e-5,
        "data_ok": True,
        "metrics_calibrated": True,
    }
    if action == "sell":
        return {
            **bullish,
            "combined": -abs(combined),
            "combined_z": -abs(combined) / 10.0,
            "pio": -1.0,
            "egm": -0.5,
            "mom": -0.2,
            "tfi": -0.9,
            "ema_diff_rel": -0.001,
            "igd_n5_n20": -0.001,
        }
    return bullish


class DummyTrade:
    def __init__(self, action, profit_loss, combined, egm=0.02):
        self.action = action
        self.profit_loss = profit_loss
        self.combined = combined
        self.egm = egm
        metrics = _trade_metrics(action, combined)
        self.pio = metrics["pio"]
        self.ild = 0.0
        self.rol = 0.0
        self.ogm = 0.0
        self.mom = metrics["mom"]
        self.tfi = metrics["tfi"]
        self.bybit_raw = {"metrics_snapshot": {"metrics": metrics}}


class BybitClientUsageTests(unittest.TestCase):
    def setUp(self):
        self._config = nertzh.config
        self._saved = {
            "LIVE_TRADING_ENABLED": getattr(self._config, "LIVE_TRADING_ENABLED", False),
            "BYBIT_API_KEY": getattr(self._config, "BYBIT_API_KEY", None),
            "BYBIT_API_SECRET": getattr(self._config, "BYBIT_API_SECRET", None),
            "BYBIT_ENV": getattr(self._config, "BYBIT_ENV", "mainnet"),
        }
        self._saved_client = nertzh.BybitV5Client

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(self._config, k, v)
        nertzh.BybitV5Client = self._saved_client

    def test_unidad_bybit_client_deshabilitado(self):
        self._config.LIVE_TRADING_ENABLED = False
        self._config.BYBIT_API_KEY = "k"
        self._config.BYBIT_API_SECRET = "s"
        engine = nertzh.NertzMetalEngine()
        self.assertIsNone(engine._bybit_client())

    def test_unidad_bybit_client_demo(self):
        self._config.LIVE_TRADING_ENABLED = True
        self._config.BYBIT_API_KEY = "k"
        self._config.BYBIT_API_SECRET = "s"
        self._config.BYBIT_ENV = "demo"
        nertzh.BybitV5Client = DummyBybitClient
        engine = nertzh.NertzMetalEngine()
        client = engine._bybit_client()
        assert client is not None
        self.assertIsNotNone(client)
        self.assertEqual(client.base_url, "https://api-demo.bybit.com")

    def test_unidad_bybit_client_mainnet(self):
        self._config.LIVE_TRADING_ENABLED = True
        self._config.BYBIT_API_KEY = "k"
        self._config.BYBIT_API_SECRET = "s"
        self._config.BYBIT_ENV = "mainnet"
        nertzh.BybitV5Client = DummyBybitClient
        engine = nertzh.NertzMetalEngine()
        client = engine._bybit_client()
        assert client is not None
        self.assertIsNotNone(client)
        self.assertEqual(client.base_url, "https://api.bybit.com")


class FlujoYOperacionTests(unittest.TestCase):
    def setUp(self):
        self._config = nertzh.config
        self._saved = {
            "LIVE_TRADING_ENABLED": getattr(self._config, "LIVE_TRADING_ENABLED", False),
            "BYBIT_API_KEY": getattr(self._config, "BYBIT_API_KEY", None),
            "BYBIT_API_SECRET": getattr(self._config, "BYBIT_API_SECRET", None),
        }

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(self._config, k, v)

    def test_operaciones_decision_metrics(self):
        res_hold = determine_decision_from_metrics({"combined": 0.4, "egm": 0.02})
        res_buy = determine_decision_from_metrics(_trade_metrics("buy", 7.0))
        res_sell = determine_decision_from_metrics(_trade_metrics("sell", 7.0))
        self.assertEqual(res_hold, "hold")
        self.assertEqual(res_buy, "buy")
        self.assertEqual(res_sell, "sell")

    def test_consolidacion_evaluacion_trades(self):
        trades = [
            DummyTrade("buy", 3.0, 7.0, 0.02),
            DummyTrade("sell", -2.0, -7.0, -0.02),
            DummyTrade("buy", 1.0, 0.5, 0.02),
        ]
        th = Thresholds(6.5, -6.5, 1.5).symmetrized()
        ev = _evaluate_system(trades, th, None)
        self.assertEqual(ev["total_trades"], 3)
        self.assertEqual(ev["selected"], 2)
        self.assertEqual(ev["wins"], 1)
        self.assertEqual(ev["losses"], 1)

    def test_flujo_preflight_disabled(self):
        self._config.LIVE_TRADING_ENABLED = False
        engine = nertzh.NertzMetalEngine()
        res = asyncio.run(engine.preflight())
        self.assertTrue(res.get("success"))
        self.assertEqual(res.get("mode"), "disabled")

    def test_ejecucion_preflight_sin_credenciales(self):
        self._config.LIVE_TRADING_ENABLED = True
        self._config.BYBIT_API_KEY = ""
        self._config.BYBIT_API_SECRET = ""
        engine = nertzh.NertzMetalEngine()
        res = asyncio.run(engine.preflight())
        self.assertFalse(res.get("success"))
        self.assertEqual(res.get("mode"), "live")


class AutoaprendizajeYAutonomiaTests(unittest.TestCase):
    def test_escalado_autoaprendizaje_optimizacion(self):
        trades = []
        for i in range(20):
            action = "buy" if i % 2 == 0 else "sell"
            combined = 7.0 if action == "buy" else -7.0
            egm = 0.02 if action == "buy" else -0.02
            profit = 1.0 if i % 3 else -0.5
            trades.append(DummyTrade(action, profit, combined, egm))
        start = Thresholds(6.5, -6.5, 1.5)
        res = optimize_system_from_trades(trades, start_thresholds=start, iterations=20, seed=123)
        self.assertTrue(res.success)
        best = res.best
        th = best["thresholds"]
        w = best["weights"]
        self.assertTrue(1.0 <= float(th["combined_buy_threshold"]) <= 15.0)
        self.assertTrue(-15.0 <= float(th["combined_sell_threshold"]) <= -1.0)
        self.assertTrue(0.5 <= float(th["combined_hold_band"]) <= 6.0)
        self.assertTrue(1.0 <= float(w["scale"]) <= 25.0)
        sum_abs = (
            abs(float(w["pio"]))
            + abs(float(w["egm"]))
            + abs(float(w["ild"]))
            + abs(float(w["rol"]))
            + abs(float(w["ogm"]))
            + abs(float(w["mom"]))
            + abs(float(w.get("tfi", 0.0)))
        )
        self.assertTrue(0.9 <= sum_abs <= 1.1)

    def test_autonomia_sin_trades(self):
        start = Thresholds(6.5, -6.5, 1.5)
        res = optimize_system_from_trades([], start_thresholds=start, iterations=5, seed=1)
        self.assertFalse(res.success)
        self.assertEqual(res.baseline.get("error"), "no_trades")


class ResultsPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_save_preserves_trades_on_event_append(self):
        save_results(
            {
                "metadata": {"capital_actual": 1000.0},
                "trades": {"BTCUSDT": [{"trade_id": "t1", "action": "buy"}]},
                "summary": {"net_profit": 5.0},
            },
            log_dir=self.log_dir,
        )
        append_results_event({"type": "balance", "total_equity": 1005.0}, log_dir=self.log_dir)
        data = load_results_json(self.log_dir)
        self.assertEqual(len(data.get("trades", {}).get("BTCUSDT", [])), 1)
        self.assertIn("events", data)
        self.assertEqual(data["metadata"]["capital_actual"], 1000.0)

    def test_update_last_balance(self):
        update_last_balance(
            {"total_equity": 5000.0, "available_balance": 4800.0, "coin": "USDT"},
            log_dir=self.log_dir,
        )
        data = load_results_json(self.log_dir)
        self.assertEqual(data["last_balance"]["total_equity"], 5000.0)
        self.assertEqual(data["metadata"]["capital_actual"], 5000.0)

    def test_patch_does_not_wipe_metadata(self):
        save_results({"metadata": {"total_pnl": 10.0}, "trades": {}}, log_dir=self.log_dir)
        patch_results({"thresholds": {"values": {"buy": 4.5}}}, log_dir=self.log_dir)
        data = load_results_json(self.log_dir)
        self.assertEqual(data["metadata"]["total_pnl"], 10.0)
        self.assertIn("thresholds", data)

    def test_validate_ok(self):
        save_results(
            {"trades": {"BTCUSDT": []}, "metadata": {"capital_actual": 1.0}},
            log_dir=self.log_dir,
        )
        rep = validate_results_file(self.log_dir)
        self.assertTrue(rep["ok"])

    def test_atomic_json_parseable(self):
        save_results({"summary": {"net_profit": 1.0}}, log_dir=self.log_dir)
        path = os.path.join(self.log_dir, "results.json")
        with open(path, encoding="utf-8") as f:
            json.load(f)


if __name__ == "__main__":
    unittest.main()
