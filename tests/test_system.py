import asyncio
import os
import sys
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


class DummyBybitClient:
    def __init__(self, api_key, api_secret, base_url, recv_window="5000", **kwargs):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.recv_window = recv_window


class DummyTrade:
    def __init__(self, action, profit_loss, combined, egm=0.02):
        self.action = action
        self.profit_loss = profit_loss
        self.combined = combined
        self.egm = egm
        self.pio = 0.0
        self.ild = 0.0
        self.rol = 0.0
        self.ogm = 0.0


class BybitClientUsageTests(unittest.TestCase):
    def setUp(self):
        self._config = nertzh.config
        self._saved = {
            "LIVE_TRADING_ENABLED": getattr(self._config, "LIVE_TRADING_ENABLED", False),
            "BYBIT_API_KEY": getattr(self._config, "BYBIT_API_KEY", None),
            "BYBIT_API_SECRET": getattr(self._config, "BYBIT_API_SECRET", None),
            "BYBIT_ENV": getattr(self._config, "BYBIT_ENV", ""),
            "USE_TESTNET": getattr(self._config, "USE_TESTNET", True),
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
        self._config.USE_TESTNET = True
        nertzh.BybitV5Client = DummyBybitClient
        engine = nertzh.NertzMetalEngine()
        client = engine._bybit_client()
        assert client is not None
        self.assertIsNotNone(client)
        self.assertEqual(client.base_url, "https://api-demo.bybit.com")

    def test_unidad_bybit_client_testnet(self):
        self._config.LIVE_TRADING_ENABLED = True
        self._config.BYBIT_API_KEY = "k"
        self._config.BYBIT_API_SECRET = "s"
        self._config.BYBIT_ENV = ""
        self._config.USE_TESTNET = True
        nertzh.BybitV5Client = DummyBybitClient
        engine = nertzh.NertzMetalEngine()
        client = engine._bybit_client()
        assert client is not None
        self.assertIsNotNone(client)
        self.assertEqual(client.base_url, "https://api-testnet.bybit.com")


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
        res_buy = determine_decision_from_metrics({"combined": 7.0, "egm": 0.02})
        res_sell = determine_decision_from_metrics({"combined": -7.0, "egm": -0.02})
        self.assertEqual(res_hold, "hold")
        self.assertEqual(res_buy, "buy")
        self.assertEqual(res_sell, "sell")

    def test_consolidacion_evaluacion_trades(self):
        trades = [
            DummyTrade("buy", 3.0, 7.0, 0.02),
            DummyTrade("sell", -2.0, -7.0, -0.02),
            DummyTrade("buy", 1.0, 0.5, 0.02),
        ]
        th = Thresholds(6.5, -6.5, 1.5)
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
        )
        self.assertTrue(0.9 <= sum_abs <= 1.1)

    def test_autonomia_sin_trades(self):
        start = Thresholds(6.5, -6.5, 1.5)
        res = optimize_system_from_trades([], start_thresholds=start, iterations=5, seed=1)
        self.assertFalse(res.success)
        self.assertEqual(res.baseline.get("error"), "no_trades")


if __name__ == "__main__":
    unittest.main()
