"""Tests unitarios de los módulos de nertz_core y del registro de configuración."""
import asyncio
import os
import random
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
for p in (SRC_DIR, BASE_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from nertz_core.accounting import pnl_summary, resolve_capital_inicial, trade_pnl  # noqa: E402
from nertz_core.history import MetricHistory  # noqa: E402
from nertz_core.market import OrderBook, parse_public_trade, parse_ticker  # noqa: E402
from nertz_core.sizing import InstrumentRules, SizingInputs, price_decimals, protective_levels, size_order  # noqa: E402
from settings import ConfigError, ConfigSettings, render_env_example  # noqa: E402
from signal_engine import evaluate_signal, raw_weights  # noqa: E402
import utils  # noqa: E402

XRP_RULES = InstrumentRules(tick_size=0.0001, qty_step=0.01, min_qty=1.0, min_notional=5.0)
BTC_RULES = InstrumentRules(tick_size=0.01, qty_step=0.000001, min_qty=0.000048, min_notional=5.0)


class SettingsTests(unittest.TestCase):
    def test_any_valid_symbol_is_accepted(self):
        cfg = ConfigSettings({"SYMBOL": "solusdt, 1000PEPEUSDT ,SOLUSDT"})
        self.assertEqual(cfg.symbols, ["SOLUSDT", "1000PEPEUSDT"])

    def test_invalid_values_fail_fast(self):
        for env in ({"SYMBOL": "BTC-USDT"}, {"RISK_FACTOR": "2"}, {"TIMEFRAME": "7m"}, {"LIVE_TRADING_ENABLED": "maybe"},
                    {"SIGNAL_PARAMS_JSON": '{"nope": 1}'}, {"COMBINED_WEIGHTS_JSON": '{"xyz": 1}'}):
            with self.subTest(env=env), self.assertRaises(ConfigError):
                ConfigSettings(env)

    def test_bool_aliases_and_normalization(self):
        cfg = ConfigSettings({"LIVE_TRADING_ENABLED": "1", "ORDER_TYPE": "market", "TIME_IN_FORCE": "ImmediateOrCancel",
                              "STORAGE_BACKEND": "sqlite"})
        self.assertTrue(cfg.LIVE_TRADING_ENABLED)
        self.assertEqual(cfg.ORDER_TYPE, "Market")
        self.assertEqual(cfg.TIME_IN_FORCE, "IOC")
        self.assertEqual(cfg.STORAGE_BACKEND, "sqlite_legacy")
        self.assertFalse(cfg.STORAGE_DISABLE_JSONL)

    def test_runtime_update_is_validated_and_atomic(self):
        cfg = ConfigSettings({})
        with self.assertRaises(ConfigError):
            cfg.update({"RISK_FACTOR": 0.5, "FEE_RATE": 9})
        self.assertEqual(cfg.RISK_FACTOR, 0.01)
        changes = cfg.update({"risk_factor": "0.5"})
        self.assertEqual(changes["RISK_FACTOR"]["after"], 0.5)
        with self.assertRaises(ConfigError):
            cfg.update({"UNKNOWN_KEY": 1})
        with self.assertRaises(ConfigError):
            cfg.RISK_FACTOR = 3
        cfg.COMBINED_BUY_THRESHOLD = "5"
        self.assertEqual(cfg.COMBINED_BUY_THRESHOLD, 5.0)

    def test_symbol_overrides(self):
        cfg = ConfigSettings({"SYMBOL_OVERRIDES_JSON": '{"XRPUSDT": {"RISK_FACTOR": 0.05, "MAX_TRADE_SIZE": 500}}'})
        self.assertEqual(cfg.for_symbol("XRPUSDT", "RISK_FACTOR"), 0.05)
        self.assertEqual(cfg.for_symbol("xrpusdt", "MAX_TRADE_SIZE"), 500.0)
        self.assertEqual(cfg.for_symbol("BTCUSDT", "RISK_FACTOR"), 0.01)

    def test_endpoints_and_overrides(self):
        self.assertEqual(ConfigSettings({"BYBIT_ENV": "demo"}).endpoints().rest_private, "https://api-demo.bybit.com")
        self.assertEqual(ConfigSettings({"BYBIT_ENV": "demo"}).endpoints().rest_public, "https://api.bybit.com")
        cfg = ConfigSettings({"BYBIT_REST_URL": "https://proxy.local/", "BYBIT_WS_PUBLIC_URL": "wss://ws.local"})
        self.assertEqual(cfg.endpoints().rest_private, "https://proxy.local")
        self.assertEqual(cfg.endpoints().ws_public, "wss://ws.local")
        self.assertEqual(ConfigSettings({"ORDERBOOK_DEPTH": "120"}).ws_orderbook_depth, 200)

    def test_env_example_covers_every_setting(self):
        text = render_env_example()
        for s in ConfigSettings({}).schema():
            self.assertIn(f"\n{s['key']}=", text)

    def test_signal_params_override(self):
        cfg = ConfigSettings({"SIGNAL_PARAMS_JSON": '{"mom_confirm": 0.5}'})
        self.assertEqual(cfg.signal_params.mom_confirm, 0.5)
        m = {"combined": 7.0, "combined_z": 0.7, "pio": 1.0, "egm": 1.0, "mom": 0.2, "tfi": 0.9,
             "volatility": 0.003, "rvol": 1e-5}
        self.assertEqual(evaluate_signal(m, buy_th=4.5, sell_th=-4.5, hold_band=3.0)["decision"], "buy")
        self.assertEqual(evaluate_signal(m, buy_th=4.5, sell_th=-4.5, hold_band=3.0,
                                         params=cfg.signal_params)["decision"], "hold")


class SizingTests(unittest.TestCase):
    def test_low_price_symbol_uses_tick_precision(self):
        tp, sl = protective_levels("buy", 0.5123, 0.004, 1.5, 0.5, XRP_RULES)
        self.assertEqual(tp, Decimal("0.5154"))
        self.assertEqual(sl, Decimal("0.5113"))
        self.assertEqual(price_decimals(XRP_RULES), 4)

    def test_levels_always_on_correct_side(self):
        tp, sl = protective_levels("sell", 100.0, 0.0, 1.5, 0.5, BTC_RULES)
        self.assertLess(tp, Decimal("100"))
        self.assertGreater(sl, Decimal("100"))

    def test_notional_cap_and_exchange_minimums(self):
        inp = SizingInputs(capital=2000, risk_factor=0.01, volatility=0.002, last_price=60000, entry_price=60000,
                           max_position_notional_pct=0.10, min_notional_buffer=1.1)
        res = size_order(inp, BTC_RULES)
        self.assertTrue(res.ok)
        self.assertLessEqual(float(res.quantity) * 60000, 200.0 + 1e-6)
        tiny = size_order(SizingInputs(capital=2000, risk_factor=0.0, volatility=0.5, last_price=0.5, entry_price=0.5,
                                       max_position_notional_pct=0.10, min_notional_buffer=1.1), XRP_RULES)
        self.assertTrue(tiny.ok)
        self.assertGreaterEqual(float(tiny.quantity) * 0.5, 5.0)

    def test_rejects_when_capital_cannot_cover_minimum(self):
        res = size_order(SizingInputs(capital=3, risk_factor=0.01, volatility=0.01, last_price=0.5, entry_price=0.5,
                                      max_position_notional_pct=1.0, min_notional_buffer=1.1), XRP_RULES)
        self.assertFalse(res.ok)
        self.assertEqual(res.reason, "capital_insuficiente_para_minimo_exchange")

    def test_rules_from_bybit(self):
        r = InstrumentRules.from_bybit({"priceFilter": {"tickSize": "0.0001"},
                                        "lotSizeFilter": {"basePrecision": "0.01", "minOrderQty": "1",
                                                          "minOrderAmt": "5"}})
        self.assertEqual((r.tick_size, r.qty_step, r.min_qty, r.min_notional), (0.0001, 0.01, 1.0, 5.0))


class AccountingTests(unittest.TestCase):
    def test_fees_increase_losses(self):
        loss = trade_pnl("buy", 100, 99, 1, 0.001)
        self.assertAlmostEqual(loss.gross, -1.0)
        self.assertAlmostEqual(loss.net, -1.0 - 0.199)
        win = trade_pnl("sell", 100, 98, 2, 0.001)
        self.assertAlmostEqual(win.net, 4.0 - 0.001 * (200 + 196))

    def test_summary_only_counts_final(self):
        class T:
            def __init__(self, sym, st, pl):
                self.symbol, self.outcome_status, self.profit_loss = sym, st, pl

        rows = [T("A", "final", 2.0), T("A", "final", -1.0), T("A", "filled", -0.1), T("B", "final", 1.0)]
        s = pnl_summary(rows, ["A", "B"])
        self.assertEqual(s["total_trades"], 3)
        self.assertAlmostEqual(s["net_profit"], 2.0)
        self.assertEqual(s["by_symbol"]["A"]["trade_count"], 2)

    def test_capital_inicial_prefers_wallet_over_simulated(self):
        self.assertEqual(resolve_capital_inicial(2000, "simulated", "bybit_wallet_balance", 950, 2000), 950)
        self.assertEqual(resolve_capital_inicial(900, "bybit_wallet_balance", "bybit_wallet_balance", 950, 2000), 900)
        self.assertEqual(resolve_capital_inicial(None, None, "simulated", 0, 2000), 2000)


class MarketTests(unittest.TestCase):
    def test_orderbook_snapshot_delta(self):
        ob = OrderBook(depth=2)
        self.assertFalse(ob.apply_delta([["1", "1"]], []))
        ob.apply_snapshot([["10", "1"], ["9", "2"], ["8", "3"]], [["11", "1"], ["12", "2"]])
        self.assertEqual(ob["bids"], [["10", "1"], ["9", "2"]])
        ob.apply_delta([["10", "0"], ["9.5", "4"]], [["10.5", "7"]])
        self.assertEqual(ob["bids"], [["9.5", "4"], ["9", "2"]])
        self.assertEqual(ob["asks"][0], ["10.5", "7"])
        self.assertEqual(ob.best_bid(), 9.5)
        self.assertEqual(dict(ob)["asks"], ob["asks"])

    def test_parsers(self):
        self.assertIsNone(parse_ticker({"lastPrice": "1"}))
        t = parse_ticker({"lastPrice": "1", "volume24h": "2", "highPrice24h": "3", "lowPrice24h": "0.5"})
        self.assertEqual(t["turnover_24h"], 0.0)
        tr = parse_public_trade({"T": 1_700_000_000_000, "v": "2", "p": "3", "S": "Sell"}, 0.0)
        self.assertEqual((tr["ts"], tr["qty"], tr["side"]), (1_700_000_000.0, 2.0, "Sell"))
        self.assertIsNone(parse_public_trade({"v": "0"}, 0.0))


class MetricsTests(unittest.TestCase):
    def _inputs(self):
        candles = [{"open": 100 + i * 0.1, "high": 100.5 + i * 0.1, "low": 99.5 + i * 0.1, "close": 100 + i * 0.12,
                    "volume": 10 + i} for i in range(50)]
        ob = {"bids": [[str(100 - i * 0.01), str(1 + i % 3)] for i in range(50)],
              "asks": [[str(100.01 + i * 0.01), str(1 + i % 4)] for i in range(50)]}
        trades = [{"ts": 1e10, "qty": 1, "price": 100, "side": "Buy" if i % 3 else "Sell"} for i in range(20)]
        return candles, ob, trades

    def test_columnar_history_matches_legacy_list(self):
        rng = random.Random(7)
        rows = [{"ts": i, **{k: rng.gauss(0, 1) for k in ("pio", "ild", "egm", "rol", "ogm", "mom_raw", "tfi_raw",
                                                           "asymmetry", "spread_pct")}} for i in range(400)]
        legacy = [{k: v for k, v in r.items() if k != "ts"} for r in rows]
        candles, ob, trades = self._inputs()
        a = utils.calculate_metrics(candles, ob, {"last_price": 100, "metric_history": legacy}, depth=50,
                                    recent_trades=trades)
        b = utils.calculate_metrics(candles, ob, {"last_price": 100, "metric_history": MetricHistory(rows)}, depth=50,
                                    recent_trades=trades)
        for k, v in a.items():
            if isinstance(v, float):
                self.assertAlmostEqual(v, b[k], places=9, msg=k)
        self.assertNotEqual(a["tfi_z"], 0.0)

    def test_zero_weight_is_respected(self):
        self.assertEqual(raw_weights({"tfi": 0.0})["tfi"], 0.0)
        candles, ob, trades = self._inputs()
        rows = [{"ts": i, "tfi_raw": (-1) ** i * 0.5} for i in range(20)]
        base = {"last_price": 100, "metric_history": MetricHistory(rows)}
        with_tfi = utils.calculate_metrics(candles, ob, dict(base), recent_trades=trades)
        no_tfi = utils.calculate_metrics(candles, ob, {**base, "combined_weights": {"tfi": 0.0}}, recent_trades=trades)
        self.assertEqual(no_tfi["combined_weights"]["tfi"], 0.0)
        self.assertNotAlmostEqual(with_tfi["combined"], no_tfi["combined"])

    def test_history_eviction(self):
        h = MetricHistory([{"ts": t, "pio": float(t)} for t in range(10)])
        h.evict_older_than(5)
        self.assertEqual(len(h), 5)
        self.assertEqual(h.column("pio").tolist(), [5.0, 6.0, 7.0, 8.0, 9.0])
        self.assertEqual(h.tail("pio", 2), [8.0, 9.0])

    def test_tp_sl_helper_does_not_round(self):
        tp, sl = utils.calculate_tp_sl(0.5123, 0.004, "buy", 1.5, 0.5)
        self.assertAlmostEqual(tp, 0.5123 + 0.5123 * 0.004 * 1.5)


class DuckDBWriterTests(unittest.TestCase):
    def test_flush_persists_everything_enqueued(self):
        from nertz_engine.storage import DuckDBBackend, MetricRow

        async def run(path):
            b = DuckDBBackend(path, flush_interval_ms=10)
            await b.start()
            for i in range(300):
                await b.enqueue_metric(MetricRow(timestamp=datetime.now(timezone.utc), symbol="BTCUSDT",
                                                 last_price=1.0, metrics={"pio_raw": float(i)}))
                if i % 7 == 0:
                    await b.flush()
            await b.flush()
            hist = await b.fetch_metric_history("BTCUSDT", window_s=600, max_rows=1000)
            await b.stop()
            return hist

        with tempfile.TemporaryDirectory() as tmp:
            hist = asyncio.run(run(os.path.join(tmp, "t.duckdb")))
        self.assertEqual(len(hist), 300)
        self.assertEqual(hist[-1]["pio"], 299.0)
        self.assertLess(abs(hist[-1]["ts"] - time.time()), 60)


if __name__ == "__main__":
    unittest.main()
