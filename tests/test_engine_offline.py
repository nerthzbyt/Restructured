"""Integración offline del motor con un exchange Bybit simulado (sin red).

Cubre el ciclo completo: carga inicial REST -> mensajes WS -> métricas ->
colocación de orden -> sincronización del fill -> TP/SL virtual -> cierre con PnL.
"""
import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
for p in (SRC_DIR, BASE_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from nertz_core.accounting import trade_pnl  # noqa: E402
from nertz_core.db import Database, Trade  # noqa: E402
from nertz_core.engine import NertzMetalEngine  # noqa: E402
from nertz_core.runtime import RuntimePaths  # noqa: E402
from settings import ConfigSettings  # noqa: E402


class FakeBybit:
    """Implementa la parte del cliente que usa el motor; registra órdenes creadas."""

    instances = []

    def __init__(self, api_key, api_secret, base_url, recv_window="5000", **kwargs):
        self.api_key = api_key
        self.base_url = base_url
        self.created = []
        self.orders = {}
        self.tick_size = "0.0001"
        FakeBybit.instances.append(self)

    async def aclose(self):
        pass

    async def get_server_time(self):
        return {"retCode": 0, "result": {"timeSecond": str(int(time.time()))}}

    async def get_instruments_info(self, category, symbol=None):
        return {"retCode": 0, "result": {"list": [{
            "symbol": symbol,
            "priceFilter": {"tickSize": self.tick_size},
            "lotSizeFilter": {"basePrecision": "0.01", "minOrderQty": "1", "minOrderAmt": "5"},
        }]}}

    async def get_kline(self, category, symbol, interval, limit=200):
        now_ms = int(time.time() // 60 * 60 * 1000)
        rows = []
        for i in range(limit):
            p = 0.5 + 0.0005 * ((limit - i) % 7)
            rows.append([str(now_ms - i * 60000), str(p), str(p + 0.002), str(p - 0.002), str(p + 0.0003), "1000", "500"])
        return {"retCode": 0, "result": {"list": rows}}

    async def get_orderbook(self, category, symbol, limit=50):
        bids = [[f"{0.5 - i * 0.0001:.4f}", "1000"] for i in range(limit)]
        asks = [[f"{0.5001 + i * 0.0001:.4f}", "900"] for i in range(limit)]
        return {"retCode": 0, "result": {"b": bids, "a": asks, "u": 1}}

    async def get_tickers(self, category, symbol=None):
        return {"retCode": 0, "result": {"list": [{
            "lastPrice": "0.5", "volume24h": "1000000", "highPrice24h": "0.52", "lowPrice24h": "0.48",
            "turnover24h": "500000",
        }]}}

    async def wallet_balance(self, account_type="UNIFIED", coin=None):
        return {"http_status": 200, "retCode": 0, "result": {"list": [
            {"totalEquity": "1000", "totalAvailableBalance": "900"}]}}

    async def create_order(self, body):
        oid = f"oid-{len(self.created) + 1}"
        self.created.append(dict(body))
        self.orders[oid] = {**body, "orderId": oid, "orderStatus": "New", "cumExecQty": "0", "avgPrice": "0"}
        return {"http_status": 200, "retCode": 0, "result": {"orderId": oid}}

    async def cancel_order(self, body):
        return {"http_status": 200, "retCode": 0, "result": {}}

    async def amend_order(self, body):
        return {"http_status": 200, "retCode": 0, "result": {}}

    async def get_open_orders_merged(self, category, *, symbol=None, limit=50):
        rows = [o for o in self.orders.values() if o["orderStatus"] in {"New", "PartiallyFilled"}]
        return {"retCode": 0, "result": {"list": rows}}

    async def order_history(self, category, symbol=None, order_id=None, **kw):
        o = self.orders.get(order_id)
        return {"retCode": 0, "result": {"list": [o] if o else []}}

    async def order_realtime(self, category, symbol=None, order_id=None, **kw):
        return await self.order_history(category, symbol, order_id)

    async def execution_list(self, category, **kw):
        return {"retCode": 0, "result": {"list": []}}

    def fill(self, oid, price):
        o = self.orders[oid]
        o.update(orderStatus="Filled", cumExecQty=o["qty"], avgPrice=str(price), cumExecFee="0")


def _make_engine(tmp: str, **overrides) -> NertzMetalEngine:
    env = {
        "SYMBOL": "XRPUSDT",
        "LIVE_TRADING_ENABLED": "true",
        "BYBIT_API_KEY": "k",
        "BYBIT_API_SECRET": "s",
        "BYBIT_ENV": "testnet",
        "DATA_DIR": tmp,
        "LOGS_DIR": os.path.join(tmp, "logs"),
        "STORAGE_BACKEND": "sqlite_legacy",
        "STORAGE_DISABLE_JSONL": "true",
        "ORDER_TYPE": "Market",
        **{k: str(v) for k, v in overrides.items()},
    }
    cfg = ConfigSettings(env)
    paths = RuntimePaths.from_config(cfg)
    return NertzMetalEngine(cfg, Database(paths.sqlite_path), client_factory=FakeBybit, paths=paths)


class EngineOfflineTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        FakeBybit.instances = []

    def tearDown(self):
        self._tmp.cleanup()

    def test_endpoints_from_config(self):
        eng = _make_engine(self._tmp.name)
        ep = eng.config.endpoints()
        self.assertEqual(ep.rest_private, "https://api-testnet.bybit.com")
        self.assertEqual(ep.ws_public, "wss://stream-testnet.bybit.com/v5/public/spot")
        self.assertEqual(eng._topics("XRPUSDT")[1], "orderbook.50.XRPUSDT")

    def test_full_lifecycle_open_fill_close(self):
        eng = _make_engine(self._tmp.name, AUTO_TPSL_INTERVAL_S=0.25)

        async def scenario():
            self.assertTrue((await eng.preflight())["success"])
            await eng.fetch_initial_data()
            self.assertEqual(len(eng.candles["XRPUSDT"]), eng.config.CANDLE_BUFFER_SIZE)
            self.assertTrue(eng.orderbook_data["XRPUSDT"].is_ready())

            # WS: delta de libro + trades públicos + ticker.
            await eng._on_message(None, json.dumps({
                "topic": "orderbook.50.XRPUSDT", "type": "delta",
                "data": {"b": [["0.5000", "0"], ["0.4999", "2500"]], "a": [["0.5001", "100"]], "u": 2}}))
            book = eng.orderbook_data["XRPUSDT"]
            self.assertEqual(book["bids"][0], ["0.4999", "2500"])
            self.assertEqual(book["asks"][0], ["0.5001", "100"])
            await eng._on_message(None, json.dumps({
                "topic": "publicTrade.XRPUSDT",
                "data": [{"T": int(time.time() * 1000), "v": "10", "p": "0.5", "S": "Buy"}]}))
            self.assertEqual(len(eng.recent_trades["XRPUSDT"]), 1)
            await eng._on_message(None, json.dumps({
                "topic": "tickers.XRPUSDT",
                "data": {"lastPrice": "0.5", "volume24h": "1", "highPrice24h": "0.6", "lowPrice24h": "0.4"}}))

            # Pesos personalizados sobreviven a las actualizaciones de ticker.
            eng.set_combined_weights("XRPUSDT", {"tfi": 0.0})
            await eng._on_message(None, json.dumps({
                "topic": "tickers.XRPUSDT",
                "data": {"lastPrice": "0.5", "volume24h": "1", "highPrice24h": "0.6", "lowPrice24h": "0.4"}}))
            self.assertEqual(eng.get_combined_weights("XRPUSDT"), {"tfi": 0.0})

            # Métricas del ciclo alimentan la historia (incluida TFI).
            with eng.SessionLocal() as db:
                await eng.core_cycle("XRPUSDT", db, collect_only=True)
            self.assertTrue(eng._last_metrics_by_symbol["XRPUSDT"].get("data_ok"))
            self.assertEqual(len(eng._metrics_raw_history["XRPUSDT"]), 1)
            self.assertEqual(eng._metrics_raw_history["XRPUSDT"].column("tfi_raw").size, 1)

            # Apertura: símbolo de precio bajo -> precios cuantizados al tick real (no a 2 decimales).
            metrics = {"volatility": 0.004, "combined": 8.0}
            with eng.SessionLocal() as db:
                await eng._open_position("XRPUSDT", "buy", db, metrics, 0.5, datetime.now(timezone.utc), None)
            client = eng._bybit
            self.assertEqual(len(client.created), 1)
            body = client.created[0]
            self.assertEqual(body["orderType"], "Market")
            self.assertTrue(body["orderLinkId"].startswith(eng.config.ORDER_LINK_PREFIX))
            qty = float(body["qty"])
            # Tope de nocional = 10% de 1000 USDT de equity.
            self.assertLessEqual(qty * 0.5, 100.0 + 1e-9)
            with eng.SessionLocal() as db:
                t = db.query(Trade).one()
                self.assertNotEqual(round(t.tp_price, 2), t.tp_price)  # conserva la precisión del tick 0.0001
                self.assertGreater(t.tp_price, 0.5)
                self.assertLess(t.sl_price, 0.5)

            # Fill en el exchange -> sync lo marca filled y deja de consultarlo.
            client.fill("oid-1", 0.5)
            eng._last_orders_sync_ts = 0.0
            with eng.SessionLocal() as db:
                res = await eng.sync_open_orders(db, timeout_seconds=999, update_after_seconds=999)
                self.assertTrue(res["success"])
                t = db.query(Trade).one()
                self.assertEqual(t.outcome_status, "filled")
                self.assertAlmostEqual(t.quantity, qty)
                tp = t.tp_price

            # Sin TP/SL tocado: el horizonte NO cierra posiciones gestionadas por TP/SL en live.
            with eng.SessionLocal() as db:
                self.assertIsNone(await eng._finalize_due_outcomes(db, "XRPUSDT", 0.6))

            # El precio toca el TP -> cierre real con orden Market y PnL neto de comisiones.
            eng.ticker_data["XRPUSDT"]["last_price"] = tp + 0.001
            eng._auto_tpsl_last_tick_ts = 0.0
            with eng.SessionLocal() as db:
                res = await eng._auto_tpsl_tick(db)
            self.assertEqual(res["results"]["executed_virtual"], 1)
            close = client.created[-1]
            self.assertEqual(close["side"], "Sell")
            self.assertEqual(close["orderType"], "Market")
            with eng.SessionLocal() as db:
                t = db.query(Trade).one()
                self.assertEqual(t.outcome_status, "final")
                expected = trade_pnl("buy", 0.5, tp + 0.001, qty, eng.config.FEE_RATE)
                self.assertAlmostEqual(t.profit_loss, expected.net, places=9)
                self.assertLess(t.profit_loss, t.pnl_gross)
            eng.stop()

        asyncio.run(scenario())

    def test_partial_fill_then_cancel_keeps_position(self):
        eng = _make_engine(self._tmp.name)

        async def scenario():
            with eng.SessionLocal() as db:
                trade = Trade(trade_id=1, timestamp=datetime.now(timezone.utc), symbol="XRPUSDT", action="buy",
                              order_id="x", entry_price=0.5, quantity=100, decision="buy")
                db.add(trade)
                db.commit()
                changed = await eng._update_trade_from_bybit(
                    trade, {"orderStatus": "PartiallyFilledCanceled", "cumExecQty": "40", "avgPrice": "0.49"})
                self.assertTrue(changed)
                self.assertEqual(trade.outcome_status, "filled")
                self.assertEqual(trade.quantity, 40)
                self.assertEqual(trade.entry_price, 0.49)
                changed = await eng._update_trade_from_bybit(trade, {"orderStatus": "Cancelled", "cumExecQty": "0"})
                self.assertEqual(trade.outcome_status, "cancelled")

        asyncio.run(scenario())

    def test_add_symbol_hot(self):
        eng = _make_engine(self._tmp.name)

        async def scenario():
            res = await eng.add_symbol("solusdt")
            self.assertTrue(res["success"])
            self.assertIn("SOLUSDT", eng.symbols)
            self.assertIn("SOLUSDT", eng.config.symbols)
            self.assertTrue(eng.orderbook_data["SOLUSDT"].is_ready())
            self.assertFalse((await eng.add_symbol("SOLUSDT"))["added"])

        asyncio.run(scenario())

    def test_secondary_systems_respect_config(self):
        eng = _make_engine(self._tmp.name)
        eng._boot_ts = 0.0

        async def scenario():
            with eng.SessionLocal() as db:
                await eng._enable_secondary_systems_if_due(db)
            self.assertFalse(eng.config.AUTO_AGENT_ENABLED)
            eng.config.AUTO_ENABLE_SECONDARY_SYSTEMS = True
            with eng.SessionLocal() as db:
                await eng._enable_secondary_systems_if_due(db)
            self.assertTrue(eng.config.AUTO_AGENT_ENABLED)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
