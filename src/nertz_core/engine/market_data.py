"""Ingesta de mercado: carga REST inicial, WebSocket público y persistencia de ticks/libro."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List

from nertz_core.db import MarketData, MarketTicker, Orderbook
from nertz_core.engine.host import EngineHost
from nertz_core.history import MetricHistory
from nertz_core.market import candle_from_bybit_row, candle_from_ws, parse_public_trade, parse_ticker
from utils import load_metrics_raw_history_from_jsonl

logger = logging.getLogger("NertzMetalEngine")

try:
    from nertz_engine.storage import OrderbookRow, TickRow
except ImportError:  # pragma: no cover
    OrderbookRow = None  # type: ignore[assignment]
    TickRow = None  # type: ignore[assignment]


def duckdb_lock_hint(exc: BaseException, project_root: str) -> str:
    """Mensaje accionable cuando DuckDB no abre por un lock de archivo."""
    msg = str(exc)
    pid_match = re.search(r"\(PID\s+(\d+)\)", msg, re.IGNORECASE)
    release_script = os.path.join(project_root, "scripts", "release_duckdb_lock.ps1")
    parts = [
        "Otra instancia de Python tiene abierto nertz.duckdb.",
        "Detén el bot anterior (Ctrl+C en su terminal) o ejecuta:",
        f'  PowerShell -ExecutionPolicy Bypass -File "{release_script}"',
    ]
    if pid_match:
        pid = pid_match.group(1)
        parts.insert(1, f"Proceso bloqueante: PID {pid} → Stop-Process -Id {pid} -Force")
    return " ".join(parts)


class MarketDataMixin(EngineHost):
    # ------------------------------------------------------ public REST
    def _public_client(self):
        if self._public is None:
            ep = self.config.endpoints()
            self._public = self.client_factory(
                "",
                "",
                base_url=ep.rest_public,
                recv_window=str(self.config.BYBIT_RECV_WINDOW),
                timeout_s=self.config.BYBIT_HTTP_TIMEOUT_S,
                max_retries=self.config.BYBIT_HTTP_MAX_RETRIES,
            )
        return self._public

    async def fetch_initial_data(self):
        results = await asyncio.gather(*(self._fetch_symbol_data(s) for s in self.symbols), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"❌ Error al obtener datos iniciales: {result}")
        await self.restore_metrics_history()

    async def _fetch_symbol_data(self, symbol: str, *_legacy: Any) -> None:
        cfg = self.config
        client = self._public_client()
        category = cfg.BYBIT_CATEGORY
        buffer = int(cfg.CANDLE_BUFFER_SIZE)

        kline = await client.get_kline(category, symbol, cfg.kline_interval, limit=buffer)
        rows = ((kline.get("result") or {}).get("list") or []) if kline.get("retCode") == 0 else None
        if rows is not None:
            candles = [c for c in (candle_from_bybit_row(symbol, r) for r in rows) if c is not None]
            candles.sort(key=lambda c: c.timestamp, reverse=True)
            self._persist_candles(symbol, candles)
            self.candles[symbol] = candles[:buffer]
            if candles:
                self._last_kline_ts[symbol] = float(int(candles[0].timestamp.timestamp() * 1000))
            logger.info(f"📈 Velas iniciales para {symbol}: {len(candles)}")
        else:
            logger.error(f"❌ Kline inesperado para {symbol}: {kline}")

        book = await client.get_orderbook(category, symbol, limit=max(1, int(cfg.ORDERBOOK_DEPTH)))
        if book.get("retCode") == 0:
            res = book.get("result") or {}
            self.orderbook_data[symbol].apply_snapshot(res.get("b"), res.get("a"), res.get("u"))
            ob = self.orderbook_data[symbol]
            logger.info(f"📊 Orderbook inicial para {symbol}: Bids={len(ob['bids'])}, Asks={len(ob['asks'])}")
        else:
            logger.error(f"❌ Orderbook inesperado para {symbol}: {book}")

        tick = await client.get_tickers(category, symbol)
        lst = ((tick.get("result") or {}).get("list") or []) if tick.get("retCode") == 0 else []
        parsed = parse_ticker(lst[0]) if lst else None
        if parsed:
            self.ticker_data.setdefault(symbol, {}).update(parsed)
            logger.info(f"⚡ Ticker inicial para {symbol}: {parsed['last_price']}")
        else:
            logger.error(f"❌ Ticker inesperado para {symbol}: {tick}")

    def _persist_candles(self, symbol: str, candles: List[Any]) -> None:
        if not candles:
            return
        with self.SessionLocal() as db:
            existing = {
                r[0]
                for r in db.query(MarketData.timestamp)
                .filter(MarketData.symbol == symbol, MarketData.timestamp >= min(c.timestamp for c in candles).replace(tzinfo=None))
                .all()
            }
            for c in candles:
                if c.timestamp.replace(tzinfo=None) in existing:
                    continue
                db.add(
                    MarketData(
                        timestamp=c.timestamp, symbol=symbol, open=c.open, high=c.high,
                        low=c.low, close=c.close, volume=c.volume,
                    )
                )
            db.commit()

    async def restore_metrics_history(self) -> None:
        """Rehidrata la historia de z-scores tras un reinicio (DuckDB o JSONL)."""
        window_s = max(60.0, float(self.config.METRICS_WINDOW_MINUTES) * 60.0)
        for symbol in self.symbols:
            loaded: List[Dict[str, Any]] = []
            storage = self._storage
            if storage is not None and hasattr(storage, "fetch_metric_history"):
                try:
                    loaded = await storage.fetch_metric_history(symbol, window_s=window_s)
                except Exception as e:
                    logger.debug(f"restore history duckdb {symbol}: {e}")
            if not loaded:
                loaded = await asyncio.to_thread(
                    load_metrics_raw_history_from_jsonl, self.paths.data_dir, symbol, window_s=window_s
                )
            self._metrics_raw_history[symbol] = MetricHistory(loaded)
            if loaded:
                logger.info(f"📊 Historial de métricas restaurado para {symbol}: {len(loaded)} muestras")

    # Compatibilidad con el nombre antiguo (síncrono).
    def restore_metrics_history_sync(self) -> None:
        window_s = max(60.0, float(self.config.METRICS_WINDOW_MINUTES) * 60.0)
        for symbol in self.symbols:
            loaded = load_metrics_raw_history_from_jsonl(self.paths.data_dir, symbol, window_s=window_s)
            self._metrics_raw_history[symbol] = MetricHistory(loaded)

    # ------------------------------------------------------- WebSocket
    def _topics(self, symbol: str) -> List[str]:
        cfg = self.config
        return [
            f"kline.{cfg.kline_interval}.{symbol}",
            f"orderbook.{cfg.ws_orderbook_depth}.{symbol}",
            f"tickers.{symbol}",
            f"publicTrade.{symbol}",
        ]

    async def _connect_websocket_async(self):
        import websockets

        url = self.config.endpoints().ws_public
        while self.running:
            try:
                async with websockets.connect(url) as ws:
                    self.ws = ws
                    logger.info(f"🌐 WebSocket abierto ({url})")
                    await self._resubscribe_async()
                    async for message in ws:
                        await self._on_message(ws, message)
            except websockets.ConnectionClosed as e:
                logger.warning(f"⚠️ WebSocket cerrado: {e}, reconectando...")
            except Exception as e:
                logger.error(f"❌ Error en WebSocket: {e}")
            if self.running:
                await asyncio.sleep(float(self.config.WS_RECONNECT_DELAY_S))

    async def _subscribe_symbols(self, symbols: List[str]) -> None:
        if not self.ws:
            return
        for symbol in symbols:
            await self.ws.send(json.dumps({"op": "subscribe", "args": self._topics(symbol)}))
            logger.info(f"📡 Suscrito a {symbol}")

    async def _resubscribe_async(self):
        await self._subscribe_symbols(list(self.symbols))

    async def _on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            if not isinstance(message, str):
                logger.error(f"❌ Mensaje no procesable. Tipo recibido: {type(message)}")
                return
            data = json.loads(message)
            topic = data.get("topic")
            if not topic:
                if data.get("op") == "ping" and ws is not None:
                    await ws.send(json.dumps({"op": "pong", "ts": data.get("ts", int(time.time() * 1000))}))
                return

            kind, _, rest = topic.partition(".")
            symbol = rest.rsplit(".", 1)[-1]
            if symbol not in self.orderbook_data:
                self._rl_log(f"ws_unknown:{symbol}", "warning", f"⚠️ Símbolo desconocido: {symbol}")
                return
            payload = data.get("data")
            if not payload:
                return
            if kind == "kline":
                await self._handle_kline(symbol, payload[0])
            elif kind == "orderbook":
                await self._handle_orderbook(symbol, data)
            elif kind == "tickers":
                await self._handle_ticker(symbol, payload)
            elif kind == "publicTrade":
                await self._handle_public_trade(symbol, payload)
            else:
                self._rl_log(f"ws_topic:{kind}", "warning", f"⚠️ Tema no manejado: {topic}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error de decodificación JSON: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado en mensaje: {e}", exc_info=True)

    # --------------------------------------------------------- handlers
    async def _handle_kline(self, symbol: str, kline: Dict):
        candle = candle_from_ws(symbol, kline)
        if candle is None:
            logger.warning(f"⚠️ Kline inválido para {symbol}: {kline}")
            return
        # El kline ya pasó por candle_from_ws, que valida que "start" exista y sea numérico.
        incoming_ts = float(kline["start"])
        is_confirmed = kline.get("confirm") in (True, "true", 1, "1")

        buf = self.candles.setdefault(symbol, [])
        is_new = False
        if buf and getattr(buf[0], "timestamp", None) == candle.timestamp:
            buf[0] = candle
        elif incoming_ts > float(self._last_kline_ts.get(symbol, 0.0) or 0.0):
            buf.insert(0, candle)
            del buf[int(self.config.CANDLE_BUFFER_SIZE):]
            self._last_kline_ts[symbol] = incoming_ts
            is_new = True

        # Solo se persiste al abrir y al confirmar la vela (antes: un commit por mensaje).
        if not (is_new or is_confirmed):
            return
        with self.SessionLocal() as session:
            row = session.query(MarketData).filter_by(timestamp=candle.timestamp, symbol=symbol).first()
            if row is None:
                session.add(
                    MarketData(
                        timestamp=candle.timestamp, symbol=symbol, open=candle.open, high=candle.high,
                        low=candle.low, close=candle.close, volume=candle.volume,
                    )
                )
            else:
                row.open, row.high, row.low = candle.open, candle.high, candle.low
                row.close, row.volume = candle.close, candle.volume
            session.commit()
            if is_confirmed:
                await self._execute_trade(symbol, session)

    async def _execute_trade(self, symbol: str, db):
        await self._core_cycle(symbol, db, collect_only=False)

    async def _handle_orderbook(self, symbol: str, data: Dict):
        book = self.orderbook_data[symbol]
        payload = data.get("data") or {}
        kind = data.get("type")
        if kind == "snapshot":
            book.apply_snapshot(payload.get("b"), payload.get("a"), payload.get("u"))
        elif kind == "delta":
            if not book.apply_delta(payload.get("b"), payload.get("a"), payload.get("u")):
                self._rl_log(f"ob_nosnap:{symbol}", "warning", f"⚠️ No hay orderbook previo para {symbol}, esperando snapshot")
                return
        else:
            return
        await self._store_orderbook(symbol)

    async def _store_orderbook(self, symbol: str):
        now_ts = time.time()
        if now_ts - float(self._last_orderbook_store_ts.get(symbol, 0.0) or 0.0) < self.config.ORDERBOOK_PERSIST_INTERVAL_MS / 1000.0:
            return
        self._last_orderbook_store_ts[symbol] = now_ts
        view = self.orderbook_data[symbol].as_dict()
        now = datetime.now(timezone.utc)
        try:
            if self._storage is not None and OrderbookRow is not None:
                await self._storage.enqueue_orderbook(
                    OrderbookRow(timestamp=now, symbol=symbol, bids=view["bids"], asks=view["asks"])
                )
            if self._storage is None or self.config.STORAGE_SQLITE_MIRROR:
                with self.SessionLocal() as session:
                    session.add(Orderbook(timestamp=now, symbol=symbol, bids=view["bids"], asks=view["asks"]))
                    session.commit()
            if now_ts - self.last_orderbook_log >= 5:
                logger.info(f"🤘 Orderbook guardado para {symbol}: Bids={len(view['bids'])}, Asks={len(view['asks'])}")
                self.last_orderbook_log = now_ts
        except Exception as e:
            logger.error(f"❌ Error al guardar orderbook para {symbol}: {e}")

    async def _handle_public_trade(self, symbol: str, trades: Any) -> None:
        if not isinstance(trades, list):
            return
        q = self.recent_trades.setdefault(symbol, deque(maxlen=self.config.RECENT_TRADES_BUFFER))
        now_s = time.time()
        for t in trades[-q.maxlen:] if q.maxlen else trades:
            if isinstance(t, dict):
                row = parse_public_trade(t, now_s)
                if row is not None:
                    q.append(row)

    async def _handle_ticker(self, symbol: str, ticker: Dict):
        parsed = parse_ticker(ticker)
        if parsed is None:
            self._rl_log(f"ticker_bad:{symbol}", "warning", f"⚠️ Ticker inválido para {symbol}: {ticker}")
            return
        # Actualización in-place: conserva claves añadidas por API/agente (p. ej. combined_weights).
        live = self.ticker_data.setdefault(symbol, {})
        live.update(parsed)

        now_ts = time.time()
        if now_ts - float(self._last_ticker_store_ts.get(symbol, 0.0) or 0.0) < self.config.TICKER_PERSIST_INTERVAL_MS / 1000.0:
            return
        self._last_ticker_store_ts[symbol] = now_ts
        now = datetime.now(timezone.utc)
        try:
            if self._storage is not None and TickRow is not None:
                await self._storage.enqueue_tick(
                    TickRow(
                        timestamp=now,
                        symbol=symbol,
                        last_price=live["last_price"],
                        volume_24h=live["volume_24h"],
                        high_24h=live["high_24h"],
                        low_24h=live["low_24h"],
                        usd_index_price=live.get("usd_index_price"),
                    )
                )
            if self._storage is None or self.config.STORAGE_SQLITE_MIRROR:
                with self.SessionLocal() as session:
                    session.add(
                        MarketTicker(
                            timestamp=now,
                            symbol=symbol,
                            last_price=live["last_price"],
                            volume_24h=live["volume_24h"],
                            high_24h=live["high_24h"],
                            low_24h=live["low_24h"],
                        )
                    )
                    session.commit()
        except Exception as e:
            logger.error(f"❌ Error guardando ticker de {symbol}: {type(e).__name__} - {e}")
