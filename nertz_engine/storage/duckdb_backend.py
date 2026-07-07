"""DuckDB-backed async batch storage."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Sequence

import duckdb

from nertz_engine.storage.base import EventRow, MetricRow, OrderbookRow, TickRow

logger = logging.getLogger(__name__)

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS market_ticks (
        timestamp TIMESTAMP NOT NULL,
        symbol VARCHAR NOT NULL,
        last_price DOUBLE NOT NULL,
        volume_24h DOUBLE NOT NULL DEFAULT 0,
        high_24h DOUBLE NOT NULL DEFAULT 0,
        low_24h DOUBLE NOT NULL DEFAULT 0,
        usd_index_price DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orderbook_snapshots (
        timestamp TIMESTAMP NOT NULL,
        symbol VARCHAR NOT NULL,
        bids JSON NOT NULL,
        asks JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS metric_snapshots (
        timestamp TIMESTAMP NOT NULL,
        symbol VARCHAR NOT NULL,
        last_price DOUBLE NOT NULL,
        decision VARCHAR NOT NULL DEFAULT 'hold',
        combined DOUBLE NOT NULL DEFAULT 0,
        ild DOUBLE NOT NULL DEFAULT 0,
        egm DOUBLE NOT NULL DEFAULT 0,
        rol DOUBLE NOT NULL DEFAULT 0,
        pio DOUBLE NOT NULL DEFAULT 0,
        ogm DOUBLE NOT NULL DEFAULT 0,
        volatility DOUBLE NOT NULL DEFAULT 0,
        thresholds JSON NOT NULL DEFAULT '{}',
        metrics JSON NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS engine_events (
        timestamp TIMESTAMP NOT NULL,
        event_type VARCHAR NOT NULL,
        symbol VARCHAR,
        level VARCHAR NOT NULL DEFAULT 'info',
        message VARCHAR NOT NULL DEFAULT '',
        payload JSON NOT NULL DEFAULT '{}'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_market_ticks_timestamp ON market_ticks(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol ON market_ticks(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_timestamp ON orderbook_snapshots(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_symbol ON orderbook_snapshots(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_metric_snapshots_timestamp ON metric_snapshots(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_metric_snapshots_symbol ON metric_snapshots(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_engine_events_timestamp ON engine_events(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_engine_events_event_type ON engine_events(event_type)",
)


class _RecordKind(str, Enum):
    TICK = "tick"
    ORDERBOOK = "orderbook"
    METRIC = "metric"
    EVENT = "event"


@dataclass(frozen=True, slots=True)
class _QueuedRecord:
    kind: _RecordKind
    row: TickRow | OrderbookRow | MetricRow | EventRow


class AsyncBatchWriter:
    """Background writer that drains an asyncio queue on a fixed interval."""

    def __init__(
        self,
        *,
        flush_interval_ms: float,
        write_batch: Callable[[Sequence[_QueuedRecord]], None],
    ) -> None:
        self._flush_interval_s = max(0.001, float(flush_interval_ms) / 1000.0)
        self._write_batch = write_batch
        self._queue: asyncio.Queue[_QueuedRecord | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._force_flush = asyncio.Event()
        self._flush_done = asyncio.Event()
        self._flush_done.set()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="duckdb-batch-writer")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        await self._queue.put(None)
        if self._task is not None:
            await self._task
            self._task = None

    async def enqueue(self, record: _QueuedRecord) -> None:
        await self._queue.put(record)

    async def flush(self) -> None:
        if not self._running:
            return
        self._flush_done.clear()
        self._force_flush.set()
        await self._flush_done.wait()

    async def _run(self) -> None:
        pending: list[_QueuedRecord] = []

        async def _flush_pending(*, signal_done: bool = False) -> None:
            nonlocal pending
            if pending:
                batch = pending
                pending = []
                await asyncio.to_thread(self._write_batch, batch)
            if signal_done or self._force_flush.is_set():
                self._force_flush.clear()
                self._flush_done.set()

        try:
            while self._running or not self._queue.empty():
                get_task = asyncio.create_task(self._queue.get())
                flush_task = asyncio.create_task(self._force_flush.wait())
                try:
                    done, pending_tasks = await asyncio.wait(
                        {get_task, flush_task},
                        timeout=self._flush_interval_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for task in (get_task, flush_task):
                        if not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass

                if flush_task in done or self._force_flush.is_set():
                    await _flush_pending(signal_done=True)
                    if get_task in done:
                        item = get_task.result()
                    else:
                        continue
                elif get_task in done:
                    item = get_task.result()
                else:
                    await _flush_pending()
                    continue

                if item is None:
                    if not self._running:
                        break
                    continue

                pending.append(item)

                if len(pending) >= 256:
                    await _flush_pending()
        finally:
            while not self._queue.empty():
                queued = self._queue.get_nowait()
                if queued is not None:
                    pending.append(queued)
            if pending:
                await asyncio.to_thread(self._write_batch, pending)
            self._force_flush.clear()
            self._flush_done.set()
            self._force_flush.clear()
            self._flush_done.set()


class DuckDBBackend:
    """Thread-safe DuckDB storage with async batched writes.

    A single persistent RW connection is reused for all batch flushes. On Windows,
    repeatedly opening and closing the database file often triggers transient
    "file in use" errors; external tools can still attach read-only concurrently.
    """

    def __init__(
        self,
        path: str,
        *,
        flush_interval_ms: float = 50.0,
        read_only: bool = False,
    ) -> None:
        self._path = os.path.abspath(path)
        self._flush_interval_ms = float(flush_interval_ms)
        self._read_only = bool(read_only)
        self._lock = threading.RLock()
        self._schema_ready = False
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._writer: AsyncBatchWriter | None = None

    @property
    def path(self) -> str:
        return self._path

    def _ensure_parent_dir(self) -> None:
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def _open_rw_connection(self) -> duckdb.DuckDBPyConnection:
        self._ensure_parent_dir()
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                return duckdb.connect(self._path)
            except duckdb.IOException as exc:
                last_exc = exc
                if attempt < 4:
                    time.sleep(0.05 * (2 ** attempt))
        assert last_exc is not None
        raise last_exc

    def _init_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        for statement in _SCHEMA_STATEMENTS:
            conn.execute(statement)
        self._schema_ready = True

    def _ensure_rw_connection_locked(self) -> duckdb.DuckDBPyConnection:
        """Return the persistent RW connection. Caller must hold ``self._lock``."""
        if self._conn is None:
            self._conn = self._open_rw_connection()
            if not self._schema_ready:
                self._init_schema(self._conn)
        return self._conn

    def _reset_rw_connection_locked(self) -> None:
        """Close and discard the RW connection. Caller must hold ``self._lock``."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _connect(self) -> None:
        """Open a persistent RW connection (migration/CLI helpers only)."""
        with self._lock:
            self._ensure_rw_connection_locked()

    def _write_batch(self, batch: Sequence[_QueuedRecord]) -> None:
        if not batch:
            return

        ticks: list[TickRow] = []
        orderbooks: list[OrderbookRow] = []
        metrics: list[MetricRow] = []
        events: list[EventRow] = []

        for record in batch:
            if record.kind is _RecordKind.TICK:
                ticks.append(record.row)  # type: ignore[arg-type]
            elif record.kind is _RecordKind.ORDERBOOK:
                orderbooks.append(record.row)  # type: ignore[arg-type]
            elif record.kind is _RecordKind.METRIC:
                metrics.append(record.row)  # type: ignore[arg-type]
            elif record.kind is _RecordKind.EVENT:
                events.append(record.row)  # type: ignore[arg-type]

        try:
            with self._lock:
                conn = self._ensure_rw_connection_locked()
                if ticks:
                    self._insert_ticks(conn, ticks)
                if orderbooks:
                    self._insert_orderbooks(conn, orderbooks)
                if metrics:
                    self._insert_metrics(conn, metrics)
                if events:
                    self._insert_events(conn, events)
        except duckdb.IOException:
            with self._lock:
                self._reset_rw_connection_locked()
            logger.exception("DuckDB batch write failed (%d records)", len(batch))
            raise
        except Exception:
            logger.exception("DuckDB batch write failed (%d records)", len(batch))
            raise

    def _insert_ticks(self, conn: duckdb.DuckDBPyConnection, rows: Sequence[TickRow]) -> None:
        conn.executemany(
            """
            INSERT INTO market_ticks (
                timestamp, symbol, last_price, volume_24h, high_24h, low_24h, usd_index_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.timestamp,
                    row.symbol,
                    row.last_price,
                    row.volume_24h,
                    row.high_24h,
                    row.low_24h,
                    row.usd_index_price,
                )
                for row in rows
            ],
        )

    def _insert_orderbooks(self, conn: duckdb.DuckDBPyConnection, rows: Sequence[OrderbookRow]) -> None:
        conn.executemany(
            """
            INSERT INTO orderbook_snapshots (timestamp, symbol, bids, asks)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    row.timestamp,
                    row.symbol,
                    json.dumps(row.bids, separators=(",", ":"), default=str),
                    json.dumps(row.asks, separators=(",", ":"), default=str),
                )
                for row in rows
            ],
        )

    def _insert_metrics(self, conn: duckdb.DuckDBPyConnection, rows: Sequence[MetricRow]) -> None:
        conn.executemany(
            """
            INSERT INTO metric_snapshots (
                timestamp, symbol, last_price, decision, combined, ild, egm, rol, pio, ogm,
                volatility, thresholds, metrics
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.timestamp,
                    row.symbol,
                    row.last_price,
                    row.decision,
                    row.combined,
                    row.ild,
                    row.egm,
                    row.rol,
                    row.pio,
                    row.ogm,
                    row.volatility,
                    json.dumps(row.thresholds, separators=(",", ":"), default=str),
                    json.dumps(row.metrics, separators=(",", ":"), default=str),
                )
                for row in rows
            ],
        )

    def _insert_events(self, conn: duckdb.DuckDBPyConnection, rows: Sequence[EventRow]) -> None:
        conn.executemany(
            """
            INSERT INTO engine_events (
                timestamp, event_type, symbol, level, message, payload
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.timestamp,
                    row.event_type,
                    row.symbol,
                    row.level,
                    row.message,
                    json.dumps(row.payload, separators=(",", ":"), default=str),
                )
                for row in rows
            ],
        )

    def _bootstrap_schema(self) -> None:
        with self._lock:
            self._ensure_rw_connection_locked()

    async def start(self) -> None:
        await asyncio.to_thread(self._bootstrap_schema)
        if self._writer is None:
            self._writer = AsyncBatchWriter(
                flush_interval_ms=self._flush_interval_ms,
                write_batch=self._write_batch,
            )
            await self._writer.start()

    async def stop(self) -> None:
        if self._writer is not None:
            await self._writer.stop()
            self._writer = None

        def _close() -> None:
            with self._lock:
                self._reset_rw_connection_locked()
                self._schema_ready = False

        await asyncio.to_thread(_close)

    async def enqueue_tick(self, row: TickRow) -> None:
        await self._ensure_writer()
        await self._writer.enqueue(_QueuedRecord(_RecordKind.TICK, row))

    async def enqueue_orderbook(self, row: OrderbookRow) -> None:
        await self._ensure_writer()
        await self._writer.enqueue(_QueuedRecord(_RecordKind.ORDERBOOK, row))

    async def enqueue_metric(self, row: MetricRow) -> None:
        await self._ensure_writer()
        await self._writer.enqueue(_QueuedRecord(_RecordKind.METRIC, row))

    async def enqueue_event(self, row: EventRow) -> None:
        await self._ensure_writer()
        await self._writer.enqueue(_QueuedRecord(_RecordKind.EVENT, row))

    async def flush(self) -> None:
        if self._writer is None:
            return
        await self._writer.flush()

    def _fetch_recent_sync(self, symbol: str, limit: int) -> dict[str, Any]:
        sym = str(symbol or "").strip().upper()
        lim = max(1, min(100, int(limit)))
        out: dict[str, Any] = {
            "symbol": sym,
            "limit": lim,
            "counts": {},
            "market_ticks": [],
            "orderbook_snapshots": [],
            "metric_snapshots": [],
        }
        with self._lock:
            conn = self._ensure_rw_connection_locked()
            for table, key in (
                ("market_ticks", "market_ticks"),
                ("orderbook_snapshots", "orderbook_snapshots"),
                ("metric_snapshots", "metric_snapshots"),
            ):
                try:
                    out["counts"][key] = int(
                        conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE symbol = ?",
                            [sym],
                        ).fetchone()[0]
                    )
                except Exception:
                    out["counts"][key] = 0

            tick_rows = conn.execute(
                """
                SELECT timestamp, symbol, last_price, volume_24h, high_24h, low_24h
                FROM market_ticks
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [sym, lim],
            ).fetchall()
            out["market_ticks"] = [
                {
                    "timestamp": str(r[0]),
                    "symbol": r[1],
                    "last_price": float(r[2]),
                    "volume_24h": float(r[3]),
                    "high_24h": float(r[4]),
                    "low_24h": float(r[5]),
                }
                for r in tick_rows
            ]

            ob_rows = conn.execute(
                """
                SELECT timestamp, symbol, bids, asks
                FROM orderbook_snapshots
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [sym, lim],
            ).fetchall()
            ob_out: list[dict[str, Any]] = []
            for r in ob_rows:
                bids = r[2]
                asks = r[3]
                if isinstance(bids, str):
                    try:
                        bids = json.loads(bids)
                    except Exception:
                        bids = []
                if isinstance(asks, str):
                    try:
                        asks = json.loads(asks)
                    except Exception:
                        asks = []
                ob_out.append(
                    {
                        "timestamp": str(r[0]),
                        "symbol": r[1],
                        "bid_levels": len(bids) if isinstance(bids, list) else 0,
                        "ask_levels": len(asks) if isinstance(asks, list) else 0,
                        "best_bid": float(bids[0][0]) if isinstance(bids, list) and bids else None,
                        "best_ask": float(asks[0][0]) if isinstance(asks, list) and asks else None,
                    }
                )
            out["orderbook_snapshots"] = ob_out

            met_rows = conn.execute(
                """
                SELECT timestamp, symbol, last_price, decision, combined, metrics
                FROM metric_snapshots
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [sym, lim],
            ).fetchall()
            met_out: list[dict[str, Any]] = []
            for r in met_rows:
                metrics_raw = r[5]
                metrics_obj: dict[str, Any] = {}
                if isinstance(metrics_raw, dict):
                    metrics_obj = metrics_raw
                elif isinstance(metrics_raw, str) and metrics_raw.strip():
                    try:
                        metrics_obj = json.loads(metrics_raw)
                    except Exception:
                        metrics_obj = {}
                met_out.append(
                    {
                        "timestamp": str(r[0]),
                        "symbol": r[1],
                        "last_price": float(r[2]),
                        "decision": r[3],
                        "combined": float(r[4]),
                        "mom": metrics_obj.get("mom"),
                        "mom_raw": metrics_obj.get("mom_raw"),
                        "pio": metrics_obj.get("pio"),
                        "egm": metrics_obj.get("egm"),
                    }
                )
            out["metric_snapshots"] = met_out
        return out

    async def fetch_recent(self, symbol: str, *, limit: int = 10) -> dict[str, Any]:
        return await asyncio.to_thread(self._fetch_recent_sync, symbol, int(limit))

    async def _ensure_writer(self) -> None:
        if self._writer is None:
            raise RuntimeError("DuckDBBackend.start() must be called before enqueueing records")

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now().astimezone()