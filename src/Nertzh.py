import argparse
import asyncio
import csv
import getpass
import io
import json
import logging
import os
import re
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Dict, Any, Optional

import aiohttp
import numpy as np
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, Integer, String, Float, DateTime, JSON, text, or_
from sqlalchemy.orm import sessionmaker, Session, declarative_base, Mapped, mapped_column


from bybit_v5 import BybitV5Client
from optimizer import optimize_system_from_trades
from signal_engine import (
    DEFAULT_COMBINED_WEIGHTS,
    CombinedWeights,
    Thresholds,
    blend_thresholds_symmetric,
    check_execution_gates,
    evaluate_signal,
    relax_thresholds_symmetric,
    symmetrize_threshold_values,
)


from settings import ConfigSettings
from utils import (
    calculate_metrics,
    calculate_discovery_metrics,

    save_results,
    append_results_event,
    patch_results,
    update_last_balance,
    maybe_auto_git_commit,
    append_metrics_snapshot,
    load_metrics_raw_history_from_jsonl,
    load_results_json,
    timestamp_to_datetime,
    calculate_tp_sl,
)

import sys

_PROJECT_ROOT = os.path.abspath (os.path.join (os.path.dirname (__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert (0, _PROJECT_ROOT)

try:
    from nertz_engine.storage import (
        create_storage,
        EventRow,
        MetricRow,
        OrderbookRow,
        TickRow,
    )
except ImportError:
    create_storage = None  # type: ignore[misc, assignment]
    EventRow = None  # type: ignore[misc, assignment]
    MetricRow = None  # type: ignore[misc, assignment]
    OrderbookRow = None  # type: ignore[misc, assignment]
    TickRow = None  # type: ignore[misc, assignment]

# Cargar variables desde el archivo .env
load_dotenv (dotenv_path=os.path.join (os.path.dirname (__file__), "..", ".env"), override=False)

# Instanciar ConfigSettings
config = ConfigSettings ()

# Configuración de logging
logging.basicConfig (
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger ("NertzMetalEngine")

# URL y base de datos (mainnet; demo usa api-demo en _bybit_client)

BASE_URL = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/spot"

DATABASE_DIR = os.path.join (_PROJECT_ROOT, "data")
os.makedirs (DATABASE_DIR, exist_ok=True)
DATABASE_URL = os.path.join (DATABASE_DIR, "trading.db")


def _resolve_storage_path (raw_path: str | None = None) -> str:
    path = str (raw_path or getattr (config, "STORAGE_PATH", "") or "").strip () or "data/nertz.duckdb"
    if not os.path.isabs (path):
        path = os.path.join (_PROJECT_ROOT, path)
    return os.path.abspath (path)


def _duckdb_lock_hint (exc: BaseException) -> str:
    """Build an actionable hint when DuckDB cannot open due to a file lock."""
    msg = str (exc)
    pid_match = re.search (r"\(PID\s+(\d+)\)", msg, re.IGNORECASE)
    release_script = os.path.join (_PROJECT_ROOT, "scripts", "release_duckdb_lock.ps1")
    parts = [
        "Otra instancia de Python tiene abierto nertz.duckdb.",
        "Detén el bot anterior (Ctrl+C en su terminal) o ejecuta:",
        f"  powershell -ExecutionPolicy Bypass -File \"{release_script}\"",
    ]
    if pid_match:
        pid = pid_match.group (1)
        parts.insert (1, f"Proceso bloqueante: PID {pid} → Stop-Process -Id {pid} -Force")
    return " ".join (parts)


engine = create_engine (f"sqlite:///{DATABASE_URL}", connect_args={"check_same_thread": False})
Base = declarative_base ()


# Modelos de la base de datos
class MarketData (Base):
    __tablename__ = "market_data"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, index=True)
    symbol: Mapped[str] = mapped_column (String (10), nullable=False)
    open: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    high: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    low: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    close: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    volume: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)


class Orderbook (Base):
    __tablename__ = "orderbook"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column (String (10), nullable=False, index=True)
    bids: Mapped[Any] = mapped_column (JSON, nullable=False)
    asks: Mapped[Any] = mapped_column (JSON, nullable=False)


class MarketTicker (Base):
    __tablename__ = "market_ticker"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column (String (10), nullable=False, index=True)
    last_price: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    volume_24h: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    high_24h: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    low_24h: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)


class Trade (Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    trade_id: Mapped[int] = mapped_column (Integer, nullable=False, unique=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column (String (10), nullable=False, index=True)
    action: Mapped[str] = mapped_column (String, nullable=False)
    order_id: Mapped[Optional[str]] = mapped_column (String (80), nullable=True, index=True)
    bybit_raw: Mapped[Optional[Any]] = mapped_column (JSON, nullable=True)
    entry_price: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    exit_price: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    tp_price: Mapped[Optional[float]] = mapped_column (Float, nullable=True)
    sl_price: Mapped[Optional[float]] = mapped_column (Float, nullable=True)
    quantity: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    profit_loss: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    pnl_gross: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    outcome_status: Mapped[str] = mapped_column (String (20), nullable=False, default="pending")
    outcome_timestamp: Mapped[Optional[datetime]] = mapped_column (DateTime, nullable=True)
    decision: Mapped[str] = mapped_column (String, nullable=False)
    combined: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    ild: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    egm: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    rol: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    pio: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    ogm: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    risk_reward_ratio: Mapped[float] = mapped_column (Float, nullable=False, default=1.5)


class MetricSnapshot (Base):
    __tablename__ = "metric_snapshots"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column (String (10), nullable=False, index=True)
    last_price: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    decision: Mapped[str] = mapped_column (String, nullable=False, default="hold")
    combined: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    ild: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    egm: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    rol: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    pio: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    ogm: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    volatility: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    thresholds: Mapped[dict] = mapped_column (JSON, nullable=False, default=dict)


class BalanceSnapshot (Base):
    __tablename__ = "balance_snapshots"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    account_type: Mapped[str] = mapped_column (String (20), nullable=False, default="UNIFIED")
    coin: Mapped[Optional[str]] = mapped_column (String (20), nullable=True)
    total_equity: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    available_balance: Mapped[float] = mapped_column (Float, nullable=False, default=0.0)
    raw: Mapped[dict] = mapped_column (JSON, nullable=False, default=dict)


class ThresholdSnapshot (Base):
    __tablename__ = "threshold_snapshots"
    id: Mapped[int] = mapped_column (Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column (DateTime, nullable=False, index=True)
    egm_buy_threshold: Mapped[float] = mapped_column (Float, nullable=False)
    egm_sell_threshold: Mapped[float] = mapped_column (Float, nullable=False)
    combined_buy_threshold: Mapped[float] = mapped_column (Float, nullable=False)
    combined_sell_threshold: Mapped[float] = mapped_column (Float, nullable=False)
    stats: Mapped[dict] = mapped_column (JSON, nullable=False, default=dict)


Base.metadata.create_all (bind=engine)


def _persist_thresholds_to_env (env_path: str) -> Dict[str, Any]:
    before = {
        "EGM_BUY_THRESHOLD": float (config.EGM_BUY_THRESHOLD),
        "EGM_SELL_THRESHOLD": float (config.EGM_SELL_THRESHOLD),
        "COMBINED_BUY_THRESHOLD": float (getattr (config, "COMBINED_BUY_THRESHOLD", 8.0)),
        "COMBINED_SELL_THRESHOLD": float (getattr (config, "COMBINED_SELL_THRESHOLD", -8.0)),
        "COMBINED_HOLD_BAND": float (getattr (config, "COMBINED_HOLD_BAND", 2.0)),
    }
    try:
        env_path = os.path.abspath (env_path)
        if not os.path.exists (env_path):
            return {"success": False, "message": "env_not_found", "path": env_path, "values": before}

        with open (env_path, "r", encoding="utf-8") as f:
            lines = f.read ().splitlines ()

        values = {
            "EGM_BUY_THRESHOLD": str (before["EGM_BUY_THRESHOLD"]),
            "EGM_SELL_THRESHOLD": str (before["EGM_SELL_THRESHOLD"]),
            "COMBINED_BUY_THRESHOLD": str (before["COMBINED_BUY_THRESHOLD"]),
            "COMBINED_SELL_THRESHOLD": str (before["COMBINED_SELL_THRESHOLD"]),
            "COMBINED_HOLD_BAND": str (before["COMBINED_HOLD_BAND"]),
        }

        keys = list (values.keys ())
        patterns = {k: re.compile (rf"^\s*{re.escape (k)}\s*=") for k in keys}

        found = {k: False for k in keys}
        new_lines: list[str] = []
        for line in lines:
            replaced = False
            for k in keys:
                if patterns[k].match (line):
                    new_lines.append (f"{k}={values[k]}")
                    found[k] = True
                    replaced = True
                    break
            if not replaced:
                new_lines.append (line)

        for k in keys:
            if not found[k]:
                new_lines.append (f"{k}={values[k]}")

        with open (env_path, "w", encoding="utf-8") as f:
            f.write ("\n".join (new_lines) + "\n")

        return {"success": True, "path": env_path, "values": before}
    except Exception as e:
        return {"success": False, "message": str (e), "path": env_path, "values": before}


def _ensure_sqlite_columns (table: str, desired: Dict[str, str]) -> None:
    with engine.begin () as conn:
        rows = conn.execute (text (f"PRAGMA table_info({table})")).fetchall ()
        existing = {row[1] for row in rows} if rows else set ()
        for name, type_sql in desired.items ():
            if name in existing:
                continue
            conn.execute (text (f"ALTER TABLE {table} ADD COLUMN {name} {type_sql}"))


_ensure_sqlite_columns (
    "trades",
    {
        "order_id": "TEXT",
        "bybit_raw": "TEXT",
        "tp_price": "REAL",
        "sl_price": "REAL",
        "pnl_gross": "REAL DEFAULT 0.0",
        "outcome_status": "TEXT DEFAULT 'pending'",
        "outcome_timestamp": "DATETIME",
    },
)

SessionLocal = sessionmaker (autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)


# Dependencia para la base de datos
def get_db ():
    db = SessionLocal ()
    try:
        yield db
    finally:
        db.close ()


# Función para obtener datos de la API
async def fetch_data (session, url, params=None):
    async with session.get (url, params=params) as response:
        if response.status == 200:
            return await response.json ()
        logger.error (f"❌ Error en {url}: {response.status}")
        return None


def timeframe_to_bybit_interval (timeframe: str) -> str:
    mapping = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "6h": "360",
        "12h": "720",
        "1d": "D",
    }
    return mapping.get (timeframe, timeframe.replace ("m", ""))


def _wallet_balance_to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _parse_wallet_balance_payload(payload: Dict[str, Any], coin: Optional[str] = None) -> Dict[str, Any]:
    ret_code = payload.get("retCode")
    if ret_code not in (0, "0"):
        return {
            "total_equity": 0.0,
            "available_balance": 0.0,
            "valid": False,
            "ret_code": ret_code,
            "ret_msg": payload.get("retMsg"),
        }

    result = payload.get("result") or {}
    lst = result.get("list") or []
    row = lst[0] if isinstance(lst, list) and lst else {}

    total_equity = _wallet_balance_to_float(
        row.get("totalEquity") or row.get("totalWalletBalance") or 0.0
    )
    available_balance = _wallet_balance_to_float(
        row.get("totalAvailableBalance") or row.get("totalAvailableToWithdraw") or 0.0
    )

    coin_key = str(coin or "").strip().upper()
    if coin_key and (total_equity <= 0.0 or available_balance <= 0.0):
        coins = row.get("coin") or []
        if isinstance(coins, list):
            for coin_row in coins:
                if not isinstance(coin_row, dict):
                    continue
                if str(coin_row.get("coin") or "").strip().upper() != coin_key:
                    continue
                if total_equity <= 0.0:
                    total_equity = _wallet_balance_to_float(
                        coin_row.get("equity")
                        or coin_row.get("walletBalance")
                        or coin_row.get("usdValue")
                        or 0.0
                    )
                if available_balance <= 0.0:
                    available_balance = _wallet_balance_to_float(
                        coin_row.get("availableToWithdraw")
                        or coin_row.get("availableBalance")
                        or coin_row.get("free")
                        or 0.0
                    )
                break

    valid = total_equity > 0.0 or available_balance > 0.0
    return {
        "total_equity": total_equity,
        "available_balance": available_balance,
        "valid": valid,
        "ret_code": ret_code,
        "ret_msg": payload.get("retMsg"),
    }


def _latest_valid_balance (db: Session) -> Optional[BalanceSnapshot]:
    return (
        db.query (BalanceSnapshot)
        .filter (or_ (BalanceSnapshot.total_equity > 0.0, BalanceSnapshot.available_balance > 0.0))
        .order_by (BalanceSnapshot.timestamp.desc ())
        .first ()
    )


def _resolve_capital_inicial(prev_initial: Any, prev_source: Any, capital_source: str, capital_actual: float) -> float:
    try:
        cfg_capital = float(config.CAPITAL_USDT)
    except Exception:
        cfg_capital = 0.0

    if cfg_capital > 0:
        return cfg_capital

    try:
        prev_initial_f = float(prev_initial)
    except Exception:
        prev_initial_f = 0.0

    if prev_initial_f > 0:
        return prev_initial_f

    try:
        capital_actual_f = float(capital_actual)
    except Exception:
        capital_actual_f = 0.0

    if capital_source == "bybit_wallet_balance" and capital_actual_f > 0:
        return capital_actual_f

    return 0.0


# Función para actualizar orderbook
def _update_orderbook (bid_dict, ask_dict, data):
    for price, qty in data["data"]["b"]:
        price = float (price)
        qty = float (qty)
        if qty > 0:
            bid_dict[price] = qty
        elif price in bid_dict:
            del bid_dict[price]
    for price, qty in data["data"]["a"]:
        price = float (price)
        qty = float (qty)
        if qty > 0:
            ask_dict[price] = qty
        elif price in ask_dict:
            del ask_dict[price]


class NertzMetalEngine:
    def __init__ (self) -> None:
        self.timeframe = config.TIMEFRAME
        self.symbols = config.SYMBOL.split (",")
        from nertz_engine.engine.symbols import OperationManager
        self.operations = OperationManager (
            self.symbols,
            max_concurrent_orders=int (getattr (config, "MAX_CONCURRENT_ORDERS", 3) or 3),
            default_cooldown_s=float (getattr (config, "TRADE_COOLDOWN_S", 0.0) or 0.0),
        )
        self.capital = config.CAPITAL_USDT
        self.trades_cache = {symbol: [] for symbol in self.symbols}
        self.iterations = 0
        self.ws = None
        self.running = True
        self.orderbook_data = {symbol: {"bids": [], "asks": []} for symbol in self.symbols}
        self.ticker_data = {symbol: {"last_price": 0.0, "volume_24h": 0.0, "high_24h": 0.0, "low_24h": 0.0} for symbol
                            in self.symbols}
        self.candles = {symbol: [] for symbol in self.symbols}
        self._last_kline_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self.trade_id_counter = self._load_initial_trade_id ()
        self._load_trades_cache ()
        self.last_orderbook_log = 0
        self._orderbook_store_interval_s = float (config.ORDERBOOK_PERSIST_INTERVAL_MS) / 1000.0
        self._ticker_store_interval_s = float (config.TICKER_PERSIST_INTERVAL_MS) / 1000.0
        self._storage = None
        if callable (create_storage):
            try:
                _storage_path = str (config.STORAGE_PATH or "").strip () or "data/nertz.duckdb"
                if not os.path.isabs (_storage_path):
                    _storage_path = os.path.join (_PROJECT_ROOT, _storage_path)
                self._storage = create_storage (
                    str (config.STORAGE_BACKEND or "duckdb"),
                    _storage_path,
                    flush_interval_ms=float (config.STORAGE_BATCH_INTERVAL_MS),
                )
            except Exception as e:
                logger.warning (f"⚠️ Storage backend no disponible: {e}")
        self._last_orderbook_store_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self._last_ticker_store_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self.last_trade_time = {symbol: datetime.min.replace (tzinfo=timezone.utc) for symbol in self.symbols}
        self.hft_tasks: Dict[str, asyncio.Task] = {}
        self._last_tune_ts = 0.0
        self._last_metrics_json_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self._last_metrics_snapshot_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self._last_live_metrics_ts: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self._last_balance_sync_ts = 0.0
        self._balance_dirty = False
        self._boot_full_reset_done = False
        self.instrument_rules: Dict[str, Dict[str, float]] = {}
        self._instrument_rules_ts: Dict[str, float] = {}
        self._start_task: Optional[asyncio.Task] = None
        self._core_cycle_locks: Dict[str, asyncio.Lock] = {}
        self.order_status: Dict[str, Dict[str, Any]] = {}
        self._support_task: Optional[asyncio.Task] = None
        self._support_interval_s = float (getattr (config, "SUPPORT_LOOP_INTERVAL_S", 1.0) or 1.0)
        self._last_orders_sync_ts = 0.0
        self._last_orders_sync_results: Dict[str, Any] = {}
        self._orders_sync_lock = asyncio.Lock ()
        self._metrics_raw_history: Dict[str, Any] = {symbol: deque () for symbol in self.symbols}
        self._last_weighted_liquidity: Dict[str, Any] = {symbol: None for symbol in self.symbols}
        self.recent_trades: Dict[str, Any] = {symbol: deque (maxlen=500) for symbol in self.symbols}
        self._metrics_window: Dict[str, Any] = {symbol: deque (maxlen=2500) for symbol in self.symbols}
        self._last_metrics_by_symbol: Dict[str, Dict[str, float]] = {symbol: {} for symbol in self.symbols}
        self._boot_ts = time.time ()
        self._secondary_auto_enabled_ts = 0.0
        self._bybit: Optional[BybitV5Client] = None
        self._ml_models: Dict[str, Dict[str, Any]] = {}
        self._ml_last_train_ts: Dict[str, float] = {}
        self._ml_lock = asyncio.Lock ()
        self._agent_last_tick_ts = 0.0
        self._agent_last_relax_ts = 0.0
        self._agent_events: Dict[str, Any] = {"actions": deque (maxlen=250)}
        self.mode = "full"
        self._start_on_boot = True
        self._hft_params: Dict[str, Dict[str, Any]] = {}
        self._auto_hft_enabled = False
        self._auto_hft_last_tick_ts = 0.0
        self._auto_hft_state: Dict[str, Dict[str, Any]] = {symbol: {"last_change_ts": 0.0} for symbol in self.symbols}
        self._auto_tpsl_last_tick_ts = 0.0
        self._auto_tpsl_lock = asyncio.Lock ()
        self._rl_last: Dict[str, float] = {}

    def _rl_log (self, key: str, level: str, message: str, *, interval_s: float = 5.0) -> None:
        try:
            now = time.time ()
            k = str (key or "").strip () or "log"
            last = float (self._rl_last.get (k, 0.0) or 0.0)
            if float (interval_s) > 0 and (now - last) < float (interval_s):
                return
            self._rl_last[k] = float (now)
            lvl = str (level or "").strip ().lower ()
            if lvl in {"error", "err"}:
                logger.error (str (message))
            elif lvl in {"warning", "warn"}:
                logger.warning (str (message))
            else:
                logger.info (str (message))
        except Exception:
            try:
                logger.warning (str (message))
            except Exception:
                pass

    @property
    def start_on_boot (self) -> bool:
        return bool (self._start_on_boot)

    @start_on_boot.setter
    def start_on_boot (self, value: bool) -> None:
        self._start_on_boot = bool (value)

    @property
    def start_task (self) -> Optional[asyncio.Task]:
        return self._start_task

    @property
    def support_task (self) -> Optional[asyncio.Task]:
        return self._support_task

    @property
    def support_interval_s (self) -> float:
        return float (self._support_interval_s)

    @support_interval_s.setter
    def support_interval_s (self, value: float) -> None:
        self._support_interval_s = float (value)

    @property
    def ml_models (self) -> Dict[str, Dict[str, Any]]:
        return self._ml_models

    @property
    def agent_last_tick_ts (self) -> float:
        return float (self._agent_last_tick_ts)

    @property
    def agent_last_relax_ts (self) -> float:
        return float (self._agent_last_relax_ts)

    @property
    def agent_events (self) -> Dict[str, Any]:
        return self._agent_events

    @property
    def metrics_window (self) -> Dict[str, Any]:
        return self._metrics_window

    @property
    def metrics_raw_history (self) -> Dict[str, Any]:
        return self._metrics_raw_history

    @property
    def last_weighted_liquidity (self) -> Dict[str, Any]:
        return self._last_weighted_liquidity

    @property
    def hft_params (self) -> Dict[str, Dict[str, Any]]:
        return self._hft_params

    @property
    def auto_hft_enabled (self) -> bool:
        return bool (self._auto_hft_enabled)

    @auto_hft_enabled.setter
    def auto_hft_enabled (self, value: bool) -> None:
        self._auto_hft_enabled = bool (value)

    def thresholds_payload (self) -> Dict[str, float]:
        return self._thresholds_payload ()

    async def core_cycle (self, symbol: str, db: Session, collect_only: bool = False,
                          force_trade: bool = False) -> None:
        await self._core_cycle (symbol, db, collect_only=bool (collect_only), force_trade=bool (force_trade))

    async def agent_tick (self, db: Session) -> None:
        await self._agent_tick (db)

    async def save_results (self, symbol, trade_result):
        await self._save_results (symbol, trade_result)

    def bybit_client (self) -> Optional[BybitV5Client]:
        return self._bybit_client ()

    def _outcome_horizon_seconds (self) -> int:
        try:
            return max (5, int (getattr (config, "OUTCOME_HORIZON_S", config.DEFAULT_SLEEP_TIME)))
        except Exception:
            return 10

    def _normalize_outcome_status (self, value: Any) -> str:
        if isinstance (value, str) and value.strip ():
            return value
        return "legacy"

    @staticmethod
    def _ml_sigmoid (z: np.ndarray) -> np.ndarray:
        zc = np.clip (z, -50.0, 50.0)
        return 1.0 / (1.0 + np.exp (-zc))

    @staticmethod
    def _ml_action_sign (action: str) -> float:
        a = (action or "").lower ()
        if a == "buy":
            return 1.0
        if a == "sell":
            return -1.0
        return 0.0

    def _ml_feature_names (self) -> list[str]:
        return ["action_sign", "combined", "ild", "egm", "rol", "pio", "ogm", "risk_reward_ratio"]

    def _ml_extract_features (self, action: str, metrics: Dict[str, Any]) -> np.ndarray:
        rr = float (config.TP_PERCENTAGE) / float (config.SL_PERCENTAGE) if float (
            config.SL_PERCENTAGE or 0.0) > 0 else 0.0
        v = np.array (
            [
                self._ml_action_sign (action),
                float (metrics.get ("combined", 0.0) or 0.0),
                float (metrics.get ("ild", 0.0) or 0.0),
                float (metrics.get ("egm", 0.0) or 0.0),
                float (metrics.get ("rol", 0.0) or 0.0),
                float (metrics.get ("pio", 0.0) or 0.0),
                float (metrics.get ("ogm", 0.0) or 0.0),
                float (metrics.get ("risk_reward_ratio", rr) or rr),
            ],
            dtype=np.float64,
        )
        return v

    def train_ml_model_from_trades (
            self,
            db: Session,
            *,
            symbol: Optional[str] = None,
            min_samples: Optional[int] = None,
            epochs: int = 250,
            lr: float = 0.15,
            l2: float = 0.02,
    ) -> Dict[str, Any]:
        ms = int (min_samples) if min_samples is not None else int (getattr (config, "ML_MIN_SAMPLES", 150) or 150)
        q = db.query (Trade).filter (Trade.outcome_status == "final")
        if isinstance (symbol, str) and symbol:
            q = q.filter (Trade.symbol == symbol)
        trades = q.order_by (Trade.timestamp.desc ()).limit (max (ms * 50, 500)).all ()
        if not trades or len (trades) < ms:
            return {"success": False, "message": "insufficient_samples", "samples": len (trades or [])}

        feats: list[np.ndarray] = []
        labels: list[float] = []
        for t in trades:
            pl = float (getattr (t, "profit_loss", 0.0) or 0.0)
            y = 1.0 if pl > 0 else 0.0
            x = np.array (
                [
                    self._ml_action_sign (getattr (t, "action", "")),
                    float (getattr (t, "combined", 0.0) or 0.0),
                    float (getattr (t, "ild", 0.0) or 0.0),
                    float (getattr (t, "egm", 0.0) or 0.0),
                    float (getattr (t, "rol", 0.0) or 0.0),
                    float (getattr (t, "pio", 0.0) or 0.0),
                    float (getattr (t, "ogm", 0.0) or 0.0),
                    float (getattr (t, "risk_reward_ratio", 0.0) or 0.0),
                ],
                dtype=np.float64,
            )
            if not np.all (np.isfinite (x)):
                continue
            feats.append (x)
            labels.append (y)

        if len (feats) < ms:
            return {"success": False, "message": "insufficient_clean_samples", "samples": len (feats)}

        X = np.vstack (feats)
        yv = np.array (labels, dtype=np.float64)
        mu = X.mean (axis=0)
        sigma = X.std (axis=0)
        sigma = np.where (sigma > 1e-9, sigma, 1.0)
        Xn = (X - mu) / sigma
        Xb = np.concatenate ([np.ones ((Xn.shape[0], 1), dtype=np.float64), Xn], axis=1)

        w = np.zeros ((Xb.shape[1],), dtype=np.float64)
        n = float (Xb.shape[0])
        for _ in range (int (max (10, epochs))):
            p = self._ml_sigmoid (Xb @ w)
            grad = (Xb.T @ (p - yv)) / n
            grad[1:] = grad[1:] + float (l2) * w[1:]
            w = w - float (lr) * grad

        p_final = self._ml_sigmoid (Xb @ w)
        pred = (p_final >= 0.5).astype (np.float64)
        acc = float ((pred == yv).mean ()) if yv.size else 0.0

        key = symbol or "__all__"
        self._ml_models[key] = {
            "features": self._ml_feature_names (),
            "mu": mu.tolist (),
            "sigma": sigma.tolist (),
            "w": w.tolist (),
            "samples": int (Xb.shape[0]),
            "accuracy_train": acc,
            "trained_at": datetime.now (timezone.utc).isoformat (),
        }
        self._ml_last_train_ts[key] = time.time ()
        return {"success": True, "key": key, "model": self._ml_models[key]}

    def ml_predict_proba (self, *, symbol: str, action: str, metrics: Dict[str, Any]) -> Optional[float]:
        key = symbol if symbol in self._ml_models else "__all__"
        model = self._ml_models.get (key)
        if not isinstance (model, dict):
            return None
        try:
            mu = np.array (model.get ("mu") or [], dtype=np.float64)
            sigma = np.array (model.get ("sigma") or [], dtype=np.float64)
            w = np.array (model.get ("w") or [], dtype=np.float64)
            if mu.size == 0 or sigma.size == 0 or w.size == 0:
                return None
            x = self._ml_extract_features (action, metrics)
            if x.size != mu.size:
                return None
            xn = (x - mu) / np.where (sigma > 1e-9, sigma, 1.0)
            xb = np.concatenate ([np.ones ((1,), dtype=np.float64), xn], axis=0)
            if xb.size != w.size:
                return None
            p = float (self._ml_sigmoid (xb @ w))
            if not np.isfinite (p):
                return None
            return p
        except Exception:
            return None

    async def _agent_tick (self, db: Session) -> None:
        now_ts = time.time ()
        if now_ts - float (self._agent_last_tick_ts or 0.0) < 0.5:
            return
        self._agent_last_tick_ts = now_ts

        actions = self._agent_events.get ("actions")
        if not isinstance (actions, deque):
            actions = deque (maxlen=250)
            self._agent_events["actions"] = actions

        start_task = getattr (self, "_start_task", None)
        if self.running and (start_task is None or getattr (start_task, "done", lambda: True) ()):
            ok = self.schedule_start ()
            if ok:
                actions.append ({"type": "restart_start_task", "ts": datetime.now (timezone.utc).isoformat ()})

        relax_interval_s = 120.0
        last_relax = float (getattr (self, "_agent_last_relax_ts", 0.0) or 0.0)
        if now_ts - last_relax >= relax_interval_s:
            try:
                recent_trade_ts = None
                for sym in self.symbols:
                    t = self.last_trade_time.get (sym)
                    if isinstance (t, datetime):
                        if recent_trade_ts is None or t > recent_trade_ts:
                            recent_trade_ts = t
                age_trade_s = (
                        datetime.now (timezone.utc) - recent_trade_ts).total_seconds () if recent_trade_ts else None

                window_min = float (getattr (config, "METRICS_WINDOW_MINUTES", 15.0) or 15.0)
                window_s = max (60.0, window_min * 60.0)
                decisions: list[str] = []
                cutoff_ts = now_ts - float (window_s)
                for sym in self.symbols:
                    q = self._metrics_window.get (sym)
                    if not isinstance (q, deque):
                        continue
                    for row in reversed (q):
                        if not isinstance (row, dict):
                            continue
                        ts = row.get ("ts")
                        if ts is None:
                            continue
                        try:
                            if float (ts) < float (cutoff_ts):
                                break
                        except Exception:
                            continue
                        d = row.get ("decision")
                        if isinstance (d, str):
                            decisions.append (d.lower ())
                        if len (decisions) >= 250:
                            break
                    if len (decisions) >= 250:
                        break
                total = len (decisions)
                hold_count = sum (1 for d in decisions if d == "hold")
                hold_ratio = (hold_count / total) if total > 0 else 0.0

                if total >= 80 and hold_ratio >= 0.92 and (age_trade_s is None or age_trade_s >= 600.0):
                    before = self._thresholds_payload ()
                    relaxed = relax_thresholds_symmetric (
                        float (getattr (config, "COMBINED_BUY_THRESHOLD", 4.5) or 4.5),
                        float (getattr (config, "COMBINED_SELL_THRESHOLD", -4.5) or -4.5),
                        float (getattr (config, "COMBINED_HOLD_BAND", 3.0) or 3.0),
                        factor=0.85,
                    )
                    config.COMBINED_BUY_THRESHOLD = float (relaxed.combined_buy_threshold)
                    config.COMBINED_SELL_THRESHOLD = float (relaxed.combined_sell_threshold)
                    config.COMBINED_HOLD_BAND = float (relaxed.combined_hold_band)

                    self._agent_last_relax_ts = now_ts
                    after = self._thresholds_payload ()
                    actions.append (
                        {
                            "type": "relax_thresholds",
                            "ts": datetime.now (timezone.utc).isoformat (),
                            "before": before,
                            "after": after,
                            "metrics_window_s": window_s,
                            "snapshots_seen": total,
                            "hold_ratio": hold_ratio,
                            "age_trade_s": age_trade_s,
                        }
                    )
                    try:
                        append_results_event (
                            {
                                "type": "agent_action",
                                "action": "relax_thresholds",
                                "before": before,
                                "after": after,
                                "metrics_window_s": window_s,
                                "snapshots_seen": total,
                                "hold_ratio": hold_ratio,
                                "age_trade_s": age_trade_s,
                            },
                            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
                        )
                    except Exception:
                        pass
            except Exception as e:
                actions.append (
                    {"type": "relax_thresholds_error", "ts": datetime.now (timezone.utc).isoformat (),
                     "message": str (e)})

        if bool (getattr (config, "ML_ENABLED", False)):
            interval_s = float (getattr (config, "AUTO_AGENT_TRAIN_INTERVAL_MIN", 30.0) or 30.0) * 60.0
            last_train = float (self._ml_last_train_ts.get ("__all__", 0.0) or 0.0)
            final_count = None
            try:
                final_count = int (db.query (Trade).filter (Trade.outcome_status == "final").count ())
            except Exception:
                final_count = None
            min_samples = int (getattr (config, "ML_MIN_SAMPLES", 50) or 50)
            should_fast_train = bool (
                final_count is not None and final_count >= 50 and "__all__" not in self._ml_models)
            should_interval_train = bool (now_ts - last_train >= interval_s)
            if should_fast_train or should_interval_train:
                res = self.train_ml_model_from_trades (db, symbol=None)
                actions.append (
                    {
                        "type": "ml_train",
                        "ts": datetime.now (timezone.utc).isoformat (),
                        "success": bool (res.get ("success")),
                        "samples": (
                            (res.get ("model") or {}).get ("samples") if isinstance (res.get ("model"),
                                                                                     dict) else None),
                        "final_trades": final_count,
                        "min_samples": min_samples,
                    }
                )

    async def _finalize_due_outcomes (self, db: Session, symbol: str, exit_price: float) -> Optional[Trade]:
        if exit_price <= 0:
            return None
        horizon = self._outcome_horizon_seconds ()
        cutoff = datetime.now (timezone.utc) - timedelta (seconds=horizon)
        live = bool (getattr (config, "LIVE_TRADING_ENABLED", False))
        pending = (
            db.query (Trade)
            .filter (Trade.symbol == symbol)
            .filter (Trade.timestamp <= cutoff)
            .filter (~Trade.outcome_status.in_ (["final", "cancelled", "invalid_entry"]))
            .filter (Trade.outcome_status.in_ (["filled"]) if live else True)
            .order_by (Trade.timestamp.asc ())
            .limit (50)
            .all ()
        )
        if not pending:
            return None

        last_finalized: Optional[Trade] = None
        fee_factor = 1 - float (config.FEE_RATE)
        now = datetime.now (timezone.utc)
        for t in pending:
            status = self._normalize_outcome_status (getattr (t, "outcome_status", None))
            if status == "final":
                continue
            entry = float (t.entry_price or 0.0)
            qty = float (t.quantity or 0.0)
            raw = getattr (t, "bybit_raw", None)
            if isinstance (raw, dict):
                order_info = raw.get ("order_realtime") or raw.get ("order_history") or {}
                if isinstance (order_info, dict):
                    try:
                        avg_price = float (order_info.get ("avgPrice") or 0.0)
                    except Exception:
                        avg_price = 0.0
                    try:
                        cum_exec_qty = float (order_info.get ("cumExecQty") or 0.0)
                    except Exception:
                        cum_exec_qty = 0.0
                    if avg_price > 0:
                        entry = avg_price
                    if cum_exec_qty > 0:
                        qty = cum_exec_qty
            if entry <= 0 or qty <= 0:
                t.outcome_status = "invalid_entry"
                t.outcome_timestamp = now
                continue
            if t.action == "buy":
                pnl = (exit_price - entry) * qty * fee_factor
            else:
                pnl = (entry - exit_price) * qty * fee_factor
            t.exit_price = float (exit_price)
            t.profit_loss = float (pnl)
            t.outcome_status = "final"
            t.outcome_timestamp = now
            last_finalized = t

        if last_finalized is not None:
            db.commit ()
        return last_finalized

    def schedule_start (self) -> bool:
        if self._start_task and not self._start_task.done ():
            return False
        self.running = True
        self._start_task = asyncio.create_task (self.start_async ())
        self.start_support_loop (interval_s=self._support_interval_s)
        return True

    def _load_initial_trade_id (self):
        with SessionLocal () as db:
            last_trade = db.query (Trade.trade_id).order_by (Trade.trade_id.desc ()).first ()
            return last_trade[0] + 1 if last_trade else 1

    def _serialize_trade_for_api(self, t: Trade) -> Dict[str, Any]:
        status = self._normalize_outcome_status(getattr(t, "outcome_status", None))
        is_final = status == "final"
        raw = getattr(t, "bybit_raw", None)

        # 1. Extracción de información nativa del Exchange (Bybit)
        order_info: Dict[str, Any] = {}
        if isinstance(raw, dict):
            for key in ("order_realtime", "order_history"):
                block = raw.get(key)
                if isinstance(block, dict) and block:
                    order_info = block
                    break

        exchange_status = str(order_info.get("orderStatus") or "").strip().lower() or None

        avg_price = None
        cum_exec_qty = None
        try:
            if order_info.get("avgPrice") is not None:
                avg_price = float(order_info.get("avgPrice"))
        except Exception:
            avg_price = None

        try:
            if order_info.get("cumExecQty") is not None:
                cum_exec_qty = float(order_info.get("cumExecQty"))
        except Exception:
            cum_exec_qty = None

        # 2. Extracción del Snapshot de Métricas (Donde vive la microestructura)
        metrics_snapshot = raw.get("metrics_snapshot") if isinstance(raw, dict) else None
        if not isinstance(metrics_snapshot, dict):
            metrics_snapshot = {}

        # Ajuste de precio y cantidad ejecutada real
        entry_price = float(t.entry_price or 0.0)
        if avg_price and avg_price > 0:
            entry_price = float(avg_price)

        qty = float(t.quantity or 0.0)
        if cum_exec_qty and cum_exec_qty > 0:
            qty = float(cum_exec_qty)

        # 🚀 FIX HFT #3: EXTRACCIÓN DE COMPONENTES ATÓMICOS PARA ML
        # Intenta leer de la estructura anidada (FIX #4) o hace fallback a claves planas.
        components = metrics_snapshot.get("components", {})
        if not isinstance(components, dict):
            components = {}

        egm_comp = components.get("egm", {}) if isinstance(components.get("egm"), dict) else {}
        micro_comp = components.get("microstructure", {}) if isinstance(components.get("microstructure"), dict) else {}

        atomic_features = {
            # Desglose de EGM (Para que el ML detecte Spoofing vs Flujo Real)
            "egm_pressure": float(egm_comp.get("pressure") or metrics_snapshot.get("orderbook_pressure", 0.0)),
            "egm_flow_tfi": float(egm_comp.get("flow") or metrics_snapshot.get("tfi", 0.0)),
            "egm_momentum": float(egm_comp.get("momentum") or metrics_snapshot.get("mom_raw", 0.0)),

            # Desglose de Microestructura (Para que el ML mida la fricción del spread)
            "micro_spread_bps": float(micro_comp.get("spread_bps") or metrics_snapshot.get("spread_bps", 0.0)),
            "micro_offset_bps": float(
                micro_comp.get("microprice_offset") or metrics_snapshot.get("microprice_offset_bps", 0.0)),

            # Variables de Régimen y Toxicidad
            "rvol_raw": float(metrics_snapshot.get("rvol", 0.0)),
            "imbalance_qty_pct": float(metrics_snapshot.get("recent_trades_imbalance_qty_pct", 0.0)),
            "ild_raw": float(metrics_snapshot.get("ild_raw", 0.0)),
            "rol_raw": float(metrics_snapshot.get("rol_raw", 0.0)),
        }
        # --------------------------------------------------------------

        # 3. Construcción del Payload Final
        return {
            "trade_id": t.trade_id,
            "timestamp": t.timestamp.isoformat(),
            "symbol": t.symbol,
            "action": t.action,
            "order_id": getattr(t, "order_id", None),
            "entry_price": entry_price,
            "exit_price": float(t.exit_price) if is_final else None,
            "tp_price": getattr(t, "tp_price", None),
            "sl_price": getattr(t, "sl_price", None),
            "quantity": qty,
            "profit_loss": float(t.profit_loss) if is_final else None,
            "pnl_open": float(t.profit_loss) if status in {"filled", "partial"} and t.profit_loss else None,
            "outcome_status": status,
            "outcome_timestamp": t.outcome_timestamp.isoformat() if t.outcome_timestamp else None,
            "exchange_order_status": exchange_status,
            "exchange_avg_price": avg_price,
            "exchange_cum_exec_qty": cum_exec_qty,
            "bybit_raw": raw,
            "metrics_snapshot": metrics_snapshot,
            "decision": t.decision,

            # Métricas Compuestas (Blindadas contra NoneType)
            "combined": float(t.combined or 0.0),
            "ild": float(t.ild or 0.0),
            "egm": float(t.egm or 0.0),
            "rol": float(t.rol or 0.0),
            "pio": float(t.pio or 0.0),
            "ogm": float(t.ogm or 0.0),
            "risk_reward_ratio": float(t.risk_reward_ratio or 0.0),

            # 🧠 INYECCIÓN DE ALFA ATÓMICO (Se despliegan en el root del JSON para Pandas/XGBoost)
            **atomic_features
        }

    def _refresh_trades_cache (self, symbol: Optional[str] = None) -> None:
        with SessionLocal () as db:
            symbols = [symbol] if symbol else list (self.symbols)
            for sym in symbols:
                trades = (
                    db.query (Trade)
                    .filter_by (symbol=sym)
                    .order_by (Trade.timestamp.desc ())
                    .all ()
                )
                self.trades_cache[sym] = [self._serialize_trade_for_api (t) for t in trades]

    def _load_trades_cache (self):
        self._refresh_trades_cache ()

    def restore_metrics_history (self) -> None:
        window_min = float (getattr (config, "METRICS_WINDOW_MINUTES", 15.0) or 15.0)
        window_s = max (60.0, window_min * 60.0)
        for symbol in self.symbols:
            loaded = load_metrics_raw_history_from_jsonl (
                DATABASE_DIR,
                symbol,
                window_s=window_s,
            )
            history_q = self._metrics_raw_history.setdefault (symbol, deque ())
            history_q.clear ()
            for entry in loaded:
                history_q.append (entry)
            if loaded:
                logger.info (
                    f"📊 Historial de métricas restaurado para {symbol}: {len (loaded)} muestras"
                )

    async def fetch_initial_data (self):
        async with aiohttp.ClientSession () as session:
            tasks = [self._fetch_symbol_data (session, symbol) for symbol in self.symbols]
            results = await asyncio.gather (*tasks, return_exceptions=True)
            for result in results:
                if isinstance (result, Exception):
                    logger.error (f"❌ Error al obtener datos iniciales: {result}")
        self.restore_metrics_history ()

    async def _fetch_symbol_data (self, session, symbol):
        try:
            kline_url = f"{BASE_URL}/v5/market/kline"
            interval = timeframe_to_bybit_interval (self.timeframe)
            params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": 50}
            kline_response = await fetch_data (session, kline_url, params)
            if kline_response and "result" in kline_response and "list" in kline_response["result"]:
                candles = [
                    MarketData (
                        timestamp=timestamp_to_datetime (int (k[0])),
                        symbol=symbol,
                        open=float (k[1]),
                        high=float (k[2]),
                        low=float (k[3]),
                        close=float (k[4]),
                        volume=float (k[5])
                    ) for k in kline_response["result"]["list"]
                ]
                with SessionLocal () as db:
                    for candle in candles:
                        if not db.query (MarketData).filter_by (timestamp=candle.timestamp, symbol=symbol).first ():
                            db.add (candle)
                    db.commit ()
                candles.sort (key=lambda c: c.timestamp, reverse=True)
                self.candles[symbol] = list (candles[:50])
                if candles:
                    self._last_kline_ts[symbol] = float (
                        int (candles[0].timestamp.timestamp () * 1000)
                    )
                logger.info (f"📈 Velas iniciales para {symbol}: {len (candles)}")
            else:
                logger.error (f"❌ Kline inesperado para {symbol}: {kline_response}")

            orderbook_url = f"{BASE_URL}/v5/market/orderbook"
            params = {"category": "spot", "symbol": symbol, "limit": 100}
            orderbook_response = await fetch_data (session, orderbook_url, params)
            if orderbook_response and "result" in orderbook_response:
                depth = int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50)
                depth = max (1, min (depth, 50))
                self.orderbook_data[symbol] = {
                    "bids": (orderbook_response["result"].get ("b") or [])[:depth],
                    "asks": (orderbook_response["result"].get ("a") or [])[:depth],
                }
                logger.info (
                    f"📊 Orderbook inicial para {symbol}: Bids={len (self.orderbook_data[symbol]['bids'])}, Asks={len (self.orderbook_data[symbol]['asks'])}")
            else:
                logger.error (f"❌ Orderbook inesperado para {symbol}: {orderbook_response}")

            ticker_url = f"{BASE_URL}/v5/market/tickers"
            params = {"category": "spot", "symbol": symbol}
            ticker_response = await fetch_data (session, ticker_url, params)
            if ticker_response and "result" in ticker_response and "list" in ticker_response["result"]:
                ticker_data = ticker_response["result"]["list"][0]
                self.ticker_data[symbol] = {
                    "last_price": float (ticker_data["lastPrice"]),
                    "volume_24h": float (ticker_data["volume24h"]),
                    "high_24h": float (ticker_data["highPrice24h"]),
                    "low_24h": float (ticker_data["lowPrice24h"])
                }
                logger.info (f"⚡ Ticker inicial para {symbol}: {self.ticker_data[symbol]['last_price']}")
            else:
                logger.error (f"❌ Ticker inesperado para {symbol}: {ticker_response}")
        except Exception as e:
            logger.error (f"❌ Fetch inicial falló para {symbol}: {e}")

    async def start_async (self):
        logger.info (f"🔥 Iniciando bot para {self.symbols}")
        if (not bool (getattr (self, "_boot_full_reset_done", False))) and bool (
                getattr (config, "FULL_RESET_ON_BOOT", False)
        ):
            try:
                with SessionLocal () as db:
                    self.wipe_database (db)
                self.reset_runtime_state ()
                self.trade_id_counter = 1
                self.reset_results_json ()
            except Exception as e:
                logger.error (f"❌ FULL_RESET_ON_BOOT falló: {e}")
            self._boot_full_reset_done = True
        try:
            preflight = await self.preflight ()
            if not preflight.get ("success"):
                logger.error (f"❌ Preflight falló: {preflight.get ('message') or 'error'}")
                return
        except Exception as e:
            logger.error (f"❌ Preflight falló: {e}")
            return
        max_initial_attempts = 3
        for attempt in range (max_initial_attempts):
            if not self.running:
                logger.info ("🛑 Bot detenido antes de iniciar.")
                return
            try:
                await self.fetch_initial_data ()
                break
            except Exception as e:
                logger.error (f"❌ Error al obtener datos iniciales (intento {attempt + 1}/{max_initial_attempts}): {e}")
                await asyncio.sleep (min (10, 2 ** attempt))

        if not self.running:
            return
        await self._connect_websocket_async ()

    async def preflight (self) -> Dict[str, Any]:
        mode = "live" if bool (getattr (config, "LIVE_TRADING_ENABLED", False)) else "disabled"

        if mode != "live":
            return {"success": True, "mode": mode}

        client = self._bybit_client ()
        if client is None:
            return {"success": False, "mode": mode,
                    "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        time_payload = await client.get_server_time ()
        if time_payload.get ("retCode") != 0:
            return {"success": False, "mode": mode, "message": time_payload.get ("retMsg") or "server_time_failed",
                    "raw": time_payload}

        drift_s = None
        try:
            result = time_payload.get ("result") or {}
            server_s = float (result.get ("timeSecond") or 0.0)
            if server_s > 0:
                drift_s = abs (time.time () - server_s)
        except Exception:
            drift_s = None

        if drift_s is not None and drift_s > 10.0:
            return {"success": False, "mode": mode,
                    "message": f"Deriva de reloj alta ({drift_s:.2f}s). Sincroniza tu hora local."}

        balance = await self.record_balance (account_type="UNIFIED", coin="USDT")
        if not balance.get ("success"):
            return {"success": False, "mode": mode, "message": balance.get ("message") or "wallet_balance_failed",
                    "raw": balance}

        if isinstance (balance.get ("balance"), dict):
            try:
                total = float (balance["balance"].get ("total_equity") or 0.0)
                avail = float (balance["balance"].get ("available_balance") or 0.0)
                if total > 0:
                    self.capital = total
                elif avail > 0:
                    self.capital = avail
            except Exception:
                pass

        instr_errors: list[str] = []
        for sym in self.symbols:
            try:
                await self._get_instrument_rules (sym)
            except Exception as e:
                instr_errors.append (f"{sym}:{e}")

        if instr_errors:
            return {"success": False, "mode": mode, "message": "instrument_rules_failed", "errors": instr_errors[:10]}

        return {"success": True, "mode": mode, "drift_s": drift_s}

    async def _connect_websocket_async (self):
        import websockets
        while self.running:
            try:
                async with websockets.connect (WS_URL) as ws:
                    self.ws = ws
                    logger.info ("🌐 WebSocket abierto")
                    await self._resubscribe_async ()
                    async for message in ws:
                        await self._on_message (ws, message)
            except websockets.ConnectionClosed as e:
                logger.warning (f"⚠️ WebSocket cerrado: {e}, intentando reconectar en 5s...")
                await asyncio.sleep (5)
            except Exception as e:
                logger.error (f"❌ Error en WebSocket: {e}")
                await asyncio.sleep (5)

    async def _resubscribe_async (self):
        interval = timeframe_to_bybit_interval (self.timeframe)
        for symbol in self.symbols:
            subscription = {"op": "subscribe",
                            "args": [f"kline.{interval}.{symbol}", f"orderbook.50.{symbol}", f"tickers.{symbol}",
                                     f"publicTrade.{symbol}"]}
            if self.ws:
                await self.ws.send (json.dumps (subscription))
                logger.info (f"📡 Suscrito a {symbol}")

    async def _on_message (self, ws, message):
        with SessionLocal () as db:
            try:
                if isinstance (message, bytes):
                    message = message.decode ('utf-8')
                elif isinstance (message, tuple):
                    message = message[0]
                elif message is None:
                    logger.warning ("⚠️ Mensaje recibido es None, ignorando.")
                    return

                if isinstance (message, str):
                    data = json.loads (message)
                    logger.debug (f"📨 Mensaje procesado: {json.dumps (data, indent=2)}")

                    if "topic" not in data:
                        logger.debug ("⚠️ Mensaje sin tema ('topic'), posiblemente ping/pong.")
                        if data.get ("op") == "ping" and ws is not None:
                            await ws.send (
                                json.dumps ({"op": "pong", "ts": data.get ("ts", int (time.time () * 1000))}))
                        return

                    symbol = data["topic"].split (".")[-1]
                    if symbol not in self.symbols:
                        logger.warning (f"⚠️ Símbolo desconocido: {symbol}")
                        return

                    if "kline" in data["topic"] and data.get ("data") and len (data["data"]) > 0:
                        await self._handle_kline (symbol, data["data"][0], db)
                    elif "orderbook" in data["topic"] and data.get ("data"):
                        await self._handle_orderbook (symbol, data, db)
                    elif "tickers" in data["topic"] and data.get ("data"):
                        await self._handle_ticker (symbol, data["data"], db)
                    elif "publicTrade" in data["topic"] and data.get ("data"):
                        await self._handle_public_trade (symbol, data.get ("data"), db)
                    else:
                        logger.warning (f"⚠️ Tema no manejado o datos inválidos: {data.get ('topic', 'desconocido')}")
                else:
                    logger.error (f"❌ Mensaje no procesable. Tipo recibido: {type (message)}")
            except json.JSONDecodeError as e:
                logger.error (f"❌ Error de decodificación JSON: {e}")
            except Exception as e:
                logger.error (f"❌ Error inesperado en mensaje: {e}")

    async def _handle_kline (self, symbol: str, kline: Dict, db: Session):
        try:
            timestamp_value = kline.get ("start")
            if not timestamp_value or not str (timestamp_value).isdigit ():
                logger.warning (f"⚠️ Timestamp inválido '{timestamp_value}' para {symbol}. Saltando.")
                return
            timestamp = timestamp_to_datetime (int (timestamp_value))

            try:
                volume = float (kline.get ("volume", 0))
                open_price = float (kline.get ("open", 0))
                high_price = float (kline.get ("high", 0))
                low_price = float (kline.get ("low", 0))
                close_price = float (kline.get ("close", 0))
            except (ValueError, TypeError) as e:
                logger.warning (f"⚠️ Valores inválidos en Kline ('{kline}') para {symbol}: {e}")
                return

            logger.debug (
                f"📥 Kline recibido para {symbol}: timestamp={timestamp}, close={close_price}, volume={volume}")

            last_ts = float (self._last_kline_ts.get (symbol, 0.0) or 0.0)
            incoming_ts = float (timestamp_value)
            confirm_raw = kline.get ("confirm")
            is_confirmed = confirm_raw in (True, "true", 1, "1")
            cache_candle = MarketData (
                timestamp=timestamp, symbol=symbol, open=open_price, high=high_price,
                low=low_price, close=close_price, volume=volume
            )
            buf = self.candles.setdefault (symbol, [])
            if buf and getattr (buf[0], "timestamp", None) == timestamp:
                buf[0] = cache_candle
            elif incoming_ts > last_ts:
                buf.insert (0, cache_candle)
                if len (buf) > 50:
                    del buf[50:]
                self._last_kline_ts[symbol] = incoming_ts
            logger.debug (f"📈 Acumulados {len (buf)} velas para {symbol}")

            db_candle = db.query (MarketData).filter_by (timestamp=timestamp, symbol=symbol).first ()
            if db_candle:
                db_candle.open = open_price
                db_candle.high = high_price
                db_candle.low = low_price
                db_candle.close = close_price
                db_candle.volume = volume
            else:
                db_candle = MarketData (
                    timestamp=timestamp, symbol=symbol, open=open_price, high=high_price,
                    low=low_price, close=close_price, volume=volume
                )
                db.add (db_candle)
            db.commit ()
            logger.debug (f"⚡ Kline para {symbol}: Close={db_candle.close}, Volume={db_candle.volume}")

            if is_confirmed:
                await self._execute_trade (symbol, db)

        except Exception as e:
            logger.error (f"❌ Error inesperado en _handle_kline para {symbol}: {e}")

    async def _handle_orderbook (self, symbol: str, data: Dict, db: Session):
        try:
            if data.get ("type") == "snapshot":
                depth = int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50)
                depth = max (1, min (depth, 50))
                self.orderbook_data[symbol] = {
                    "bids": (data["data"].get ("b") or [])[:depth],
                    "asks": (data["data"].get ("a") or [])[:depth],
                }
                await self._store_orderbook (symbol, db)
                logger.debug (
                    f"📊 Snapshot para {symbol}: Bids={len (self.orderbook_data[symbol]['bids'])}, Asks={len (self.orderbook_data[symbol]['asks'])}")
            elif data.get ("type") == "delta":
                if symbol not in self.orderbook_data or not self.orderbook_data[symbol]["bids"]:
                    logger.warning (f"⚠️ No hay orderbook previo para {symbol}, esperando snapshot")
                    return
                current = self.orderbook_data[symbol]
                bid_dict = {float (b[0]): float (b[1]) for b in current["bids"]}
                ask_dict = {float (a[0]): float (a[1]) for a in current["asks"]}
                _update_orderbook (bid_dict, ask_dict, data)
                depth = int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50)
                depth = max (1, min (depth, 50))
                self.orderbook_data[symbol] = {
                    "bids": [[str (p), str (q)] for p, q in sorted (bid_dict.items (), reverse=True) if q > 0][:depth],
                    "asks": [[str (p), str (q)] for p, q in sorted (ask_dict.items ()) if q > 0][:depth],
                }
                await self._store_orderbook (symbol, db)
                logger.debug (
                    f"📊 Delta para {symbol}: Bids={len (self.orderbook_data[symbol]['bids'])}, Asks={len (self.orderbook_data[symbol]['asks'])}")
        except Exception as e:
            logger.error (f"❌ Error en _handle_orderbook para {symbol}: {e}")

    async def _store_orderbook (self, symbol: str, db: Session):
        try:
            now_ts = time.time ()
            last_ts = float (self._last_orderbook_store_ts.get (symbol, 0.0) or 0.0)
            if now_ts - last_ts < float (self._orderbook_store_interval_s):
                return
            if self._storage is not None:
                row = OrderbookRow (
                    timestamp=datetime.now (timezone.utc),
                    symbol=symbol,
                    bids=self.orderbook_data[symbol]["bids"],
                    asks=self.orderbook_data[symbol]["asks"],
                )
                await self._storage.enqueue_orderbook (row)
                self._last_orderbook_store_ts[symbol] = now_ts
                if time.time () - self.last_orderbook_log >= 5:
                    logger.info (
                        f"🤘 Orderbook guardado para {symbol}: Bids={len (self.orderbook_data[symbol]['bids'])}, Asks={len (self.orderbook_data[symbol]['asks'])}")
                    self.last_orderbook_log = time.time ()
                if not bool (getattr (config, "STORAGE_SQLITE_MIRROR", True)):
                    return
            orderbook = Orderbook (
                timestamp=datetime.now (timezone.utc), symbol=symbol,
                bids=self.orderbook_data[symbol]["bids"],
                asks=self.orderbook_data[symbol]["asks"]
            )
            db.add (orderbook)
            db.commit ()
            self._last_orderbook_store_ts[symbol] = now_ts
            if time.time () - self.last_orderbook_log >= 5:
                logger.info (
                    f"🤘 Orderbook guardado para {symbol}: Bids={len (self.orderbook_data[symbol]['bids'])}, Asks={len (self.orderbook_data[symbol]['asks'])}")
                self.last_orderbook_log = time.time ()
        except Exception as e:
            logger.error (f"❌ Error al guardar orderbook para {symbol}: {e}")

    async def _handle_public_trade (self, symbol: str, trades: Any, db: Session) -> None:
        try:
            if symbol not in self.recent_trades:
                self.recent_trades[symbol] = deque (maxlen=500)
            q = self.recent_trades[symbol]
            if not isinstance (trades, list):
                return
            now_s = time.time ()
            for t in trades[-50:]:
                if not isinstance (t, dict):
                    continue
                qty = 0.0
                price = 0.0
                side = None
                ts_s = None
                for k in ("v", "size", "qty", "q"):
                    if k in t:
                        try:
                            qty = float (t.get (k) or 0.0)
                        except Exception:
                            qty = 0.0
                        break
                for k in ("p", "price", "px"):
                    if k in t:
                        try:
                            price = float (t.get (k) or 0.0)
                        except Exception:
                            price = 0.0
                        break
                for k in ("T", "ts", "time", "timestamp"):
                    if k in t:
                        try:
                            raw = t.get (k)
                            if raw is None:
                                ts_s = None
                            else:
                                val = float (raw)
                                ts_s = (val / 1000.0) if val > 10_000_000_000 else val
                        except Exception:
                            ts_s = None
                        break
                if ts_s is None:
                    ts_s = now_s
                if side is None:
                    for k in ("S", "side", "m"):
                        if k in t:
                            raw_side = t.get (k)
                            if isinstance (raw_side, str):
                                side = raw_side
                            elif isinstance (raw_side, bool):
                                side = "Sell" if raw_side else "Buy"
                            break
                if qty > 0:
                    q.append ({"ts": float (ts_s), "qty": float (qty), "price": float (price),
                               "side": (str (side) if side is not None else None)})
        except Exception as e:
            logger.error (f"❌ Error en _handle_public_trade para {symbol}: {e}")

    async def _handle_ticker (self, symbol: str, ticker: Dict, db: Session):
        try:
            if not isinstance (ticker, dict) or not ticker:
                logger.warning (f"⚠️ Ticker inválido para {symbol}: {ticker}. Saltando.")
                return

            required = ["lastPrice", "volume24h", "highPrice24h", "lowPrice24h"]
            optional = ["usdIndexPrice"]
            if not all (key in ticker for key in required):
                logger.warning (f"⚠️ Faltan claves requeridas en ticker para {symbol}: {ticker}. Saltando.")
                return

            ticker_values = {}
            for key in required + optional:
                value = ticker.get (key, 0.0)
                try:
                    ticker_values[key] = float (value) if value else 0.0
                except (ValueError, TypeError) as ve:
                    logger.warning (f"⚠️ Valor inválido en {key} para {symbol}: {value} - {ve}. Usando 0.0")
                    ticker_values[key] = 0.0

            self.ticker_data[symbol] = {
                "last_price": ticker_values["lastPrice"],
                "volume_24h": ticker_values["volume24h"],
                "high_24h": ticker_values["highPrice24h"],
                "low_24h": ticker_values["lowPrice24h"],
                "usd_index_price": ticker_values["usdIndexPrice"]
            }

            now_ts = time.time ()
            last_store_ts = float (self._last_ticker_store_ts.get (symbol, 0.0) or 0.0)
            if now_ts - last_store_ts < float (self._ticker_store_interval_s):
                return

            if self._storage is not None:
                row = TickRow (
                    timestamp=datetime.now (timezone.utc),
                    symbol=symbol,
                    last_price=self.ticker_data[symbol]["last_price"],
                    volume_24h=self.ticker_data[symbol]["volume_24h"],
                    high_24h=self.ticker_data[symbol]["high_24h"],
                    low_24h=self.ticker_data[symbol]["low_24h"],
                    usd_index_price=self.ticker_data[symbol].get ("usd_index_price"),
                )
                await self._storage.enqueue_tick (row)
                self._last_ticker_store_ts[symbol] = now_ts
                logger.debug (
                    f"⚡ Ticker actualizado para {symbol}: Last={self.ticker_data[symbol]['last_price']}, USDIndex={self.ticker_data[symbol]['usd_index_price']}")
                if not bool (getattr (config, "STORAGE_SQLITE_MIRROR", True)):
                    return

            market_ticker = MarketTicker (
                timestamp=datetime.now (timezone.utc),
                symbol=symbol,
                last_price=self.ticker_data[symbol]["last_price"],
                volume_24h=self.ticker_data[symbol]["volume_24h"],
                high_24h=self.ticker_data[symbol]["high_24h"],
                low_24h=self.ticker_data[symbol]["low_24h"]
            )
            db.add (market_ticker)
            db.commit ()
            self._last_ticker_store_ts[symbol] = now_ts

            logger.debug (
                f"⚡ Ticker actualizado para {symbol}: Last={self.ticker_data[symbol]['last_price']}, USDIndex={self.ticker_data[symbol]['usd_index_price']}")
        except Exception as e:
            logger.error (f"❌ Error inesperado en _handle_ticker para {symbol}: {type (e).__name__} - {str (e)}")

    @staticmethod
    def _signal_eval (metrics: Dict) -> Dict[str, Any]:
        return evaluate_signal (
            metrics,
            buy_th=float (getattr (config, "COMBINED_BUY_THRESHOLD", 4.5) or 4.5),
            sell_th=float (getattr (config, "COMBINED_SELL_THRESHOLD", -4.5) or -4.5),
            hold_band=float (getattr (config, "COMBINED_HOLD_BAND", 3.0) or 3.0),
        )

    @staticmethod
    def _determine_decision (symbol: str, metrics: Dict) -> str:
        _ = symbol
        return str (NertzMetalEngine._signal_eval (metrics).get ("decision") or "hold")

    @staticmethod
    def _decision_detail (symbol: str, metrics: Dict) -> Dict[str, Any]:
        """Diagnóstico read-only: decisión, estado de mercado y bloqueos."""
        _ = symbol
        ev = NertzMetalEngine._signal_eval (metrics)
        return {
            "decision": ev.get ("decision"),
            "market_state": ev.get ("market_state"),
            "combined": ev.get ("combined"),
            "combined_z": ev.get ("combined_z"),
            "mom": ev.get ("mom"),
            "pio": ev.get ("pio"),
            "egm": ev.get ("egm"),
            "tfi": ev.get ("tfi"),
            "rvol": ev.get ("rvol"),
            "volatility": ev.get ("volatility"),
            "microprice_offset_bps": ev.get ("microprice_offset_bps"),
            "thresholds_effective": ev.get ("thresholds_effective"),
            "thresholds_symmetric_base": ev.get ("thresholds_symmetric_base"),
            "confirmations": ev.get ("confirmations"),
            "blockers_if_not_trading": ev.get ("blockers") or [],
        }

    @staticmethod
    def _compute_in_cooldown (
            cooldown_s: float,
            last_trade_time: datetime,
            current_time: datetime,
            metrics: Optional[Dict] = None,
    ) -> bool:
        if float (cooldown_s) <= 0.0:
            return False
        elapsed = (current_time - last_trade_time).total_seconds ()
        in_cd = elapsed < float (cooldown_s)
        if not in_cd or not bool (getattr (config, "COOLDOWN_BYPASS_STRONG_SIGNAL", True)):
            return in_cd
        try:
            comb = float ((metrics or {}).get ("combined") or 0.0)
            buy_th = float (getattr (config, "COMBINED_BUY_THRESHOLD", 1.5) or 1.5)
            mult = float (getattr (config, "COOLDOWN_BYPASS_MULT", 1.25) or 1.25)
            if abs (comb) >= buy_th * mult:
                return False
        except Exception:
            pass
        return in_cd

    @staticmethod
    def _build_spot_create_body (
            *,
            symbol: str,
            side: str,
            order_type: str,
            qty_str: str,
            order_link_id: str,
            time_in_force: Optional[str] = None,
            price_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        ot_map = {"limit": "Limit", "Limit": "Limit", "market": "Market", "Market": "Market"}
        tif_map = {
            "GoodTillCancel": "GTC", "GTC": "GTC",
            "ImmediateOrCancel": "IOC", "IOC": "IOC",
            "FillOrKill": "FOK", "FOK": "FOK", "PostOnly": "PostOnly",
        }
        ot = ot_map.get (str (order_type or "Limit").strip (), "Limit")
        tif = tif_map.get (str (time_in_force or "GTC").strip (), "GTC")
        if ot == "Market":
            tif = "IOC"
        body: Dict[str, Any] = {
            "category": "spot",
            "symbol": symbol,
            "side": "Buy" if str (side).lower () == "buy" else "Sell",
            "orderType": ot,
            "qty": qty_str,
            "timeInForce": tif,
            "orderLinkId": order_link_id,
        }
        if ot == "Limit" and price_str:
            body["price"] = price_str
        if ot == "Market":
            body["marketUnit"] = "baseCoin"
        return body

    def _default_metrics (self) -> Dict[str, float]:
        return {"combined": 0.0, "ild": 0.0, "egm": 0.0, "rol": 0.0, "pio": 0.0, "ogm": 0.0, "volatility": 0.0, "data_ok": False}

    @staticmethod
    def _serialize_metrics_for_storage (metrics: Any) -> Dict[str, Any]:
        if not isinstance (metrics, dict):
            return {}
        out: Dict[str, Any] = {}
        for k, v in metrics.items ():
            if k == "thresholds":
                continue
            if isinstance (v, bool):
                out[str (k)] = bool (v)
            elif isinstance (v, dict):
                out[str (k)] = v
            elif isinstance (v, (int, float)):
                try:
                    if bool (np.isfinite (float (v))):
                        out[str (k)] = float (v)
                except Exception:
                    pass
            elif v is not None:
                try:
                    fv = float (v)
                    if bool (np.isfinite (fv)):
                        out[str (k)] = fv
                except Exception:
                    pass
        return out

    @staticmethod
    def _d (value: Any) -> Decimal:
        try:
            return Decimal (str (value))
        except Exception:
            return Decimal ("0")

    @staticmethod
    def _format_decimal (value: Decimal) -> str:
        s = format (value, "f")
        if "." in s:
            s = s.rstrip ("0").rstrip (".")
        return s if s else "0"

    def _quantize_to_step (self, value: float, step: float, rounding) -> Decimal:
        if step is None or step <= 0:
            return self._d (value)
        dv = self._d (value)
        ds = self._d (step)
        if ds == 0:
            return dv
        units = (dv / ds).to_integral_value (rounding=rounding)
        return units * ds

    async def _get_instrument_rules (self, symbol: str) -> Dict[str, float]:
        now = time.time ()
        if symbol in self.instrument_rules and (now - self._instrument_rules_ts.get (symbol, 0.0) < 3600.0):
            return self.instrument_rules[symbol]

        url = f"{BASE_URL}/v5/market/instruments-info"
        params = {"category": "spot", "symbol": symbol}
        async with aiohttp.ClientSession () as session:
            async with session.get (url, params=params) as resp:
                data = await resp.json ()

        rules = {
            "tick_size": 0.01,
            "qty_step": float (config.MIN_TRADE_SIZE),
            "min_qty": float (config.MIN_TRADE_SIZE),
            "min_notional": 1.0,
        }
        try:
            if isinstance (data, dict) and data.get ("retCode") == 0:
                lst = ((data.get ("result") or {}).get ("list") or [])
                row = lst[0] if isinstance (lst, list) and lst else {}
                price_filter = row.get ("priceFilter") or {}
                lot_filter = row.get ("lotSizeFilter") or {}

                tick = price_filter.get ("tickSize")
                qty_step = lot_filter.get ("qtyStep")
                if qty_step is None:
                    qty_step = lot_filter.get ("basePrecision")
                min_qty = lot_filter.get ("minOrderQty")
                min_amt = lot_filter.get ("minNotionalValue")
                if min_amt is None:
                    min_amt = lot_filter.get ("minOrderAmt")

                if tick is not None:
                    rules["tick_size"] = float (tick)
                if qty_step is not None:
                    rules["qty_step"] = float (qty_step)
                if min_qty is not None:
                    rules["min_qty"] = float (min_qty)
                if min_amt is not None:
                    rules["min_notional"] = float (min_amt)
        except Exception:
            pass

        self.instrument_rules[symbol] = rules
        self._instrument_rules_ts[symbol] = now
        return rules

    def _thresholds_payload (self) -> Dict[str, float]:
        sym = symmetrize_threshold_values (
            float (getattr (config, "COMBINED_BUY_THRESHOLD", 4.5)),
            float (getattr (config, "COMBINED_SELL_THRESHOLD", -4.5)),
            float (getattr (config, "COMBINED_HOLD_BAND", 3.0)),
        )
        return {
            "egm_buy_threshold": float (config.EGM_BUY_THRESHOLD),
            "egm_sell_threshold": float (config.EGM_SELL_THRESHOLD),
            "combined_buy_threshold": float (sym.combined_buy_threshold),
            "combined_sell_threshold": float (sym.combined_sell_threshold),
            "combined_hold_band": float (sym.combined_hold_band),
        }

    async def _live_metrics_tick (self, db: Session) -> None:
        refresh_s = float (getattr (config, "METRICS_LIVE_REFRESH_S", 5.0) or 5.0)
        now_ts = time.time ()
        for symbol in self.symbols:
            if now_ts - float (self._last_live_metrics_ts.get (symbol, 0.0)) < max (1.0, refresh_s):
                continue
            self._last_live_metrics_ts[symbol] = now_ts
            try:
                await self._core_cycle (symbol, db, collect_only=True)
            except Exception as e:
                logger.debug (f"live_metrics_tick skip {symbol}: {e}")

    async def _metrics_snapshot_tick (self, db: Session) -> None:
        interval_s = float (getattr (config, "METRICS_SNAPSHOT_INTERVAL_S", 55.0) or 55.0)
        now_ts = time.time ()
        for symbol in self.symbols:
            if now_ts - float (self._last_metrics_snapshot_ts.get (symbol, 0.0)) < interval_s:
                continue
            metrics = dict (self._last_metrics_by_symbol.get (symbol) or {})
            if not bool (metrics.get ("data_ok", False)):
                try:
                    await self._core_cycle (symbol, db, collect_only=True)
                except Exception as e:
                    logger.debug (f"metrics_snapshot refresh skip {symbol}: {e}")
                metrics = dict (self._last_metrics_by_symbol.get (symbol) or {})
            if not bool (metrics.get ("data_ok", False)):
                continue
            ticker = self.ticker_data.get (symbol, {}) or {}
            last_price = float (ticker.get ("last_price", 0.0) or 0.0)
            if last_price <= 0:
                candles = self.candles.get (symbol) or []
                if candles:
                    try:
                        last_price = float (candles[0].close)
                    except Exception:
                        last_price = 0.0
            if last_price <= 0:
                continue
            decision = self._determine_decision (symbol, metrics)
            await self._record_metrics_snapshot (db, symbol, last_price, metrics, decision)

    async def _record_metrics_snapshot (self, db: Session, symbol: str, last_price: float, metrics: Dict[str, float],
                                        decision: str) -> None:
        if str (decision).lower () == "warmup":
            return
        if not bool (metrics.get ("data_ok", False)):
            return
        now = datetime.now (timezone.utc)
        now_ts = time.time ()
        dedup_s = float (getattr (config, "METRICS_SNAPSHOT_DEDUP_S", 3.0) or 3.0)
        if now_ts - float (self._last_metrics_snapshot_ts.get (symbol, 0.0)) < max (0.0, dedup_s):
            return
        self._last_metrics_snapshot_ts[symbol] = now_ts
        thresholds = self._thresholds_payload ()
        combined_v = float (metrics.get ("combined", 0.0) or 0.0)
        metrics_stored = self._serialize_metrics_for_storage (metrics)

        q = self._metrics_window.setdefault (symbol, deque (maxlen=2500))
        q.append ({"ts": float (now_ts), "decision": str (decision), "combined": float (combined_v)})

        try:
            db.add (
                MetricSnapshot (
                    timestamp=now,
                    symbol=symbol,
                    last_price=float (last_price or 0.0),
                    decision=str (decision),
                    combined=float (metrics.get ("combined", 0.0) or 0.0),
                    ild=float (metrics.get ("ild", 0.0) or 0.0),
                    egm=float (metrics.get ("egm", 0.0) or 0.0),
                    rol=float (metrics.get ("rol", 0.0) or 0.0),
                    pio=float (metrics.get ("pio", 0.0) or 0.0),
                    ogm=float (metrics.get ("ogm", 0.0) or 0.0),
                    volatility=float (metrics.get ("volatility", 0.0) or 0.0),
                    thresholds=thresholds,
                )
            )
            db.commit ()
        except Exception as e:
            logger.debug (f"metric_snapshots sqlite skip {symbol}: {e}")
            try:
                db.rollback ()
            except Exception:
                pass

        if self._storage is not None:
            row = MetricRow (
                timestamp=now,
                symbol=symbol,
                last_price=float (last_price or 0.0),
                decision=str (decision),
                combined=float (metrics.get ("combined", 0.0) or 0.0),
                ild=float (metrics.get ("ild", 0.0) or 0.0),
                egm=float (metrics.get ("egm", 0.0) or 0.0),
                rol=float (metrics.get ("rol", 0.0) or 0.0),
                pio=float (metrics.get ("pio", 0.0) or 0.0),
                ogm=float (metrics.get ("ogm", 0.0) or 0.0),
                volatility=float (metrics.get ("volatility", 0.0) or 0.0),
                thresholds=thresholds,
                metrics=metrics_stored,
            )
            await self._storage.enqueue_metric (row)

        if not bool (getattr (config, "STORAGE_DISABLE_JSONL", False)):
            snapshot_payload = {
                "timestamp": now.isoformat (),
                "ts": float (now_ts),
                "symbol": symbol,
                "last_price": float (last_price or 0.0),
                "decision": str (decision),
                "metrics": metrics_stored,
                "thresholds": thresholds,
            }
            append_metrics_snapshot (snapshot_payload, data_dir=DATABASE_DIR)

        min_gap = float (getattr (config, "METRICS_RESULTS_EVENT_MIN_S", 0.0) or 0.0)
        if now_ts - self._last_metrics_json_ts.get (symbol, 0.0) >= max (0.0, min_gap):
            append_results_event (
                {
                    "type": "metrics",
                    "symbol": symbol,
                    "last_price": float (last_price or 0.0),
                    "decision": str (decision),
                    "metrics": metrics_stored,
                    "thresholds": thresholds,
                },
                log_dir=os.path.join (os.path.dirname (__file__), '..', 'logs'),
            )
            self._last_metrics_json_ts[symbol] = now_ts
            logger.debug (
                f"📸 Snapshot {symbol} decision={decision} combined={combined_v:.2f} calibrated={metrics.get ('metrics_calibrated')}"
            )

    def _compute_threshold_targets (self, trades: list[Trade]) -> Dict[str, float]:
        buys = [t for t in trades if t.action == "buy" and t.egm is not None]
        sells = [t for t in trades if t.action == "sell" and t.egm is not None]

        win_buys = [t for t in buys if (t.profit_loss or 0.0) > 0]
        win_sells = [t for t in sells if (t.profit_loss or 0.0) > 0]

        def _median (values: list[float]) -> Optional[float]:
            if not values:
                return None
            values_sorted = sorted (values)
            mid = len (values_sorted) // 2
            if len (values_sorted) % 2 == 1:
                return float (values_sorted[mid])
            return float ((values_sorted[mid - 1] + values_sorted[mid]) / 2)

        buy_egm_target = _median ([float (t.egm) for t in win_buys]) if win_buys else None
        sell_egm_target = _median ([float (t.egm) for t in win_sells]) if win_sells else None

        buy_comb_target = _median ([float (t.combined) for t in win_buys]) if win_buys else None
        sell_comb_target = _median ([float (t.combined) for t in win_sells]) if win_sells else None

        targets: Dict[str, float] = {}
        if buy_egm_target is not None:
            targets["egm_buy_threshold"] = max (0.0, min (1.0, buy_egm_target * 0.8))
        if sell_egm_target is not None:
            targets["egm_sell_threshold"] = min (0.0, max (-1.0, sell_egm_target * 0.8))

        if buy_comb_target is not None:
            targets["combined_buy_threshold"] = max (1.0, min (15.0, buy_comb_target * 0.9))
        if sell_comb_target is not None:
            targets["combined_sell_threshold"] = min (-1.0, max (-15.0, sell_comb_target * 0.9))
        if "combined_buy_threshold" in targets and "combined_sell_threshold" in targets:
            sym = (targets["combined_buy_threshold"] - targets["combined_sell_threshold"]) / 2.0
            targets["combined_buy_threshold"], targets["combined_sell_threshold"] = sym, -sym

        return targets

    def _apply_threshold_update (self, targets: Dict[str, float], alpha: float = 0.1) -> Dict[str, Any]:
        before = self._thresholds_payload ()
        if "egm_buy_threshold" in targets:
            config.EGM_BUY_THRESHOLD = (1 - alpha) * float (config.EGM_BUY_THRESHOLD) + alpha * float (
                targets["egm_buy_threshold"])
        if "egm_sell_threshold" in targets:
            config.EGM_SELL_THRESHOLD = (1 - alpha) * float (config.EGM_SELL_THRESHOLD) + alpha * float (
                targets["egm_sell_threshold"])
        if "combined_buy_threshold" in targets or "combined_sell_threshold" in targets:
            current = Thresholds (
                float (getattr (config, "COMBINED_BUY_THRESHOLD", 4.5)),
                float (getattr (config, "COMBINED_SELL_THRESHOLD", -4.5)),
                float (getattr (config, "COMBINED_HOLD_BAND", 3.0)),
            )
            target = Thresholds (
                float (targets.get ("combined_buy_threshold", current.combined_buy_threshold)),
                float (targets.get ("combined_sell_threshold", current.combined_sell_threshold)),
                float (targets.get ("combined_hold_band", current.combined_hold_band)),
            )
            blended = blend_thresholds_symmetric (current, target, alpha)
            config.COMBINED_BUY_THRESHOLD = float (blended.combined_buy_threshold)
            config.COMBINED_SELL_THRESHOLD = float (blended.combined_sell_threshold)
            config.COMBINED_HOLD_BAND = float (blended.combined_hold_band)
        elif "combined_hold_band" in targets:
            config.COMBINED_HOLD_BAND = (1 - alpha) * float (
                getattr (config, "COMBINED_HOLD_BAND", 3.0)) + alpha * float (targets["combined_hold_band"])
        after = self._thresholds_payload ()
        return {"before": before, "after": after}

    def force_calibrate_thresholds (
            self,
            db: Session,
            sample_size: int = 500,
            alpha: float = 1.0,
            min_trades: int = 20,
    ) -> Dict[str, Any]:
        try:
            before = self._thresholds_payload ()
            trades = (
                db.query (Trade)
                .filter (Trade.outcome_status == "final")
                .order_by (Trade.timestamp.desc ())
                .limit (max (1, int (sample_size)))
                .all ()
            )
            total = len (trades)
            wins = sum (1 for t in trades if (t.profit_loss or 0.0) > 0)
            losses = sum (1 for t in trades if (t.profit_loss or 0.0) < 0)
            win_rate = (wins / total) * 100 if total > 0 else 0.0

            if total < int (min_trades):
                return {
                    "success": False,
                    "message": "not_enough_final_trades",
                    "sample_size": total,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                    "thresholds": {"before": before, "after": before},
                }

            targets = self._compute_threshold_targets (trades)
            if not targets:
                return {
                    "success": False,
                    "message": "no_targets",
                    "sample_size": total,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                    "thresholds": {"before": before, "after": before},
                }

            update = self._apply_threshold_update (targets, alpha=float (alpha))
            snapshot = ThresholdSnapshot (
                timestamp=datetime.now (timezone.utc),
                egm_buy_threshold=float (config.EGM_BUY_THRESHOLD),
                egm_sell_threshold=float (config.EGM_SELL_THRESHOLD),
                combined_buy_threshold=float (getattr (config, "COMBINED_BUY_THRESHOLD", 2.0)),
                combined_sell_threshold=float (getattr (config, "COMBINED_SELL_THRESHOLD", -2.0)),
                stats={
                    "targets": targets,
                    "sample_size": total,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                    "combined_hold_band": float (getattr (config, "COMBINED_HOLD_BAND", 1.0)),
                    **update,
                },
            )
            db.add (snapshot)
            db.commit ()

            log_dir = os.path.join (os.path.dirname (__file__), "..", "logs")
            append_results_event ({"type": "thresholds", "update": update, "targets": targets}, log_dir=log_dir)
            payload = load_results_json (log_dir=log_dir)
            payload["thresholds"] = {
                "timestamp": datetime.now (timezone.utc).isoformat (),
                "values": self._thresholds_payload (),
                "update": update,
                "targets": targets,
            }
            save_results (payload, log_dir=log_dir)
            return {
                "success": True,
                "sample_size": total,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "targets": targets,
                "thresholds": update,
            }
        except Exception as e:
            return {"success": False, "message": str (e)}

    async def cancel_all_open_orders (self, symbol: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            return {"success": True, "skipped": True, "mode": "disabled", "seen": 0, "cancelled": 0, "failed": 0,
                    "failures": []}
        client = self._bybit_client ()
        if client is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        try:
            payload = await client.get_open_orders_merged (category="spot", symbol=symbol, limit=int (limit))
            if payload.get ("retCode") != 0:
                return {"success": False, "message": payload.get ("retMsg") or "get_open_orders_failed", "raw": payload}
            orders = (payload.get ("result", {}) or {}).get ("list", []) or []
        except Exception as e:
            return {"success": False, "message": str (e)}

        cancelled = 0
        failed = 0
        failures: list[dict] = []
        for o in orders:
            if not isinstance (o, dict):
                continue
            link = o.get ("orderLinkId")
            link_str = str (link) if isinstance (link, str) and link else ""
            if not link_str.startswith ("nertzh-"):
                continue
            oid = o.get ("orderId")
            sym = o.get ("symbol")
            if not isinstance (oid, str) or not oid or not isinstance (sym, str) or not sym:
                continue
            try:
                res = await client.cancel_order ({"category": "spot", "symbol": sym, "orderId": oid})
                if res.get ("retCode") == 0:
                    cancelled += 1
                else:
                    failed += 1
                    failures.append (
                        {"orderId": oid, "symbol": sym, "retCode": res.get ("retCode"), "retMsg": res.get ("retMsg")})
            except Exception as e:
                failed += 1
                failures.append ({"orderId": oid, "symbol": sym, "error": str (e)})

        return {
            "success": True,
            "seen": len ([o for o in orders if isinstance (o, dict)]),
            "cancelled": cancelled,
            "failed": failed,
            "failures": failures[:50],
        }

    def reset_runtime_state (self) -> None:
        for sym, task in list ((getattr (self, "hft_tasks", None) or {}).items ()):
            if task is not None and not task.done ():
                try:
                    task.cancel ()
                except Exception:
                    pass
        self.hft_tasks = {}
        self._hft_params = {}
        self.trades_cache = {symbol: [] for symbol in self.symbols}
        self.iterations = 0
        self.order_status = {}
        self._metrics_raw_history = {symbol: deque () for symbol in self.symbols}
        self._last_weighted_liquidity = {symbol: None for symbol in self.symbols}
        self.recent_trades = {symbol: deque (maxlen=500) for symbol in self.symbols}
        self._metrics_window = {symbol: deque (maxlen=2500) for symbol in self.symbols}
        self.last_trade_time = {symbol: datetime.min.replace (tzinfo=timezone.utc) for symbol in self.symbols}
        self._last_metrics_json_ts = {symbol: 0.0 for symbol in self.symbols}
        self._last_metrics_snapshot_ts = {symbol: 0.0 for symbol in self.symbols}
        self._last_balance_sync_ts = 0.0
        self._balance_dirty = False
        self._last_kline_ts = {symbol: 0.0 for symbol in self.symbols}

    def wipe_database (self, db: Session) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for model, name in [
            (Trade, "trades"),
            (MetricSnapshot, "metric_snapshots"),
            (BalanceSnapshot, "balance_snapshots"),
            (ThresholdSnapshot, "threshold_snapshots"),
            (MarketTicker, "market_ticker"),
            (Orderbook, "orderbook"),
            (MarketData, "market_data"),
        ]:
            try:
                n = db.query (model).delete ()
                counts[name] = int (n or 0)
            except Exception:
                counts[name] = -1
        db.commit ()
        try:
            db.execute (text ("VACUUM"))
            db.commit ()
        except Exception:
            pass
        return counts

    def reset_results_json (self) -> str:
        log_dir = os.path.join (os.path.dirname (__file__), "..", "logs")
        try:
            fp = os.path.join (os.path.abspath (log_dir), "results.json")
            if os.path.exists (fp):
                os.remove (fp)
        except Exception:
            pass
        payload = {"events": [], "metadata": {"timestamp": datetime.now (timezone.utc).isoformat (), "reset": True}}
        save_results (payload, log_dir=log_dir)
        return os.path.join (os.path.abspath (log_dir), "results.json")

    async def _auto_tune_thresholds_if_due (self) -> None:
        if not bool (getattr (config, "AUTO_TUNE_THRESHOLDS", False)):
            return
        now_ts = time.time ()
        if now_ts - self._last_tune_ts < 60.0:
            return

        with SessionLocal () as db:
            recent_trades = (
                db.query (Trade)
                .filter (Trade.outcome_status == "final")
                .order_by (Trade.timestamp.desc ())
                .limit (200)
                .all ()
            )
            if len (recent_trades) < 20:
                return

            targets = self._compute_threshold_targets (recent_trades)
            if not targets:
                return

            update = self._apply_threshold_update (targets, alpha=0.1)
            snapshot = ThresholdSnapshot (
                timestamp=datetime.now (timezone.utc),
                egm_buy_threshold=float (config.EGM_BUY_THRESHOLD),
                egm_sell_threshold=float (config.EGM_SELL_THRESHOLD),
                combined_buy_threshold=float (getattr (config, "COMBINED_BUY_THRESHOLD", 8.0)),
                combined_sell_threshold=float (getattr (config, "COMBINED_SELL_THRESHOLD", -8.0)),
                stats={
                    "targets": targets,
                    "sample_size": len (recent_trades),
                    "wins": sum (1 for t in recent_trades if (t.profit_loss or 0.0) > 0),
                    "losses": sum (1 for t in recent_trades if (t.profit_loss or 0.0) < 0),
                    **update,
                },
            )
            db.add (snapshot)
            db.commit ()

        try:
            append_results_event (
                {"type": "thresholds", "update": update, "targets": targets},
                log_dir=os.path.join (os.path.dirname (__file__), '..', 'logs'),
            )
        except Exception as e:
            self._rl_log ("thresholds:append_event", "warning", f"⚠️ No se pudo registrar evento de thresholds: {e}",
                          interval_s=60.0)
        try:
            log_dir = os.path.join (os.path.dirname (__file__), '..', 'logs')
            patch_results (
                {
                    "thresholds": {
                        "timestamp": datetime.now (timezone.utc).isoformat (),
                        "values": self._thresholds_payload (),
                        "update": update,
                        "targets": targets,
                    }
                },
                log_dir=log_dir,
            )
        except Exception as e:
            self._rl_log ("thresholds:save_results", "warning",
                          f"⚠️ No se pudo persistir thresholds en results.json: {e}", interval_s=60.0)
        self._last_tune_ts = now_ts

    async def _core_cycle (self, symbol: str, db: Session, collect_only: bool = False,
                           force_trade: bool = False) -> None:
        lock = self._core_cycle_locks.setdefault (symbol, asyncio.Lock ())
        async with lock:
            try:
                current_time = datetime.now (timezone.utc)
                now_ts = time.time ()
                should_sync_balance = bool (
                    (now_ts - float (self._last_balance_sync_ts or 0.0) >= 60.0)
                    or (
                            bool (getattr (self, "_balance_dirty", False))
                            and (now_ts - float (self._last_balance_sync_ts or 0.0) >= 2.0)
                    )
                )
                if should_sync_balance:
                    balance = await self.record_balance (account_type="UNIFIED", coin="USDT")
                    if balance.get ("success") and isinstance (balance.get ("balance"), dict):
                        available = float (balance["balance"].get ("available_balance") or 0.0)
                        total_equity = float (balance["balance"].get ("total_equity") or 0.0)
                        if total_equity > 0:
                            self.capital = total_equity
                        elif available > 0:
                            self.capital = available
                        self._balance_dirty = False
                    self._last_balance_sync_ts = now_ts

                candles = list (self.candles.get (symbol) or [])
                if not candles:
                    candles = (
                        db.query (MarketData)
                        .filter (MarketData.symbol == symbol)
                        .order_by (MarketData.timestamp.desc ())
                        .limit (50)
                        .all ()
                    )
                    if candles:
                        self.candles[symbol] = list (candles)
                cooldown_s = float (getattr (config, "TRADE_COOLDOWN_S", 0.0) or 0.0)
                last_trade_time = self.last_trade_time.get (symbol, datetime.min.replace (tzinfo=timezone.utc))

                candle_data = [
                    {"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume}
                    for c in candles
                ]
                orderbook = self.orderbook_data.get (symbol, {"bids": [], "asks": []})
                ticker = self.ticker_data.get (symbol, {"last_price": 0.0})
                warmup = False

                if len (candle_data) >= 2 and orderbook.get ("bids") and orderbook.get ("asks") and ticker.get (
                        "last_price"):
                    window_min = float (getattr (config, "METRICS_WINDOW_MINUTES", 15.0) or 15.0)
                    window_s = max (60.0, window_min * 60.0)
                    history_q = self._metrics_raw_history.setdefault (symbol, deque ())
                    cutoff = now_ts - window_s
                    while history_q:
                        head = history_q[0]
                        ts = head.get ("ts") if isinstance (head, dict) else None
                        if ts is None or float (ts) >= cutoff:
                            break
                        history_q.popleft ()

                    history_payload = []
                    for h in history_q:
                        if not isinstance (h, dict):
                            continue
                        history_payload.append ({k: v for k, v in h.items () if k != "ts"})

                    prev_entry = self._last_weighted_liquidity.get (symbol)
                    prev_liq = None
                    prev_ts = None
                    if isinstance (prev_entry, tuple) and len (prev_entry) == 2:
                        prev_liq = prev_entry[0]
                        prev_ts = prev_entry[1]

                    ticker_payload = dict (ticker)
                    ticker_payload["orderbook_lambda"] = float (getattr (config, "ORDERBOOK_LAMBDA", 0.03) or 0.03)
                    ticker_payload["orderbook_pct_band"] = float (
                        getattr (config, "ORDERBOOK_PCT_BAND", 0.015) or 0.015)
                    ticker_payload["ild_target_move"] = float (getattr (config, "ILD_TARGET_MOVE", 0.002) or 0.002)
                    ticker_payload["metric_history"] = history_payload
                    ticker_payload["prev_weighted_liquidity"] = prev_liq
                    ticker_payload["rol_dt_s"] = (now_ts - float (prev_ts)) if prev_ts else None
                    ticker_payload["formulas"] = getattr (config, "FORMULAS", {}) or {}

                    recent_trades_payload = list (self.recent_trades.get (symbol) or [])[-50:]
                    ticker_payload["recent_trades"] = recent_trades_payload
                    metrics = calculate_metrics (
                        candle_data,
                        orderbook,
                        ticker_payload,
                        depth=int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50),
                        recent_trades=recent_trades_payload,
                    )
                    try:
                        m2: Dict[str, float] = {}
                        if isinstance (metrics, dict):
                            for k, v in metrics.items ():
                                try:
                                    fv = float (v)
                                except Exception:
                                    continue
                                if bool (np.isfinite (fv)):
                                    m2[str (k)] = float (fv)
                        self._last_metrics_by_symbol[symbol] = m2
                    except Exception:
                        self._last_metrics_by_symbol[symbol] = {}

                    try:
                        wl = metrics.get ("weighted_liquidity")
                        if wl is not None:
                            self._last_weighted_liquidity[symbol] = (float (wl), now_ts)
                    except Exception:
                        pass

                    try:
                        raw_sample = {
                            "ts": now_ts,
                            "pio": float (metrics.get ("pio_raw", 0.0) or 0.0),
                            "ild": float (metrics.get ("ild_raw", 0.0) or 0.0),
                            "egm": float (metrics.get ("egm_raw", 0.0) or 0.0),
                            "rol": float (metrics.get ("rol_raw", 0.0) or 0.0),
                            "ogm": float (metrics.get ("ogm_raw", 0.0) or 0.0),
                            "mom_raw": float (metrics.get ("mom_raw", 0.0) or 0.0),
                            "asymmetry": float (metrics.get ("asymmetry", 0.0) or 0.0),
                            "spread_pct": float (metrics.get ("spread_pct", 0.0) or 0.0),
                        }
                        history_q.append (raw_sample)
                    except Exception:
                        pass
                else:
                    metrics = self._default_metrics ()
                    warmup = True
                    try:
                        self._last_metrics_by_symbol[symbol] = {
                            k: float (v) for k, v in (metrics.items () if isinstance (metrics, dict) else []) if
                            v is not None
                        }
                    except Exception:
                        self._last_metrics_by_symbol[symbol] = {}
                logger.debug (
                    f"📊 Métricas calculadas para {symbol}: pio={metrics.get ('pio', 0)}, ild={metrics.get ('ild', 0)}, egm={metrics.get ('egm', 0)}, rol={metrics.get ('rol', 0)}, combined={metrics.get ('combined', 0)}")

                in_cooldown = self._compute_in_cooldown (
                    cooldown_s, last_trade_time, current_time, metrics
                )
                decision = self._determine_decision (symbol, metrics)
                snapshot_decision = "warmup" if bool (warmup) else decision
                if warmup:
                    decision = "hold"
                last_price = float (ticker.get ("last_price", 0.0) or 0.0)
                if last_price <= 0 and candles:
                    try:
                        last_price = float (candles[0].close)
                    except Exception:
                        last_price = 0.0

                finalized = await self._finalize_due_outcomes (db, symbol, last_price)
                if finalized is not None:
                    await self._save_results (symbol, finalized)
                await self._record_metrics_snapshot (db, symbol, last_price, metrics, snapshot_decision)

                await self._auto_tune_thresholds_if_due ()

                if (
                        decision == "hold"
                        and force_trade
                        and not collect_only
                        and not in_cooldown
                ):
                    last = (self.trades_cache.get (symbol) or [])
                    last_action = (last[-1].get ("action") if last else None)
                    decision = "sell" if last_action == "buy" else "buy"

                if decision in {"buy", "sell"} and bool (getattr (config, "ML_ENABLED", False)):
                    p = self.ml_predict_proba (symbol=symbol, action=decision, metrics=metrics)
                    th = float (getattr (config, "ML_PROB_THRESHOLD", 0.6) or 0.6)
                    if p is not None and p < th:
                        decision = "hold"

                if decision == "hold" or collect_only or in_cooldown:
                    return

                spread_avg_bps = float(getattr(config, "AVG_SPREAD_BPS", 1.5) or 1.5)
                allowed, gate_reason = check_execution_gates(
                    metrics, spread_avg_bps=spread_avg_bps
                )
                if not allowed:
                    logger.debug(
                        f"🛑 [EXEC GATE] {symbol} | {gate_reason} | "
                        f"spread={float(metrics.get('spread_bps', 0) or 0):.2f}bps "
                        f"rvol={float(metrics.get('rvol', 0) or 0):.2e}"
                    )
                    return

                ctx = self.operations.get(symbol)
                ctx.cooldown_s = float(getattr(config, "TRADE_COOLDOWN_S", 0.0) or 0.0)
                if not ctx.can_trade():
                    return

                if not bool (getattr (config, "ALLOW_MULTIPLE_ACTIVE_TRADES", True)):
                    active_trade = (
                        db.query (Trade)
                        .filter (Trade.symbol == symbol)
                        .filter (Trade.outcome_status.in_ (["pending", "partial", "filled"]))
                        .order_by (Trade.timestamp.desc ())
                        .first ()
                    )
                    if active_trade is not None:
                        return

                rules = await self._get_instrument_rules (symbol)
                tick_size = float (rules.get ("tick_size") or 0.01)
                qty_step = float (rules.get ("qty_step") or float (config.MIN_TRADE_SIZE))
                min_qty = float (rules.get ("min_qty") or float (config.MIN_TRADE_SIZE))
                min_notional = float (rules.get ("min_notional") or 1.0)

                risk_per_trade = self.capital * config.RISK_FACTOR
                volatility = metrics.get ("volatility", 0.01)
                if volatility <= 0:
                    logger.warning (f"⚠️ Volatilidad inválida ({volatility}) para {symbol}, usando 0.01")
                    volatility = 0.01

                min_risk_notional = max (min_notional * 1.1, 1.0)
                if risk_per_trade < min_risk_notional:
                    risk_per_trade = min_risk_notional

                if last_price <= 0:
                    logger.error (f"❌ Precio inválido ({last_price}) para {symbol}")
                    return

                quantity = risk_per_trade / (volatility * last_price)
                quantity = max (min (quantity, config.MAX_TRADE_SIZE), config.MIN_TRADE_SIZE)

                order_type_raw = config.ORDER_TYPE or "Limit"
                order_type = {
                    "limit": "Limit",
                    "Limit": "Limit",
                    "market": "Market",
                    "Market": "Market",
                }.get (order_type_raw, "Limit")

                entry_price = last_price
                if order_type == "Limit":
                    book = self.orderbook_data.get (symbol, {"bids": [], "asks": []})
                    try:
                        best_bid = float (book.get ("bids", [])[0][0]) if book.get ("bids") else last_price
                        best_ask = float (book.get ("asks", [])[0][0]) if book.get ("asks") else last_price
                    except Exception:
                        best_bid = last_price
                        best_ask = last_price
                    entry_price = best_bid if decision == "buy" else best_ask
                    entry_price = float (self._quantize_to_step (entry_price, tick_size, ROUND_HALF_UP))

                qty_dec = self._quantize_to_step (quantity, qty_step, ROUND_DOWN)
                min_qty_dec = self._quantize_to_step (min_qty, qty_step, ROUND_UP)
                if qty_dec < min_qty_dec:
                    qty_dec = min_qty_dec

                notional = float (qty_dec) * float (entry_price)
                if min_notional > 0 and notional < min_notional:
                    target_qty = (self._d (min_notional) / self._d (entry_price)) if entry_price > 0 else self._d (0)
                    qty_dec = self._quantize_to_step (float (target_qty), qty_step, ROUND_UP)

                quantity = float (qty_dec)
                trade_value = quantity * entry_price
                if trade_value > self.capital:
                    logger.warning (f"⚠️ Cantidad excesiva ({trade_value:.2f}) para {symbol}. Ajustando...")
                    quantity = (self.capital * 0.1) / max (entry_price, 1e-9)
                    qty_dec = self._quantize_to_step (quantity, qty_step, ROUND_DOWN)
                    if qty_dec < min_qty_dec:
                        qty_dec = min_qty_dec
                    notional = float (qty_dec) * float (entry_price)
                    if min_notional > 0 and notional < min_notional:
                        target_qty = (self._d (min_notional) / self._d (entry_price)) if entry_price > 0 else self._d (
                            0)
                        qty_dec = self._quantize_to_step (float (target_qty), qty_step, ROUND_UP)
                        notional = float (qty_dec) * float (entry_price)
                    if notional > self.capital:
                        logger.warning (
                            f"⚠️ No alcanza capital para mínimo del exchange ({min_notional}). Saltando trade.")
                        return
                    quantity = float (qty_dec)
                    if quantity < float (min_qty_dec):
                        logger.warning (f"⚠️ Cantidad ajustada ({quantity}) por debajo del mínimo. Saltando trade.")
                        return

                tp, sl = calculate_tp_sl (entry_price, volatility, decision, config.TP_PERCENTAGE, config.SL_PERCENTAGE)
                tp_dec = self._quantize_to_step (tp, tick_size, ROUND_HALF_UP)
                sl_dec = self._quantize_to_step (sl, tick_size, ROUND_HALF_UP)
                entry_dec = self._d (entry_price)
                tick_dec = self._d (tick_size)
                if decision == "buy":
                    if tp_dec <= entry_dec:
                        tp_dec = entry_dec + tick_dec
                    if sl_dec >= entry_dec:
                        sl_dec = entry_dec - tick_dec
                else:
                    if tp_dec >= entry_dec:
                        tp_dec = entry_dec - tick_dec
                    if sl_dec <= entry_dec:
                        sl_dec = entry_dec + tick_dec
                tp = float (tp_dec)
                sl = float (sl_dec)

                order_result = await self._place_order (symbol, decision, quantity, entry_price, tp, sl)
                if not order_result.get ("success", False):
                    logger.error (
                        f"❌ Fallo al colocar orden para {symbol}: {order_result.get ('message', 'Error desconocido')}")
                    return

                self.trade_id_counter += 1
                order_id = str (order_result.get ("order_id") or "")
                bybit_raw = order_result.get ("raw")
                order_link_id = str (order_result.get ("order_link_id") or "")
                thresholds = self._thresholds_payload ()
                metrics_payload = self._serialize_metrics_for_storage (metrics)

                metrics_snapshot = {
                    "timestamp": current_time.isoformat (),
                    "ts": float (time.time ()),
                    "symbol": symbol,
                    "last_price": float (last_price or 0.0),
                    "decision": str (decision),
                    "metrics": metrics_payload,
                    "thresholds": thresholds,
                }

                merged_raw: Dict[str, Any] = dict (bybit_raw) if isinstance (bybit_raw, dict) else {}
                if order_link_id:
                    merged_raw["order_link_id"] = order_link_id
                merged_raw["metrics_snapshot"] = metrics_snapshot
                bybit_raw = merged_raw

                trade = Trade (
                    trade_id=self.trade_id_counter - 1,
                    timestamp=current_time,
                    symbol=symbol,
                    action=decision,
                    order_id=order_id,
                    bybit_raw=bybit_raw,
                    entry_price=entry_price,
                    exit_price=0.0,
                    tp_price=float (tp),
                    sl_price=float (sl),
                    quantity=quantity,
                    profit_loss=0.0,
                    outcome_status="pending",
                    decision=decision,
                    combined=metrics.get ("combined", 0),
                    ild=metrics.get ("ild", 0),
                    egm=metrics.get ("egm", 0),
                    rol=metrics.get ("rol", 0),
                    pio=metrics.get ("pio", 0),
                    ogm=metrics.get ("ogm", 0),
                    risk_reward_ratio=config.TP_PERCENTAGE / config.SL_PERCENTAGE
                )
                db.add (trade)
                db.commit ()

                self.trades_cache.setdefault (symbol, []).append ({
                    "trade_id": trade.trade_id,
                    "timestamp": trade.timestamp.isoformat (),
                    "symbol": trade.symbol,
                    "action": trade.action,
                    "order_id": trade.order_id,
                    "entry_price": trade.entry_price,
                    "exit_price": None,
                    "tp_price": trade.tp_price,
                    "sl_price": trade.sl_price,
                    "quantity": trade.quantity,
                    "profit_loss": None,
                    "outcome_status": trade.outcome_status,
                    "outcome_timestamp": trade.outcome_timestamp.isoformat () if trade.outcome_timestamp else None,
                    "decision": trade.decision,
                    "combined": trade.combined,
                    "ild": trade.ild,
                    "egm": trade.egm,
                    "rol": trade.rol,
                    "pio": trade.pio,
                    "ogm": trade.ogm,
                    "risk_reward_ratio": trade.risk_reward_ratio,
                    "metrics_snapshot": metrics_snapshot,
                })
                self.last_trade_time[symbol] = current_time
                ctx.mark_trade ()
                if order_id:
                    self.order_status[order_id] = {
                        "order_id": order_id,
                        "trade_id": int (trade.trade_id),
                        "symbol": symbol,
                        "status": "pending",
                        "timestamp": datetime.now (timezone.utc).isoformat (),
                    }

                logger.info (
                    f"💰 Orden colocada: {decision.upper ()} {quantity:.4f} {symbol} @ {entry_price:.2f}, TP={tp:.2f}, SL={sl:.2f}, OrderID={order_id}")

                self.iterations += 1
                if self.iterations >= config.MAX_ITERATIONS > 0:
                    logger.info ("🏁 Máximo de iteraciones alcanzado. Deteniendo bot.")
                    self.stop ()
                self._refresh_trades_cache (symbol)
                await self._save_results (symbol, trade)

            except Exception as e:
                logger.error (f"❌ Error en _execute_trade para {symbol}: {e}")

    async def _execute_trade (self, symbol: str, db: Session):
        await self._core_cycle (symbol, db, collect_only=False)

    async def run_cycles (self, symbol: str, cycles: int, interval_ms: int, collect_only: bool) -> None:
        if cycles < 0:
            return
        remaining = cycles
        while self.running and (remaining > 0 or cycles == 0):
            with SessionLocal () as db:
                await self._core_cycle (symbol, db, collect_only=collect_only)
            if cycles != 0:
                remaining -= 1
            if interval_ms > 0:
                await asyncio.sleep (interval_ms / 1000)
            else:
                await asyncio.sleep (0)

    def start_hft (self, symbol: str, interval_ms: int = 250, collect_only: bool = False) -> bool:
        if symbol in self.hft_tasks and not self.hft_tasks[symbol].done ():
            return False
        task = asyncio.create_task (
            self.run_cycles (symbol, cycles=0, interval_ms=interval_ms, collect_only=collect_only))
        self.hft_tasks[symbol] = task
        self._hft_params[symbol] = {
            "interval_ms": int (interval_ms),
            "collect_only": bool (collect_only),
            "started_at": datetime.now (timezone.utc).isoformat (),
        }
        return True

    def stop_hft (self, symbol: str) -> bool:
        task = self.hft_tasks.get (symbol)
        if not task:
            return False
        task.cancel ()
        try:
            prev = self._hft_params.get (symbol) or {}
            self._hft_params[symbol] = {**prev, "stopped_at": datetime.now (timezone.utc).isoformat ()}
        except Exception:
            pass
        return True

    def is_hft_running (self, symbol: str) -> bool:
        task = self.hft_tasks.get (symbol)
        return bool (task is not None and not task.done ())

    def stop_all_hft (self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for sym in self.symbols:
            results[sym] = self.stop_hft (sym)
        return results

    def _auto_hft_enabled_effective (self) -> bool:
        try:
            return bool (getattr (config, "AUTO_HFT_ENABLED", False)) or bool (
                getattr (self, "_auto_hft_enabled", False))
        except Exception:
            return bool (getattr (self, "_auto_hft_enabled", False))

    async def _auto_hft_tick (self, db: Session) -> None:
        if not self.running:
            return
        if not self._auto_hft_enabled_effective ():
            return
        now_ts = time.time ()
        tick_s = float (getattr (config, "AUTO_HFT_TICK_S", 2.0) or 2.0)
        if now_ts - float (self._auto_hft_last_tick_ts or 0.0) < max (0.25, tick_s):
            return
        self._auto_hft_last_tick_ts = now_ts

        window_s = float (getattr (config, "AUTO_HFT_WINDOW_S", 60.0) or 60.0)
        min_snaps = int (getattr (config, "AUTO_HFT_MIN_SNAPSHOTS", 30) or 30)
        start_ratio = float (getattr (config, "AUTO_HFT_START_RATIO", 0.35) or 0.35)
        stop_ratio = float (getattr (config, "AUTO_HFT_STOP_RATIO", 0.15) or 0.15)
        combined_abs_th = float (getattr (config, "AUTO_HFT_COMBINED_ABS_THRESHOLD", 10.0) or 10.0)
        interval_ms = int (getattr (config, "AUTO_HFT_INTERVAL_MS", 250) or 250)
        collect_only = bool (getattr (config, "AUTO_HFT_COLLECT_ONLY", True))
        cooldown_s = float (getattr (config, "AUTO_HFT_COOLDOWN_S", 60.0) or 60.0)

        cutoff_ts = float (now_ts) - float (max (10.0, window_s))
        actions = self._agent_events.get ("actions")
        if not isinstance (actions, deque):
            actions = deque (maxlen=250)
            self._agent_events["actions"] = actions

        for sym in self.symbols:
            st = self._auto_hft_state.setdefault (sym, {"last_change_ts": 0.0})
            last_change = float (st.get ("last_change_ts") or 0.0)
            if now_ts - last_change < max (5.0, cooldown_s):
                continue

            decisions: list[str] = []
            combined_vals: list[float] = []
            q = self._metrics_window.get (sym)
            if not isinstance (q, deque):
                continue
            for row in reversed (q):
                if not isinstance (row, dict):
                    continue
                ts = row.get ("ts")
                if ts is None:
                    continue
                try:
                    if float (ts) < float (cutoff_ts):
                        break
                except Exception:
                    continue
                d = row.get ("decision")
                if isinstance (d, str):
                    decisions.append (d.lower ())
                try:
                    combined_vals.append (float (row.get ("combined") or 0.0))
                except Exception:
                    combined_vals.append (0.0)
                if len (decisions) >= 250:
                    break

            total = len (decisions)
            if total <= 0:
                continue
            non_hold = sum (1 for d in decisions if d in {"buy", "sell"})
            ratio = float (non_hold) / float (total) if total > 0 else 0.0
            abs_avg = (sum (abs (v) for v in combined_vals) / float (len (combined_vals))) if combined_vals else 0.0
            running = self.is_hft_running (sym)

            if (not running) and total >= max (5, min_snaps) and ratio >= start_ratio and abs_avg >= combined_abs_th:
                ok = self.start_hft (sym, interval_ms=max (0, int (interval_ms)), collect_only=bool (collect_only))
                if ok:
                    st["last_change_ts"] = now_ts
                    st["last_action"] = "start"
                    st["reason"] = {"total": total, "ratio": ratio, "abs_avg": abs_avg}
                    actions.append (
                        {"type": "auto_hft_start", "ts": datetime.now (timezone.utc).isoformat (), "symbol": sym,
                         "ratio": ratio, "abs_avg": abs_avg})
                    try:
                        append_results_event (
                            {"type": "auto_hft", "action": "start", "symbol": sym, "ratio": ratio, "abs_avg": abs_avg,
                             "interval_ms": int (interval_ms), "collect_only": bool (collect_only)},
                            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
                        )
                    except Exception:
                        pass
                continue

            if running and (total < max (5, min_snaps) or ratio <= stop_ratio or abs_avg < (combined_abs_th * 0.75)):
                stopped = self.stop_hft (sym)
                if stopped:
                    st["last_change_ts"] = now_ts
                    st["last_action"] = "stop"
                    st["reason"] = {"total": total, "ratio": ratio, "abs_avg": abs_avg}
                    actions.append (
                        {"type": "auto_hft_stop", "ts": datetime.now (timezone.utc).isoformat (), "symbol": sym,
                         "ratio": ratio, "abs_avg": abs_avg})
                    try:
                        append_results_event (
                            {"type": "auto_hft", "action": "stop", "symbol": sym, "ratio": ratio, "abs_avg": abs_avg},
                            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
                        )
                    except Exception:
                        pass

    async def _auto_tpsl_tick (self, db: Session) -> Dict[str, Any]:
        if not self.running:
            return {"success": True, "results": {"skipped": 1, "reason": "not_running"}}
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            return {"success": True, "results": {"skipped": 1, "mode": "disabled"}}

        interval_s = float (getattr (config, "AUTO_TPSL_INTERVAL_S", 3.0) or 3.0)
        now_ts = time.time ()
        if now_ts - float (getattr (self, "_auto_tpsl_last_tick_ts", 0.0) or 0.0) < max (0.25, interval_s):
            return {"success": True, "results": {"skipped": 1, "reason": "rate_limited"}}

        client = self._bybit_client ()
        if client is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        async with self._auto_tpsl_lock:
            now_ts = time.time ()
            if now_ts - float (getattr (self, "_auto_tpsl_last_tick_ts", 0.0) or 0.0) < max (0.25, interval_s):
                return {"success": True, "results": {"skipped": 1, "reason": "rate_limited"}}
            self._auto_tpsl_last_tick_ts = now_ts

            min_tp_ticks = int (getattr (config, "AUTO_TPSL_MIN_TP_MOVE_TICKS", 1) or 1)
            min_sl_ticks = int (getattr (config, "AUTO_TPSL_MIN_SL_MOVE_TICKS", 1) or 1)
            gap_mult = float (getattr (config, "AUTO_TPSL_TRAIL_GAP_MULT", 1.2) or 1.2)
            gap_min = float (getattr (config, "AUTO_TPSL_TRAIL_GAP_MIN", 0.001) or 0.001)
            tp_ext_mult = float (getattr (config, "AUTO_TPSL_TP_EXT_MULT", 1.25) or 1.25)
            ml_tp_boost = float (getattr (config, "AUTO_TPSL_ML_TP_BOOST", 1.0) or 1.0)
            ml_enabled = bool (getattr (config, "ML_ENABLED", False))
            ml_th = float (getattr (config, "ML_PROB_THRESHOLD", 0.6) or 0.6)
            fee_rate = float (getattr (config, "FEE_RATE", 0.0) or 0.0)

            actions = self._agent_events.get ("actions")
            if not isinstance (actions, deque):
                actions = deque (maxlen=250)
                self._agent_events["actions"] = actions

            results: Dict[str, Any] = {
                "checked": 0,
                "amended": 0,
                "db_updated": 0,
                "skipped": 0,
                "errors": 0,
                "executed_virtual": 0,
            }

            pending_trades = (
                db.query (Trade)
                .filter (Trade.outcome_status.in_ (["pending", "partial", "filled"]))
                .order_by (Trade.timestamp.desc ())
                .limit (500)
                .all ()
            )
            trades_by_symbol: Dict[str, list[Trade]] = {}
            for t in pending_trades:
                sym = str (getattr (t, "symbol", "") or "").strip ()
                if not sym:
                    continue
                trades_by_symbol.setdefault (sym, []).append (t)

            now_iso = datetime.now (timezone.utc).isoformat ()
            changed_any = False

            for sym, trades in trades_by_symbol.items ():
                if not trades:
                    continue
                last_price = float ((self.ticker_data.get (sym) or {}).get ("last_price") or 0.0)
                if last_price <= 0:
                    results["skipped"] += len (trades)
                    continue

                try:
                    rules = await self._get_instrument_rules (sym)
                    tick_size = float (rules.get ("tick_size") or 0.01)
                except Exception:
                    tick_size = 0.01

                min_tp_move = float (tick_size) * float (max (1, min_tp_ticks))
                min_sl_move = float (tick_size) * float (max (1, min_sl_ticks))

                metrics = self._last_metrics_by_symbol.get (sym) or {}
                vol = float (metrics.get ("volatility", 0.0) or 0.0)
                if not bool (np.isfinite (vol)):
                    vol = 0.0
                vol = max (0.0, min (0.25, vol))
                trail_gap = max (float (gap_min), float (vol) * float (gap_mult))

                for trade in trades:
                    order_id = str (getattr (trade, "order_id", "") or "").strip ()
                    if not order_id:
                        results["skipped"] += 1
                        continue

                    trade_status = str(trade.outcome_status).strip().lower()

                    action = str (getattr (trade, "action", "") or "").strip ().lower ()
                    if action not in {"buy", "sell"}:
                        results["skipped"] += 1
                        continue

                    entry = float (getattr (trade, "entry_price", 0.0) or 0.0)
                    if entry <= 0:
                        results["skipped"] += 1
                        continue

                    tp_old = float (getattr (trade, "tp_price", 0.0) or 0.0) if getattr (trade, "tp_price", None) is not None else 0.0
                    sl_old = float (getattr (trade, "sl_price", 0.0) or 0.0) if getattr (trade, "sl_price", None) is not None else 0.0

                    if action == "buy":
                        profit_pct = (last_price - entry) / max (entry, 1e-12)
                    else:
                        profit_pct = (entry - last_price) / max (entry, 1e-12)

                    ml_p = None
                    if ml_enabled:
                        try:
                            ml_p = self.ml_predict_proba (symbol=sym, action=action, metrics=metrics if isinstance (metrics, dict) else {})
                        except Exception:
                            ml_p = None

                    gap_eff = float (trail_gap)
                    if isinstance (ml_p, float) and bool (np.isfinite (ml_p)) and ml_p < 0.5:
                        gap_eff = max (float (gap_min), gap_eff * 0.85)

                    breakeven = entry * (1.0 + (fee_rate * 2.0)) if action == "buy" else entry * (1.0 - (fee_rate * 2.0))

                    tp_new = tp_old
                    sl_new = sl_old

                    if action == "buy":
                        sl_candidate = last_price * (1.0 - gap_eff)
                        if profit_pct > 0:
                            sl_candidate = max (sl_candidate, breakeven)
                        if sl_new <= 0:
                            sl_new = sl_candidate
                        else:
                            sl_new = max (sl_new, sl_candidate)

                        if tp_new <= 0:
                            tp_new = last_price * (1.0 + max (gap_eff, 0.001))
                        if tp_old > 0 and last_price >= (tp_old * 0.995):
                            ext = gap_eff * float (tp_ext_mult)
                            if isinstance (ml_p, float) and bool (np.isfinite (ml_p)) and ml_p >= ml_th:
                                ext = ext * max (1.0, float (ml_tp_boost))
                            tp_new = max (tp_new, last_price * (1.0 + max (ext, 0.001)))

                        tp_new = max (tp_new, last_price + float (tick_size))
                        sl_new = min (sl_new, last_price - float (tick_size))
                    else:
                        sl_candidate = last_price * (1.0 + gap_eff)
                        if profit_pct > 0:
                            sl_candidate = min (sl_candidate, breakeven)
                        if sl_new <= 0:
                            sl_new = sl_candidate
                        else:
                            sl_new = min (sl_new, sl_candidate)

                        if tp_new <= 0:
                            tp_new = last_price * (1.0 - max (gap_eff, 0.001))
                        if tp_old > 0 and last_price <= (tp_old * 1.005):
                            ext = gap_eff * float (tp_ext_mult)
                            if isinstance (ml_p, float) and bool (np.isfinite (ml_p)) and ml_p >= ml_th:
                                ext = ext * max (1.0, float (ml_tp_boost))
                            tp_new = min (tp_new, last_price * (1.0 - max (ext, 0.001)))

                        tp_new = min (tp_new, last_price - float (tick_size))
                        sl_new = max (sl_new, last_price + float (tick_size))

                    if not bool (np.isfinite (tp_new)) or not bool (np.isfinite (sl_new)):
                        results["errors"] += 1
                        continue

                    try:
                        tp_new = float (self._quantize_to_step (tp_new, float (tick_size), ROUND_HALF_UP))
                        sl_new = float (self._quantize_to_step (sl_new, float (tick_size), ROUND_HALF_UP))
                    except Exception:
                        pass

                    if action == "buy" and not (sl_new < last_price and tp_new > last_price):
                        results["skipped"] += 1
                        continue
                    if action == "sell" and not (sl_new > last_price and tp_new < last_price):
                        results["skipped"] += 1
                        continue

                    tp_move = abs (tp_new - tp_old) if tp_old > 0 else float ("inf")
                    sl_move = abs (sl_new - sl_old) if sl_old > 0 else float ("inf")
                    should_update_tp = (tp_old <= 0) or (tp_move >= min_tp_move)
                    should_update_sl = (sl_old <= 0) or (sl_move >= min_sl_move)

                    results["checked"] += 1

                    # 1. Update SQLite Virtual TP/SL (Sin usar ByBit amend)
                    if should_update_tp or should_update_sl:
                        if should_update_tp:
                            trade.tp_price = float (tp_new)
                        if should_update_sl:
                            trade.sl_price = float (sl_new)

                        current_raw = getattr (trade, "bybit_raw", None)
                        merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                        merged["auto_tpsl"] = {
                            "ts": now_ts,
                            "timestamp": now_iso,
                            "symbol": sym,
                            "order_id": order_id,
                            "action": action,
                            "last_price": float (last_price),
                            "entry_price": float (entry),
                            "profit_pct": float (profit_pct),
                            "volatility": float (vol),
                            "trail_gap": float (gap_eff),
                            "tp_old": float (tp_old),
                            "sl_old": float (sl_old),
                            "tp_new": float (tp_new),
                            "sl_new": float (sl_new),
                            "ml_p": float (ml_p) if isinstance (ml_p, float) and bool (np.isfinite (ml_p)) else None,
                        }
                        trade.bybit_raw = merged
                        results["db_updated"] += 1
                        changed_any = True
                        actions.append (
                            {
                                "type": "auto_tpsl_amend",
                                "ts": datetime.now (timezone.utc).isoformat (),
                                "symbol": sym,
                                "order_id": order_id,
                                "tp": float (tp_new),
                                "sl": float (sl_new),
                                "ml_p": float (ml_p) if isinstance (ml_p, float) and bool (np.isfinite (ml_p)) else None,
                            }
                        )

                    # 2. Ejecucion de TPSL Virtual Activa
                    if trade_status == "filled":
                        triggered = False
                        reason = ""
                        if action == "buy":
                            if tp_new > 0 and last_price >= tp_new:
                                triggered = True
                                reason = "tp"
                            elif sl_new > 0 and last_price <= sl_new:
                                triggered = True
                                reason = "sl"
                        elif action == "sell":
                            if tp_new > 0 and last_price <= tp_new:
                                triggered = True
                                reason = "tp"
                            elif sl_new > 0 and last_price >= sl_new:
                                triggered = True
                                reason = "sl"

                        if triggered:
                            close_action = "Sell" if action == "buy" else "Buy"
                            logger.info(f"?? Disparo Virtual TPSL [{reason.upper()}]: {sym} {close_action} (Ultimo precio: {last_price})")
                            exec_res = await self._place_order(
                                symbol=sym,
                                action=close_action,
                                quantity=float(trade.quantity),
                                price=float(last_price),
                                tp=0.0,
                                sl=0.0
                            )
                            if exec_res.get("success"):
                                trade.outcome_status = "closed"
                                trade.outcome_timestamp = datetime.now(timezone.utc)
                                trade.exit_price = float(last_price)
                                trade.profit_loss = float(profit_pct)
                                results["executed_virtual"] += 1
                                changed_any = True

            if changed_any:
                try:
                    db.commit ()
                except Exception:
                    try:
                        db.rollback ()
                    except Exception:
                        pass

            return {"success": True, "results": results}

    def start_support_loop (self, interval_s: float = 2.0) -> bool:
        if self._support_task and not self._support_task.done ():
            return False
        self._support_interval_s = float (max (0.25, min (30.0, float (interval_s))))
        self._support_task = asyncio.create_task (self._support_loop ())
        return True

    async def _support_loop (self) -> None:
        while self.running:
            try:
                with SessionLocal () as db:
                    await self._enable_secondary_systems_if_due (db)
                    await self.sync_open_orders (
                        db,
                        timeout_seconds=float (getattr (config, "ORDERS_SYNC_TIMEOUT_S", 30.0) or 30.0),
                        update_after_seconds=float (getattr (config, "ORDERS_SYNC_UPDATE_AFTER_S", 20.0) or 20.0),
                        limit=int (getattr (config, "ORDERS_SYNC_LIMIT", 100) or 100),
                    )
                    if bool (getattr (config, "AUTO_AGENT_ENABLED", False)):
                        await self._agent_tick (db)
                    if self._auto_hft_enabled_effective ():
                        await self._auto_hft_tick (db)
                    if bool (getattr (config, "AUTO_TPSL_ENABLED", False)):
                        await self._auto_tpsl_tick (db)
                    await self._live_metrics_tick (db)
                    await self._metrics_snapshot_tick (db)
            except Exception as e:
                logger.error (f"❌ Error en support loop: {e}")
            await asyncio.sleep (self._support_interval_s)

    async def _enable_secondary_systems_if_due (self, db: Session) -> None:
        now_ts = time.time ()
        if now_ts - float (getattr (self, "_boot_ts", 0.0) or 0.0) < 20.0:
            return
        if float (getattr (self, "_secondary_auto_enabled_ts", 0.0) or 0.0) > 0.0:
            return

        actions = self._agent_events.get ("actions")
        if not isinstance (actions, deque):
            actions = deque (maxlen=250)
            self._agent_events["actions"] = actions

        enabled_any = False
        if not bool (getattr (config, "AUTO_AGENT_ENABLED", False)):
            try:
                setattr (config, "AUTO_AGENT_ENABLED", True)
            except Exception:
                pass
            enabled_any = True
            actions.append (
                {"type": "auto_enable", "ts": datetime.now (timezone.utc).isoformat (), "system": "AUTO_AGENT_ENABLED",
                 "enabled": True})
            try:
                append_results_event (
                    {"type": "auto_enable", "system": "AUTO_AGENT_ENABLED", "enabled": True},
                    log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
                )
            except Exception:
                pass

        self._secondary_auto_enabled_ts = now_ts if enabled_any else 0.0

    async def _fetch_bybit_order_for_trade (
            self,
            client: BybitV5Client,
            symbol: str,
            trade: Trade,
    ) -> Optional[Dict[str, Any]]:
        order_id = str (getattr (trade, "order_id", "") or "").strip ()
        if not order_id:
            return None
        order_link_id = None
        raw = getattr (trade, "bybit_raw", None)
        if isinstance (raw, dict):
            order_link_id = raw.get ("order_link_id") or raw.get ("orderLinkId")
        link = order_link_id if isinstance (order_link_id, str) and order_link_id.strip () else None

        def _first_row (payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            if payload.get ("retCode") != 0:
                return None
            lst = (payload.get ("result", {}) or {}).get ("list", []) or []
            if isinstance (lst, list) and lst and isinstance (lst[0], dict):
                return lst[0]
            return None

        try:
            hist = await client.order_history (
                category="spot", symbol=symbol, order_id=order_id, order_link_id=link, limit=1,
            )
            row = _first_row (hist)
            if row is not None:
                current_raw = getattr (trade, "bybit_raw", None)
                merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                merged["order_history"] = row
                trade.bybit_raw = merged
                return row
        except Exception:
            pass
        try:
            rt = await client.order_realtime (category="spot", symbol=symbol, order_id=order_id)
            row = _first_row (rt)
            if row is not None:
                return row
        except Exception:
            pass
        try:
            ex = await client.execution_list (category="spot", symbol=symbol, order_id=order_id, limit=20)
            if ex.get ("retCode") == 0:
                rows = (ex.get ("result", {}) or {}).get ("list", []) or []
                if isinstance (rows, list) and rows:
                    total_qty = 0.0
                    total_val = 0.0
                    for r in rows:
                        if not isinstance (r, dict):
                            continue
                        try:
                            q = float (r.get ("execQty") or 0.0)
                            p = float (r.get ("execPrice") or 0.0)
                        except Exception:
                            continue
                        if q > 0 and p > 0:
                            total_qty += q
                            total_val += q * p
                    if total_qty > 0:
                        return {
                            "orderId": order_id,
                            "orderStatus": "Filled",
                            "avgPrice": str (total_val / total_qty),
                            "cumExecQty": str (total_qty),
                            "from_execution_list": True,
                        }
        except Exception:
            pass
        return None

    async def sync_open_orders (
            self,
            db: Session,
            symbol: Optional[str] = None,
            timeout_seconds: float = 30.0,
            update_after_seconds: float = 20.0,
            limit: int = 100,
    ) -> Dict[str, Any]:
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            return {"success": True, "results": {"skipped": 1, "mode": "disabled"}}
        client = self._bybit_client ()
        if client is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        now = datetime.now (timezone.utc)
        now_ts = time.time ()
        sync_gap_s = float (max (0.5, float (getattr (config, "ORDERS_SYNC_INTERVAL_S", 5.0) or 5.0)))
        if now_ts - float (self._last_orders_sync_ts or 0.0) < sync_gap_s:
            return {"success": True, "results": {"skipped": 1, "reason": "sync_interval"}}

        async with self._orders_sync_lock:
            if now_ts - float (self._last_orders_sync_ts or 0.0) < sync_gap_s:
                return {"success": True, "results": {"skipped": 1, "reason": "sync_interval"}}
            self._last_orders_sync_ts = now_ts

            symbols = [symbol] if symbol else list (self.symbols)
            results: Dict[str, Any] = {
                "checked": 0,
                "updated": 0,
                "amended": 0,
                "cancelled": 0,
                "tpsl_cancelled": 0,
                "replaced": 0,
                "imported_orphan": 0,
                "orphan_open": 0,
                "no_action": 0,
                "errors": 0,
            }

            changed = False
            for sym in symbols:
                bybit_open: list[dict] = []
                try:
                    payload = await client.get_open_orders_merged (category="spot", symbol=sym, limit=int (limit))
                    if payload.get ("retCode") == 0:
                        bybit_open = list (((payload.get ("result", {}) or {}).get ("list", []) or []))
                except Exception:
                    bybit_open = []

                open_by_id: Dict[str, Dict[str, Any]] = {}
                for o in bybit_open:
                    if not isinstance (o, dict):
                        continue
                    oid = o.get ("orderId")
                    if isinstance (oid, str) and oid:
                        open_by_id[oid] = o
                        self.order_status[oid] = {
                            "order_id": oid,
                            "symbol": sym,
                            "status": str (o.get ("orderStatus") or "").lower (),
                            "timestamp": now.isoformat (),
                            "raw": o,
                        }

                tpsl_cancel_after_s = float (getattr (config, "TPSL_CANCEL_AFTER_S", 90.0) or 90.0)
                if tpsl_cancel_after_s > 0:
                    for oid, o in list (open_by_id.items ()):
                        if not isinstance (o, dict):
                            continue
                        order_filter = str (o.get ("orderFilter") or "").strip ().lower ()
                        stop_type = str (o.get ("stopOrderType") or "").strip ().lower ()
                        if "tpsl" not in order_filter and "tpsl" not in stop_type:
                            continue
                        status = str (o.get ("orderStatus") or "").strip ().lower ()
                        status_norm = status.replace ("_", "").replace (" ", "")
                        if status_norm in {"filled", "cancelled", "canceled", "rejected", "deactivated", "expired"}:
                            continue
                        created_time = o.get ("createdTime") or o.get ("updatedTime")
                        order_ts = now
                        try:
                            if created_time is not None:
                                order_ts = timestamp_to_datetime (int (created_time))
                        except Exception:
                            order_ts = now
                        age_s = (now - order_ts).total_seconds () if isinstance (order_ts, datetime) else 0.0
                        if age_s < tpsl_cancel_after_s:
                            continue
                        try:
                            cancel_body = {"category": "spot", "symbol": sym, "orderId": oid}
                            cancel_result = await client.cancel_order (cancel_body)
                            if cancel_result.get ("retCode") == 0:
                                results["tpsl_cancelled"] += 1
                                open_by_id.pop (oid, None)
                                self._balance_dirty = True
                                self.order_status[oid] = {
                                    "order_id": oid,
                                    "symbol": sym,
                                    "status": "cancelled",
                                    "timestamp": now.isoformat (),
                                    "raw": cancel_result,
                                }
                            else:
                                results["errors"] += 1
                        except Exception:
                            results["errors"] += 1

                trades = (
                    db.query (Trade)
                    .filter (Trade.symbol == sym)
                    .filter (Trade.order_id.isnot (None))
                    .filter (Trade.order_id != "")
                    .filter (~Trade.outcome_status.in_ (["final", "cancelled"]))
                    .order_by (Trade.timestamp.desc ())
                    .limit (300)
                    .all ()
                )

                tracked_order_ids: set[str] = set ()
                tracked_link_ids: set[str] = set ()
                for trade in trades:
                    order_id = str (getattr (trade, "order_id", "") or "")
                    if not order_id:
                        continue
                    tracked_order_ids.add (order_id)
                    raw = getattr (trade, "bybit_raw", None)
                    if isinstance (raw, dict):
                        link = raw.get ("order_link_id") or raw.get ("orderLinkId")
                        if isinstance (link, str) and link:
                            tracked_link_ids.add (link)

                    order = open_by_id.get (order_id)
                    if order is None:
                        order = await self._fetch_bybit_order_for_trade (client, sym, trade)
                        if order is not None:
                            changed = True

                    if order is None:
                        results["no_action"] += 1
                        continue

                    results["checked"] += 1
                    order_status = str (order.get ("orderStatus") or "").lower ()
                    order_link_id = order.get ("orderLinkId")
                    if isinstance (order_link_id, str) and order_link_id:
                        tracked_link_ids.add (order_link_id)
                    order_link_id_str = order_link_id if isinstance (order_link_id, str) else ""
                    trade_link_id_str = ""
                    trade_raw = getattr (trade, "bybit_raw", None)
                    if isinstance (trade_raw, dict):
                        tl = trade_raw.get ("order_link_id") or trade_raw.get ("orderLinkId")
                        if isinstance (tl, str):
                            trade_link_id_str = tl
                    is_bot_order = bool (
                        order_link_id_str.startswith ("nertzh-") or trade_link_id_str.startswith ("nertzh-"))
                    self.order_status[order_id] = {
                        "order_id": order_id,
                        "symbol": sym,
                        "status": order_status,
                        "timestamp": now.isoformat (),
                        "raw": order,
                    }

                    ts = getattr (trade, "timestamp", None)
                    if isinstance (ts, datetime) and ts.tzinfo is None:
                        ts = ts.replace (tzinfo=timezone.utc)
                    seconds_elapsed = (now - ts).total_seconds () if isinstance (ts, datetime) else 0.0

                    order_filter = str (order.get ("orderFilter") or "").strip ().lower ()
                    order_status_norm = order_status.replace ("_", "").replace (" ", "")
                    is_conditional = bool (order_filter and order_filter != "order")

                    if (
                            is_bot_order
                            and seconds_elapsed >= float (update_after_seconds)
                            and (order_status_norm in {"new", "partiallyfilled"})
                            and not is_conditional
                    ):
                        try:
                            order_type = str (order.get ("orderType") or "")
                            if order_type.lower () == "limit":
                                side = str (order.get ("side") or "").lower ()
                                book = self.orderbook_data.get (sym, {"bids": [], "asks": []})
                                best_bid = float (book.get ("bids", [])[0][0]) if book.get ("bids") else 0.0
                                best_ask = float (book.get ("asks", [])[0][0]) if book.get ("asks") else 0.0
                                target_price = best_bid if side == "buy" else best_ask
                                if target_price > 0:
                                    rules = await self._get_instrument_rules (sym)
                                    tick_size = float (rules.get ("tick_size") or 0.01)
                                    target_price = float (
                                        self._quantize_to_step (target_price, tick_size, ROUND_HALF_UP))
                                    amend_body = {
                                        "category": "spot",
                                        "symbol": sym,
                                        "orderId": order_id,
                                        "price": self._format_decimal (self._d (target_price)),
                                    }
                                    try:
                                        tp_target = getattr (trade, "tp_price", None)
                                        sl_target = getattr (trade, "sl_price", None)
                                        if tp_target is not None:
                                            amend_body["takeProfit"] = self._format_decimal (
                                                self._d (float (tp_target)))
                                        if sl_target is not None:
                                            amend_body["stopLoss"] = self._format_decimal (self._d (float (sl_target)))
                                    except Exception:
                                        pass
                                    amend_res = await client.amend_order (amend_body)
                                    if amend_res.get ("retCode") == 0:
                                        current_raw = getattr (trade, "bybit_raw", None)
                                        merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                                        merged["amend"] = amend_res
                                        trade.bybit_raw = merged
                                        results["amended"] += 1
                                        changed = True
                                        continue
                        except Exception:
                            pass

                    if is_bot_order and seconds_elapsed >= float (timeout_seconds):
                        if order_status_norm in {"filled", "cancelled", "canceled", "rejected", "deactivated",
                                                 "expired"}:
                            if await self._update_trade_from_bybit (trade, order):
                                results["updated"] += 1
                                changed = True
                                self._balance_dirty = True
                            continue

                        try:
                            cancel_body = {"category": "spot", "symbol": sym, "orderId": order_id}
                            cancel_result = await client.cancel_order (cancel_body)
                            if cancel_result.get ("retCode") == 0:
                                current_raw = getattr (trade, "bybit_raw", None)
                                merged: Dict[str, Any] = dict (current_raw) if isinstance (current_raw, dict) else {}
                                merged["cancel"] = cancel_result
                                merged["order_realtime"] = order
                                trade.bybit_raw = merged
                                trade.outcome_status = "cancelled"
                                trade.outcome_timestamp = now
                                trade.exit_price = 0.0
                                trade.profit_loss = 0.0
                                self.order_status[order_id] = {
                                    "order_id": order_id,
                                    "symbol": sym,
                                    "status": "cancelled",
                                    "timestamp": now.isoformat (),
                                    "raw": cancel_result,
                                }
                                results["cancelled"] += 1
                                changed = True
                                self._balance_dirty = True
                            else:
                                results["errors"] += 1
                        except Exception:
                            results["errors"] += 1
                        continue

                    if (
                            is_bot_order
                            and seconds_elapsed >= float (update_after_seconds)
                            and (order_status_norm in {"new", "partiallyfilled"})
                            and not is_conditional
                    ):
                        rep_res = await self._replace_order_with_market (sym, order_id, trade, bybit_order=order)
                        if rep_res.get ("success"):
                            results["replaced"] += 1
                            changed = True
                            self._balance_dirty = True
                        else:
                            results["errors"] += 1
                        continue

                    if await self._update_trade_from_bybit (trade, order):
                        results["updated"] += 1
                        changed = True
                        self._balance_dirty = True

                for oid, orphan in open_by_id.items ():
                    link = orphan.get ("orderLinkId")
                    link_str = str (link) if isinstance (link, str) and link else ""
                    if oid in tracked_order_ids or (link_str and link_str in tracked_link_ids):
                        continue
                    results["orphan_open"] += 1
                    try:
                        if not link_str.startswith ("nertzh-"):
                            continue
                        exists = (
                            db.query (Trade)
                            .filter (Trade.symbol == sym)
                            .filter (Trade.order_id == str (oid))
                            .first ()
                        )
                        if exists is not None:
                            continue

                        side = str (orphan.get ("side") or "").strip ().lower ()
                        action = "buy" if side == "buy" else ("sell" if side == "sell" else "")
                        if not action:
                            continue

                        created_time = orphan.get ("createdTime")
                        ts = now
                        try:
                            if created_time is not None:
                                ts = timestamp_to_datetime (int (created_time))
                        except Exception:
                            ts = now

                        entry_price = 0.0
                        try:
                            entry_price = float (orphan.get ("price") or 0.0)
                        except Exception:
                            entry_price = 0.0
                        if entry_price <= 0:
                            try:
                                entry_price = float (orphan.get ("avgPrice") or 0.0)
                            except Exception:
                                entry_price = 0.0

                        quantity = 0.0
                        try:
                            quantity = float (orphan.get ("qty") or 0.0)
                        except Exception:
                            quantity = 0.0
                        if quantity <= 0:
                            try:
                                quantity = float (orphan.get ("leavesQty") or 0.0)
                            except Exception:
                                quantity = 0.0

                        last_trade = db.query (Trade.trade_id).order_by (Trade.trade_id.desc ()).first ()
                        next_id = (int (last_trade[0]) + 1) if last_trade else 1
                        if int (self.trade_id_counter) < int (next_id):
                            self.trade_id_counter = int (next_id)
                        trade_id = int (self.trade_id_counter)
                        self.trade_id_counter = int (trade_id) + 1

                        tp_price = None
                        sl_price = None
                        try:
                            tp_val = orphan.get ("takeProfit")
                            if tp_val is not None and str (tp_val).strip ():
                                tp_price = float (tp_val)
                        except Exception:
                            tp_price = None
                        try:
                            sl_val = orphan.get ("stopLoss")
                            if sl_val is not None and str (sl_val).strip ():
                                sl_price = float (sl_val)
                        except Exception:
                            sl_price = None

                        raw_payload: Dict[str, Any] = {
                            "order_realtime": orphan,
                            "order_link_id": link_str,
                            "imported_orphan": True,
                            "imported_at": now.isoformat (),
                        }

                        trade = Trade (
                            trade_id=trade_id,
                            timestamp=ts,
                            symbol=sym,
                            action=action,
                            order_id=str (oid),
                            bybit_raw=raw_payload,
                            entry_price=float (entry_price or 0.0),
                            exit_price=0.0,
                            tp_price=tp_price,
                            sl_price=sl_price,
                            quantity=float (quantity or 0.0),
                            profit_loss=0.0,
                            outcome_status="pending",
                            decision=action,
                            combined=0.0,
                            ild=0.0,
                            egm=0.0,
                            rol=0.0,
                            pio=0.0,
                            ogm=0.0,
                            risk_reward_ratio=float (config.TP_PERCENTAGE) / float (config.SL_PERCENTAGE)
                            if float (getattr (config, "SL_PERCENTAGE", 0.0) or 0.0) > 0
                            else 0.0,
                        )
                        db.add (trade)
                        try:
                            await self._update_trade_from_bybit (trade, orphan)
                        except Exception:
                            pass
                        self.order_status[str (oid)] = {
                            "order_id": str (oid),
                            "trade_id": int (trade_id),
                            "symbol": sym,
                            "status": str (orphan.get ("orderStatus") or "pending").lower (),
                            "timestamp": now.isoformat (),
                            "raw": orphan,
                        }
                        results["imported_orphan"] += 1
                        changed = True
                        self._balance_dirty = True
                    except Exception:
                        results["errors"] += 1

            if changed:
                db.commit ()
                try:
                    self._refresh_trades_cache ()
                    for sym in symbols:
                        await self._save_results (sym, None)
                except Exception as e:
                    logger.warning (f"⚠️ Post-sync refresh falló: {e}")

            self._last_orders_sync_results = {
                "ts": now.isoformat (),
                "results": dict (results),
                "changed": bool (changed),
            }
            return {"success": True, "results": results}

    async def _update_trade_from_bybit (self, trade: Trade, bybit_order: Dict[str, Any]) -> bool:
        try:
            order_status_raw = str (bybit_order.get ("orderStatus") or "").strip ().lower ()
            order_status_norm = order_status_raw.replace ("_", "").replace (" ", "")
            avg_price = float (bybit_order.get ("avgPrice") or 0.0)
            cum_exec_qty = float (bybit_order.get ("cumExecQty") or 0.0)
            cum_fee = float (bybit_order.get ("cumExecFee") or 0.0)
            now = datetime.now (timezone.utc)

            current_raw = getattr (trade, "bybit_raw", None)
            if isinstance (current_raw, dict):
                merged = dict (current_raw)
                merged["order_realtime"] = bybit_order
                trade.bybit_raw = merged
            else:
                trade.bybit_raw = {"order_realtime": bybit_order}

            prev_status = str (getattr (trade, "outcome_status", "") or "")
            prev_entry = float (getattr (trade, "entry_price", 0.0) or 0.0)
            prev_qty = float (getattr (trade, "quantity", 0.0) or 0.0)
            if order_status_norm in {"new", "created", "active", "untriggered", "triggered"}:
                trade.outcome_status = "pending"
            elif order_status_norm in {"partiallyfilled"}:
                trade.outcome_status = "partial"
                if avg_price > 0:
                    trade.entry_price = float (avg_price)
                if cum_exec_qty > 0:
                    trade.exit_price = 0.0
                    trade.profit_loss = float (-cum_fee) if cum_fee > 0 else 0.0
            elif order_status_norm in {"filled"}:
                trade.outcome_status = "filled"
                trade.outcome_timestamp = now
                if avg_price > 0:
                    trade.entry_price = float (avg_price)
                if cum_exec_qty > 0:
                    trade.quantity = float (cum_exec_qty)
                trade.exit_price = 0.0
                trade.profit_loss = float (-cum_fee) if cum_fee > 0 else 0.0
            elif order_status_norm in {"cancelled", "canceled", "rejected", "deactivated", "expired"}:
                trade.outcome_status = "cancelled"
                trade.outcome_timestamp = now
                trade.exit_price = 0.0
                trade.profit_loss = 0.0
            else:
                trade.outcome_status = order_status_raw or prev_status or "pending"

            return (
                    prev_status != str (trade.outcome_status or "")
                    or prev_entry != float (getattr (trade, "entry_price", 0.0) or 0.0)
                    or prev_qty != float (getattr (trade, "quantity", 0.0) or 0.0)
            )
        except Exception as e:
            logger.error (f"❌ Error actualizando trade {getattr (trade, 'trade_id', None)}: {e}")
            return False

    async def _replace_order_with_market (
            self,
            symbol: str,
            order_id: str,
            trade: Trade,
            bybit_order: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
                return {"success": False, "message": "LIVE_TRADING_ENABLED deshabilitado"}

            client = self._bybit_client ()
            if client is None:
                return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

            cancel_body = {"category": "spot", "symbol": symbol, "orderId": order_id}
            cancel_result = await client.cancel_order (cancel_body)
            if cancel_result.get ("retCode") != 0:
                return {"success": False, "message": cancel_result.get ("retMsg") or "cancel_failed",
                        "raw": cancel_result}
            self._balance_dirty = True

            executed = 0.0
            if isinstance (bybit_order, dict):
                try:
                    executed = float (bybit_order.get ("cumExecQty") or 0.0)
                except Exception:
                    executed = 0.0
            if executed <= 0 and isinstance (getattr (trade, "bybit_raw", None), dict):
                try:
                    rt = (trade.bybit_raw or {}).get ("order_realtime") or {}
                    executed = float ((rt or {}).get ("cumExecQty") or 0.0)
                except Exception:
                    executed = 0.0

            rules = await self._get_instrument_rules (symbol)
            qty_step = float (rules.get ("qty_step") or float (config.MIN_TRADE_SIZE))
            min_qty = float (rules.get ("min_qty") or float (config.MIN_TRADE_SIZE))
            min_notional = float (rules.get ("min_notional") or 0.0)
            original_qty = float (trade.quantity or 0.0)
            remaining = max (0.0, original_qty - float (executed or 0.0)) if original_qty > 0 else 0.0
            if remaining <= 0:
                current_raw = getattr (trade, "bybit_raw", None)
                merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                merged["replace"] = {"cancel": cancel_result, "create": None}
                trade.bybit_raw = merged
                return {"success": True, "old_order_id": order_id, "new_order_id": "", "raw": merged.get ("replace")}

            qty_dec = self._quantize_to_step (float (remaining), qty_step, ROUND_DOWN)
            min_qty_dec = self._quantize_to_step (min_qty, qty_step, ROUND_UP)
            if qty_dec < min_qty_dec:
                current_raw = getattr (trade, "bybit_raw", None)
                merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                merged["replace"] = {"cancel": cancel_result, "create": None, "skipped": "remaining_below_min_qty"}
                trade.bybit_raw = merged
                return {"success": True, "old_order_id": order_id, "new_order_id": "", "raw": merged.get ("replace")}

            approx_price = float (getattr (trade, "entry_price", 0.0) or 0.0)
            if min_notional > 0 and approx_price > 0 and (float (qty_dec) * approx_price) < min_notional:
                current_raw = getattr (trade, "bybit_raw", None)
                merged = dict (current_raw) if isinstance (current_raw, dict) else {}
                merged["replace"] = {"cancel": cancel_result, "create": None, "skipped": "remaining_below_min_notional"}
                trade.bybit_raw = merged
                return {"success": True, "old_order_id": order_id, "new_order_id": "", "raw": merged.get ("replace")}

            qty_str = self._format_decimal (qty_dec)
            side = "Buy" if str (getattr (trade, "action", "")).lower () == "buy" else "Sell"
            order_link_id = f"nertzh-{uuid.uuid4 ().hex[:20]}"
            create_body = {
                "category": "spot",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": qty_str,
                "timeInForce": "IOC",
                "marketUnit": "baseCoin",
                "orderLinkId": order_link_id,
            }
            try:
                tick_size = float (rules.get ("tick_size") or 0.01)
            except Exception:
                tick_size = 0.01
            # Spot Market: sin TP/SL nativo — virtual TPSL vía AUTO_TPSL/outcomes
            create_result = await client.create_order (create_body)
            if create_result.get ("retCode") != 0:
                return {"success": False, "message": create_result.get ("retMsg") or "create_failed",
                        "raw": create_result}
            self._balance_dirty = True

            new_order_id = str (((create_result.get ("result") or {}).get ("orderId")) or "")
            now = datetime.now (timezone.utc)

            current_raw = getattr (trade, "bybit_raw", None)
            merged: Dict[str, Any] = dict (current_raw) if isinstance (current_raw, dict) else {}
            merged["replace"] = {"cancel": cancel_result, "create": create_result}
            merged["order_link_id"] = order_link_id
            trade.bybit_raw = merged
            trade.order_id = new_order_id or trade.order_id
            trade.outcome_status = "pending"
            trade.outcome_timestamp = None

            if new_order_id:
                self.order_status[new_order_id] = {
                    "order_id": new_order_id,
                    "symbol": symbol,
                    "status": "pending",
                    "timestamp": now.isoformat (),
                    "raw": create_result,
                }

            return {"success": True, "old_order_id": order_id, "new_order_id": new_order_id,
                    "raw": merged.get ("replace")}
        except Exception as e:
            logger.error (f"❌ Error reemplazando {order_id}: {e}")
            return {"success": False, "message": str (e)}

    def _bybit_client (self) -> Optional[BybitV5Client]:
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            return None
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return None
        if self._bybit is not None:
            return self._bybit
        bybit_env = str (getattr (config, "BYBIT_ENV", "") or "").strip ().lower ()
        if bybit_env == "demo":
            base_url = "https://api-demo.bybit.com"
        else:
            base_url = "https://api.bybit.com"
        self._bybit = BybitV5Client (config.BYBIT_API_KEY, config.BYBIT_API_SECRET, base_url=base_url)
        return self._bybit

    async def record_balance (self, account_type: str = "UNIFIED", coin: Optional[str] = "USDT") -> Dict[str, Any]:
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            total_equity = float (self.capital or 0.0)
            available_balance = float (self.capital or 0.0)
            payload = {"mode": "disabled", "coin": coin, "accountType": account_type}
            with SessionLocal () as db:
                snap = BalanceSnapshot (
                    timestamp=datetime.now (timezone.utc),
                    account_type=account_type,
                    coin=coin,
                    total_equity=total_equity,
                    available_balance=available_balance,
                    raw=payload,
                )
                db.add (snap)
                db.commit ()
            log_dir = os.path.join (os.path.dirname (__file__), '..', 'logs')
            bal_body = {
                "account_type": account_type,
                "coin": coin,
                "total_equity": total_equity,
                "available_balance": available_balance,
                "mode": payload.get ("mode"),
            }
            append_results_event ({"type": "balance", **bal_body}, log_dir=log_dir)
            update_last_balance (bal_body, log_dir=log_dir)
            return {"success": True, "balance": {"total_equity": total_equity, "available_balance": available_balance},
                    "raw": payload}
        client = self._bybit_client ()
        if client is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        resolved_account_type = account_type
        payload: Dict[str, Any] = {}
        parsed: Dict[str, Any] = {"valid": False}

        attempts = [
            ("UNIFIED", coin),
            ("UNIFIED", None),
            ("SPOT", coin),
            ("SPOT", None),
        ]
        for attempt_account_type, attempt_coin in attempts:
            payload = await client.wallet_balance (account_type=attempt_account_type, coin=attempt_coin)
            parsed = _parse_wallet_balance_payload (payload, coin=coin)
            if parsed.get ("valid"):
                resolved_account_type = attempt_account_type
                break

        if not parsed.get ("valid"):
            return {
                "success": False,
                "message": parsed.get ("ret_msg") or "wallet_balance_invalid",
                "raw": payload,
            }

        total_equity = float (parsed.get ("total_equity") or 0.0)
        available_balance = float (parsed.get ("available_balance") or 0.0)

        with SessionLocal () as db:
            snap = BalanceSnapshot (
                timestamp=datetime.now (timezone.utc),
                account_type=resolved_account_type,
                coin=coin,
                total_equity=total_equity,
                available_balance=available_balance,
                raw=payload,
            )
            db.add (snap)
            db.commit ()

        log_dir = os.path.join (os.path.dirname (__file__), '..', 'logs')
        bal_body = {
            "account_type": resolved_account_type,
            "coin": coin,
            "total_equity": total_equity,
            "available_balance": available_balance,
            "http_status": payload.get ("http_status"),
            "retCode": payload.get ("retCode"),
            "retMsg": payload.get ("retMsg"),
        }
        append_results_event ({"type": "balance", **bal_body}, log_dir=log_dir)
        update_last_balance (bal_body, log_dir=log_dir)

        return {"success": True, "balance": {"total_equity": total_equity, "available_balance": available_balance},
                "raw": payload}

    async def _place_order (self, symbol: str, action: str, quantity: float, price: float, tp: float,
                            sl: float) -> Dict:
        if not bool (getattr (config, "LIVE_TRADING_ENABLED", False)):
            return {"success": False, "message": "LIVE_TRADING_ENABLED deshabilitado"}
        max_retries = 3
        for attempt in range (max_retries):
            try:
                client = self._bybit_client ()
                if client is None:
                    logger.error ("❌ Credenciales de API no configuradas. No se puede colocar la orden.")
                    return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

                rules = await self._get_instrument_rules (symbol)
                tick_size = float (rules.get ("tick_size") or 0.01)
                qty_step = float (rules.get ("qty_step") or float (config.MIN_TRADE_SIZE))

                side = "Buy" if action.lower () == "buy" else "Sell"

                qty_str = self._format_decimal (self._quantize_to_step (quantity, qty_step, ROUND_DOWN))
                order_link_id = f"nertzh-{uuid.uuid4 ().hex[:20]}"
                price_str = None
                if (config.ORDER_TYPE or "Limit").lower () != "market":
                    price_str = self._format_decimal (self._quantize_to_step (price, tick_size, ROUND_HALF_UP))
                body_params = self._build_spot_create_body (
                    symbol=symbol,
                    side=side,
                    order_type=config.ORDER_TYPE or "Limit",
                    qty_str=qty_str,
                    order_link_id=order_link_id,
                    time_in_force=config.TIME_IN_FORCE or "GTC",
                    price_str=price_str,
                )
                order_type = body_params.get ("orderType", "Limit")
                result = await client.create_order (body_params)
                http_status = result.get ("http_status")
                ret_code = result.get ("retCode")
                if http_status == 200 and ret_code == 0:
                    order_id = ((result.get ("result") or {}).get ("orderId")) or ""
                    logger.info (
                        f"✅ Orden colocada: {symbol} {side} {quantity:.6f} @ {price if order_type == 'Limit' else 'Market'}, TP={tp:.2f}, SL={sl:.2f}, OrderID={order_id}"
                    )
                    self._balance_dirty = True
                    return {"success": True, "order_id": order_id, "order_link_id": order_link_id, "raw": result}

                if http_status == 429:
                    logger.warning (f"⚠️ Rate limit alcanzado. Reintentando en {2 ** attempt}s...")
                    await asyncio.sleep (2 ** attempt)
                    continue

                if order_type == "Limit" and http_status == 200 and ret_code in {170193, 170194}:
                    msg = str (result.get ("retMsg") or "")
                    nums = []
                    cur = ""
                    for ch in msg:
                        if ch.isdigit () or ch == ".":
                            cur += ch
                        else:
                            if cur:
                                nums.append (cur)
                                cur = ""
                    if cur:
                        nums.append (cur)

                    if nums:
                        try:
                            limit_price = float (nums[-1])
                            if limit_price > 0:
                                current_price = float (body_params.get ("price") or 0.0)
                                if ret_code == 170193:
                                    new_price = min (current_price, limit_price)
                                else:
                                    new_price = max (current_price, limit_price)
                                new_price = float (self._quantize_to_step (new_price, tick_size, ROUND_HALF_UP))
                                price = new_price
                                body_params["price"] = self._format_decimal (self._d (new_price))
                                await asyncio.sleep (0)
                                continue
                        except Exception:
                            pass

                error_msg = result.get ("retMsg", "Error desconocido")
                logger.error (
                    f"❌ Error al colocar orden (HTTP {http_status}): retCode={ret_code}, retMsg={error_msg}"
                )
                return {"success": False, "message": error_msg, "raw": result}
            except Exception as e:
                logger.error (f"❌ Error en intento {attempt + 1}/{max_retries}: {str (e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep (2 ** attempt)
                else:
                    return {"success": False, "message": f"Error tras {max_retries} intentos: {str (e)}"}
        return {"success": False, "message": f"Falló tras {max_retries} intentos"}

    def reset_trades (self):
        self.trades_cache = {symbol: [] for symbol in self.symbols}
        self.trade_id_counter = self._load_initial_trade_id ()
        with SessionLocal () as db:
            db.query (Trade).delete ()
            db.commit ()
        logger.info ("🧹 Trades reseteados")

    async def _save_results (self, symbol, trade_result):
        precision = 6
        with SessionLocal () as db:
            trades_all = (
                db.query (Trade)
                .order_by (Trade.timestamp.asc ())
                .all ()
            )
            latest_balance = _latest_valid_balance (db)

        trades_by_symbol: Dict[str, list[dict]] = {s: [] for s in self.symbols}
        for t in trades_all:
            outcome_status = self._normalize_outcome_status (getattr (t, "outcome_status", None))
            is_final = outcome_status == "final"
            raw = getattr (t, "bybit_raw", None)
            metrics_snapshot = raw.get ("metrics_snapshot") if isinstance (raw, dict) else None
            trades_by_symbol.setdefault (t.symbol, []).append (
                {
                    "trade_id": t.trade_id,
                    "timestamp": t.timestamp.isoformat (),
                    "symbol": t.symbol,
                    "action": t.action,
                    "order_id": getattr (t, "order_id", None),
                    "entry_price": float (t.entry_price),
                    "exit_price": float (t.exit_price) if is_final else None,
                    "tp_price": float (getattr (t, "tp_price", 0.0) or 0.0) if getattr (t, "tp_price",
                                                                                        None) is not None else None,
                    "sl_price": float (getattr (t, "sl_price", 0.0) or 0.0) if getattr (t, "sl_price",
                                                                                        None) is not None else None,
                    "quantity": float (t.quantity),
                    "profit_loss": float (t.profit_loss) if is_final else None,
                    "outcome_status": outcome_status,
                    "outcome_timestamp": t.outcome_timestamp.isoformat () if t.outcome_timestamp else None,
                    "bybit_raw": raw,
                    "metrics_snapshot": metrics_snapshot,
                    "decision": t.decision,
                    "combined": float (t.combined),
                    "ild": float (t.ild),
                    "egm": float (t.egm),
                    "rol": float (t.rol),
                    "pio": float (t.pio),
                    "ogm": float (t.ogm),
                    "risk_reward_ratio": float (t.risk_reward_ratio),
                }
            )

        finalized = [t for t in trades_all if
                     self._normalize_outcome_status (getattr (t, "outcome_status", None)) == "final"]
        open_trades = [
            t for t in trades_all
            if self._normalize_outcome_status (getattr (t, "outcome_status", None)) in {"pending", "partial", "filled"}
        ]
        total_profit = sum ((t.profit_loss or 0.0) for t in finalized if (t.profit_loss or 0.0) > 0)
        total_loss = sum ((t.profit_loss or 0.0) for t in finalized if (t.profit_loss or 0.0) < 0)
        net_profit = total_profit + total_loss
        total_trades = len (finalized)
        wins = sum (1 for t in finalized if (t.profit_loss or 0.0) > 0)
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0
        avg_profit_per_trade = (net_profit / total_trades) if total_trades > 0 else 0.0

        by_symbol: Dict[str, dict] = {}
        for s in self.symbols:
            s_trades = trades_by_symbol.get (s, [])
            s_profit = sum ((x["profit_loss"] or 0.0) for x in s_trades if
                            (x.get ("outcome_status") == "final" and (x["profit_loss"] or 0.0) > 0))
            s_loss = sum ((x["profit_loss"] or 0.0) for x in s_trades if
                          (x.get ("outcome_status") == "final" and (x["profit_loss"] or 0.0) < 0))
            by_symbol[s] = {
                "profit": round (s_profit, precision),
                "loss": round (s_loss, precision),
                "net_profit": round (s_profit + s_loss, precision),
                "trade_count": sum (1 for x in s_trades if x.get ("outcome_status") == "final"),
            }

        log_dir = os.path.join (os.path.dirname (__file__), '..', 'logs')
        previous = load_results_json (log_dir=log_dir)
        prev_meta = previous.get ("metadata") or {}
        prev_initial = prev_meta.get ("capital_inicial")
        prev_source = prev_meta.get ("capital_source")

        capital_source = "simulated"
        capital_actual = float (self.capital)
        balance_meta: Dict[str, Any] = {}
        if latest_balance and (latest_balance.total_equity or 0.0) > 0:
            capital_source = "bybit_wallet_balance"
            capital_actual = float (latest_balance.total_equity)
            balance_meta = {
                "balance_timestamp": latest_balance.timestamp.isoformat (),
                "balance_total_equity": float (latest_balance.total_equity),
                "balance_available_balance": float (latest_balance.available_balance),
                "balance_account_type": latest_balance.account_type,
                "balance_coin": latest_balance.coin,
            }
        last_balance_block = None
        if latest_balance is not None:
            last_balance_block = {
                "timestamp": latest_balance.timestamp.isoformat (),
                "account_type": latest_balance.account_type,
                "coin": latest_balance.coin,
                "total_equity": float (latest_balance.total_equity or 0.0),
                "available_balance": float (latest_balance.available_balance or 0.0),
            }

        capital_inicial = _resolve_capital_inicial (prev_initial, prev_source, capital_source, capital_actual)

        capital_pnl = capital_actual - capital_inicial

        results = {
            "metadata": {
                "timestamp": datetime.now (timezone.utc).isoformat (),
                "capital_inicial": round (capital_inicial, precision),
                "capital_actual": round (capital_actual, precision),
                "capital_final": round (capital_actual, precision),
                "capital_source": capital_source,
                "capital_pnl": round (capital_pnl, precision),
                "total_pnl": round (float (net_profit), precision),
                "total_trades": total_trades,
                "open_trades": len (open_trades),
                "iterations": self.iterations,
                "running": self.running,
                **balance_meta,
            },
            "summary": {
                "total_profit": round (float (total_profit), precision),
                "total_loss": round (float (total_loss), precision),
                "net_profit": round (float (net_profit), precision),
                "win_rate": round (win_rate, 2),
                "avg_profit_per_trade": round (float (avg_profit_per_trade), precision)
            },
            "by_symbol": by_symbol,
            "trades": trades_by_symbol,
        }
        if trade_result:
            results["metadata"]["last_trade_timestamp"] = trade_result.timestamp.isoformat ()
            outcome_status = self._normalize_outcome_status (getattr (trade_result, "outcome_status", None))
            is_final = outcome_status == "final"
            last_raw = getattr (trade_result, "bybit_raw", None)
            last_metrics_snapshot = last_raw.get ("metrics_snapshot") if isinstance (last_raw, dict) else None
            results["last_trade"] = {
                "trade_id": trade_result.trade_id,
                "timestamp": trade_result.timestamp.isoformat (),
                "symbol": trade_result.symbol,
                "action": trade_result.action,
                "order_id": getattr (trade_result, "order_id", None),
                "entry_price": trade_result.entry_price,
                "exit_price": trade_result.exit_price if is_final else None,
                "tp_price": getattr (trade_result, "tp_price", None),
                "sl_price": getattr (trade_result, "sl_price", None),
                "quantity": trade_result.quantity,
                "profit_loss": trade_result.profit_loss if is_final else None,
                "outcome_status": outcome_status,
                "outcome_timestamp": trade_result.outcome_timestamp.isoformat () if trade_result.outcome_timestamp else None,
                "bybit_raw": last_raw,
                "metrics_snapshot": last_metrics_snapshot,
                "decision": trade_result.decision,
                "combined": trade_result.combined,
                "ild": trade_result.ild,
                "egm": trade_result.egm,
                "rol": trade_result.rol,
                "pio": trade_result.pio,
                "ogm": trade_result.ogm,
                "risk_reward_ratio": trade_result.risk_reward_ratio
            }

        prev_events = previous.get ("events")
        if isinstance (prev_events, list) and prev_events:
            results["events"] = prev_events
        if last_balance_block:
            results["last_balance"] = last_balance_block
        save_results (results, log_dir=log_dir)
        maybe_auto_git_commit ("results_snapshot")
        logger.info (
            f"📊 Resultados guardados: Total PNL={round (float (net_profit), precision)} USDT, Capital={round (float (capital_actual), precision)} USDT")

    async def start_storage (self) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.start ()
            logger.info (
                f"✅ Storage DuckDB activo: {getattr (self._storage, 'path', getattr (config, 'STORAGE_PATH', ''))}"
            )
        except Exception as e:
            logger.error (f"❌ Storage DuckDB no pudo iniciar, fallback SQLite legacy: {e}")
            err = str (e)
            if "being utilized by another process" in err or "already open" in err.lower ():
                logger.error (f"🔒 {_duckdb_lock_hint (e)}")
            self._storage = None

    async def stop_storage (self) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.flush ()
            await self._storage.stop ()
        except Exception as e:
            logger.warning (f"⚠️ Error cerrando storage DuckDB: {e}")
        finally:
            self._storage = None

    def stop (self):
        self.running = False
        if self.ws:
            asyncio.create_task (self.ws.close ())
        client = getattr (self, "_bybit", None)
        if client is not None:
            try:
                asyncio.create_task (client.aclose ())
            except Exception:
                pass
        self._bybit = None
        task = self._start_task
        if task and not task.done ():
            task.cancel ()
        self._start_task = None
        support = getattr (self, "_support_task", None)
        if support is not None and not support.done ():
            try:
                support.cancel ()
            except Exception:
                pass
        self._support_task = None
        logger.info ("🛑 Bot detenido.")


# FastAPI
bot = NertzMetalEngine ()


@asynccontextmanager
async def lifespan (_: FastAPI):
    try:
        preflight = await bot.preflight ()
    except Exception as e:
        preflight = {"success": False, "message": str (e)}

    if preflight.get ("success"):
        await bot.save_results (symbol=(bot.symbols[0] if bot.symbols else "BTCUSDT"), trade_result=None)
        await bot.start_storage ()
        if bool (getattr (bot, "start_on_boot", True)):
            bot.schedule_start ()
            bot.start_support_loop (interval_s=bot.support_interval_s)
    else:
        logger.error (f"❌ Preflight falló en startup: {preflight.get ('message') or 'error'}")
    try:
        yield
    finally:
        await bot.stop_storage ()
        bot.stop ()


app = FastAPI (lifespan=lifespan)


@app.get ("/settings")
async def get_settings ():
    settings = {}
    with SessionLocal () as db:
        for symbol in bot.symbols:
            settings[symbol] = {
                "symbol": symbol,
                "capital": bot.capital,
                "risk_factor": config.RISK_FACTOR,
                "min_trade_size": config.MIN_TRADE_SIZE,
                "max_trade_size": config.MAX_TRADE_SIZE,
                "metrics": await get_metrics (symbol, db),
            }
    return settings


@app.get ("/ml/dataset/trades")
async def ml_dataset_trades (
        symbol: Optional[str] = None,
        limit: int = Query (default=5000, ge=1, le=200000),
        include_pending: bool = False,
        output: str = Query (default="json", pattern="^(json|csv)$"),
        db: Session = Depends (get_db),
):
    q = db.query (Trade)
    if isinstance (symbol, str) and symbol:
        q = q.filter (Trade.symbol == symbol)
    if not bool (include_pending):
        q = q.filter (Trade.outcome_status == "final")
    trades = q.order_by (Trade.timestamp.desc ()).limit (int (limit)).all ()

    rows: list[dict] = []
    for t in trades:
        pl = float (getattr (t, "profit_loss", 0.0) or 0.0)
        rows.append (
            {
                "timestamp": t.timestamp.isoformat () if getattr (t, "timestamp", None) else None,
                "symbol": t.symbol,
                "action": t.action,
                "decision": t.decision,
                "order_id": getattr (t, "order_id", None),
                "entry_price": float (t.entry_price or 0.0),
                "exit_price": float (getattr (t, "exit_price", 0.0) or 0.0),
                "tp_price": float (getattr (t, "tp_price", 0.0) or 0.0) if getattr (t, "tp_price",
                                                                                    None) is not None else None,
                "sl_price": float (getattr (t, "sl_price", 0.0) or 0.0) if getattr (t, "sl_price",
                                                                                    None) is not None else None,
                "quantity": float (t.quantity or 0.0),
                "profit_loss": pl,
                "win": 1 if pl > 0 else 0,
                "combined": float (t.combined or 0.0),
                "ild": float (t.ild or 0.0),
                "egm": float (t.egm or 0.0),
                "rol": float (t.rol or 0.0),
                "pio": float (t.pio or 0.0),
                "ogm": float (t.ogm or 0.0),
                "risk_reward_ratio": float (getattr (t, "risk_reward_ratio", 0.0) or 0.0),
                "outcome_status": getattr (t, "outcome_status", None),
                "outcome_timestamp": t.outcome_timestamp.isoformat () if t.outcome_timestamp else None,
            }
        )

    if output == "csv":
        buf = io.StringIO ()
        fieldnames = list (rows[0].keys ()) if rows else [
            "timestamp",
            "symbol",
            "action",
            "decision",
            "order_id",
            "entry_price",
            "exit_price",
            "tp_price",
            "sl_price",
            "quantity",
            "profit_loss",
            "win",
            "combined",
            "ild",
            "egm",
            "rol",
            "pio",
            "ogm",
            "risk_reward_ratio",
            "outcome_status",
            "outcome_timestamp",
        ]
        w = csv.DictWriter (buf, fieldnames=fieldnames)
        w.writeheader ()
        for r in rows:
            w.writerow (r)
        return PlainTextResponse (content=buf.getvalue (), media_type="text/csv")

    return {"count": len (rows), "rows": rows, "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.get ("/ml/status")
async def ml_status ():
    return {
        "enabled": bool (getattr (config, "ML_ENABLED", False)),
        "models": bot.ml_models,
        "auto_agent_enabled": bool (getattr (config, "AUTO_AGENT_ENABLED", False)),
        "auto_agent": {
            "last_tick_ts": bot.agent_last_tick_ts,
            "recent_actions": list (bot.agent_events.get ("actions") or []),
        },
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/ml/train")
async def ml_train (
        symbol: Optional[str] = None,
        min_samples: Optional[int] = Query (default=None, ge=10, le=50000),
        db: Session = Depends (get_db),
):
    return bot.train_ml_model_from_trades (db, symbol=symbol, min_samples=min_samples)


@app.get ("/admin/agent/status")
async def admin_agent_status (db: Session = Depends (get_db)):
    window_min = float (getattr (config, "METRICS_WINDOW_MINUTES", 15.0) or 15.0)
    window_s = max (60.0, window_min * 60.0)
    now_ts = time.time ()
    cutoff_ts = now_ts - float (window_s)
    decisions: list[str] = []
    for sym in bot.symbols:
        q = bot.metrics_window.get (sym)
        if not isinstance (q, deque):
            continue
        for row in reversed (q):
            if not isinstance (row, dict):
                continue
            ts = row.get ("ts")
            if ts is None:
                continue
            try:
                if float (ts) < float (cutoff_ts):
                    break
            except Exception:
                continue
            d = row.get ("decision")
            if isinstance (d, str):
                decisions.append (d.lower ())
            if len (decisions) >= 250:
                break
        if len (decisions) >= 250:
            break
    total = len (decisions)
    hold_count = sum (1 for d in decisions if d == "hold")
    buy_count = sum (1 for d in decisions if d == "buy")
    sell_count = sum (1 for d in decisions if d == "sell")
    hold_ratio = (hold_count / total) if total > 0 else 0.0
    return {
        "enabled": bool (getattr (config, "AUTO_AGENT_ENABLED", False)),
        "last_tick_ts": bot.agent_last_tick_ts,
        "last_relax_ts": bot.agent_last_relax_ts,
        "thresholds": bot.thresholds_payload (),
        "recent_actions": list (bot.agent_events.get ("actions") or []),
        "metrics_window_s": window_s,
        "snapshots_seen": total,
        "hold_count": hold_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "hold_ratio": hold_ratio,
        "note": "Snapshots cada ~55s; trades solo en cierre vela 1m si decision pasa gates de ejecución.",
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/admin/tpsl/status")
async def admin_tpsl_status ():
    return {
        "enabled": bool (getattr (config, "AUTO_TPSL_ENABLED", False)),
        "interval_s": float (getattr (config, "AUTO_TPSL_INTERVAL_S", 3.0) or 3.0),
        "last_tick_ts": float (getattr (bot, "_auto_tpsl_last_tick_ts", 0.0) or 0.0),
        "recent_actions": [a for a in list (bot.agent_events.get ("actions") or [])[-80:] if
                           isinstance (a, dict) and a.get ("type") in {"auto_tpsl_amend"}],
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/admin/tpsl/tick")
async def admin_tpsl_tick (db: Session = Depends (get_db)):
    return await bot._auto_tpsl_tick (db)


@app.post ("/admin/tpsl/enabled")
async def admin_tpsl_enabled (enabled: bool = True):
    try:
        setattr (config, "AUTO_TPSL_ENABLED", bool (enabled))
    except Exception:
        pass
    return {
        "success": True,
        "enabled": bool (getattr (config, "AUTO_TPSL_ENABLED", False)),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/admin/optimize/system")
async def admin_optimize_system (
        symbol: Optional[str] = None,
        limit: int = Query (default=2000, ge=50, le=200000),
        iterations: int = Query (default=900, ge=50, le=50000),
        seed: Optional[int] = None,
        apply: bool = False,
        db: Session = Depends (get_db),
):
    q = db.query (Trade).filter (Trade.outcome_status == "final")
    if isinstance (symbol, str) and symbol:
        q = q.filter (Trade.symbol == symbol)
    trades = q.order_by (Trade.timestamp.desc ()).limit (int (limit)).all ()

    start_th = Thresholds (
        combined_buy_threshold=float (getattr (config, "COMBINED_BUY_THRESHOLD", 8.0) or 8.0),
        combined_sell_threshold=float (getattr (config, "COMBINED_SELL_THRESHOLD", -8.0) or -8.0),
        combined_hold_band=float (getattr (config, "COMBINED_HOLD_BAND", 2.0) or 2.0),
    )

    def _safe_float (x: Any, default: float) -> float:
        try:
            v = float (x)
        except Exception:
            return float (default)
        return float (v) if bool (np.isfinite (v)) else float (default)

    def _weights_from_symbol (sym: str) -> CombinedWeights:
        td = bot.ticker_data.get (sym) if isinstance (sym, str) else None
        cw = td.get ("combined_weights") if isinstance (td, dict) else None
        return CombinedWeights.from_dict (cw if isinstance (cw, dict) else None)

    start_w = _weights_from_symbol (symbol) if isinstance (symbol, str) and symbol else DEFAULT_COMBINED_WEIGHTS
    before = {
        "thresholds": bot.thresholds_payload (),
        "weights": (start_w.as_dict () if isinstance (start_w, CombinedWeights) else None),
    }

    res = optimize_system_from_trades (
        trades,
        start_thresholds=start_th,
        start_weights=start_w,
        iterations=int (iterations),
        seed=seed,
    )

    applied = False
    persisted = None
    if bool (apply) and bool (res.success) and isinstance (res.best, dict):
        best_th = res.best.get ("thresholds")
        if isinstance (best_th, dict):
            try:
                config.COMBINED_BUY_THRESHOLD = float (
                    best_th.get ("combined_buy_threshold") or config.COMBINED_BUY_THRESHOLD)
            except Exception:
                pass
            try:
                config.COMBINED_SELL_THRESHOLD = float (
                    best_th.get ("combined_sell_threshold") or config.COMBINED_SELL_THRESHOLD)
            except Exception:
                pass
            try:
                config.COMBINED_HOLD_BAND = float (
                    best_th.get ("combined_hold_band") or getattr (config, "COMBINED_HOLD_BAND", 2.0))
            except Exception:
                pass

        best_w = res.best.get ("weights")
        if isinstance (best_w, dict):
            if isinstance (symbol, str) and symbol:
                bot.ticker_data.setdefault (symbol, {})["combined_weights"] = dict (best_w)
            else:
                for sym in bot.symbols:
                    bot.ticker_data.setdefault (sym, {})["combined_weights"] = dict (best_w)

        if bool (getattr (config, "PERSIST_THRESHOLDS_TO_ENV", False)):
            env_path = os.path.join (os.path.dirname (__file__), "..", ".env")
            persisted = _persist_thresholds_to_env (env_path)

        applied = True

    return {
        "success": bool (res.success),
        "symbol": symbol,
        "trades_used": len (trades),
        "before": before,
        "result": {"baseline": res.baseline, "best": res.best, "searched": res.searched, "timestamp": res.timestamp},
        "applied": bool (applied),
        "persisted": persisted,
        "after": {"thresholds": bot.thresholds_payload (),
                  "weights": (bot.ticker_data.get (symbol or "") or {}).get ("combined_weights") if isinstance (symbol,
                                                                                                                str) and symbol else None},
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/admin/agent/enable")
async def admin_agent_enable (enabled: bool = True):
    config.AUTO_AGENT_ENABLED = bool (enabled)
    actions = bot.agent_events.get ("actions")
    if isinstance (actions, deque):
        actions.append (
            {"type": "set_auto_agent", "ts": datetime.now (timezone.utc).isoformat (), "enabled": bool (enabled)})
    try:
        append_results_event (
            {"type": "agent_action", "action": "set_auto_agent", "enabled": bool (enabled)},
            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
        )
    except Exception:
        pass
    return {"success": True, "auto_agent_enabled": bool (getattr (config, "AUTO_AGENT_ENABLED", False)),
            "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.post ("/admin/agent/tick")
async def admin_agent_tick (db: Session = Depends (get_db)):
    await bot.agent_tick (db)
    return {"success": True, "last_tick_ts": bot.agent_last_tick_ts,
            "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.post ("/admin/agent/relax_thresholds")
async def admin_agent_relax_thresholds (
        factor: float = Query (default=0.9, gt=0.5, lt=1.0),
):
    before = bot.thresholds_payload ()
    buy_th = float (getattr (config, "COMBINED_BUY_THRESHOLD", 8.0) or 8.0)
    sell_th = float (getattr (config, "COMBINED_SELL_THRESHOLD", -8.0) or -8.0)
    hold_band = float (getattr (config, "COMBINED_HOLD_BAND", 2.0) or 2.0)

    new_buy = max (1.0, min (15.0, buy_th * float (factor)))
    new_sell = -max (1.0, min (15.0, abs (sell_th) * float (factor)))
    new_hold = max (0.5, min (6.0, hold_band * float (factor)))

    config.COMBINED_BUY_THRESHOLD = float (new_buy)
    config.COMBINED_SELL_THRESHOLD = float (new_sell)
    config.COMBINED_HOLD_BAND = float (new_hold)

    actions = bot.agent_events.get ("actions")
    if isinstance (actions, deque):
        actions.append (
            {"type": "manual_relax_thresholds", "ts": datetime.now (timezone.utc).isoformat (),
             "factor": float (factor),
             "before": before, "after": bot.thresholds_payload ()})
    try:
        append_results_event (
            {"type": "agent_action", "action": "manual_relax_thresholds", "factor": float (factor), "before": before,
             "after": bot.thresholds_payload ()},
            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
        )
    except Exception:
        pass

    return {"success": True, "before": before, "after": bot.thresholds_payload (),
            "timestamp": datetime.now (timezone.utc).isoformat ()}


def _resolve_candles (symbol: str, db: Session, *, limit: int = 50, prefer_memory: bool = True):
    """Velas del loop en memoria (verdad del motor) con fallback a DB."""
    lim = max (1, min (200, int (limit)))
    if prefer_memory:
        buf = bot.candles.get (symbol)
        if isinstance (buf, list) and buf:
            return list (buf[:lim])
    return (
        db.query (MarketData)
        .filter (MarketData.symbol == symbol)
        .order_by (MarketData.timestamp.desc ())
        .limit (lim)
        .all ()
    )


@app.get ("/market_data/{symbol}")
async def get_market_data (symbol: str, db: Session = Depends (get_db)):
    buf = bot.candles.get (symbol)
    if isinstance (buf, list) and buf:
        candles = buf[:5]
    else:
        candles = db.query (MarketData).filter (MarketData.symbol == symbol).order_by (
            MarketData.timestamp.desc ()).limit (5).all ()
    return {
        "symbol": symbol,
        "candles": [
            {"timestamp": c.timestamp.isoformat (), "open": c.open, "high": c.high, "low": c.low,
             "close": c.close, "volume": c.volume} for c in candles
        ]
    }


@app.get ("/ticker/{symbol}")
async def get_ticker (symbol: str, db: Session = Depends (get_db)):
    live = bot.ticker_data.get (symbol, {}) or {}
    last_price = float (live.get ("last_price") or 0.0)
    if last_price <= 0:
        ticker = db.query (MarketTicker).filter (MarketTicker.symbol == symbol).order_by (
            MarketTicker.timestamp.desc ()).first ()
        return {
            "symbol": symbol,
            "source": "sqlite" if ticker else "none",
            "last_price": float (ticker.last_price) if ticker else 0.0,
            "volume_24h": float (ticker.volume_24h) if ticker else 0.0,
            "high_24h": float (ticker.high_24h) if ticker else 0.0,
            "low_24h": float (ticker.low_24h) if ticker else 0.0,
            "timestamp": ticker.timestamp.isoformat () if ticker else datetime.now (timezone.utc).isoformat (),
        }
    return {
        "symbol": symbol,
        "source": "websocket_live",
        "last_price": last_price,
        "volume_24h": float (live.get ("volume_24h") or 0.0),
        "high_24h": float (live.get ("high_24h") or 0.0),
        "low_24h": float (live.get ("low_24h") or 0.0),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/metrics/{symbol}")
async def get_metrics (symbol: str, db: Session = Depends (get_db)):
    candles = _resolve_candles (symbol, db, limit=50)
    candle_data = [{"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in
                   candles]
    orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    ticker = bot.ticker_data.get (symbol, {"last_price": 0.0})
    recent_trades_payload = list (bot.recent_trades.get (symbol) or [])[-50:]
    now_ts = time.time ()
    window_min = float (getattr (config, "METRICS_WINDOW_MINUTES", 15.0) or 15.0)
    window_s = max (60.0, window_min * 60.0)
    history_q = bot.metrics_raw_history.setdefault (symbol, deque ())
    cutoff = now_ts - window_s
    while history_q:
        head = history_q[0]
        ts = head.get ("ts") if isinstance (head, dict) else None
        if ts is None or float (ts) >= cutoff:
            break
        history_q.popleft ()

    history_payload = []
    for h in history_q:
        if not isinstance (h, dict):
            continue
        history_payload.append ({k: v for k, v in h.items () if k != "ts"})

    prev_entry = bot.last_weighted_liquidity.get (symbol)
    prev_liq = None
    prev_ts = None
    if isinstance (prev_entry, tuple) and len (prev_entry) == 2:
        prev_liq = prev_entry[0]
        prev_ts = prev_entry[1]

    ticker_payload = dict (ticker)
    ticker_payload["orderbook_lambda"] = float (getattr (config, "ORDERBOOK_LAMBDA", 0.03) or 0.03)
    ticker_payload["orderbook_pct_band"] = float (getattr (config, "ORDERBOOK_PCT_BAND", 0.015) or 0.015)
    ticker_payload["ild_target_move"] = float (getattr (config, "ILD_TARGET_MOVE", 0.002) or 0.002)
    ticker_payload["metric_history"] = history_payload
    ticker_payload["prev_weighted_liquidity"] = prev_liq
    ticker_payload["rol_dt_s"] = (now_ts - float (prev_ts)) if prev_ts else None
    ticker_payload["formulas"] = getattr (config, "FORMULAS", {}) or {}
    ticker_payload["recent_trades"] = recent_trades_payload

    metrics = calculate_metrics (
        candle_data,
        orderbook,
        ticker_payload,
        depth=int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50),
        recent_trades=recent_trades_payload,
    )
    return {
        "symbol": symbol,
        "metrics": metrics,
        "timestamp": datetime.now (timezone.utc).isoformat ()
    }


@app.get ("/combined/{symbol}")
async def get_combined (symbol: str, db: Session = Depends (get_db)):
    candles = _resolve_candles (symbol, db, limit=50)
    live_ob = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    live_ticker = bot.ticker_data.get (symbol, {})
    orderbook_row = (
        db.query (Orderbook)
        .filter (Orderbook.symbol == symbol)
        .order_by (Orderbook.timestamp.desc ())
        .first ()
    )
    ticker_row = (
        db.query (MarketTicker)
        .filter (MarketTicker.symbol == symbol)
        .order_by (MarketTicker.timestamp.desc ())
        .first ()
    )
    recent = list (bot.recent_trades.get (symbol) or [])[-10:]
    return {
        "symbol": symbol,
        "candles": [
            {
                "timestamp": c.timestamp.isoformat () if c.timestamp else None,
                "open": float (c.open),
                "high": float (c.high),
                "low": float (c.low),
                "close": float (c.close),
                "volume": float (c.volume),
            }
            for c in candles
        ],
        "orderbook": {
            "timestamp": orderbook_row.timestamp.isoformat () if orderbook_row else None,
            "bids": (live_ob.get ("bids") or (orderbook_row.bids if orderbook_row else [])),
            "asks": (live_ob.get ("asks") or (orderbook_row.asks if orderbook_row else [])),
        },
        "ticker": {
            "timestamp": ticker_row.timestamp.isoformat () if ticker_row else None,
            "last_price": float (live_ticker.get ("last_price") or 0.0) or (
                float (ticker_row.last_price) if ticker_row else 0.0),
            "volume_24h": float (live_ticker.get ("volume_24h") or 0.0) or (
                float (ticker_row.volume_24h) if ticker_row else 0.0),
            "high_24h": float (live_ticker.get ("high_24h") or 0.0) or (
                float (ticker_row.high_24h) if ticker_row else 0.0),
            "low_24h": float (live_ticker.get ("low_24h") or 0.0) or (
                float (ticker_row.low_24h) if ticker_row else 0.0),
        },
        "recent_trades": recent,
        "metrics": (await get_metrics (symbol, db))["metrics"],
        "decision": bot._decision_detail (symbol, (await get_metrics (symbol, db))["metrics"]),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/ild/{symbol}")
async def get_ild (symbol: str, db: Session = Depends (get_db)):
    candles = (
        db.query (MarketData)
        .filter (MarketData.symbol == symbol)
        .order_by (MarketData.timestamp.desc ())
        .limit (50)
        .all ()
    )
    candle_data = [{"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in
                   candles]
    orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    ticker = bot.ticker_data.get (symbol, {"last_price": 0.0})
    recent = list (bot.recent_trades.get (symbol) or [])
    metrics = calculate_discovery_metrics (candle_data, orderbook, ticker, recent)
    return {
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "ild": float ((await get_metrics (symbol, db))["metrics"].get ("ild") or 0.0),
        "ild_raw": float ((await get_metrics (symbol, db))["metrics"].get ("ild_raw") or 0.0),
        "components": metrics.get ("combined") or {},
    }


@app.get ("/rol/{symbol}")
async def get_rol (symbol: str, db: Session = Depends (get_db)):
    candles = (
        db.query (MarketData)
        .filter (MarketData.symbol == symbol)
        .order_by (MarketData.timestamp.desc ())
        .limit (50)
        .all ()
    )
    candle_data = [{"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in
                   candles]
    orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    ticker = bot.ticker_data.get (symbol, {"last_price": 0.0})
    recent = list (bot.recent_trades.get (symbol) or [])
    metrics = calculate_discovery_metrics (candle_data, orderbook, ticker, recent)
    return {
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "rol": float ((await get_metrics (symbol, db))["metrics"].get ("rol") or 0.0),
        "rol_raw": float ((await get_metrics (symbol, db))["metrics"].get ("rol_raw") or 0.0),
        "components": metrics.get ("combined") or {},
    }


@app.get ("/pio/{symbol}")
async def get_pio (symbol: str, db: Session = Depends (get_db)):
    prod = (await get_metrics (symbol, db))["metrics"]
    return {
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "pio": float (prod.get ("pio") or 0.0),
        "pio_raw": float (prod.get ("pio_raw") or 0.0),
        "components": prod,
    }


@app.get ("/egm/{symbol}")
async def get_egm (symbol: str, db: Session = Depends (get_db)):
    prod = (await get_metrics (symbol, db))["metrics"]
    return {
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "egm": float (prod.get ("egm") or 0.0),
        "egm_raw": float (prod.get ("egm_raw") or 0.0),
        "components": prod,
    }


@app.get ("/ogm/{symbol}")
async def get_ogm (symbol: str, db: Session = Depends (get_db)):
    prod = (await get_metrics (symbol, db))["metrics"]
    return {
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "ogm": float (prod.get ("ogm") or 0.0),
        "ogm_raw": float (prod.get ("ogm_raw") or 0.0),
        "components": prod,
    }


@app.get ("/discovery/metrics/{symbol}")
async def get_discovery_metrics (symbol: str, db: Session = Depends (get_db)):
    candles = (
        db.query (MarketData)
        .filter (MarketData.symbol == symbol)
        .order_by (MarketData.timestamp.desc ())
        .limit (500)
        .all ()
    )
    candle_data = [{"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in
                   candles]
    orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    ticker = bot.ticker_data.get (symbol, {"last_price": 0.0})
    recent = list (bot.recent_trades.get (symbol) or [])
    metrics = calculate_discovery_metrics(candle_data, orderbook, ticker, recent)
    base_metrics = (await get_metrics(symbol, db))["metrics"]

    # FIX HFT #4: ESTRUCTURACIÓN DE TOPOLOGÍA ATÓMICA PARA SOR Y ML
    atomic_components = {
        "egm": {
            "pressure": float(base_metrics.get("orderbook_pressure", 0.0)),
            "flow": float(base_metrics.get("tfi", 0.0)),
            "momentum": float(base_metrics.get("mom_raw", 0.0))
        },
        "pio": {
            "rvol": float(base_metrics.get("rvol", 0.0)),
            "turnover": float(base_metrics.get("turnover_24h", 0.0))
        },
        "microstructure": {
            "spread_bps": float(base_metrics.get("spread_bps", 0.0)),
            "microprice_offset": float(base_metrics.get("microprice_offset_bps", 0.0)),
            "weighted_liquidity": float(base_metrics.get("weighted_liquidity", 0.0))
        },
        "ild_rol_raw": {
            "ild": float(base_metrics.get("ild_raw", 0.0)),
            "rol": float(base_metrics.get("rol_raw", 0.0))
        }
    }

    return {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rol": float(base_metrics.get("rol") or 0.0),
        "rol_raw": float(base_metrics.get("rol_raw") or 0.0),
        "components": atomic_components,  # ✅ Devuelve los bloques de construcción reales
        "legacy_combined": metrics.get("combined") or {}
    }


@app.get ("/profit")
async def get_profit (db: Session = Depends (get_db)):
    precision = 6
    trades_all = db.query (Trade).order_by (Trade.timestamp.asc ()).all ()
    latest_balance = _latest_valid_balance (db)

    finalized = [t for t in trades_all if (getattr (t, "outcome_status", None) == "final")]
    total_profit = sum ((t.profit_loss or 0.0) for t in finalized if (t.profit_loss or 0.0) > 0)
    total_loss = sum ((t.profit_loss or 0.0) for t in finalized if (t.profit_loss or 0.0) < 0)
    total_trades = len (finalized)
    wins = sum (1 for t in finalized if (t.profit_loss or 0.0) > 0)
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0

    log_dir = os.path.join (os.path.dirname (__file__), '..', 'logs')
    previous = load_results_json (log_dir=log_dir)
    prev_meta = previous.get ("metadata") or {}
    prev_initial = prev_meta.get ("capital_inicial")
    prev_source = prev_meta.get ("capital_source")

    capital_source = "simulated"
    capital_actual = float (bot.capital)
    if latest_balance and (latest_balance.total_equity or 0.0) > 0:
        capital_source = "bybit_wallet_balance"
        capital_actual = float (latest_balance.total_equity)

    capital_inicial = _resolve_capital_inicial (prev_initial, prev_source, capital_source, capital_actual)

    capital_pnl = capital_actual - capital_inicial
    return {
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "capital_inicial": round (capital_inicial, precision),
        "capital_actual": round (capital_actual, precision),
        "capital_source": capital_source,
        "capital_pnl": round (capital_pnl, precision),
        "total_pnl": round (float (total_profit + total_loss), precision),
        "total_profit": round (float (total_profit), precision),
        "total_loss": round (float (total_loss), precision),
        "net_profit": round (float (total_profit + total_loss), precision),
        "win_rate": round (win_rate, 2),
        "by_symbol": {
            symbol: {
                "profit": round (
                    float (
                        sum (t.profit_loss for t in trades_all if t.symbol == symbol and (t.profit_loss or 0.0) > 0)),
                    precision),
                "loss": round (
                    float (
                        sum (t.profit_loss for t in trades_all if t.symbol == symbol and (t.profit_loss or 0.0) < 0)),
                    precision),
                "net_profit": round (float (sum (t.profit_loss for t in trades_all if t.symbol == symbol)), precision),
                "trade_count": len ([t for t in trades_all if t.symbol == symbol]),
            } for symbol in bot.symbols
        }
    }


@app.post ("/config/update_thresholds")
async def update_thresholds (egm_buy_threshold: float, egm_sell_threshold: float):
    config.EGM_BUY_THRESHOLD = egm_buy_threshold
    config.EGM_SELL_THRESHOLD = egm_sell_threshold
    logger.info (f"✅ Umbrales actualizados: buy={egm_buy_threshold}, sell={egm_sell_threshold}")
    return {"message": "Umbrales actualizados"}


@app.get ("/orderbook/{symbol}")
async def get_orderbook (symbol: str, db: Session = Depends (get_db)):
    orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
    return {
        "symbol": symbol,
        "bids": orderbook["bids"],
        "asks": orderbook["asks"],
        "timestamp": datetime.now (timezone.utc).isoformat ()
    }


@app.get ("/candles/{symbol}/{limit}")
async def get_candles (symbol: str, limit: int = 5, db: Session = Depends (get_db)):
    candles = _resolve_candles (symbol, db, limit=int (limit))
    return {
        "symbol": symbol,
        "candles": [
            {"timestamp": c.timestamp.isoformat (), "open": c.open, "high": c.high, "low": c.low,
             "close": c.close, "volume": c.volume} for c in candles
        ],
        "timestamp": datetime.now (timezone.utc).isoformat ()
    }


@app.get ("/trades/{symbol}")
async def get_trades (symbol: str, db: Session = Depends (get_db)):
    rows = (
        db.query (Trade)
        .filter (Trade.symbol == symbol)
        .order_by (Trade.timestamp.desc ())
        .all ()
    )
    trades = [bot._serialize_trade_for_api (t) for t in rows]
    bot.trades_cache[symbol] = trades
    return {
        "symbol": symbol,
        "trades": trades,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "source": "sqlite_live",
    }


@app.get ("/last_trade/{symbol}")
async def get_last_trade (symbol: str, db: Session = Depends (get_db)):
    last = db.query (Trade).filter_by (symbol=symbol).order_by (Trade.timestamp.desc ()).first ()
    payload = bot._serialize_trade_for_api (last) if last is not None else None
    return {
        "symbol": symbol,
        "last_trade": payload,
        "timestamp": datetime.now (timezone.utc).isoformat (),
        "source": "sqlite_live",
    }


@app.post ("/execute_trade/{symbol}")
async def execute_trade (symbol: str, collect_only: bool = False, force_trade: bool = False,
                         db: Session = Depends (get_db)):
    if symbol not in bot.symbols:
        return {"message": f"⚠️ Símbolo no soportado: {symbol}", "timestamp": datetime.now (timezone.utc).isoformat ()}
    await bot.core_cycle (symbol, db, collect_only=collect_only, force_trade=force_trade)
    return {
        "message": f"✅ Ciclo ejecutado para {symbol}",
        "collect_only": collect_only,
        "force_trade": force_trade,
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/hft/start/{symbol}")
async def start_hft (symbol: str, interval_ms: int = 250, collect_only: bool = True):
    if symbol not in bot.symbols:
        return {"message": f"⚠️ Símbolo no soportado: {symbol}", "timestamp": datetime.now (timezone.utc).isoformat ()}
    started = bot.start_hft (symbol, interval_ms=max (0, int (interval_ms)), collect_only=bool (collect_only))
    return {
        "message": "✅ HFT iniciado" if started else "⚠️ HFT ya estaba corriendo",
        "symbol": symbol,
        "interval_ms": max (0, int (interval_ms)),
        "collect_only": bool (collect_only),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/hft/stop/{symbol}")
async def stop_hft (symbol: str):
    if symbol not in bot.symbols:
        return {"message": f"⚠️ Símbolo no soportado: {symbol}", "timestamp": datetime.now (timezone.utc).isoformat ()}
    stopped = bot.stop_hft (symbol)
    return {
        "message": "🛑 HFT detenido" if stopped else "⚠️ HFT no estaba corriendo",
        "symbol": symbol,
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/hft/run/{symbol}")
async def run_hft (symbol: str, cycles: int = 100, interval_ms: int = 250, collect_only: bool = True):
    if symbol not in bot.symbols:
        return {"message": f"⚠️ Símbolo no soportado: {symbol}", "timestamp": datetime.now (timezone.utc).isoformat ()}
    asyncio.create_task (bot.run_cycles (symbol, cycles=int (cycles), interval_ms=max (0, int (interval_ms)),
                                         collect_only=bool (collect_only)))
    return {
        "message": "✅ HFT run programado",
        "symbol": symbol,
        "cycles": int (cycles),
        "interval_ms": max (0, int (interval_ms)),
        "collect_only": bool (collect_only),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/balance")
async def get_balance (account_type: str = "UNIFIED", coin: str = "USDT"):
    return await bot.record_balance (account_type=account_type, coin=coin)


@app.get ("/config")
async def get_config ():
    return {
        "symbol": config.SYMBOL,
        "timeframe": config.TIMEFRAME,
        "order_type": config.ORDER_TYPE,
        "time_in_force": config.TIME_IN_FORCE,
        "orderbook_depth": config.ORDERBOOK_DEPTH,
        "bybit_env": getattr (config, "BYBIT_ENV", "mainnet"),
        "live_trading_enabled": bool (getattr (config, "LIVE_TRADING_ENABLED", False)),
        "capital_usdt": config.CAPITAL_USDT,
        "risk_factor": config.RISK_FACTOR,
        "min_trade_size": config.MIN_TRADE_SIZE,
        "max_trade_size": config.MAX_TRADE_SIZE,
        "fee_rate": config.FEE_RATE,
        "tp_percentage": config.TP_PERCENTAGE,
        "sl_percentage": config.SL_PERCENTAGE,
        "egm_buy_threshold": config.EGM_BUY_THRESHOLD,
        "egm_sell_threshold": config.EGM_SELL_THRESHOLD,
        "combined_buy_threshold": float (getattr (config, "COMBINED_BUY_THRESHOLD", 2.0)),
        "combined_sell_threshold": float (getattr (config, "COMBINED_SELL_THRESHOLD", -2.0)),
        "timestamp": datetime.now (timezone.utc).isoformat ()
    }


@app.post ("/config/update_all")
async def update_all_config (config_data: dict):
    if "capital_usdt" in config_data:
        config.CAPITAL_USDT = float (config_data["capital_usdt"]) if float (
            config_data["capital_usdt"]) > 0 else config.CAPITAL_USDT
    if "risk_factor" in config_data:
        config.RISK_FACTOR = max (0.0, min (1.0, float (config_data["risk_factor"])))
    if "egm_buy_threshold" in config_data:
        config.EGM_BUY_THRESHOLD = float (config_data["egm_buy_threshold"])
    if "egm_sell_threshold" in config_data:
        config.EGM_SELL_THRESHOLD = float (config_data["egm_sell_threshold"])
    if "combined_buy_threshold" in config_data:
        config.COMBINED_BUY_THRESHOLD = float (config_data["combined_buy_threshold"])
    if "combined_sell_threshold" in config_data:
        config.COMBINED_SELL_THRESHOLD = float (config_data["combined_sell_threshold"])
    logger.info (f"✅ Configuración actualizada: {config_data}")
    return {"message": "Configuración actualizada", "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.post ("/admin/full_reset")
async def admin_full_reset (
        sample_size: int = 500,
        alpha: float = 1.0,
        cancel_bybit_orders: bool = True,
        db: Session = Depends (get_db),
):
    calibrate = bot.force_calibrate_thresholds (db, sample_size=int (sample_size), alpha=float (alpha))
    env_update = {"success": False, "message": "persist_disabled"}
    if bool (getattr (config, "PERSIST_THRESHOLDS_TO_ENV", False)):
        env_update = _persist_thresholds_to_env (os.path.join (os.path.dirname (__file__), "..", ".env"))

    cancel_result = None
    if bool (cancel_bybit_orders):
        cancel_result = await bot.cancel_all_open_orders (symbol=None, limit=200)

    bot.stop ()
    if bot.support_task is not None and not bot.support_task.done ():
        try:
            bot.support_task.cancel ()
        except Exception:
            logger.warning ("⚠️ No se pudo cancelar support_task durante full_reset.")

    wiped = bot.wipe_database (db)
    bot.reset_runtime_state ()
    results_path = bot.reset_results_json ()

    bot.schedule_start ()
    bot.start_support_loop (
        interval_s=float (getattr (config, "SUPPORT_LOOP_INTERVAL_S", 1.0) or 1.0)
    )

    return {
        "success": True,
        "thresholds": bot.thresholds_payload (),
        "calibration": calibrate,
        "persist_env": env_update,
        "cancel_bybit_orders": cancel_result,
        "wiped": wiped,
        "results_json": results_path,
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/start")
async def start_bot ():
    started_main = bot.schedule_start ()
    started_support = bot.start_support_loop (
        interval_s=float (getattr (config, "SUPPORT_LOOP_INTERVAL_S", 1.0) or 1.0)
    )
    if started_main or started_support:
        return {"message": "✅ Bot iniciado", "timestamp": datetime.now (timezone.utc).isoformat ()}
    return {"message": "⚠️ Bot ya está corriendo", "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.post ("/stop")
async def stop_bot ():
    if bot.running:
        bot.stop ()
        return {"message": "🛑 Bot detenido", "timestamp": datetime.now (timezone.utc).isoformat ()}
    return {"message": "⚠️ Bot ya está detenido", "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.get ("/status")
async def get_status ():
    return {
        "running": bot.running,
        "iterations": bot.iterations,
        "symbols": bot.symbols,
        "support_loop_running": bool (bot.support_task is not None and not bot.support_task.done ()),
        "mode": getattr (bot, "mode", "full"),
        "auto_hft_enabled": bool (getattr (config, "AUTO_HFT_ENABLED", False)) or bool (
            getattr (bot, "auto_hft_enabled", False)),
        "hft": {
            sym: {
                "running": bot.is_hft_running (sym),
                "params": (bot.hft_params.get (sym) or {}),
            }
            for sym in bot.symbols
        },
        "timestamp": datetime.now (timezone.utc).isoformat ()
    }


@app.get ("/mode/status")
async def mode_status ():
    return {
        "mode": getattr (bot, "mode", "full"),
        "auto_hft_enabled": bool (getattr (config, "AUTO_HFT_ENABLED", False)) or bool (
            getattr (bot, "auto_hft_enabled", False)),
        "hft": {
            sym: {
                "running": bot.is_hft_running (sym),
                "params": (bot.hft_params.get (sym) or {}),
                "auto_hft_state": (getattr (bot, "_auto_hft_state", {}).get (sym) or {}),
            }
            for sym in bot.symbols
        },
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/mode/set")
async def mode_set (
        mode: str = Query (pattern="^(normal|full|hft)$"),
        symbol: Optional[str] = None,
        interval_ms: int = Query (default=250, ge=0, le=60000),
        collect_only: bool = True,
):
    m = (mode or "").lower ()
    bot.mode = m
    if not bot.running:
        bot.schedule_start ()
        bot.start_support_loop (interval_s=float (getattr (config, "SUPPORT_LOOP_INTERVAL_S", 1.0) or 1.0))

    if m in {"normal", "full"}:
        stopped = bot.stop_all_hft ()
        return {
            "success": True,
            "mode": bot.mode,
            "stopped_hft": stopped,
            "timestamp": datetime.now (timezone.utc).isoformat (),
        }

    target_symbols = []
    if isinstance (symbol, str) and symbol.strip ():
        target_symbols = [s.strip () for s in symbol.split (",") if s.strip ()]
    else:
        target_symbols = list (bot.symbols)

    started: Dict[str, bool] = {}
    for sym in target_symbols:
        if sym in bot.symbols:
            started[sym] = bot.start_hft (sym, interval_ms=int (interval_ms), collect_only=bool (collect_only))

    return {
        "success": True,
        "mode": bot.mode,
        "started_hft": started,
        "interval_ms": int (interval_ms),
        "collect_only": bool (collect_only),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/admin/auto_hft/enable")
async def admin_auto_hft_enable (enabled: bool = True):
    try:
        setattr (config, "AUTO_HFT_ENABLED", bool (enabled))
    except Exception:
        pass
    bot.auto_hft_enabled = bool (enabled)
    actions = bot.agent_events.get ("actions")
    if isinstance (actions, deque):
        actions.append (
            {"type": "set_auto_hft", "ts": datetime.now (timezone.utc).isoformat (), "enabled": bool (enabled)})
    try:
        append_results_event (
            {"type": "auto_hft", "action": "set_enabled", "enabled": bool (enabled)},
            log_dir=os.path.join (os.path.dirname (__file__), "..", "logs"),
        )
    except Exception:
        pass
    return {"success": True, "auto_hft_enabled": bool (enabled), "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.get ("/admin/auto_hft/status")
async def admin_auto_hft_status ():
    return {
        "enabled": bool (getattr (config, "AUTO_HFT_ENABLED", False)) or bool (
            getattr (bot, "auto_hft_enabled", False)),
        "tick_s": float (getattr (config, "AUTO_HFT_TICK_S", 2.0) or 2.0),
        "window_s": float (getattr (config, "AUTO_HFT_WINDOW_S", 60.0) or 60.0),
        "min_snapshots": int (getattr (config, "AUTO_HFT_MIN_SNAPSHOTS", 30) or 30),
        "start_ratio": float (getattr (config, "AUTO_HFT_START_RATIO", 0.35) or 0.35),
        "stop_ratio": float (getattr (config, "AUTO_HFT_STOP_RATIO", 0.15) or 0.15),
        "combined_abs_threshold": float (getattr (config, "AUTO_HFT_COMBINED_ABS_THRESHOLD", 10.0) or 10.0),
        "interval_ms": int (getattr (config, "AUTO_HFT_INTERVAL_MS", 250) or 250),
        "collect_only": bool (getattr (config, "AUTO_HFT_COLLECT_ONLY", True)),
        "cooldown_s": float (getattr (config, "AUTO_HFT_COOLDOWN_S", 60.0) or 60.0),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/validation")
async def get_validation (db: Session = Depends (get_db)):
    now = datetime.now (timezone.utc)
    now_s = time.time ()

    def _to_utc_aware (dt: Optional[datetime]) -> Optional[datetime]:
        if not isinstance (dt, datetime):
            return None
        if dt.tzinfo is None:
            return dt.replace (tzinfo=timezone.utc)
        try:
            return dt.astimezone (timezone.utc)
        except Exception:
            return dt

    start_task = bot.start_task
    support_task = bot.support_task
    ws = getattr (bot, "ws", None)
    ws_open = bool (ws is not None and not getattr (ws, "closed", False))

    layer1 = {
        "ok": bool (bot.running and (start_task is not None and not start_task.done ()) and (
                support_task is not None and not support_task.done ()) and ws_open),
        "running_flag": bool (bot.running),
        "start_task_running": bool (start_task is not None and not start_task.done ()),
        "support_task_running": bool (support_task is not None and not support_task.done ()),
        "websocket_open": bool (ws_open),
    }

    market: Dict[str, Any] = {}
    market_ok = True
    for sym in bot.symbols:
        last_ob = db.query (Orderbook).filter (Orderbook.symbol == sym).order_by (Orderbook.timestamp.desc ()).first ()
        last_tk = db.query (MarketTicker).filter (MarketTicker.symbol == sym).order_by (
            MarketTicker.timestamp.desc ()).first ()
        last_kl = db.query (MarketData).filter (MarketData.symbol == sym).order_by (
            MarketData.timestamp.desc ()).first ()

        ob_ts = _to_utc_aware (getattr (last_ob, "timestamp", None))
        tk_ts = _to_utc_aware (getattr (last_tk, "timestamp", None))
        kl_ts = _to_utc_aware (getattr (last_kl, "timestamp", None))

        ob_age = (now - ob_ts).total_seconds () if ob_ts else None
        tk_age = (now - tk_ts).total_seconds () if tk_ts else None
        kl_age = (now - kl_ts).total_seconds () if kl_ts else None

        entry = {"orderbook_age_s": ob_age, "ticker_age_s": tk_age, "kline_age_s": kl_age}
        market[sym] = entry

        for age in (ob_age, tk_age):
            if age is None or age > 15.0:
                market_ok = False

    layer2 = {"ok": bool (market_ok), "by_symbol": market}

    db_pending_trades = (
        db.query (Trade)
        .filter (Trade.outcome_status.in_ (["pending", "partial", "filled"]))
        .order_by (Trade.timestamp.desc ())
        .limit (500)
        .all ()
    )
    tracked_order_ids: set[str] = {
        str (t.order_id)
        for t in db_pending_trades
        if isinstance (getattr (t, "order_id", None), str) and str (t.order_id).strip ()
    }
    tracked_link_ids: set[str] = set ()
    for t in db_pending_trades:
        raw = getattr (t, "bybit_raw", None)
        if isinstance (raw, dict):
            link = raw.get ("order_link_id") or raw.get ("orderLinkId")
            if isinstance (link, str) and link.strip ():
                tracked_link_ids.add (link.strip ())

    layer3 = {
        "ok": True,
        "db_pending_trades": len (db_pending_trades),
        "tracked_order_ids": len (tracked_order_ids),
        "tracked_link_ids": len (tracked_link_ids),
        "now": now.isoformat (),
        "now_s": now_s,
    }

    client = bot.bybit_client ()
    bybit_orders: list[dict] = []
    if client:
        merged: Dict[str, Dict[str, Any]] = {}
        for sym in bot.symbols:
            try:
                payload = await client.get_open_orders_merged (category="spot", symbol=sym, limit=200)
                if payload.get ("retCode") != 0:
                    continue
                rows = list (((payload.get ("result", {}) or {}).get ("list", []) or []))
                for row in rows:
                    if not isinstance (row, dict):
                        continue
                    oid = row.get ("orderId")
                    if isinstance (oid, str) and oid:
                        merged[oid] = row
            except Exception:
                continue
        bybit_orders = list (merged.values ())

    bybit_open_ids: set[str] = set ()
    orphan_count = 0
    orphan_bot_candidates = 0
    for o in bybit_orders:
        if not isinstance (o, dict):
            continue
        oid = o.get ("orderId")
        if not isinstance (oid, str) or not oid:
            continue
        bybit_open_ids.add (oid)
        link = o.get ("orderLinkId")
        link_str = str (link) if isinstance (link, str) and link else ""
        if oid not in tracked_order_ids and not (link_str and link_str in tracked_link_ids):
            orphan_count += 1
            if link_str.startswith ("nertzh-"):
                orphan_bot_candidates += 1

    layer4_ok = (orphan_bot_candidates == 0)
    layer4 = {
        "ok": bool (layer4_ok),
        "bybit_open_orders": len (bybit_open_ids),
        "orphan_open_orders": orphan_count,
        "orphan_bot_candidates": orphan_bot_candidates,
        "linked_open_orders": sum (
            1
            for o in bybit_orders
            if isinstance (o, dict)
            and isinstance (o.get ("orderId"), str)
            and (
                    (o.get ("orderId") in tracked_order_ids)
                    or (
                            isinstance (o.get ("orderLinkId"), str)
                            and str (o.get ("orderLinkId")).strip ()
                            and str (o.get ("orderLinkId")).strip () in tracked_link_ids
                    )
            )
        ),
    }

    overall = bool (layer1["ok"] and layer2["ok"] and layer3["ok"] and layer4["ok"])
    return {"ok": overall, "layer1_process": layer1, "layer2_market_data": layer2, "layer3_db": layer3,
            "layer4_orders": layer4, "timestamp": now.isoformat ()}


@app.get ("/orders/status")
async def get_orders_status (db: Session = Depends (get_db)):
    client = bot.bybit_client ()
    bybit_orders: list[dict] = []
    if client:
        merged: Dict[str, Dict[str, Any]] = {}
        for sym in bot.symbols:
            try:
                payload = await client.get_open_orders_merged (category="spot", symbol=sym, limit=200)
                if payload.get ("retCode") != 0:
                    continue
                rows = list (((payload.get ("result", {}) or {}).get ("list", []) or []))
                for row in rows:
                    if not isinstance (row, dict):
                        continue
                    oid = row.get ("orderId")
                    if isinstance (oid, str) and oid:
                        merged[oid] = row
            except Exception:
                continue
        bybit_orders = list (merged.values ())

    pending_trades = (
        db.query (Trade)
        .filter (Trade.outcome_status.in_ (["pending", "partial", "filled"]))
        .order_by (Trade.timestamp.desc ())
        .limit (200)
        .all ()
    )

    tracked_order_ids: set[str] = {
        str (t.order_id)
        for t in pending_trades
        if isinstance (getattr (t, "order_id", None), str) and str (t.order_id).strip ()
    }
    tracked_link_ids: set[str] = set ()
    for t in pending_trades:
        raw = getattr (t, "bybit_raw", None)
        if isinstance (raw, dict):
            link = raw.get ("order_link_id") or raw.get ("orderLinkId")
            if isinstance (link, str) and link.strip ():
                tracked_link_ids.add (link.strip ())

    bybit_open_ids: set[str] = set ()
    bybit_open_link_ids: set[str] = set ()
    bybit_orders_payload: list[dict] = []
    orphan_orders_payload: list[dict] = []
    for o in bybit_orders:
        if not isinstance (o, dict):
            continue
        oid = o.get ("orderId")
        if not isinstance (oid, str) or not oid:
            continue
        bybit_open_ids.add (oid)
        symbol = o.get ("symbol")
        sym = str (symbol) if isinstance (symbol, str) and symbol else ""
        status_raw = o.get ("orderStatus")
        status = str (status_raw) if status_raw is not None else ""
        side_raw = o.get ("side")
        side = str (side_raw) if side_raw is not None else ""
        link_raw = o.get ("orderLinkId")
        link = str (link_raw) if link_raw is not None else ""
        if link.strip ():
            bybit_open_link_ids.add (link.strip ())
        stop_type_raw = o.get ("stopOrderType")
        stop_type = str (stop_type_raw) if stop_type_raw is not None else ""
        tracked_in_db = (oid in tracked_order_ids) or (link.strip () and link.strip () in tracked_link_ids)
        order_payload = {
            "orderId": oid,
            "symbol": sym,
            "status": status,
            "side": side,
            "orderLinkId": link,
            "orderFilter": o.get ("orderFilter"),
            "orderType": o.get ("orderType"),
            "timeInForce": o.get ("timeInForce"),
            "stopOrderType": stop_type,
            "triggerPrice": o.get ("triggerPrice"),
            "takeProfit": o.get ("takeProfit"),
            "stopLoss": o.get ("stopLoss"),
            "qty": o.get ("qty"),
            "price": o.get ("price"),
            "avgPrice": o.get ("avgPrice"),
            "cumExecQty": o.get ("cumExecQty"),
            "createdTime": o.get ("createdTime"),
            "updatedTime": o.get ("updatedTime"),
            "tracked_in_db": tracked_in_db,
        }
        bybit_orders_payload.append (order_payload)
        if not tracked_in_db:
            orphan_orders_payload.append (order_payload)
        if sym:
            bot.order_status[oid] = {
                "order_id": oid,
                "symbol": sym,
                "status": status.lower (),
                "timestamp": datetime.now (timezone.utc).isoformat (),
                "raw": o,
            }

    return {
        "last_sync": getattr (bot, "_last_orders_sync_results", {}) or {},
        "agent_last_tick_ts": float (getattr (bot, "_agent_last_tick_ts", 0.0) or 0.0),
        "auto_agent_enabled": bool (getattr (config, "AUTO_AGENT_ENABLED", False)),
        "bybit_open_orders": len (bybit_orders),
        "db_pending_trades": len (pending_trades),
        "linked_open_orders": sum (1 for row in bybit_orders_payload if bool (row.get ("tracked_in_db"))),
        "orphan_open_orders": len (orphan_orders_payload),
        "bybit_orders": bybit_orders_payload,
        "orphan_bybit_orders": orphan_orders_payload[:50],
        "db_pending": [
            {
                "trade_id": t.trade_id,
                "order_id": t.order_id,
                "symbol": t.symbol,
                "action": t.action,
                "status": t.outcome_status,
                "timestamp": t.timestamp.isoformat (),
                "seconds_elapsed": (datetime.now (timezone.utc) - (t.timestamp.replace (
                    tzinfo=timezone.utc) if t.timestamp.tzinfo is None else t.timestamp)).total_seconds (),
                "present_in_bybit_open_orders": (
                                                    (str (t.order_id) in bybit_open_ids)
                                                    if isinstance (getattr (t, "order_id", None), str)
                                                    else False
                                                )
                                                or (
                                                        isinstance (getattr (t, "bybit_raw", None), dict)
                                                        and isinstance ((t.bybit_raw or {}).get ("order_link_id"), str)
                                                        and str ((t.bybit_raw or {}).get (
                                                    "order_link_id") or "").strip () in bybit_open_link_ids
                                                )
                                                or (
                                                        isinstance (getattr (t, "bybit_raw", None), dict)
                                                        and isinstance ((t.bybit_raw or {}).get ("orderLinkId"), str)
                                                        and str ((t.bybit_raw or {}).get (
                                                    "orderLinkId") or "").strip () in bybit_open_link_ids
                                                ),
            }
            for t in pending_trades
        ],
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.post ("/orders/sync")
async def sync_orders (db: Session = Depends (get_db)):
    try:
        result = await bot.sync_open_orders (
            db,
            timeout_seconds=float (getattr (config, "ORDERS_SYNC_TIMEOUT_S", 30.0) or 30.0),
            update_after_seconds=float (getattr (config, "ORDERS_SYNC_UPDATE_AFTER_S", 20.0) or 20.0),
            limit=int (getattr (config, "ORDERS_SYNC_LIMIT", 100) or 100),
        )
        if result.get ("success"):
            return {
                "success": True,
                "message": "Órdenes sincronizadas correctamente",
                "details": result.get ("results", {}),
                "timestamp": datetime.now (timezone.utc).isoformat (),
            }
        return {
            "success": False,
            "message": result.get ("message", "Error desconocido"),
            "timestamp": datetime.now (timezone.utc).isoformat (),
        }
    except Exception as e:
        logger.error (f"❌ Error en sync_orders: {e}")
        return {
            "success": False,
            "message": f"Error interno: {str (e)}",
            "timestamp": datetime.now (timezone.utc).isoformat (),
        }


@app.get ("/order_status/{order_id}")
async def get_order_status (order_id: str):
    order_status = bot.order_status.get (order_id)
    if order_status:
        return order_status
    return {"message": "Orden no encontrada", "order_id": order_id,
            "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.get ("/health")
async def health_check ():
    return {"status": "healthy" if bot.running else "unhealthy", "timestamp": datetime.now (timezone.utc).isoformat ()}


@app.get ("/storage/status")
async def storage_status ():
    storage = getattr (bot, "_storage", None)
    backend = str (getattr (config, "STORAGE_BACKEND", "") or "")
    storage_path = (
        getattr (storage, "path", None)
        if storage is not None
        else _resolve_storage_path (str (getattr (config, "STORAGE_PATH", "") or ""))
    )
    duckdb_path = _resolve_storage_path ()
    return {
        "backend": backend,
        "active": storage is not None,
        "path": storage_path,
        "duckdb_path": duckdb_path,
        "sqlite_path": os.path.abspath (DATABASE_URL),
        "jsonl_path": os.path.join (DATABASE_DIR, "metrics_snapshots.jsonl"),
        "wal_present": os.path.exists (f"{duckdb_path}.wal"),
        "analysis_jsonl": "data/metrics_snapshots.jsonl — espejo legible con el IDE; DuckDB es HF exclusivo del bot",
        "pycharm_hint": "DuckDB: jdbc:duckdb:path/to/nertz.duckdb?duckdb.read_only=true (NO abrir .wal). Si falla: deten el bot o scripts/release_duckdb_lock.ps1. Trades en data/trading.db (SQLite).",
        "batch_interval_ms": float (getattr (config, "STORAGE_BATCH_INTERVAL_MS", 50.0) or 50.0),
        "orderbook_persist_interval_ms": float (
            getattr (config, "ORDERBOOK_PERSIST_INTERVAL_MS", 200.0) or 200.0
        ),
        "ticker_persist_interval_ms": float (
            getattr (config, "TICKER_PERSIST_INTERVAL_MS", 200.0) or 200.0
        ),
        "jsonl_disabled": bool (getattr (config, "STORAGE_DISABLE_JSONL", False)),
        "sqlite_mirror": bool (getattr (config, "STORAGE_SQLITE_MIRROR", True)),
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/storage/recent/{symbol}")
async def storage_recent (symbol: str, limit: int = 10, db: Session = Depends (get_db)):
    sym = str (symbol or "").strip ().upper ()
    lim = max (1, min (100, int (limit)))
    storage = getattr (bot, "_storage", None)
    payload: Dict[str, Any] = {
        "symbol": sym,
        "limit": lim,
        "duckdb_active": storage is not None,
        "sqlite_mirror": bool (getattr (config, "STORAGE_SQLITE_MIRROR", True)),
    }
    if storage is not None and hasattr (storage, "fetch_recent"):
        try:
            duck = await storage.fetch_recent (sym, limit=lim)
            payload["duckdb"] = duck
        except Exception as e:
            payload["duckdb_error"] = str (e)
    try:
        ob_rows = (
            db.query (Orderbook)
            .filter (Orderbook.symbol == sym)
            .order_by (Orderbook.timestamp.desc ())
            .limit (lim)
            .all ()
        )
        tick_rows = (
            db.query (MarketTicker)
            .filter (MarketTicker.symbol == sym)
            .order_by (MarketTicker.timestamp.desc ())
            .limit (lim)
            .all ()
        )
        met_rows = (
            db.query (MetricSnapshot)
            .filter (MetricSnapshot.symbol == sym)
            .order_by (MetricSnapshot.timestamp.desc ())
            .limit (lim)
            .all ()
        )
        payload["sqlite"] = {
            "orderbook_count": db.query (Orderbook).filter (Orderbook.symbol == sym).count (),
            "ticker_count": db.query (MarketTicker).filter (MarketTicker.symbol == sym).count (),
            "metric_count": db.query (MetricSnapshot).filter (MetricSnapshot.symbol == sym).count (),
            "orderbook": [
                {
                    "timestamp": r.timestamp.isoformat (),
                    "bid_levels": len (r.bids or []),
                    "ask_levels": len (r.asks or []),
                    "best_bid": float ((r.bids or [[0]])[0][0]) if r.bids else None,
                    "best_ask": float ((r.asks or [[0]])[0][0]) if r.asks else None,
                }
                for r in ob_rows
            ],
            "ticks": [
                {
                    "timestamp": r.timestamp.isoformat (),
                    "last_price": float (r.last_price),
                    "volume_24h": float (r.volume_24h),
                }
                for r in tick_rows
            ],
            "metrics": [
                {
                    "timestamp": r.timestamp.isoformat (),
                    "decision": r.decision,
                    "combined": float (r.combined),
                    "last_price": float (r.last_price),
                }
                for r in met_rows
            ],
        }
    except Exception as e:
        payload["sqlite_error"] = str (e)
    payload["live_memory"] = {
        "orderbook_levels": {
            "bids": len ((bot.orderbook_data.get (sym) or {}).get ("bids") or []),
            "asks": len ((bot.orderbook_data.get (sym) or {}).get ("asks") or []),
        },
        "last_price": float ((bot.ticker_data.get (sym) or {}).get ("last_price") or 0.0),
        "candles_in_memory": len (bot.candles.get (sym) or []),
        "trade_cycle": "kline_1m_close_only",
    }
    payload["timestamp"] = datetime.now (timezone.utc).isoformat ()
    return payload


@app.get ("/decisions/{symbol}")
async def get_decisions_audit (symbol: str, db: Session = Depends (get_db)):
    metrics = dict (bot._last_metrics_by_symbol.get (symbol) or {})
    if not metrics:
        candles = _resolve_candles (symbol, db, limit=50)
        candle_data = [{"open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume} for c in candles]
        orderbook = bot.orderbook_data.get (symbol, {"bids": [], "asks": []})
        ticker = bot.ticker_data.get (symbol, {"last_price": 0.0})
        metrics = calculate_metrics (
            candle_data, orderbook, dict (ticker),
            depth=int (getattr (config, "ORDERBOOK_DEPTH", 50) or 50),
            recent_trades=list (bot.recent_trades.get (symbol) or [])[-50:],
        )
    detail = bot._decision_detail (symbol, metrics)
    ctx = bot.operations.get (symbol)
    pending = (
        db.query (Trade)
        .filter (Trade.symbol == symbol)
        .filter (Trade.outcome_status.in_ (["pending", "partial", "filled"]))
        .count ()
    )
    execution_gates = {
        "live_trading_enabled": bool (getattr (config, "LIVE_TRADING_ENABLED", False)),
        "cooldown_s": float (getattr (config, "TRADE_COOLDOWN_S", 0.0) or 0.0),
        "can_trade": bool (ctx.can_trade ()),
        "allow_multiple_active_trades": bool (getattr (config, "ALLOW_MULTIPLE_ACTIVE_TRADES", True)),
        "pending_trades_db": int (pending),
        "trade_cycle": "kline_1m_close_only",
        "ml_enabled": bool (getattr (config, "ML_ENABLED", False)),
    }
    q = bot._metrics_window.get (symbol)
    recent_snapshots = []
    if isinstance (q, deque):
        for row in list (q)[-15:]:
            if isinstance (row, dict):
                recent_snapshots.append (row)
    would_trade = detail.get ("decision") in {"buy", "sell"} and execution_gates["live_trading_enabled"] and execution_gates["can_trade"]
    if not bool (getattr (config, "ALLOW_MULTIPLE_ACTIVE_TRADES", True)) and pending > 0:
        would_trade = False
        execution_gates["blocked_by"] = "active_trade_single_mode"
    elif detail.get ("decision") == "hold":
        execution_gates["blocked_by"] = "decision_hold"
    elif not execution_gates["live_trading_enabled"]:
        execution_gates["blocked_by"] = "live_trading_disabled"
    else:
        execution_gates["blocked_by"] = None
    return {
        "symbol": symbol,
        "decision_detail": detail,
        "execution_gates": execution_gates,
        "would_trade_on_next_kline": bool (would_trade),
        "recent_snapshot_decisions": recent_snapshots,
        "timestamp": datetime.now (timezone.utc).isoformat (),
    }


@app.get ("/operations/status")
async def operations_status ():
    snap = bot.operations.snapshot ()
    for sym in bot.symbols:
        q = bot._metrics_window.get (sym)
        if sym in snap:
            snap[sym]["decisions_window_len"] = len (q) if isinstance (q, deque) else 0
    return snap


@app.get ("/exchange/open_orders/{symbol}")
async def exchange_open_orders (symbol: str, limit: int = 200):
    client = bot.bybit_client ()
    if client is None:
        return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}
    try:
        payload = await client.get_open_orders_merged (category="spot", symbol=symbol, limit=int (limit))
        return {"success": True, "symbol": symbol, "payload": payload,
                "timestamp": datetime.now (timezone.utc).isoformat ()}
    except Exception as e:
        return {"success": False, "symbol": symbol, "message": str (e),
                "timestamp": datetime.now (timezone.utc).isoformat ()}


# Ejecución principal
async def _launcher_loop (server: uvicorn.Server) -> None:
    expected = os.getenv ("NERTZ_LAUNCHER_PASSWORD") or ""
    if expected:
        pw = await asyncio.to_thread (getpass.getpass, "Password: ")
        if pw != expected:
            print ("Login failed.")
            await server.shutdown ()
            return

    while True:
        print ("")
        print ("1) Status")
        print ("2) Start bot")
        print ("3) Stop bot")
        print ("4) Mode normal")
        print ("5) Mode full")
        print ("6) Start HFT (all symbols)")
        print ("7) Stop HFT (all symbols)")
        print ("8) Enable auto-HFT")
        print ("9) Disable auto-HFT")
        print ("0) Exit")
        choice = (await asyncio.to_thread (input, "> ")).strip ()
        if choice == "1":
            hft = {s: {"running": bot.is_hft_running (s), "params": (bot.hft_params.get (s) or {})} for s in
                   bot.symbols}
            print (
                json.dumps (
                    {
                        "running": bot.running,
                        "mode": getattr (bot, "mode", "full"),
                        "support_loop_running": bool (bot.support_task is not None and not bot.support_task.done ()),
                        "auto_hft_enabled": bool (getattr (config, "AUTO_HFT_ENABLED", False)) or bool (
                            getattr (bot, "auto_hft_enabled", False)),
                        "hft": hft,
                    },
                    indent=2,
                )
            )
        elif choice == "2":
            if not bot.running:
                bot.schedule_start ()
            bot.start_support_loop (interval_s=float (getattr (config, "SUPPORT_LOOP_INTERVAL_S", 1.0) or 1.0))
        elif choice == "3":
            bot.stop ()
        elif choice == "4":
            bot.mode = "normal"
            bot.stop_all_hft ()
        elif choice == "5":
            bot.mode = "full"
            bot.stop_all_hft ()
        elif choice == "6":
            bot.mode = "hft"
            for s in bot.symbols:
                bot.start_hft (s, interval_ms=int (getattr (config, "AUTO_HFT_INTERVAL_MS", 250) or 250),
                               collect_only=bool (getattr (config, "AUTO_HFT_COLLECT_ONLY", True)))
        elif choice == "7":
            bot.stop_all_hft ()
        elif choice == "8":
            try:
                setattr (config, "AUTO_HFT_ENABLED", True)
            except Exception:
                pass
            bot.auto_hft_enabled = True
        elif choice == "9":
            try:
                setattr (config, "AUTO_HFT_ENABLED", False)
            except Exception:
                pass
            bot.auto_hft_enabled = False
            bot.stop_all_hft ()
        elif choice == "0":
            await server.shutdown ()
            bot.stop ()
            return


async def main () -> None:
    parser = argparse.ArgumentParser ()
    parser.add_argument ("--host", default="0.0.0.0")
    parser.add_argument ("--port", type=int, default=8081)
    parser.add_argument ("--api-only", action="store_true")
    parser.add_argument ("--launcher", action="store_true")
    parser.add_argument ("--auto-hft", action="store_true")
    args = parser.parse_args ()

    bot.start_on_boot = not bool (args.api_only)
    if bool (args.auto_hft):
        try:
            setattr (config, "AUTO_HFT_ENABLED", True)
        except Exception:
            pass
        bot.auto_hft_enabled = True

    server = uvicorn.Server (uvicorn.Config (app, host=str (args.host), port=int (args.port)))
    try:
        logger.info ("🚀 Iniciando servidor API...")
        if bool (args.launcher):
            serve_task = asyncio.create_task (server.serve ())
            await asyncio.sleep (0.25)
            await _launcher_loop (server)
            if not serve_task.done ():
                serve_task.cancel ()
            return
        await server.serve ()
    except Exception as e:
        logger.error (f"❌ Error crítico en main(): {e}")
        await server.shutdown ()
    except KeyboardInterrupt:
        logger.info ("🛑 Interrupción del usuario detectada.")
        await server.shutdown ()
        bot.stop ()


if __name__ == "__main__":
    asyncio.run (main ())
