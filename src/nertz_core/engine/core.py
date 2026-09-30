"""NertzMetalEngine: estado, ciclo de vida y ciclo de decisión.

El motor se compone de mixins por responsabilidad (mismo proceso, mismo
estado): datos de mercado, órdenes, TP/SL virtual, automatismos y reporting.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy.orm import Session

from bybit_v5 import BybitV5Client
from nertz_core import ml
from nertz_core.db import OPEN_STATUSES, Database, MarketData, Trade
from nertz_core.engine.automation import AutomationMixin
from nertz_core.engine.market_data import MarketDataMixin
from nertz_core.engine.orders import OrdersMixin
from nertz_core.engine.reporting import ReportingMixin
from nertz_core.engine.tpsl import TPSLMixin
from nertz_core.history import MetricHistory, raw_sample_from_metrics
from nertz_core.market import OrderBook, candle_inputs
from nertz_core.runtime import RuntimePaths, default_config, default_database
from nertz_core.sizing import SizingInputs, price_decimals, protective_levels, size_order
from settings import ConfigSettings
from signal_engine import check_execution_gates, evaluate_signal
from utils import calculate_metrics

logger = logging.getLogger("NertzMetalEngine")

try:
    from nertz_engine.engine.symbols import OperationManager
    from nertz_engine.storage import create_storage
except ImportError:  # pragma: no cover - paquete opcional
    OperationManager = None  # type: ignore[assignment]
    create_storage = None  # type: ignore[assignment]


def log_s(value: Any, max_len: int = 200) -> str:
    return str(value).replace("\r", "").replace("\n", " ")[:max_len]


class NertzMetalEngine(MarketDataMixin, OrdersMixin, TPSLMixin, AutomationMixin, ReportingMixin):
    def __init__(
        self,
        config: Optional[ConfigSettings] = None,
        database: Optional[Database] = None,
        *,
        client_factory: Optional[Callable[..., BybitV5Client]] = None,
        paths: Optional[RuntimePaths] = None,
    ) -> None:
        self.config = config if config is not None else default_config()
        self.database = database if database is not None else default_database()
        self.SessionLocal = self.database.SessionLocal
        self.client_factory = client_factory or BybitV5Client
        self.paths = paths or RuntimePaths.from_config(self.config)

        cfg = self.config
        self.timeframe = cfg.TIMEFRAME
        self.symbols: List[str] = list(cfg.symbols)
        self.operations = (
            OperationManager(
                self.symbols,
                max_concurrent_orders=cfg.MAX_CONCURRENT_ORDERS,
                default_cooldown_s=cfg.TRADE_COOLDOWN_S,
            )
            if OperationManager is not None
            else None
        )
        self._order_slots = asyncio.Semaphore(cfg.MAX_CONCURRENT_ORDERS)
        self.capital = float(cfg.CAPITAL_USDT)
        self.iterations = 0
        self.ws = None
        self.running = True
        self.mode = "full"

        # Estado por símbolo (los nombres se mantienen: NerT_AI_PRO los lee).
        self.orderbook_data: Dict[str, OrderBook] = {}
        self.ticker_data: Dict[str, Dict[str, Any]] = {}
        self.candles: Dict[str, List[Any]] = {}
        self.recent_trades: Dict[str, deque] = {}
        self.trades_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.last_trade_time: Dict[str, datetime] = {}
        self._metrics_raw_history: Dict[str, MetricHistory] = {}
        self._metrics_window: Dict[str, deque] = {}
        self._last_metrics_by_symbol: Dict[str, Dict[str, float]] = {}
        self._last_weighted_liquidity: Dict[str, Optional[Tuple[float, float]]] = {}
        self._last_kline_ts: Dict[str, float] = {}
        self._last_orderbook_store_ts: Dict[str, float] = {}
        self._last_ticker_store_ts: Dict[str, float] = {}
        self._last_metrics_json_ts: Dict[str, float] = {}
        self._last_metrics_snapshot_ts: Dict[str, float] = {}
        self._last_live_metrics_ts: Dict[str, float] = {}
        self._auto_hft_state: Dict[str, Dict[str, Any]] = {}
        self._core_cycle_locks: Dict[str, asyncio.Lock] = {}
        for sym in self.symbols:
            self._init_symbol_state(sym)

        self.hft_tasks: Dict[str, asyncio.Task] = {}
        self._hft_params: Dict[str, Dict[str, Any]] = {}
        self.order_status: Dict[str, Dict[str, Any]] = {}
        self.instrument_rules: Dict[str, Any] = {}
        self._instrument_rules_ts: Dict[str, float] = {}

        self.last_orderbook_log = 0.0
        self._last_tune_ts = 0.0
        self._last_balance_sync_ts = 0.0
        self._balance_dirty = False
        self._boot_full_reset_done = False
        self._boot_ts = time.time()
        self._secondary_auto_enabled_ts = 0.0
        self._start_task: Optional[asyncio.Task] = None
        self._support_task: Optional[asyncio.Task] = None
        self._support_interval_s = float(cfg.SUPPORT_LOOP_INTERVAL_S)
        self._start_on_boot = True
        self._last_orders_sync_ts = 0.0
        self._last_orders_sync_results: Dict[str, Any] = {}
        self._orders_sync_lock = asyncio.Lock()
        self._bybit: Optional[BybitV5Client] = None
        self._public: Optional[BybitV5Client] = None
        self._ml_models: Dict[str, Dict[str, Any]] = {}
        self._ml_last_train_ts: Dict[str, float] = {}
        self._agent_last_tick_ts = 0.0
        self._agent_last_relax_ts = 0.0
        self._agent_events: Dict[str, Any] = {"actions": deque(maxlen=250)}
        self._auto_hft_enabled = False
        self._auto_hft_last_tick_ts = 0.0
        self._auto_tpsl_last_tick_ts = 0.0
        self._auto_tpsl_lock = asyncio.Lock()
        self._rl_last: Dict[str, float] = {}

        self._storage = self._create_storage()
        self.trade_id_counter = self._load_initial_trade_id()
        self._refresh_trades_cache()

    # ----------------------------------------------------------- state
    def _init_symbol_state(self, sym: str) -> None:
        cfg = self.config
        self.orderbook_data.setdefault(sym, OrderBook(cfg.ORDERBOOK_DEPTH))
        self.ticker_data.setdefault(sym, {"last_price": 0.0, "volume_24h": 0.0, "high_24h": 0.0, "low_24h": 0.0})
        self.candles.setdefault(sym, [])
        self.recent_trades.setdefault(sym, deque(maxlen=cfg.RECENT_TRADES_BUFFER))
        self.trades_cache.setdefault(sym, [])
        self.last_trade_time.setdefault(sym, datetime.min.replace(tzinfo=timezone.utc))
        self._metrics_raw_history.setdefault(sym, MetricHistory())
        self._metrics_window.setdefault(sym, deque(maxlen=cfg.METRICS_WINDOW_BUFFER))
        self._last_metrics_by_symbol.setdefault(sym, {})
        self._last_weighted_liquidity.setdefault(sym, None)
        for d in (
            self._last_kline_ts,
            self._last_orderbook_store_ts,
            self._last_ticker_store_ts,
            self._last_metrics_json_ts,
            self._last_metrics_snapshot_ts,
            self._last_live_metrics_ts,
        ):
            d.setdefault(sym, 0.0)
        self._auto_hft_state.setdefault(sym, {"last_change_ts": 0.0})
        if self.operations is not None:
            self.operations.get(sym)

    async def add_symbol(self, symbol: str) -> Dict[str, Any]:
        """Añade un símbolo en caliente (sin reiniciar): estado, datos iniciales y suscripción WS."""
        sym = str(symbol or "").strip().upper()
        if sym in self.symbols:
            return {"success": True, "symbol": sym, "added": False}
        self.config.update({"SYMBOL": ",".join(self.symbols + [sym])}, source="add_symbol")
        self.symbols.append(sym)
        self._init_symbol_state(sym)
        rules = await self._get_instrument_rules(sym)
        if rules is None:
            self.symbols.remove(sym)
            self.config.update({"SYMBOL": ",".join(self.symbols)}, source="add_symbol_rollback")
            return {"success": False, "symbol": sym, "message": "instrumento_no_encontrado"}
        await self._fetch_symbol_data(sym)
        await self._subscribe_symbols([sym])
        return {"success": True, "symbol": sym, "added": True, "rules": rules.as_dict()}

    def _reset_symbol_runtime(self) -> None:
        cfg = self.config
        for sym in self.symbols:
            self.trades_cache[sym] = []
            self._metrics_raw_history[sym] = MetricHistory()
            self._last_weighted_liquidity[sym] = None
            self.recent_trades[sym] = deque(maxlen=cfg.RECENT_TRADES_BUFFER)
            self._metrics_window[sym] = deque(maxlen=cfg.METRICS_WINDOW_BUFFER)
            self.last_trade_time[sym] = datetime.min.replace(tzinfo=timezone.utc)
            self._last_metrics_json_ts[sym] = 0.0
            self._last_metrics_snapshot_ts[sym] = 0.0
            self._last_kline_ts[sym] = 0.0

    def reset_runtime_state(self) -> None:
        self.stop_all_hft()
        self.hft_tasks = {}
        self._hft_params = {}
        self.iterations = 0
        self.order_status = {}
        self._last_balance_sync_ts = 0.0
        self._balance_dirty = False
        self._reset_symbol_runtime()

    def _create_storage(self):
        if not callable(create_storage):
            return None
        try:
            return create_storage(
                self.config.STORAGE_BACKEND,
                self.paths.storage_path,
                flush_interval_ms=self.config.STORAGE_BATCH_INTERVAL_MS,
            )
        except Exception as e:
            logger.warning(f"⚠️ Storage backend no disponible: {e}")
            return None

    def _rl_log(self, key: str, level: str, message: str, *, interval_s: float = 5.0) -> None:
        now = time.time()
        k = str(key or "").strip() or "log"
        if float(interval_s) > 0 and (now - self._rl_last.get(k, 0.0)) < float(interval_s):
            return
        self._rl_last[k] = now
        lvl = str(level or "").strip().lower()
        if lvl in {"error", "err"}:
            logger.error(str(message))
        elif lvl in {"warning", "warn"}:
            logger.warning(str(message))
        else:
            logger.info(str(message))

    def _actions(self) -> deque:
        actions = self._agent_events.get("actions")
        if not isinstance(actions, deque):
            actions = deque(maxlen=250)
            self._agent_events["actions"] = actions
        return actions

    # ------------------------------------------------------ properties
    @property
    def start_on_boot(self) -> bool:
        return bool(self._start_on_boot)

    @start_on_boot.setter
    def start_on_boot(self, value: bool) -> None:
        self._start_on_boot = bool(value)

    @property
    def start_task(self) -> Optional[asyncio.Task]:
        return self._start_task

    @property
    def support_task(self) -> Optional[asyncio.Task]:
        return self._support_task

    @property
    def support_interval_s(self) -> float:
        return float(self._support_interval_s)

    @support_interval_s.setter
    def support_interval_s(self, value: float) -> None:
        self._support_interval_s = float(value)

    @property
    def ml_models(self) -> Dict[str, Dict[str, Any]]:
        return self._ml_models

    @property
    def agent_last_tick_ts(self) -> float:
        return float(self._agent_last_tick_ts)

    @property
    def agent_last_relax_ts(self) -> float:
        return float(self._agent_last_relax_ts)

    @property
    def agent_events(self) -> Dict[str, Any]:
        return self._agent_events

    @property
    def metrics_window(self) -> Dict[str, Any]:
        return self._metrics_window

    @property
    def metrics_raw_history(self) -> Dict[str, Any]:
        return self._metrics_raw_history

    @property
    def last_weighted_liquidity(self) -> Dict[str, Any]:
        return self._last_weighted_liquidity

    @property
    def hft_params(self) -> Dict[str, Dict[str, Any]]:
        return self._hft_params

    @property
    def auto_hft_enabled(self) -> bool:
        return bool(self._auto_hft_enabled)

    @auto_hft_enabled.setter
    def auto_hft_enabled(self, value: bool) -> None:
        self._auto_hft_enabled = bool(value)

    # ------------------------------------------------ public wrappers
    def thresholds_payload(self) -> Dict[str, float]:
        return self._thresholds_payload()

    async def core_cycle(self, symbol: str, db: Session, collect_only: bool = False, force_trade: bool = False) -> None:
        await self._core_cycle(symbol, db, collect_only=bool(collect_only), force_trade=bool(force_trade))

    async def agent_tick(self, db: Session) -> None:
        await self._agent_tick(db)

    async def save_results(self, symbol, trade_result):
        await self._save_results(symbol, trade_result)

    def bybit_client(self) -> Optional[BybitV5Client]:
        return self._bybit_client()

    # ------------------------------------------------- signal / weights
    def get_combined_weights(self, symbol: str) -> Dict[str, float]:
        """Pesos del combined del símbolo: override en ticker_data (API/agente) o config."""
        td = self.ticker_data.get(symbol) or {}
        cw = td.get("combined_weights")
        if isinstance(cw, dict) and cw:
            return dict(cw)
        return dict(self.config.COMBINED_WEIGHTS_JSON)

    def set_combined_weights(self, symbol: Optional[str], weights: Dict[str, float]) -> None:
        targets = [symbol] if symbol else list(self.symbols)
        for sym in targets:
            self.ticker_data.setdefault(sym, {})["combined_weights"] = dict(weights)

    def _thresholds_for(self, symbol: Optional[str]) -> Tuple[float, float, float]:
        cfg = self.config
        return (
            float(cfg.for_symbol(symbol, "COMBINED_BUY_THRESHOLD")),
            float(cfg.for_symbol(symbol, "COMBINED_SELL_THRESHOLD")),
            float(cfg.for_symbol(symbol, "COMBINED_HOLD_BAND")),
        )

    def _signal_eval(self, metrics: Dict, symbol: Optional[str] = None) -> Dict[str, Any]:
        buy, sell, hold = self._thresholds_for(symbol)
        return evaluate_signal(metrics, buy_th=buy, sell_th=sell, hold_band=hold, params=self.config.signal_params)

    def _determine_decision(self, symbol: str, metrics: Dict) -> str:
        return str(self._signal_eval(metrics, symbol).get("decision") or "hold")

    def _decision_detail(self, symbol: str, metrics: Dict) -> Dict[str, Any]:
        """Diagnóstico read-only: decisión, estado de mercado y bloqueos."""
        ev = self._signal_eval(metrics, symbol)
        keys = (
            "decision",
            "market_state",
            "combined",
            "combined_z",
            "mom",
            "pio",
            "egm",
            "tfi",
            "rvol",
            "volatility",
            "microprice_offset_bps",
            "thresholds_effective",
            "thresholds_symmetric_base",
            "confirmations",
        )
        out = {k: ev.get(k) for k in keys}
        out["blockers_if_not_trading"] = ev.get("blockers") or []
        return out

    def _compute_in_cooldown(
        self,
        cooldown_s: float,
        last_trade_time: datetime,
        current_time: datetime,
        metrics: Optional[Dict] = None,
        symbol: Optional[str] = None,
    ) -> bool:
        if float(cooldown_s) <= 0.0:
            return False
        in_cd = (current_time - last_trade_time).total_seconds() < float(cooldown_s)
        if not in_cd or not self.config.COOLDOWN_BYPASS_STRONG_SIGNAL:
            return in_cd
        try:
            comb = float((metrics or {}).get("combined") or 0.0)
        except (TypeError, ValueError):
            return in_cd
        buy_th = self._thresholds_for(symbol)[0]
        return not (abs(comb) >= abs(buy_th) * float(self.config.COOLDOWN_BYPASS_MULT))

    def _default_metrics(self) -> Dict[str, Any]:
        return {"combined": 0.0, "ild": 0.0, "egm": 0.0, "rol": 0.0, "pio": 0.0, "ogm": 0.0, "volatility": 0.0,
                "data_ok": False}

    # -------------------------------------------------------- metrics
    def _candles_for(self, symbol: str, db: Optional[Session] = None, limit: Optional[int] = None) -> List[Any]:
        """Velas del loop en memoria (verdad del motor) con fallback a la base."""
        lim = int(limit or self.config.CANDLE_BUFFER_SIZE)
        buf = self.candles.get(symbol) or []
        if buf:
            return list(buf[:lim])
        if db is None:
            return []
        rows = (
            db.query(MarketData)
            .filter(MarketData.symbol == symbol)
            .order_by(MarketData.timestamp.desc())
            .limit(lim)
            .all()
        )
        if rows and not self.candles.get(symbol):
            self.candles[symbol] = list(rows)
        return list(rows)

    def _metrics_context(self, symbol: str, now_ts: float) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cfg = self.config
        window_s = max(60.0, float(cfg.METRICS_WINDOW_MINUTES) * 60.0)
        history = self._metrics_raw_history.setdefault(symbol, MetricHistory())
        history.evict_older_than(now_ts - window_s)
        prev = self._last_weighted_liquidity.get(symbol)
        prev_liq, prev_ts = (prev if isinstance(prev, tuple) and len(prev) == 2 else (None, None))
        recent = list(self.recent_trades.get(symbol) or [])[-int(cfg.RECENT_TRADES_METRICS_N):]
        payload = dict(self.ticker_data.get(symbol) or {})
        payload.update(
            orderbook_lambda=cfg.for_symbol(symbol, "ORDERBOOK_LAMBDA"),
            orderbook_pct_band=cfg.for_symbol(symbol, "ORDERBOOK_PCT_BAND"),
            ild_target_move=cfg.for_symbol(symbol, "ILD_TARGET_MOVE"),
            metric_history=history,
            prev_weighted_liquidity=prev_liq,
            rol_dt_s=(now_ts - float(prev_ts)) if prev_ts else None,
            formulas=cfg.FORMULAS_JSON,
            combined_weights=self.get_combined_weights(symbol),
            recent_trades=recent,
        )
        return payload, recent

    def compute_metrics(
        self, symbol: str, candles: Optional[List[Any]] = None, *, record: bool = False
    ) -> Tuple[Dict[str, Any], bool]:
        """Métricas actuales del símbolo. ``record=True`` alimenta la historia (solo el ciclo)."""
        now_ts = time.time()
        candles = candles if candles is not None else self._candles_for(symbol)
        book = self.orderbook_data.get(symbol)
        ticker = self.ticker_data.get(symbol) or {}
        ready = len(candles) >= 2 and book is not None and book.is_ready() and ticker.get("last_price")
        if not ready:
            return self._default_metrics(), True

        payload, recent = self._metrics_context(symbol, now_ts)
        metrics = calculate_metrics(
            candle_inputs(candles),
            book,
            payload,
            depth=int(self.config.ORDERBOOK_DEPTH),
            recent_trades=recent,
        )
        if record and metrics.get("data_ok"):
            wl = metrics.get("weighted_liquidity")
            if wl is not None:
                self._last_weighted_liquidity[symbol] = (float(wl), now_ts)
            self._metrics_raw_history[symbol].append(now_ts, raw_sample_from_metrics(metrics))
        return metrics, False

    @staticmethod
    def _finite_floats(metrics: Dict[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for k, v in metrics.items():
            if isinstance(v, (dict, list, str)) or v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if np.isfinite(fv):
                out[str(k)] = fv
        return out

    def _last_price(self, symbol: str, candles: Optional[List[Any]] = None) -> float:
        last_price = float((self.ticker_data.get(symbol) or {}).get("last_price") or 0.0)
        if last_price <= 0 and candles:
            try:
                last_price = float(candles[0].close)
            except (AttributeError, TypeError, ValueError):
                last_price = 0.0
        return last_price

    # -------------------------------------------------------- ML filter
    def ml_predict_proba(self, *, symbol: str, action: str, metrics: Dict[str, Any]) -> Optional[float]:
        model = self._ml_models.get(symbol if symbol in self._ml_models else "__all__")
        rr = self._risk_reward()
        return ml.predict_proba(model, ml.features_from_metrics(action, metrics, rr))

    def _risk_reward(self) -> float:
        sl = float(self.config.SL_PERCENTAGE)
        return float(self.config.TP_PERCENTAGE) / sl if sl > 0 else 0.0

    def train_ml_model_from_trades(
        self,
        db: Session,
        *,
        symbol: Optional[str] = None,
        min_samples: Optional[int] = None,
        epochs: Optional[int] = None,
        lr: Optional[float] = None,
        l2: Optional[float] = None,
    ) -> Dict[str, Any]:
        cfg = self.config
        ms = int(min_samples) if min_samples is not None else int(cfg.ML_MIN_SAMPLES)
        q = db.query(Trade).filter(Trade.outcome_status == "final")
        if symbol:
            q = q.filter(Trade.symbol == symbol)
        trades = q.order_by(Trade.timestamp.desc()).limit(max(ms * 50, 500)).all()
        if len(trades) < ms:
            return {"success": False, "message": "insufficient_samples", "samples": len(trades)}
        res = ml.train_logistic(
            trades,
            min_samples=ms,
            epochs=int(epochs if epochs is not None else cfg.ML_EPOCHS),
            lr=float(lr if lr is not None else cfg.ML_LEARNING_RATE),
            l2=float(l2 if l2 is not None else cfg.ML_L2),
        )
        if not res.get("success"):
            return res
        key = symbol or "__all__"
        self._ml_models[key] = res["model"]
        self._ml_last_train_ts[key] = time.time()
        return {"success": True, "key": key, "model": res["model"]}

    # --------------------------------------------------------- the cycle
    async def _maybe_sync_balance(self) -> None:
        now_ts = time.time()
        elapsed = now_ts - float(self._last_balance_sync_ts or 0.0)
        due = elapsed >= self.config.BALANCE_SYNC_INTERVAL_S or (
            self._balance_dirty and elapsed >= self.config.BALANCE_DIRTY_SYNC_S
        )
        if not due:
            return
        balance = await self.record_balance()
        if balance.get("success") and isinstance(balance.get("balance"), dict):
            self._apply_balance_to_capital(balance["balance"])
            self._balance_dirty = False
        self._last_balance_sync_ts = now_ts

    def _apply_balance_to_capital(self, balance: Dict[str, Any]) -> None:
        total = float(balance.get("total_equity") or 0.0)
        avail = float(balance.get("available_balance") or 0.0)
        if total > 0:
            self.capital = total
        elif avail > 0:
            self.capital = avail

    async def _core_cycle(self, symbol: str, db: Session, collect_only: bool = False,
                          force_trade: bool = False) -> None:
        lock = self._core_cycle_locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            try:
                await self._cycle_body(symbol, db, collect_only=collect_only, force_trade=force_trade)
            except Exception as e:
                logger.error("Error en ciclo de %s: %s", log_s(symbol), log_s(e), exc_info=True)

    async def _cycle_body(self, symbol: str, db: Session, *, collect_only: bool, force_trade: bool) -> None:
        cfg = self.config
        current_time = datetime.now(timezone.utc)
        await self._maybe_sync_balance()

        candles = self._candles_for(symbol, db)
        metrics, warmup = self.compute_metrics(symbol, candles, record=True)
        self._last_metrics_by_symbol[symbol] = self._finite_floats(metrics)

        cooldown_s = float(cfg.for_symbol(symbol, "TRADE_COOLDOWN_S"))
        last_trade_time = self.last_trade_time.get(symbol, datetime.min.replace(tzinfo=timezone.utc))
        in_cooldown = self._compute_in_cooldown(cooldown_s, last_trade_time, current_time, metrics, symbol)
        decision = "hold" if warmup else self._determine_decision(symbol, metrics)
        last_price = self._last_price(symbol, candles)

        finalized = await self._finalize_due_outcomes(db, symbol, last_price)
        if finalized is not None:
            await self._save_results(symbol, finalized)
        await self._record_metrics_snapshot(db, symbol, last_price, metrics, "warmup" if warmup else decision)
        await self._auto_tune_thresholds_if_due()

        if decision == "hold" and force_trade and not collect_only and not in_cooldown:
            last = self.trades_cache.get(symbol) or []
            decision = "sell" if (last and last[0].get("action") == "buy") else "buy"

        if decision in {"buy", "sell"} and cfg.ML_ENABLED:
            p = self.ml_predict_proba(symbol=symbol, action=decision, metrics=metrics)
            if p is not None and p < float(cfg.ML_PROB_THRESHOLD):
                decision = "hold"

        if decision == "hold" or collect_only or in_cooldown:
            return

        allowed, gate_reason = check_execution_gates(
            metrics, spread_avg_bps=float(cfg.for_symbol(symbol, "AVG_SPREAD_BPS")), params=cfg.signal_params
        )
        if not allowed:
            logger.debug(
                f"🛑 [EXEC GATE] {symbol} | {gate_reason} | "
                f"spread={float(metrics.get('spread_bps', 0) or 0):.2f}bps "
                f"rvol={float(metrics.get('rvol', 0) or 0):.2e}"
            )
            return

        ctx = self.operations.get(symbol) if self.operations is not None else None
        if ctx is not None:
            ctx.cooldown_s = cooldown_s
            if not ctx.can_trade():
                return

        if not cfg.ALLOW_MULTIPLE_ACTIVE_TRADES:
            active = (
                db.query(Trade.id)
                .filter(Trade.symbol == symbol, Trade.outcome_status.in_(OPEN_STATUSES))
                .first()
            )
            if active is not None:
                return

        await self._open_position(symbol, decision, db, metrics, last_price, current_time, ctx)

    async def _open_position(
        self,
        symbol: str,
        decision: str,
        db: Session,
        metrics: Dict[str, Any],
        last_price: float,
        current_time: datetime,
        ctx: Any,
    ) -> None:
        cfg = self.config
        if last_price <= 0:
            logger.error("Precio invalido (%s) para %s", log_s(last_price), log_s(symbol))
            return
        rules = await self._get_instrument_rules(symbol)
        if rules is None:
            self._rl_log(f"rules:{symbol}", "warning", f"⚠️ Sin reglas de instrumento para {symbol}; no se opera.")
            return

        volatility = float(metrics.get("volatility") or 0.0)
        if not np.isfinite(volatility) or volatility <= 0:
            logger.warning("Volatilidad invalida (%s) para %s, usando fallback", log_s(volatility), log_s(symbol))
            volatility = float(cfg.VOLATILITY_FALLBACK)

        entry_price = last_price
        if cfg.ORDER_TYPE == "Limit":
            book = self.orderbook_data.get(symbol)
            ref = (book.best_bid() if decision == "buy" else book.best_ask()) if book is not None else 0.0
            entry_price = float(rules.price(ref if ref > 0 else last_price, ROUND_HALF_UP))

        sizing = size_order(
            SizingInputs(
                capital=float(self.capital),
                risk_factor=float(cfg.for_symbol(symbol, "RISK_FACTOR")),
                volatility=volatility,
                last_price=last_price,
                entry_price=entry_price,
                max_position_notional_pct=float(cfg.for_symbol(symbol, "MAX_POSITION_NOTIONAL_PCT")),
                min_notional_buffer=float(cfg.MIN_NOTIONAL_BUFFER),
                max_trade_size=float(cfg.for_symbol(symbol, "MAX_TRADE_SIZE")),
                min_trade_size=float(cfg.for_symbol(symbol, "MIN_TRADE_SIZE")),
            ),
            rules,
        )
        if not sizing.ok:
            logger.warning("Sizing rechazado para %s: %s %s", log_s(symbol), sizing.reason, sizing.detail)
            return
        quantity = float(sizing.quantity)

        tp_dec, sl_dec = protective_levels(
            decision,
            entry_price,
            volatility,
            float(cfg.for_symbol(symbol, "TP_PERCENTAGE")),
            float(cfg.for_symbol(symbol, "SL_PERCENTAGE")),
            rules,
        )
        tp, sl = float(tp_dec), float(sl_dec)

        order_result = await self._place_order(symbol, decision, quantity, entry_price, tp, sl)
        if not order_result.get("success", False):
            logger.error(
                "Fallo al colocar orden para %s: %s",
                log_s(symbol),
                log_s(order_result.get("message", "Error desconocido")),
            )
            return

        trade = self._record_new_trade(
            symbol=symbol,
            decision=decision,
            timestamp=current_time,
            order_result=order_result,
            entry_price=entry_price,
            quantity=quantity,
            tp=tp,
            sl=sl,
            metrics=metrics,
            last_price=last_price,
            db=db,
        )
        self.last_trade_time[symbol] = current_time
        if ctx is not None:
            ctx.mark_trade()

        dec = price_decimals(rules)
        logger.info(
            f"💰 Orden colocada: {decision.upper()} {quantity} {symbol} @ {entry_price:.{dec}f}, "
            f"TP={tp:.{dec}f}, SL={sl:.{dec}f}, OrderID={trade.order_id}"
        )

        self.iterations += 1
        if 0 < cfg.MAX_ITERATIONS <= self.iterations:
            logger.info("🏁 Máximo de iteraciones alcanzado. Deteniendo bot.")
            self.stop()
        self._refresh_trades_cache(symbol)
        await self._save_results(symbol, trade)

    def _record_new_trade(
        self,
        *,
        symbol: str,
        decision: str,
        timestamp: datetime,
        order_result: Dict[str, Any],
        entry_price: float,
        quantity: float,
        tp: float,
        sl: float,
        metrics: Dict[str, Any],
        last_price: float,
        db: Session,
    ) -> Trade:
        order_id = str(order_result.get("order_id") or "")
        metrics_snapshot = {
            "timestamp": timestamp.isoformat(),
            "ts": time.time(),
            "symbol": symbol,
            "last_price": float(last_price or 0.0),
            "decision": decision,
            "metrics": self._serialize_metrics_for_storage(metrics),
            "thresholds": self._thresholds_payload(symbol),
        }
        raw: Dict[str, Any] = dict(order_result.get("raw") or {})
        if order_result.get("order_link_id"):
            raw["order_link_id"] = str(order_result["order_link_id"])
        raw["metrics_snapshot"] = metrics_snapshot

        trade = Trade(
            trade_id=self._next_trade_id(db),
            timestamp=timestamp,
            symbol=symbol,
            action=decision,
            order_id=order_id,
            bybit_raw=raw,
            entry_price=entry_price,
            exit_price=0.0,
            tp_price=tp,
            sl_price=sl,
            quantity=quantity,
            profit_loss=0.0,
            outcome_status="pending",
            decision=decision,
            combined=float(metrics.get("combined", 0) or 0),
            ild=float(metrics.get("ild", 0) or 0),
            egm=float(metrics.get("egm", 0) or 0),
            rol=float(metrics.get("rol", 0) or 0),
            pio=float(metrics.get("pio", 0) or 0),
            ogm=float(metrics.get("ogm", 0) or 0),
            risk_reward_ratio=self._risk_reward(),
        )
        db.add(trade)
        db.commit()
        if order_id:
            self.order_status[order_id] = {
                "order_id": order_id,
                "trade_id": int(trade.trade_id),
                "symbol": symbol,
                "status": "pending",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        return trade

    def _load_initial_trade_id(self) -> int:
        with self.SessionLocal() as db:
            last = db.query(Trade.trade_id).order_by(Trade.trade_id.desc()).first()
            return int(last[0]) + 1 if last else 1

    def _next_trade_id(self, db: Session) -> int:
        last = db.query(Trade.trade_id).order_by(Trade.trade_id.desc()).first()
        next_db = int(last[0]) + 1 if last else 1
        trade_id = max(int(self.trade_id_counter), next_db)
        self.trade_id_counter = trade_id + 1
        return trade_id

    # ------------------------------------------------------- lifecycle
    def schedule_start(self) -> bool:
        if self._start_task and not self._start_task.done():
            return False
        self.running = True
        self._start_task = asyncio.create_task(self.start_async())
        self.start_support_loop(interval_s=self._support_interval_s)
        return True

    async def start_async(self):
        logger.info(f"🔥 Iniciando bot para {self.symbols}")
        if not self._boot_full_reset_done and self.config.FULL_RESET_ON_BOOT:
            try:
                with self.SessionLocal() as db:
                    self.wipe_database(db)
                self.reset_runtime_state()
                self.trade_id_counter = 1
                self.reset_results_json()
            except Exception as e:
                logger.error(f"❌ FULL_RESET_ON_BOOT falló: {e}")
            self._boot_full_reset_done = True
        try:
            preflight = await self.preflight()
            if not preflight.get("success"):
                logger.error(f"❌ Preflight falló: {preflight.get('message') or 'error'}")
                return
        except Exception as e:
            logger.error(f"❌ Preflight falló: {e}")
            return
        attempts = int(self.config.INITIAL_FETCH_ATTEMPTS)
        for attempt in range(attempts):
            if not self.running:
                logger.info("🛑 Bot detenido antes de iniciar.")
                return
            try:
                await self.fetch_initial_data()
                break
            except Exception as e:
                logger.error(f"❌ Error al obtener datos iniciales (intento {attempt + 1}/{attempts}): {e}")
                await asyncio.sleep(min(10, 2 ** attempt))
        if not self.running:
            return
        await self._connect_websocket_async()

    def start_support_loop(self, interval_s: float = 2.0) -> bool:
        if self._support_task and not self._support_task.done():
            return False
        self._support_interval_s = float(max(0.25, min(30.0, float(interval_s))))
        self._support_task = asyncio.create_task(self._support_loop())
        return True

    async def _support_loop(self) -> None:
        while self.running:
            cfg = self.config
            try:
                with self.SessionLocal() as db:
                    await self._enable_secondary_systems_if_due(db)
                    await self.sync_open_orders(
                        db,
                        timeout_seconds=cfg.ORDERS_SYNC_TIMEOUT_S,
                        update_after_seconds=cfg.ORDERS_SYNC_UPDATE_AFTER_S,
                        limit=cfg.ORDERS_SYNC_LIMIT,
                    )
                    if cfg.AUTO_AGENT_ENABLED:
                        await self._agent_tick(db)
                    if self._auto_hft_enabled_effective():
                        await self._auto_hft_tick(db)
                    if cfg.AUTO_TPSL_ENABLED:
                        await self._auto_tpsl_tick(db)
                    await self._live_metrics_tick(db)
                    await self._metrics_snapshot_tick(db)
            except Exception as e:
                logger.error(f"❌ Error en support loop: {e}", exc_info=True)
            await asyncio.sleep(self._support_interval_s)

    def stop(self):
        self.running = False
        closers = [c.aclose() for c in (self._bybit, self._public) if c is not None]
        if self.ws:
            closers.append(self.ws.close())
        try:
            loop = asyncio.get_running_loop()
            for coro in closers:
                loop.create_task(coro)
        except RuntimeError:  # sin loop activo (scripts/tests): no hay nada abierto que cerrar en segundo plano
            for coro in closers:
                coro.close()
        self._bybit = None
        self._public = None
        for task in (self._start_task, self._support_task):
            if task is not None and not task.done():
                task.cancel()
        self._start_task = None
        self._support_task = None
        logger.info("🛑 Bot detenido.")

    async def start_storage(self) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.start()
            logger.info(f"✅ Storage DuckDB activo: {getattr(self._storage, 'path', self.paths.storage_path)}")
        except Exception as e:
            logger.error(f"❌ Storage DuckDB no pudo iniciar, fallback SQLite legacy: {e}")
            err = str(e)
            if "being utilized by another process" in err or "already open" in err.lower():
                from nertz_core.engine.market_data import duckdb_lock_hint

                logger.error(f"🔒 {duckdb_lock_hint(e, self.paths.project_root)}")
            self._storage = None

    async def stop_storage(self) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.flush()
            await self._storage.stop()
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando storage DuckDB: {e}")
        finally:
            self._storage = None
