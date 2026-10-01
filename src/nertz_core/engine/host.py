"""Contrato de estado compartido entre ``NertzMetalEngine`` y sus mixins.

Cada mixin usa atributos y métodos que define el motor u otro mixin. Esta clase
solo los declara (anotaciones de clase y firmas bajo ``TYPE_CHECKING``) para que
el IDE y los type checkers los resuelvan: no crea atributos ni métodos en
runtime, así que no altera el MRO ni el comportamiento del motor.
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from bybit_v5 import BybitV5Client
    from nertz_core.db import Database, Trade
    from nertz_core.history import MetricHistory
    from nertz_core.market import OrderBook
    from nertz_core.runtime import RuntimePaths
    from nertz_core.sizing import InstrumentRules
    from settings import ConfigSettings


class EngineHost:
    # ------------------------------------------------------ configuración
    config: ConfigSettings
    database: Database
    SessionLocal: sessionmaker[Session]
    client_factory: Callable[..., BybitV5Client]
    paths: RuntimePaths
    symbols: List[str]
    capital: float
    iterations: int
    running: bool
    ws: Any

    # ------------------------------------------------- estado por símbolo
    orderbook_data: Dict[str, OrderBook]
    ticker_data: Dict[str, Dict[str, Any]]
    candles: Dict[str, List[Any]]
    recent_trades: Dict[str, deque]
    trades_cache: Dict[str, List[Dict[str, Any]]]
    trade_id_counter: int
    last_trade_time: Dict[str, datetime]
    last_orderbook_log: float
    _metrics_raw_history: Dict[str, MetricHistory]
    _metrics_window: Dict[str, deque]
    _last_metrics_by_symbol: Dict[str, Dict[str, float]]
    _last_kline_ts: Dict[str, float]
    _last_orderbook_store_ts: Dict[str, float]
    _last_ticker_store_ts: Dict[str, float]
    _last_metrics_json_ts: Dict[str, float]
    _last_metrics_snapshot_ts: Dict[str, float]
    _last_live_metrics_ts: Dict[str, float]
    _auto_hft_state: Dict[str, Dict[str, Any]]

    # --------------------------------------------------- órdenes / HFT
    hft_tasks: Dict[str, asyncio.Task]
    _hft_params: Dict[str, Dict[str, Any]]
    order_status: Dict[str, Dict[str, Any]]
    instrument_rules: Dict[str, Any]
    _instrument_rules_ts: Dict[str, float]
    _order_slots: asyncio.Semaphore
    _orders_sync_lock: asyncio.Lock
    _last_orders_sync_ts: float
    _last_orders_sync_results: Dict[str, Any]
    _bybit: Optional[BybitV5Client]
    _public: Optional[BybitV5Client]
    _balance_dirty: bool

    # ------------------------------------------------------ automatismos
    _boot_ts: float
    _start_task: Optional[asyncio.Task]
    _secondary_auto_enabled_ts: float
    _last_tune_ts: float
    _ml_models: Dict[str, Dict[str, Any]]
    _ml_last_train_ts: Dict[str, float]
    _agent_last_tick_ts: float
    _agent_last_relax_ts: float
    _auto_hft_enabled: bool
    _auto_hft_last_tick_ts: float
    _auto_tpsl_last_tick_ts: float
    _auto_tpsl_lock: asyncio.Lock
    _storage: Any

    if TYPE_CHECKING:
        # Métodos implementados en ``NertzMetalEngine`` u otro mixin.
        def _rl_log(self, key: str, level: str, message: str, *, interval_s: float = 5.0) -> None:
            ...

        def _actions(self) -> deque:
            ...

        def _apply_balance_to_capital(self, balance: Dict[str, Any]) -> None:
            ...

        def _bybit_client(self) -> Optional[BybitV5Client]:
            ...

        def _public_client(self) -> BybitV5Client:
            ...

        async def _core_cycle(self, symbol: str, db: Session, collect_only: bool = False,
                              force_trade: bool = False) -> None:
            ...

        def _determine_decision(self, symbol: str, metrics: Dict) -> str:
            ...

        async def _get_instrument_rules(self, symbol: str) -> Optional[InstrumentRules]:
            ...

        def _load_initial_trade_id(self) -> int:
            ...

        def _next_trade_id(self, db: Session) -> int:
            ...

        def _persist_thresholds_block(self, update: Dict[str, Any], targets: Dict[str, Any]) -> None:
            ...

        async def _place_order(
            self,
            symbol: str,
            action: str,
            quantity: float,
            price: float,
            tp: float,
            sl: float,
            *,
            order_type: Optional[str] = None,
        ) -> Dict:
            ...

        async def _record_event(self, event: Dict[str, Any]) -> None:
            ...

        async def _record_metrics_snapshot(self, db: Session, symbol: str, last_price: float,
                                           metrics: Dict[str, Any], decision: str) -> None:
            ...

        def _refresh_trades_cache(self, symbol: Optional[str] = None) -> None:
            ...

        def _risk_reward(self) -> float:
            ...

        async def _save_results(self, _symbol: Optional[str], trade_result: Optional[Trade]) -> None:
            ...

        def _thresholds_for(self, symbol: Optional[str]) -> Tuple[float, float, float]:
            ...

        def _thresholds_payload(self, symbol: Optional[str] = None) -> Dict[str, float]:
            ...

        def _update_last_balance(self, body: Dict[str, Any]) -> None:
            ...

        def _last_price(self, symbol: str, candles: Optional[List[Any]] = None) -> float:
            ...

        def ml_predict_proba(self, *, symbol: str, action: str, metrics: Dict[str, Any]) -> Optional[float]:
            ...

        def schedule_start(self) -> bool:
            ...

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
            ...
