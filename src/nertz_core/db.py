"""Modelos SQLAlchemy y acceso a la base SQLite operativa (trades, snapshots)."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import JSON, DateTime, Float, Integer, String, create_engine, or_, text
from sqlalchemy.orm import Mapped, Session, declarative_base, mapped_column, sessionmaker

Base = declarative_base()

# Estados del ciclo de vida de un trade.
OPEN_STATUSES = ("pending", "partial", "filled")
TERMINAL_STATUSES = ("final", "cancelled", "invalid_entry")

_SYMBOL_LEN = 32


class MarketData(Base):
    __tablename__ = "market_data"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    symbol: Mapped[str] = mapped_column(String(_SYMBOL_LEN), nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    high: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    low: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    close: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class Orderbook(Base):
    __tablename__ = "orderbook"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(_SYMBOL_LEN), nullable=False, index=True)
    bids: Mapped[Any] = mapped_column(JSON, nullable=False)
    asks: Mapped[Any] = mapped_column(JSON, nullable=False)


class MarketTicker(Base):
    __tablename__ = "market_ticker"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(_SYMBOL_LEN), nullable=False, index=True)
    last_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    high_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    low_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    trade_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(_SYMBOL_LEN), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    order_id: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)
    bybit_raw: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    exit_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    tp_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sl_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    profit_loss: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pnl_gross: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    outcome_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    outcome_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    combined: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ild: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    egm: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    rol: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ogm: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    risk_reward_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=1.5)


class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(_SYMBOL_LEN), nullable=False, index=True)
    last_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    decision: Mapped[str] = mapped_column(String, nullable=False, default="hold")
    combined: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ild: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    egm: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    rol: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ogm: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    volatility: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    thresholds: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class BalanceSnapshot(Base):
    __tablename__ = "balance_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    account_type: Mapped[str] = mapped_column(String(20), nullable=False, default="UNIFIED")
    coin: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    total_equity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    available_balance: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    raw: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class ThresholdSnapshot(Base):
    __tablename__ = "threshold_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    egm_buy_threshold: Mapped[float] = mapped_column(Float, nullable=False)
    egm_sell_threshold: Mapped[float] = mapped_column(Float, nullable=False)
    combined_buy_threshold: Mapped[float] = mapped_column(Float, nullable=False)
    combined_sell_threshold: Mapped[float] = mapped_column(Float, nullable=False)
    stats: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


ALL_MODELS = (Trade, MetricSnapshot, BalanceSnapshot, ThresholdSnapshot, MarketTicker, Orderbook, MarketData)

# Columnas añadidas después de la primera versión del esquema (migración aditiva).
_ADDITIVE_COLUMNS: Dict[str, Dict[str, str]] = {
    "trades": {
        "order_id": "TEXT",
        "bybit_raw": "TEXT",
        "tp_price": "REAL",
        "sl_price": "REAL",
        "pnl_gross": "REAL DEFAULT 0.0",
        "outcome_status": "TEXT DEFAULT 'pending'",
        "outcome_timestamp": "DATETIME",
    },
}


class Database:
    """Engine + fábrica de sesiones sobre un archivo SQLite."""

    def __init__(self, path: str) -> None:
        self.path = os.path.abspath(path)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.url = f"sqlite:///{self.path}"
        self.engine = create_engine(self.url, connect_args={"check_same_thread": False})
        Base.metadata.create_all(bind=self.engine)
        for table, cols in _ADDITIVE_COLUMNS.items():
            self.ensure_columns(table, cols)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine, expire_on_commit=False
        )

    def ensure_columns(self, table: str, desired: Dict[str, str]) -> None:
        with self.engine.begin() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
            existing = {row[1] for row in rows} if rows else set()
            for name, type_sql in desired.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {type_sql}"))

    def get_db(self) -> Iterable[Session]:
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def wipe(self, db: Session) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for model in ALL_MODELS:
            try:
                counts[model.__tablename__] = int(db.query(model).delete() or 0)
            except Exception:
                counts[model.__tablename__] = -1
        db.commit()
        try:
            db.execute(text("VACUUM"))
            db.commit()
        except Exception:
            pass
        return counts


def utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def balance_snapshot_is_live(snap: BalanceSnapshot) -> bool:
    raw = getattr(snap, "raw", None)
    if not isinstance(raw, dict):
        return True
    mode = str(raw.get("mode") or "").strip().lower()
    if mode in {"disabled", "simulated"}:
        return False
    ret_code = raw.get("retCode")
    if ret_code is not None and ret_code not in (0, "0"):
        return False
    return True


def latest_valid_balance(db: Session) -> Optional[BalanceSnapshot]:
    rows = (
        db.query(BalanceSnapshot)
        .filter(or_(BalanceSnapshot.total_equity > 0.0, BalanceSnapshot.available_balance > 0.0))
        .order_by(BalanceSnapshot.timestamp.desc())
        .limit(100)
        .all()
    )
    for row in rows:
        if balance_snapshot_is_live(row):
            return row
    return rows[0] if rows else None


def trade_order_link_id(trade: Any) -> str:
    raw = getattr(trade, "bybit_raw", None)
    if isinstance(raw, dict):
        link = raw.get("order_link_id") or raw.get("orderLinkId")
        if isinstance(link, str):
            return link.strip()
    return ""


def merge_raw(trade: Any, **blocks: Any) -> Dict[str, Any]:
    """Fusiona bloques en ``trade.bybit_raw`` (reasignando para que SQLAlchemy detecte el cambio)."""
    current = getattr(trade, "bybit_raw", None)
    merged = dict(current) if isinstance(current, dict) else {}
    merged.update(blocks)
    trade.bybit_raw = merged
    return merged
