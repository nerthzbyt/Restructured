"""Storage row models and backend protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class TickRow:
    """Real-time ticker snapshot."""

    timestamp: datetime
    symbol: str
    last_price: float
    volume_24h: float = 0.0
    high_24h: float = 0.0
    low_24h: float = 0.0
    usd_index_price: float | None = None


@dataclass(frozen=True, slots=True)
class OrderbookRow:
    """Orderbook depth snapshot."""

    timestamp: datetime
    symbol: str
    bids: list[Any]
    asks: list[Any]


@dataclass(frozen=True, slots=True)
class MetricRow:
    """Computed metric snapshot aligned with legacy MetricSnapshot."""

    timestamp: datetime
    symbol: str
    last_price: float
    decision: str = "hold"
    combined: float = 0.0
    ild: float = 0.0
    egm: float = 0.0
    rol: float = 0.0
    pio: float = 0.0
    ogm: float = 0.0
    volatility: float = 0.0
    thresholds: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventRow:
    """Engine lifecycle / trading event."""

    timestamp: datetime
    event_type: str
    symbol: str | None = None
    level: str = "info"
    message: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class StorageBackend(Protocol):
    """Async storage contract used by the engine."""

    async def start(self) -> None:
        """Open resources and begin background flushing."""
        ...

    async def stop(self) -> None:
        """Flush pending writes and release resources."""
        ...

    async def enqueue_tick(self, row: TickRow) -> None:
        """Queue a ticker row for batched persistence."""
        ...

    async def enqueue_orderbook(self, row: OrderbookRow) -> None:
        """Queue an orderbook snapshot for batched persistence."""
        ...

    async def enqueue_metric(self, row: MetricRow) -> None:
        """Queue a metric snapshot for batched persistence."""
        ...

    async def enqueue_event(self, row: EventRow) -> None:
        """Queue an engine event for batched persistence."""
        ...

    async def flush(self) -> None:
        """Force an immediate flush of queued records."""
        ...