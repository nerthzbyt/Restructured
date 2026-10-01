"""Per-symbol isolation for multi-operation trading."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional


@dataclass
class SymbolContext:
    symbol: str
    hft_running: bool = False
    hft_interval_ms: int = 250
    collect_only: bool = False
    last_trade_ts: float = 0.0
    cooldown_s: float = 0.0
    metrics_window: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=2500))
    combined_weights: Dict[str, float] = field(default_factory=dict)
    ml_model: Optional[Dict[str, Any]] = None

    def can_trade(self, now: Optional[float] = None) -> bool:
        if float(self.cooldown_s) <= 0.0:
            return True
        ts = float(now if now is not None else time.time())
        return (ts - float(self.last_trade_ts)) >= float(self.cooldown_s)

    def mark_trade(self, now: Optional[float] = None) -> None:
        self.last_trade_ts = float(now if now is not None else time.time())


class OperationManager:
    """Coordinates concurrent symbol operations with a global order semaphore."""

    def __init__(
        self,
        symbols: list[str],
        *,
        max_concurrent_orders: int = 3,
        default_cooldown_s: float = 0.0,
    ) -> None:
        self._contexts: Dict[str, SymbolContext] = {
            s: SymbolContext(symbol=s, cooldown_s=float(default_cooldown_s)) for s in symbols
        }
        self._order_sem = asyncio.Semaphore(max(1, int(max_concurrent_orders)))

    def symbols(self) -> list[str]:
        return list(self._contexts.keys())

    def get(self, symbol: str) -> SymbolContext:
        sym = str(symbol or "").strip()
        if sym not in self._contexts:
            self._contexts[sym] = SymbolContext(symbol=sym)
        return self._contexts[sym]

    async def acquire_order_slot(self) -> None:
        await self._order_sem.acquire()

    def release_order_slot(self) -> None:
        self._order_sem.release()

    def snapshot(self) -> Dict[str, Any]:
        return {
            sym: {
                "hft_running": ctx.hft_running,
                "hft_interval_ms": ctx.hft_interval_ms,
                "collect_only": ctx.collect_only,
                "last_trade_ts": ctx.last_trade_ts,
                "cooldown_s": ctx.cooldown_s,
                "metrics_window_len": len(ctx.metrics_window),
            }
            for sym, ctx in self._contexts.items()
        }