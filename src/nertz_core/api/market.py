"""Rutas de mercado y métricas (lectura)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from nertz_core.db import MarketData, MarketTicker, Orderbook
from nertz_core.market import candle_inputs
from utils import calculate_discovery_metrics


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _candle_rows(candles: List[Any]) -> List[Dict[str, Any]]:
    return [
        {"timestamp": c.timestamp.isoformat() if c.timestamp else None, "open": float(c.open), "high": float(c.high),
         "low": float(c.low), "close": float(c.close), "volume": float(c.volume)}
        for c in candles
    ]


def build(bot) -> APIRouter:
    router = APIRouter()
    get_db = bot.database.get_db
    cfg = bot.config

    def _db_candles(db: Session, symbol: str, limit: int) -> List[Any]:
        return (
            db.query(MarketData).filter(MarketData.symbol == symbol)
            .order_by(MarketData.timestamp.desc()).limit(limit).all()
        )

    def _metrics(symbol: str, db: Session) -> Dict[str, Any]:
        return bot.compute_metrics(symbol, bot.candles_for(symbol, db))[0]

    @router.get("/market_data/{symbol}")
    async def get_market_data(symbol: str, db: Session = Depends(get_db)):
        return {"symbol": symbol, "candles": _candle_rows(bot.candles_for(symbol, db, limit=5))}

    @router.get("/ticker/{symbol}")
    async def get_ticker(symbol: str, db: Session = Depends(get_db)):
        live = bot.ticker_data.get(symbol) or {}
        if float(live.get("last_price") or 0.0) > 0:
            return {"symbol": symbol, "source": "websocket_live",
                    **{k: float(live.get(k) or 0.0) for k in ("last_price", "volume_24h", "high_24h", "low_24h")},
                    "timestamp": _now()}
        row = db.query(MarketTicker).filter(MarketTicker.symbol == symbol).order_by(MarketTicker.timestamp.desc()).first()
        return {
            "symbol": symbol,
            "source": "sqlite" if row else "none",
            **{k: float(getattr(row, k)) if row else 0.0 for k in ("last_price", "volume_24h", "high_24h", "low_24h")},
            "timestamp": row.timestamp.isoformat() if row else _now(),
        }

    @router.get("/metrics/{symbol}")
    async def get_metrics(symbol: str, db: Session = Depends(get_db)):
        return {"symbol": symbol, "metrics": _metrics(symbol, db), "timestamp": _now()}

    @router.get("/combined/{symbol}")
    async def get_combined(symbol: str, db: Session = Depends(get_db)):
        candles = bot.candles_for(symbol, db)
        book = bot.orderbook_data.get(symbol)
        live_ticker = bot.ticker_data.get(symbol) or {}
        ob_row = None if book is not None and book.is_ready() else (
            db.query(Orderbook).filter(Orderbook.symbol == symbol).order_by(Orderbook.timestamp.desc()).first()
        )
        tk_row = db.query(MarketTicker).filter(MarketTicker.symbol == symbol).order_by(MarketTicker.timestamp.desc()).first()
        metrics = bot.compute_metrics(symbol, candles)[0]

        def _tk(key: str) -> float:
            return float(live_ticker.get(key) or 0.0) or (float(getattr(tk_row, key)) if tk_row else 0.0)

        return {
            "symbol": symbol,
            "candles": _candle_rows(candles),
            "orderbook": {
                "timestamp": ob_row.timestamp.isoformat() if ob_row else None,
                "bids": book["bids"] if ob_row is None and book is not None else (ob_row.bids if ob_row else []),
                "asks": book["asks"] if ob_row is None and book is not None else (ob_row.asks if ob_row else []),
            },
            "ticker": {
                "timestamp": tk_row.timestamp.isoformat() if tk_row else None,
                **{k: _tk(k) for k in ("last_price", "volume_24h", "high_24h", "low_24h")},
            },
            "recent_trades": list(bot.recent_trades.get(symbol) or [])[-10:],
            "metrics": metrics,
            "decision": bot.decision_detail(symbol, metrics),
            "timestamp": _now(),
        }

    def _discovery(symbol: str, db: Session, limit: int) -> Dict[str, Any]:
        candles = _db_candles(db, symbol, limit) or bot.candles_for(symbol)
        return calculate_discovery_metrics(
            candle_inputs(candles),
            bot.orderbook_data.get(symbol) or {"bids": [], "asks": []},
            bot.ticker_data.get(symbol) or {"last_price": 0.0},
            list(bot.recent_trades.get(symbol) or []),
        )

    def _component(name: str, with_discovery: bool):
        async def endpoint(symbol: str, db: Session = Depends(get_db)):
            prod = _metrics(symbol, db)
            payload: Dict[str, Any] = {
                "symbol": symbol,
                "timestamp": _now(),
                name: float(prod.get(name) or 0.0),
                f"{name}_raw": float(prod.get(f"{name}_raw") or 0.0),
                "components": (
                    _discovery(symbol, db, cfg.CANDLE_BUFFER_SIZE).get("combined") or {} if with_discovery else prod
                ),
            }
            return payload

        endpoint.__name__ = f"get_{name}"
        return endpoint

    for metric_name, with_disc in (("ild", True), ("rol", True), ("pio", False), ("egm", False), ("ogm", False)):
        router.add_api_route(f"/{metric_name}/{{symbol}}", _component(metric_name, with_disc), methods=["GET"])

    @router.get("/discovery/metrics/{symbol}")
    async def get_discovery_metrics(symbol: str, db: Session = Depends(get_db)):
        legacy = _discovery(symbol, db, int(cfg.DISCOVERY_CANDLES_LOOKBACK))
        base = _metrics(symbol, db)
        ticker = bot.ticker_data.get(symbol) or {}

        def _b(key: str) -> float:
            return float(base.get(key) or 0.0)

        return {
            "symbol": symbol,
            "timestamp": _now(),
            "rol": _b("rol"),
            "rol_raw": _b("rol_raw"),
            "components": {
                "egm": {"pressure": _b("asymmetry"), "flow": _b("tfi"), "momentum": _b("mom_raw")},
                "pio": {"rvol": _b("rvol"), "turnover": float(ticker.get("turnover_24h") or 0.0)},
                "microstructure": {"spread_bps": _b("spread_bps"), "microprice_offset": _b("microprice_offset_bps"),
                                   "weighted_liquidity": _b("weighted_liquidity")},
                "ild_rol_raw": {"ild": _b("ild_raw"), "rol": _b("rol_raw")},
            },
            "legacy_combined": legacy.get("combined") or {},
        }

    @router.get("/orderbook/{symbol}")
    async def get_orderbook(symbol: str):
        book = bot.orderbook_data.get(symbol)
        view = book.as_dict() if book is not None else {"bids": [], "asks": []}
        return {"symbol": symbol, **view, "timestamp": _now()}

    @router.get("/candles/{symbol}/{limit}")
    async def get_candles(symbol: str, limit: int = 5, db: Session = Depends(get_db)):
        lim = max(1, min(int(cfg.MAX_CANDLES_API), int(limit)))
        candles = bot.candles_for(symbol, db, limit=lim)
        if len(candles) < lim:
            candles = _db_candles(db, symbol, lim) or candles
        return {"symbol": symbol, "candles": _candle_rows(candles), "timestamp": _now()}

    return router
