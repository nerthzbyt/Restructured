"""Rutas de trading: trades, ejecución, HFT, órdenes, balance y auditoría de decisiones."""
from __future__ import annotations

import asyncio
import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from nertz_core.db import OPEN_STATUSES, Trade, trade_order_link_id, utc_aware

_ML_FIELDS = (
    "timestamp", "symbol", "action", "decision", "order_id", "entry_price", "exit_price", "tp_price", "sl_price",
    "quantity", "profit_loss", "win", "combined", "ild", "egm", "rol", "pio", "ogm", "risk_reward_ratio",
    "outcome_status", "outcome_timestamp",
)
_ML_METRIC_FIELDS = ("combined", "ild", "egm", "rol", "pio", "ogm", "risk_reward_ratio")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build(bot) -> APIRouter:
    router = APIRouter()
    get_db = bot.database.get_db
    cfg = bot.config

    def _unsupported(symbol: str) -> Optional[Dict[str, Any]]:
        if symbol in bot.symbols:
            return None
        return {"message": f"⚠️ Símbolo no soportado: {symbol}", "timestamp": _now()}

    # ---------------------------------------------------------- trades
    @router.get("/profit")
    async def get_profit(db: Session = Depends(get_db)):
        return bot.profit_report(db)

    @router.get("/trades/{symbol}")
    async def get_trades(symbol: str, db: Session = Depends(get_db)):
        rows = db.query(Trade).filter(Trade.symbol == symbol).order_by(Trade.timestamp.desc()).all()
        trades = [bot.serialize_trade_for_api(t) for t in rows]
        bot.trades_cache[symbol] = trades
        return {"symbol": symbol, "trades": trades, "timestamp": _now(), "source": "sqlite_live"}

    @router.get("/last_trade/{symbol}")
    async def get_last_trade(symbol: str, db: Session = Depends(get_db)):
        last = db.query(Trade).filter_by(symbol=symbol).order_by(Trade.timestamp.desc()).first()
        return {"symbol": symbol, "last_trade": bot.serialize_trade_for_api(last) if last is not None else None,
                "timestamp": _now(), "source": "sqlite_live"}

    @router.get("/ml/dataset/trades")
    async def ml_dataset_trades(
        symbol: Optional[str] = None,
        limit: int = Query(default=5000, ge=1, le=200000),
        include_pending: bool = False,
        output: str = Query(default="json", pattern="^(json|csv)$"),
        db: Session = Depends(get_db),
    ):
        q = db.query(Trade)
        if symbol:
            q = q.filter(Trade.symbol == symbol)
        if not include_pending:
            q = q.filter(Trade.outcome_status == "final")
        rows: List[Dict[str, Any]] = []
        for t in q.order_by(Trade.timestamp.desc()).limit(int(limit)).all():
            pl = float(t.profit_loss or 0.0)
            rows.append({
                "timestamp": t.timestamp.isoformat() if t.timestamp else None,
                "symbol": t.symbol,
                "action": t.action,
                "decision": t.decision,
                "order_id": t.order_id,
                "entry_price": float(t.entry_price or 0.0),
                "exit_price": float(t.exit_price or 0.0),
                "tp_price": float(t.tp_price) if t.tp_price is not None else None,
                "sl_price": float(t.sl_price) if t.sl_price is not None else None,
                "quantity": float(t.quantity or 0.0),
                "profit_loss": pl,
                "win": 1 if pl > 0 else 0,
                **{k: float(getattr(t, k) or 0.0) for k in _ML_METRIC_FIELDS},
                "outcome_status": t.outcome_status,
                "outcome_timestamp": t.outcome_timestamp.isoformat() if t.outcome_timestamp else None,
            })
        if output == "csv":
            buf = io.StringIO()
            fieldnames: List[str] = list(_ML_FIELDS)
            w = csv.DictWriter(buf, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
            return PlainTextResponse(content=buf.getvalue(), media_type="text/csv")
        return {"count": len(rows), "rows": rows, "timestamp": _now()}

    # ------------------------------------------------------- execution
    @router.post("/execute_trade/{symbol}")
    async def execute_trade(symbol: str, collect_only: bool = False, force_trade: bool = False,
                            db: Session = Depends(get_db)):
        if (err := _unsupported(symbol)) is not None:
            return err
        await bot.core_cycle(symbol, db, collect_only=collect_only, force_trade=force_trade)
        return {"message": f"✅ Ciclo ejecutado para {symbol}", "collect_only": collect_only,
                "force_trade": force_trade, "timestamp": _now()}

    @router.post("/hft/start/{symbol}")
    async def start_hft(symbol: str, interval_ms: int = 250, collect_only: bool = True):
        if (err := _unsupported(symbol)) is not None:
            return err
        interval = max(0, int(interval_ms))
        started = bot.start_hft(symbol, interval_ms=interval, collect_only=bool(collect_only))
        return {"message": "✅ HFT iniciado" if started else "⚠️ HFT ya estaba corriendo", "symbol": symbol,
                "interval_ms": interval, "collect_only": bool(collect_only), "timestamp": _now()}

    @router.post("/hft/stop/{symbol}")
    async def stop_hft(symbol: str):
        if (err := _unsupported(symbol)) is not None:
            return err
        stopped = bot.stop_hft(symbol)
        return {"message": "🛑 HFT detenido" if stopped else "⚠️ HFT no estaba corriendo", "symbol": symbol,
                "timestamp": _now()}

    @router.post("/hft/run/{symbol}")
    async def run_hft(symbol: str, cycles: int = 100, interval_ms: int = 250, collect_only: bool = True):
        if (err := _unsupported(symbol)) is not None:
            return err
        interval = max(0, int(interval_ms))
        asyncio.create_task(bot.run_cycles(symbol, cycles=int(cycles), interval_ms=interval,
                                           collect_only=bool(collect_only)))
        return {"message": "✅ HFT run programado", "symbol": symbol, "cycles": int(cycles), "interval_ms": interval,
                "collect_only": bool(collect_only), "timestamp": _now()}

    def _hft_status(extra: bool = False) -> Dict[str, Any]:
        return {
            sym: {
                "running": bot.is_hft_running(sym),
                "params": bot.hft_params.get(sym) or {},
                **({"auto_hft_state": bot.auto_hft_state.get(sym) or {}} if extra else {}),
            }
            for sym in bot.symbols
        }

    @router.get("/mode/status")
    async def mode_status():
        return {"mode": bot.mode, "auto_hft_enabled": bot.auto_hft_enabled_effective(), "hft": _hft_status(True),
                "timestamp": _now()}

    @router.post("/mode/set")
    async def mode_set(
        mode: str = Query(pattern="^(normal|full|hft)$"),
        symbol: Optional[str] = None,
        interval_ms: int = Query(default=250, ge=0, le=60000),
        collect_only: bool = True,
    ):
        bot.mode = mode.lower()
        if not bot.running:
            bot.schedule_start()
        if bot.mode in {"normal", "full"}:
            return {"success": True, "mode": bot.mode, "stopped_hft": bot.stop_all_hft(), "timestamp": _now()}
        targets = [s.strip().upper() for s in symbol.split(",") if s.strip()] if symbol else list(bot.symbols)
        started = {sym: bot.start_hft(sym, interval_ms=int(interval_ms), collect_only=bool(collect_only))
                   for sym in targets if sym in bot.symbols}
        return {"success": True, "mode": bot.mode, "started_hft": started, "interval_ms": int(interval_ms),
                "collect_only": bool(collect_only), "timestamp": _now()}

    # ------------------------------------------------------ orders/balance
    @router.get("/balance")
    async def get_balance(account_type: Optional[str] = None, coin: Optional[str] = None):
        return await bot.record_balance(account_type=account_type, coin=coin)

    @router.get("/orders/status")
    async def get_orders_status(db: Session = Depends(get_db)):
        bybit_orders = await bot.exchange_open_orders_all()
        pending = bot.open_trades(db, 200)
        tracked_ids = {str(t.order_id) for t in pending if t.order_id}
        tracked_links = {trade_order_link_id(t) for t in pending} - {""}
        open_ids = {str(o.get("orderId")) for o in bybit_orders}
        open_links = {str(o.get("orderLinkId") or "").strip() for o in bybit_orders} - {""}

        payload_rows: List[Dict[str, Any]] = []
        for o in bybit_orders:
            oid = str(o.get("orderId"))
            link = str(o.get("orderLinkId") or "")
            row: Dict[str, Any] = {
                "orderId": oid,
                "symbol": str(o.get("symbol") or ""),
                "status": str(o.get("orderStatus") or ""),
                "side": str(o.get("side") or ""),
                "orderLinkId": link,
                **{k: o.get(k) for k in ("orderFilter", "orderType", "timeInForce")},
                "stopOrderType": str(o.get("stopOrderType") or ""),
                **{k: o.get(k) for k in ("triggerPrice", "takeProfit", "stopLoss", "qty", "price", "avgPrice",
                                         "cumExecQty", "createdTime", "updatedTime")},
                "tracked_in_db": oid in tracked_ids or (link.strip() in tracked_links),
            }
            payload_rows.append(row)
            if row["symbol"]:
                bot.set_order_status(oid, row["symbol"], row["status"].lower(), o)
        orphans = [r for r in payload_rows if not r["tracked_in_db"]]
        now = datetime.now(timezone.utc)
        return {
            "last_sync": bot.last_orders_sync_results or {},
            "agent_last_tick_ts": bot.agent_last_tick_ts,
            "auto_agent_enabled": bool(cfg.AUTO_AGENT_ENABLED),
            "bybit_open_orders": len(bybit_orders),
            "db_pending_trades": len(pending),
            "linked_open_orders": len(payload_rows) - len(orphans),
            "orphan_open_orders": len(orphans),
            "bybit_orders": payload_rows,
            "orphan_bybit_orders": orphans[:50],
            "db_pending": [
                {
                    "trade_id": t.trade_id,
                    "order_id": t.order_id,
                    "symbol": t.symbol,
                    "action": t.action,
                    "status": t.outcome_status,
                    "timestamp": t.timestamp.isoformat(),
                    "seconds_elapsed": (now - (utc_aware(t.timestamp) or now)).total_seconds(),
                    "present_in_bybit_open_orders": str(t.order_id) in open_ids
                    or trade_order_link_id(t) in open_links,
                }
                for t in pending
            ],
            "timestamp": _now(),
        }

    @router.post("/orders/sync")
    async def sync_orders(db: Session = Depends(get_db)):
        try:
            result = await bot.sync_open_orders(
                db,
                timeout_seconds=cfg.ORDERS_SYNC_TIMEOUT_S,
                update_after_seconds=cfg.ORDERS_SYNC_UPDATE_AFTER_S,
                limit=cfg.ORDERS_SYNC_LIMIT,
            )
        except Exception as e:
            return {"success": False, "message": f"Error interno: {e}", "timestamp": _now()}
        if result.get("success"):
            return {"success": True, "message": "Órdenes sincronizadas correctamente",
                    "details": result.get("results", {}), "timestamp": _now()}
        return {"success": False, "message": result.get("message", "Error desconocido"), "timestamp": _now()}

    @router.get("/order_status/{order_id}")
    async def get_order_status(order_id: str):
        return bot.order_status.get(order_id) or {"message": "Orden no encontrada", "order_id": order_id,
                                                   "timestamp": _now()}

    @router.get("/exchange/open_orders/{symbol}")
    async def exchange_open_orders(symbol: str, limit: int = 200):
        client = bot.bybit_client()
        if client is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}
        try:
            payload = await client.get_open_orders_merged(category=cfg.BYBIT_CATEGORY, symbol=symbol, limit=int(limit))
            return {"success": True, "symbol": symbol, "payload": payload, "timestamp": _now()}
        except Exception as e:
            return {"success": False, "symbol": symbol, "message": str(e), "timestamp": _now()}

    # --------------------------------------------------------- auditing
    @router.get("/decisions/{symbol}")
    async def get_decisions_audit(symbol: str, db: Session = Depends(get_db)):
        metrics = dict(bot.last_metrics_by_symbol.get(symbol) or {})
        if not metrics:
            metrics = bot.compute_metrics(symbol, bot.candles_for(symbol, db))[0]
        detail = bot.decision_detail(symbol, metrics)
        ctx = bot.operations.get(symbol) if bot.operations is not None else None
        pending = db.query(Trade).filter(Trade.symbol == symbol, Trade.outcome_status.in_(OPEN_STATUSES)).count()
        gates: Dict[str, Any] = {
            "live_trading_enabled": bool(cfg.LIVE_TRADING_ENABLED),
            "cooldown_s": float(cfg.for_symbol(symbol, "TRADE_COOLDOWN_S")),
            "can_trade": bool(ctx.can_trade()) if ctx is not None else True,
            "allow_multiple_active_trades": bool(cfg.ALLOW_MULTIPLE_ACTIVE_TRADES),
            "pending_trades_db": int(pending),
            "trade_cycle": f"kline_{cfg.TIMEFRAME}_close_only",
            "ml_enabled": bool(cfg.ML_ENABLED),
        }
        would_trade = detail.get("decision") in {"buy", "sell"} and gates["live_trading_enabled"] and gates["can_trade"]
        if not cfg.ALLOW_MULTIPLE_ACTIVE_TRADES and pending > 0:
            would_trade = False
            gates["blocked_by"] = "active_trade_single_mode"
        elif detail.get("decision") == "hold":
            gates["blocked_by"] = "decision_hold"
        elif not gates["live_trading_enabled"]:
            gates["blocked_by"] = "live_trading_disabled"
        else:
            gates["blocked_by"] = None
        window = bot.metrics_window.get(symbol) or []
        return {
            "symbol": symbol,
            "decision_detail": detail,
            "execution_gates": gates,
            "would_trade_on_next_kline": bool(would_trade),
            "recent_snapshot_decisions": [r for r in list(window)[-15:] if isinstance(r, dict)],
            "timestamp": _now(),
        }

    @router.get("/operations/status")
    async def operations_status():
        snap = bot.operations.snapshot() if bot.operations is not None else {}
        for sym in bot.symbols:
            if sym in snap:
                snap[sym]["decisions_window_len"] = len(bot.metrics_window.get(sym) or [])
        return snap

    return router
