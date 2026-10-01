"""Rutas de sistema/administración: ciclo de vida, config en caliente, agente, ML, storage y validación."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from nertz_core.db import MarketData, MarketTicker, MetricSnapshot, Orderbook, Trade, trade_order_link_id, utc_aware
from optimizer import optimize_system_from_trades
from settings import ConfigError
from signal_engine import CombinedWeights, Thresholds


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build(bot) -> APIRouter:
    router = APIRouter()
    get_db = bot.database.get_db
    cfg = bot.config

    def _apply(values: Dict[str, Any], source: str) -> Dict[str, Any]:
        try:
            return cfg.update(values, source=source)
        except ConfigError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

    def _log_action(kind: str, **data: Any) -> None:
        bot.actions().append({"type": kind, "ts": _now(), **data})

    # ------------------------------------------------------- lifecycle
    @router.post("/start")
    async def start_bot():
        started_main = bot.schedule_start()
        started_support = bot.start_support_loop(interval_s=cfg.SUPPORT_LOOP_INTERVAL_S)
        msg = "✅ Bot iniciado" if (started_main or started_support) else "⚠️ Bot ya está corriendo"
        return {"message": msg, "timestamp": _now()}

    @router.post("/stop")
    async def stop_bot():
        if bot.running:
            bot.stop()
            return {"message": "🛑 Bot detenido", "timestamp": _now()}
        return {"message": "⚠️ Bot ya está detenido", "timestamp": _now()}

    @router.get("/status")
    async def get_status():
        return {
            "running": bot.running,
            "iterations": bot.iterations,
            "symbols": bot.symbols,
            "support_loop_running": bool(bot.support_task is not None and not bot.support_task.done()),
            "mode": bot.mode,
            "auto_hft_enabled": bot.auto_hft_enabled_effective(),
            "hft": {s: {"running": bot.is_hft_running(s), "params": bot.hft_params.get(s) or {}} for s in bot.symbols},
            "timestamp": _now(),
        }

    @router.get("/health")
    async def health_check():
        return {"status": "healthy" if bot.running else "unhealthy", "timestamp": _now()}

    @router.post("/symbols/add")
    async def add_symbol(symbol: str):
        try:
            return await bot.add_symbol(symbol)
        except ConfigError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

    # ---------------------------------------------------------- config
    @router.get("/config")
    async def get_config():
        return {
            "symbol": cfg.SYMBOL,
            "timeframe": cfg.TIMEFRAME,
            "order_type": cfg.ORDER_TYPE,
            "time_in_force": cfg.TIME_IN_FORCE,
            "orderbook_depth": cfg.ORDERBOOK_DEPTH,
            "bybit_env": cfg.BYBIT_ENV,
            "live_trading_enabled": cfg.LIVE_TRADING_ENABLED,
            "capital_usdt": cfg.CAPITAL_USDT,
            "risk_factor": cfg.RISK_FACTOR,
            "min_trade_size": cfg.MIN_TRADE_SIZE,
            "max_trade_size": cfg.MAX_TRADE_SIZE,
            "fee_rate": cfg.FEE_RATE,
            "tp_percentage": cfg.TP_PERCENTAGE,
            "sl_percentage": cfg.SL_PERCENTAGE,
            "egm_buy_threshold": cfg.EGM_BUY_THRESHOLD,
            "egm_sell_threshold": cfg.EGM_SELL_THRESHOLD,
            "combined_buy_threshold": cfg.COMBINED_BUY_THRESHOLD,
            "combined_sell_threshold": cfg.COMBINED_SELL_THRESHOLD,
            "all": cfg.as_dict(),
            "timestamp": _now(),
        }

    @router.get("/config/schema")
    async def config_schema():
        return {"settings": cfg.schema(), "recent_changes": cfg.recent_changes(), "timestamp": _now()}

    @router.post("/config/update")
    async def config_update(values: Dict[str, Any] = Body(...)):
        """Cambio en caliente de cualquier clave registrada (con la misma validación que el archivo .env)."""
        return {"success": True, "changes": _apply(values, "api"), "timestamp": _now()}

    @router.post("/config/update_all")
    async def update_all_config(config_data: dict):
        mapping = {
            "capital_usdt": "CAPITAL_USDT",
            "risk_factor": "RISK_FACTOR",
            "egm_buy_threshold": "EGM_BUY_THRESHOLD",
            "egm_sell_threshold": "EGM_SELL_THRESHOLD",
            "combined_buy_threshold": "COMBINED_BUY_THRESHOLD",
            "combined_sell_threshold": "COMBINED_SELL_THRESHOLD",
        }
        values: Dict[str, Any] = {mapping.get(k) or k.upper(): v for k, v in (config_data or {}).items()}
        changes = _apply(values, "api_update_all")
        return {"message": "Configuración actualizada", "changes": changes, "timestamp": _now()}

    @router.post("/config/update_thresholds")
    async def update_thresholds(egm_buy_threshold: float, egm_sell_threshold: float):
        _apply({"EGM_BUY_THRESHOLD": egm_buy_threshold, "EGM_SELL_THRESHOLD": egm_sell_threshold}, "api")
        return {"message": "Umbrales actualizados"}

    @router.get("/settings")
    async def get_settings(db: Session = Depends(get_db)):
        return {
            symbol: {
                "symbol": symbol,
                "capital": bot.capital,
                "risk_factor": cfg.for_symbol(symbol, "RISK_FACTOR"),
                "min_trade_size": cfg.for_symbol(symbol, "MIN_TRADE_SIZE"),
                "max_trade_size": cfg.for_symbol(symbol, "MAX_TRADE_SIZE"),
                "metrics": {"symbol": symbol, "metrics": bot.compute_metrics(symbol, bot.candles_for(symbol, db))[0],
                            "timestamp": _now()},
            }
            for symbol in bot.symbols
        }

    # ------------------------------------------------------------- ML
    @router.get("/ml/status")
    async def ml_status():
        return {
            "enabled": cfg.ML_ENABLED,
            "models": bot.ml_models,
            "auto_agent_enabled": cfg.AUTO_AGENT_ENABLED,
            "auto_agent": {"last_tick_ts": bot.agent_last_tick_ts,
                           "recent_actions": list(bot.agent_events.get("actions") or [])},
            "timestamp": _now(),
        }

    @router.post("/ml/train")
    async def ml_train(symbol: Optional[str] = None,
                       min_samples: Optional[int] = Query(default=None, ge=10, le=50000),
                       db: Session = Depends(get_db)):
        return bot.train_ml_model_from_trades(db, symbol=symbol, min_samples=min_samples)

    # ---------------------------------------------------------- agent
    @router.get("/admin/agent/status")
    async def admin_agent_status():
        window_s = bot.metrics_window_s()
        decisions, _ = bot.recent_decisions(bot.symbols, window_s, int(cfg.AGENT_DECISIONS_MAX))
        total = len(decisions)
        counts = {k: sum(1 for d in decisions if d == k) for k in ("hold", "buy", "sell")}
        return {
            "enabled": cfg.AUTO_AGENT_ENABLED,
            "last_tick_ts": bot.agent_last_tick_ts,
            "last_relax_ts": bot.agent_last_relax_ts,
            "thresholds": bot.thresholds_payload(),
            "recent_actions": list(bot.agent_events.get("actions") or []),
            "metrics_window_s": window_s,
            "snapshots_seen": total,
            "hold_count": counts["hold"],
            "buy_count": counts["buy"],
            "sell_count": counts["sell"],
            "hold_ratio": counts["hold"] / total if total else 0.0,
            "note": (f"Snapshots cada ~{cfg.METRICS_SNAPSHOT_INTERVAL_S:g}s; trades solo en cierre de vela "
                     f"{cfg.TIMEFRAME} si la decisión pasa los gates de ejecución."),
            "timestamp": _now(),
        }

    @router.post("/admin/agent/enable")
    async def admin_agent_enable(enabled: bool = True):
        _apply({"AUTO_AGENT_ENABLED": enabled}, "api")
        _log_action("set_auto_agent", enabled=bool(enabled))
        await bot.record_event({"type": "agent_action", "action": "set_auto_agent", "enabled": bool(enabled)})
        return {"success": True, "auto_agent_enabled": cfg.AUTO_AGENT_ENABLED, "timestamp": _now()}

    @router.post("/admin/agent/tick")
    async def admin_agent_tick(db: Session = Depends(get_db)):
        await bot.agent_tick(db)
        return {"success": True, "last_tick_ts": bot.agent_last_tick_ts, "timestamp": _now()}

    @router.post("/admin/agent/relax_thresholds")
    async def admin_agent_relax_thresholds(factor: float = Query(default=0.9, gt=0.5, lt=1.0)):
        change = bot.relax_thresholds(factor, source="manual_relax")
        _log_action("manual_relax_thresholds", factor=float(factor), **change)
        await bot.record_event({"type": "agent_action", "action": "manual_relax_thresholds",
                                "factor": float(factor), **change})
        return {"success": True, **change, "timestamp": _now()}

    # ------------------------------------------------------------ TP/SL
    @router.get("/admin/tpsl/status")
    async def admin_tpsl_status():
        recent = [a for a in list(bot.agent_events.get("actions") or [])[-80:]
                  if isinstance(a, dict) and a.get("type") == "auto_tpsl_amend"]
        return {"enabled": cfg.AUTO_TPSL_ENABLED, "interval_s": cfg.AUTO_TPSL_INTERVAL_S,
                "last_tick_ts": bot.auto_tpsl_last_tick_ts, "recent_actions": recent, "timestamp": _now()}

    @router.post("/admin/tpsl/tick")
    async def admin_tpsl_tick(db: Session = Depends(get_db)):
        return await bot.auto_tpsl_tick(db)

    @router.post("/admin/tpsl/enabled")
    async def admin_tpsl_enabled(enabled: bool = True):
        _apply({"AUTO_TPSL_ENABLED": enabled}, "api")
        return {"success": True, "enabled": cfg.AUTO_TPSL_ENABLED, "timestamp": _now()}

    # --------------------------------------------------------- auto HFT
    @router.post("/admin/auto_hft/enable")
    async def admin_auto_hft_enable(enabled: bool = True):
        _apply({"AUTO_HFT_ENABLED": enabled}, "api")
        bot.auto_hft_enabled = bool(enabled)
        _log_action("set_auto_hft", enabled=bool(enabled))
        await bot.record_event({"type": "auto_hft", "action": "set_enabled", "enabled": bool(enabled)})
        return {"success": True, "auto_hft_enabled": bool(enabled), "timestamp": _now()}

    @router.get("/admin/auto_hft/status")
    async def admin_auto_hft_status():
        return {
            "enabled": bot.auto_hft_enabled_effective(),
            "tick_s": cfg.AUTO_HFT_TICK_S,
            "window_s": cfg.AUTO_HFT_WINDOW_S,
            "min_snapshots": cfg.AUTO_HFT_MIN_SNAPSHOTS,
            "start_ratio": cfg.AUTO_HFT_START_RATIO,
            "stop_ratio": cfg.AUTO_HFT_STOP_RATIO,
            "combined_abs_threshold": cfg.AUTO_HFT_COMBINED_ABS_THRESHOLD,
            "interval_ms": cfg.AUTO_HFT_INTERVAL_MS,
            "collect_only": cfg.AUTO_HFT_COLLECT_ONLY,
            "cooldown_s": cfg.AUTO_HFT_COOLDOWN_S,
            "timestamp": _now(),
        }

    # -------------------------------------------------------- optimizer
    @router.post("/admin/optimize/system")
    async def admin_optimize_system(
        symbol: Optional[str] = None,
        limit: int = Query(default=2000, ge=50, le=200000),
        iterations: int = Query(default=900, ge=50, le=50000),
        seed: Optional[int] = None,
        apply: bool = False,
        db: Session = Depends(get_db),
    ):
        q = db.query(Trade).filter(Trade.outcome_status == "final")
        if symbol:
            q = q.filter(Trade.symbol == symbol)
        trades = q.order_by(Trade.timestamp.desc()).limit(int(limit)).all()
        start_th = Thresholds(*bot.thresholds_for(symbol))
        start_w = CombinedWeights.from_raw(bot.get_combined_weights(symbol) if symbol else cfg.COMBINED_WEIGHTS_JSON)
        before = {"thresholds": bot.thresholds_payload(), "weights": start_w.as_dict()}
        res = optimize_system_from_trades(trades, start_thresholds=start_th, start_weights=start_w,
                                          iterations=int(iterations), seed=seed, params=cfg.signal_params)
        applied, persisted = False, None
        if apply and res.success and isinstance(res.best, dict):
            th = res.best.get("thresholds") or {}
            if th:
                _apply({"COMBINED_BUY_THRESHOLD": th.get("combined_buy_threshold", cfg.COMBINED_BUY_THRESHOLD),
                        "COMBINED_SELL_THRESHOLD": th.get("combined_sell_threshold", cfg.COMBINED_SELL_THRESHOLD),
                        "COMBINED_HOLD_BAND": th.get("combined_hold_band", cfg.COMBINED_HOLD_BAND)}, "optimizer")
            if isinstance(res.best.get("weights"), dict):
                bot.set_combined_weights(symbol, res.best["weights"])
            if cfg.PERSIST_THRESHOLDS_TO_ENV:
                persisted = bot.persist_thresholds_to_env()
            applied = True
        return {
            "success": bool(res.success),
            "symbol": symbol,
            "trades_used": len(trades),
            "before": before,
            "result": {"baseline": res.baseline, "best": res.best, "searched": res.searched, "timestamp": res.timestamp},
            "applied": applied,
            "persisted": persisted,
            "after": {"thresholds": bot.thresholds_payload(),
                      "weights": bot.get_combined_weights(symbol) if symbol else None},
            "timestamp": _now(),
        }

    @router.post("/admin/full_reset")
    async def admin_full_reset(sample_size: int = 500, alpha: float = 1.0, cancel_bybit_orders: bool = True,
                               db: Session = Depends(get_db)):
        calibrate = bot.force_calibrate_thresholds(db, sample_size=int(sample_size), alpha=float(alpha))
        env_update = bot.persist_thresholds_to_env() if cfg.PERSIST_THRESHOLDS_TO_ENV else {
            "success": False, "message": "persist_disabled"}
        cancel_result = await bot.cancel_all_open_orders(symbol=None, limit=200) if cancel_bybit_orders else None
        bot.stop()
        wiped = bot.wipe_database(db)
        bot.reset_runtime_state()
        results_path = bot.reset_results_json()
        bot.schedule_start()
        return {"success": True, "thresholds": bot.thresholds_payload(), "calibration": calibrate,
                "persist_env": env_update, "cancel_bybit_orders": cancel_result, "wiped": wiped,
                "results_json": results_path, "timestamp": _now()}

    # --------------------------------------------------------- storage
    @router.get("/storage/status")
    async def storage_status():
        storage = bot.storage
        paths = bot.paths
        return {
            "backend": cfg.STORAGE_BACKEND,
            "active": storage is not None,
            "path": getattr(storage, "path", None) if storage is not None else paths.storage_path,
            "duckdb_path": paths.storage_path,
            "sqlite_path": paths.sqlite_path,
            "jsonl_path": os.path.join(paths.data_dir, "metrics_snapshots.jsonl"),
            "wal_present": os.path.exists(f"{paths.storage_path}.wal"),
            "analysis_jsonl": "metrics_snapshots.jsonl — espejo legible; DuckDB es la serie HF del bot",
            "pycharm_hint": ("DuckDB: jdbc:duckdb:path/to/nertz.duckdb?duckdb.read_only=true (NO abrir .wal). "
                             "Si falla: detén el bot o ejecuta scripts/release_duckdb_lock.ps1. Trades en SQLite."),
            "batch_interval_ms": cfg.STORAGE_BATCH_INTERVAL_MS,
            "orderbook_persist_interval_ms": cfg.ORDERBOOK_PERSIST_INTERVAL_MS,
            "ticker_persist_interval_ms": cfg.TICKER_PERSIST_INTERVAL_MS,
            "jsonl_disabled": cfg.STORAGE_DISABLE_JSONL,
            "sqlite_mirror": cfg.STORAGE_SQLITE_MIRROR,
            "timestamp": _now(),
        }

    @router.get("/storage/recent/{symbol}")
    async def storage_recent(symbol: str, limit: int = 10, db: Session = Depends(get_db)):
        sym = str(symbol or "").strip().upper()
        lim = max(1, min(100, int(limit)))
        storage = bot.storage
        payload: Dict[str, Any] = {"symbol": sym, "limit": lim, "duckdb_active": storage is not None,
                                   "sqlite_mirror": cfg.STORAGE_SQLITE_MIRROR}
        if storage is not None and hasattr(storage, "fetch_recent"):
            try:
                payload["duckdb"] = await storage.fetch_recent(sym, limit=lim)
            except Exception as e:
                payload["duckdb_error"] = str(e)
        try:
            def _q(model):
                return db.query(model).filter(model.symbol == sym)

            payload["sqlite"] = {
                "orderbook_count": _q(Orderbook).count(),
                "ticker_count": _q(MarketTicker).count(),
                "metric_count": _q(MetricSnapshot).count(),
                "orderbook": [
                    {"timestamp": r.timestamp.isoformat(), "bid_levels": len(r.bids or []),
                     "ask_levels": len(r.asks or []),
                     "best_bid": float(r.bids[0][0]) if r.bids else None,
                     "best_ask": float(r.asks[0][0]) if r.asks else None}
                    for r in _q(Orderbook).order_by(Orderbook.timestamp.desc()).limit(lim).all()
                ],
                "ticks": [
                    {"timestamp": r.timestamp.isoformat(), "last_price": float(r.last_price),
                     "volume_24h": float(r.volume_24h)}
                    for r in _q(MarketTicker).order_by(MarketTicker.timestamp.desc()).limit(lim).all()
                ],
                "metrics": [
                    {"timestamp": r.timestamp.isoformat(), "decision": r.decision, "combined": float(r.combined),
                     "last_price": float(r.last_price)}
                    for r in _q(MetricSnapshot).order_by(MetricSnapshot.timestamp.desc()).limit(lim).all()
                ],
            }
        except Exception as e:
            payload["sqlite_error"] = str(e)
        book = bot.orderbook_data.get(sym)
        payload["live_memory"] = {
            "orderbook_levels": {"bids": len(book["bids"]) if book else 0, "asks": len(book["asks"]) if book else 0},
            "last_price": float((bot.ticker_data.get(sym) or {}).get("last_price") or 0.0),
            "candles_in_memory": len(bot.candles.get(sym) or []),
            "trade_cycle": f"kline_{cfg.TIMEFRAME}_close_only",
        }
        payload["timestamp"] = _now()
        return payload

    # ------------------------------------------------------- validation
    @router.get("/validation")
    async def get_validation(db: Session = Depends(get_db)):
        now = datetime.now(timezone.utc)
        ws = bot.ws
        ws_open = bool(ws is not None and not getattr(ws, "closed", False))
        start_ok = bot.start_task is not None and not bot.start_task.done()
        support_ok = bot.support_task is not None and not bot.support_task.done()
        layer1 = {"ok": bool(bot.running and start_ok and support_ok and ws_open), "running_flag": bool(bot.running),
                  "start_task_running": start_ok, "support_task_running": support_ok, "websocket_open": ws_open}

        max_age = float(cfg.VALIDATION_MARKET_MAX_AGE_S)
        market: Dict[str, Any] = {}
        market_ok = True
        for sym in bot.symbols:
            book = bot.orderbook_data.get(sym)
            ob_age = (now.timestamp() - book.updated_ts) if book is not None and book.updated_ts else None
            tk = db.query(MarketTicker.timestamp).filter(MarketTicker.symbol == sym).order_by(
                MarketTicker.timestamp.desc()).first()
            kl = db.query(MarketData.timestamp).filter(MarketData.symbol == sym).order_by(
                MarketData.timestamp.desc()).first()
            tk_age = (now - utc_aware(tk[0])).total_seconds() if tk else None
            kl_age = (now - utc_aware(kl[0])).total_seconds() if kl else None
            market[sym] = {"orderbook_age_s": ob_age, "ticker_age_s": tk_age, "kline_age_s": kl_age}
            if ob_age is None or ob_age > max_age or (cfg.STORAGE_SQLITE_MIRROR and (tk_age is None or tk_age > max_age)):
                market_ok = False

        pending = bot.open_trades(db, 500)
        tracked_ids = {str(t.order_id) for t in pending if t.order_id}
        tracked_links = {trade_order_link_id(t) for t in pending} - {""}
        layer3 = {"ok": True, "db_pending_trades": len(pending), "tracked_order_ids": len(tracked_ids),
                  "tracked_link_ids": len(tracked_links), "now": now.isoformat(), "now_s": now.timestamp()}

        orders = await bot.exchange_open_orders_all()
        orphan = orphan_bot = linked = 0
        for o in orders:
            oid = str(o.get("orderId"))
            link = str(o.get("orderLinkId") or "").strip()
            if oid in tracked_ids or (link and link in tracked_links):
                linked += 1
                continue
            orphan += 1
            orphan_bot += 1 if cfg.is_bot_order_link(link) else 0
        layer4 = {"ok": orphan_bot == 0, "bybit_open_orders": len(orders), "orphan_open_orders": orphan,
                  "orphan_bot_candidates": orphan_bot, "linked_open_orders": linked}
        layer2 = {"ok": market_ok, "by_symbol": market}
        overall = all(layer["ok"] for layer in (layer1, layer2, layer3, layer4))
        return {"ok": overall, "layer1_process": layer1, "layer2_market_data": layer2, "layer3_db": layer3,
                "layer4_orders": layer4, "timestamp": now.isoformat()}

    return router
