"""TP/SL virtual para spot: trailing en BD y cierre con orden real al dispararse."""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from nertz_core.accounting import executed_entry, trade_pnl
from nertz_core.db import OPEN_STATUSES, Trade, merge_raw
from nertz_core.engine.host import EngineHost

logger = logging.getLogger("NertzMetalEngine")


@dataclass(frozen=True)
class TrailParams:
    gap: float
    gap_min: float
    tp_ext_mult: float
    ml_tp_boost: float
    ml_threshold: float
    tp_proximity: float
    min_ext: float
    fee_rate: float


def trail_levels(
    action: str,
    last_price: float,
    entry: float,
    tp_old: float,
    sl_old: float,
    tick: float,
    p: TrailParams,
    ml_p: Optional[float],
) -> tuple[float, float]:
    """Nuevo (TP, SL) con trailing: el SL solo avanza, protege break-even con fees y extiende el TP cerca del objetivo."""
    profit = (last_price - entry) / entry if action == "buy" else (entry - last_price) / entry
    confident = ml_p is not None and ml_p >= p.ml_threshold
    ext = p.gap * p.tp_ext_mult * (max(1.0, p.ml_tp_boost) if confident else 1.0)
    if action == "buy":
        breakeven = entry * (1.0 + 2.0 * p.fee_rate)
        sl_cand = last_price * (1.0 - p.gap)
        if profit > 0:
            sl_cand = max(sl_cand, breakeven)
        sl = sl_cand if sl_old <= 0 else max(sl_old, sl_cand)
        tp = tp_old if tp_old > 0 else last_price * (1.0 + max(p.gap, p.min_ext))
        if tp_old > 0 and last_price >= tp_old * (1.0 - p.tp_proximity):
            tp = max(tp, last_price * (1.0 + max(ext, p.min_ext)))
        return max(tp, last_price + tick), min(sl, last_price - tick)
    breakeven = entry * (1.0 - 2.0 * p.fee_rate)
    sl_cand = last_price * (1.0 + p.gap)
    if profit > 0:
        sl_cand = min(sl_cand, breakeven)
    sl = sl_cand if sl_old <= 0 else min(sl_old, sl_cand)
    tp = tp_old if tp_old > 0 else last_price * (1.0 - max(p.gap, p.min_ext))
    if tp_old > 0 and last_price <= tp_old * (1.0 + p.tp_proximity):
        tp = min(tp, last_price * (1.0 - max(ext, p.min_ext)))
    return min(tp, last_price - tick), max(sl, last_price + tick)


def trigger_reason(action: str, last_price: float, tp: float, sl: float) -> Optional[str]:
    if action == "buy":
        if 0 < tp <= last_price:
            return "tp"
        if sl > 0 and last_price <= sl:
            return "sl"
    else:
        if tp > 0 and last_price <= tp:
            return "tp"
        if 0 < sl <= last_price:
            return "sl"
    return None


class TPSLMixin(EngineHost):
    async def _auto_tpsl_tick(self, db: Session) -> Dict[str, Any]:
        cfg = self.config
        if not self.running:
            return {"success": True, "results": {"skipped": 1, "reason": "not_running"}}
        if not cfg.LIVE_TRADING_ENABLED:
            return {"success": True, "results": {"skipped": 1, "mode": "disabled"}}
        interval_s = max(0.25, float(cfg.AUTO_TPSL_INTERVAL_S))
        if time.time() - self._auto_tpsl_last_tick_ts < interval_s:
            return {"success": True, "results": {"skipped": 1, "reason": "rate_limited"}}
        if self._bybit_client() is None:
            return {"success": False, "message": "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"}

        async with self._auto_tpsl_lock:
            now_ts = time.time()
            if now_ts - self._auto_tpsl_last_tick_ts < interval_s:
                return {"success": True, "results": {"skipped": 1, "reason": "rate_limited"}}
            self._auto_tpsl_last_tick_ts = now_ts

            results: Dict[str, int] = dict.fromkeys(("checked", "amended", "db_updated", "skipped", "errors", "executed_virtual"), 0)
            trades = (
                db.query(Trade)
                .filter(Trade.outcome_status.in_(OPEN_STATUSES))
                .order_by(Trade.timestamp.desc())
                .limit(500)
                .all()
            )
            by_symbol: Dict[str, List[Trade]] = {}
            for t in trades:
                if t.symbol:
                    by_symbol.setdefault(t.symbol, []).append(t)

            changed = False
            for sym, rows in by_symbol.items():
                changed |= await self._tpsl_symbol(sym, rows, now_ts, results)
            if changed:
                try:
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
                self._refresh_trades_cache()
                if results["executed_virtual"]:
                    await self._save_results(None, None)
            return {"success": True, "results": results}

    async def _tpsl_symbol(self, sym: str, trades: List[Trade], now_ts: float, results: Dict[str, int]) -> bool:
        cfg = self.config
        last_price = float((self.ticker_data.get(sym) or {}).get("last_price") or 0.0)
        rules = await self._get_instrument_rules(sym)
        if last_price <= 0 or rules is None:
            results["skipped"] += len(trades)
            return False

        tick = float(rules.tick_size)
        min_tp_move = tick * max(1, int(cfg.AUTO_TPSL_MIN_TP_MOVE_TICKS))
        min_sl_move = tick * max(1, int(cfg.AUTO_TPSL_MIN_SL_MOVE_TICKS))
        metrics = self._last_metrics_by_symbol.get(sym) or {}
        vol = float(metrics.get("volatility", 0.0) or 0.0)
        vol = max(0.0, min(float(cfg.AUTO_TPSL_MAX_VOLATILITY), vol if math.isfinite(vol) else 0.0))
        base_gap = max(float(cfg.AUTO_TPSL_TRAIL_GAP_MIN), vol * float(cfg.AUTO_TPSL_TRAIL_GAP_MULT))
        now_iso = datetime.now(timezone.utc).isoformat()
        actions = self._actions()
        changed = False

        for trade in trades:
            action = str(trade.action or "").lower()
            order_id = str(trade.order_id or "").strip()
            entry, qty = executed_entry(trade)
            if not order_id or action not in {"buy", "sell"} or entry <= 0:
                results["skipped"] += 1
                continue

            tp_old = float(trade.tp_price or 0.0)
            sl_old = float(trade.sl_price or 0.0)

            # 1) Disparo contra los niveles vigentes (los que el mercado ya pudo tocar).
            if trade.outcome_status == "filled":
                reason = trigger_reason(action, last_price, tp_old, sl_old)
                if reason:
                    if await self._close_virtual_position(trade, sym, action, entry, qty, last_price, reason, rules):
                        results["executed_virtual"] += 1
                        changed = True
                    continue

            # 2) Trailing de niveles.
            ml_p = self.ml_predict_proba(symbol=sym, action=action, metrics=metrics) if cfg.ML_ENABLED else None
            gap = base_gap
            if ml_p is not None and ml_p < 0.5:
                gap = max(float(cfg.AUTO_TPSL_TRAIL_GAP_MIN), gap * float(cfg.AUTO_TPSL_LOW_ML_GAP_MULT))
            params = TrailParams(
                gap=gap,
                gap_min=float(cfg.AUTO_TPSL_TRAIL_GAP_MIN),
                tp_ext_mult=float(cfg.AUTO_TPSL_TP_EXT_MULT),
                ml_tp_boost=float(cfg.AUTO_TPSL_ML_TP_BOOST),
                ml_threshold=float(cfg.ML_PROB_THRESHOLD),
                tp_proximity=float(cfg.AUTO_TPSL_TP_PROXIMITY),
                min_ext=float(cfg.AUTO_TPSL_MIN_EXT),
                fee_rate=float(cfg.FEE_RATE),
            )
            tp_new, sl_new = trail_levels(action, last_price, entry, tp_old, sl_old, tick, params, ml_p)
            if not (math.isfinite(tp_new) and math.isfinite(sl_new)):
                results["errors"] += 1
                continue
            tp_new = float(rules.price(tp_new, ROUND_HALF_UP))
            sl_new = float(rules.price(sl_new, ROUND_HALF_UP))
            valid = (sl_new < last_price < tp_new) if action == "buy" else (tp_new < last_price < sl_new)
            if not valid:
                results["skipped"] += 1
                continue

            results["checked"] += 1
            upd_tp = tp_old <= 0 or abs(tp_new - tp_old) >= min_tp_move
            upd_sl = sl_old <= 0 or abs(sl_new - sl_old) >= min_sl_move
            if not (upd_tp or upd_sl):
                continue
            if upd_tp:
                trade.tp_price = tp_new
            if upd_sl:
                trade.sl_price = sl_new
            merge_raw(trade, auto_tpsl={
                "ts": now_ts, "timestamp": now_iso, "symbol": sym, "order_id": order_id, "action": action,
                "last_price": last_price, "entry_price": entry, "volatility": vol, "trail_gap": gap,
                "tp_old": tp_old, "sl_old": sl_old, "tp_new": tp_new, "sl_new": sl_new, "ml_p": ml_p,
            })
            results["db_updated"] += 1
            changed = True
            actions.append({"type": "auto_tpsl_amend", "ts": now_iso, "symbol": sym, "order_id": order_id,
                            "tp": tp_new, "sl": sl_new, "ml_p": ml_p})
        return changed

    async def _close_virtual_position(self, trade: Trade, sym: str, action: str, entry: float, qty: float,
                                      last_price: float, reason: str, rules: Any) -> bool:
        cfg = self.config
        close_qty = qty
        info = (trade.bybit_raw or {}).get("order_realtime") if isinstance(trade.bybit_raw, dict) else None
        fee = float((info or {}).get("cumExecFee") or 0.0) if isinstance(info, dict) else 0.0
        # En spot, la comisión de una compra se cobra en la moneda base: no se puede vender lo que no llegó.
        if action == "buy" and 0 < fee <= qty * max(2 * float(cfg.FEE_RATE), 0.005):
            close_qty = qty - fee
        qty_dec = rules.qty(close_qty, ROUND_DOWN)
        if qty_dec <= 0 or qty_dec < rules.qty(rules.min_qty, ROUND_UP):
            merge_raw(trade, close_blocked={"reason": "qty_below_min", "qty": close_qty})
            self._rl_log(f"close_blocked:{trade.trade_id}", "warning",
                         f"⚠️ No se puede cerrar trade {trade.trade_id}: cantidad {close_qty} bajo el mínimo")
            return False

        close_side = "sell" if action == "buy" else "buy"
        logger.info(f"🎯 Disparo TP/SL virtual [{reason.upper()}]: {sym} {close_side} {qty_dec} @ ~{last_price}")
        res = await self._place_order(
            sym, close_side, float(qty_dec), last_price, 0.0, 0.0, order_type=cfg.AUTO_TPSL_CLOSE_ORDER_TYPE
        )
        if not res.get("success"):
            merge_raw(trade, close_error={"reason": reason, "message": res.get("message")})
            return False

        pnl = trade_pnl(action, entry, last_price, float(qty_dec), float(cfg.FEE_RATE))
        now = datetime.now(timezone.utc)
        trade.outcome_status = "final"
        trade.outcome_timestamp = now
        trade.exit_price = float(last_price)
        trade.pnl_gross = pnl.gross
        trade.profit_loss = pnl.net
        merge_raw(trade, close_order={
            "reason": reason,
            "order_id": res.get("order_id"),
            "order_link_id": res.get("order_link_id"),
            "qty": float(qty_dec),
            "exit_price_source": "last_price_at_trigger",
            "timestamp": now.isoformat(),
        })
        self.last_trade_time[sym] = now
        return True
