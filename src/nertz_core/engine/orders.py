"""Órdenes y cuenta: cliente Bybit, preflight, colocación, sincronización y balance."""
from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from nertz_core.accounting import executed_entry, parse_wallet_balance, trade_pnl
from nertz_core.db import BalanceSnapshot, Trade, merge_raw, trade_order_link_id, utc_aware
from nertz_core.sizing import InstrumentRules, format_decimal, to_decimal
from utils import timestamp_to_datetime

logger = logging.getLogger("NertzMetalEngine")

_NO_CREDS = "Credenciales BYBIT_API_KEY/BYBIT_API_SECRET no configuradas"
_TERMINAL_EXCHANGE = {"filled", "cancelled", "canceled", "rejected", "deactivated", "expired", "partiallyfilledcanceled"}
_OPEN_EXCHANGE = {"new", "partiallyfilled"}
# Estados de trade que ya no necesitan consultar la orden de entrada al exchange.
_SYNC_DONE = ("final", "cancelled", "invalid_entry", "filled")


def _norm_status(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "").replace(" ", "")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class OrdersMixin:
    # ----------------------------------------------------------- clients
    def _bybit_client(self):
        if not self.config.LIVE_TRADING_ENABLED:
            return None
        return self._bybit_client_for_balance()

    def _bybit_client_for_balance(self):
        cfg = self.config
        if not cfg.BYBIT_API_KEY or not cfg.BYBIT_API_SECRET:
            return None
        if self._bybit is None:
            ep = cfg.endpoints()
            self._bybit = self.client_factory(
                cfg.BYBIT_API_KEY,
                cfg.BYBIT_API_SECRET,
                base_url=ep.rest_private,
                recv_window=str(cfg.BYBIT_RECV_WINDOW),
                timeout_s=cfg.BYBIT_HTTP_TIMEOUT_S,
                max_retries=cfg.BYBIT_HTTP_MAX_RETRIES,
                public_base_url=ep.rest_public,
            )
        return self._bybit

    def _link_id(self) -> str:
        return f"{self.config.ORDER_LINK_PREFIX}{uuid.uuid4().hex[:20]}"

    # ---------------------------------------------------------- preflight
    async def preflight(self) -> Dict[str, Any]:
        cfg = self.config
        mode = "live" if cfg.LIVE_TRADING_ENABLED else "disabled"
        if mode != "live":
            return {"success": True, "mode": mode}

        client = self._bybit_client()
        if client is None:
            return {"success": False, "mode": mode, "message": _NO_CREDS}

        time_payload = await client.get_server_time()
        if time_payload.get("retCode") != 0:
            return {"success": False, "mode": mode, "message": time_payload.get("retMsg") or "server_time_failed",
                    "raw": time_payload}

        drift_s = None
        server_s = _f((time_payload.get("result") or {}).get("timeSecond"))
        if server_s > 0:
            drift_s = abs(time.time() - server_s)
        if drift_s is not None and drift_s > cfg.CLOCK_DRIFT_MAX_S:
            return {"success": False, "mode": mode,
                    "message": f"Deriva de reloj alta ({drift_s:.2f}s). Sincroniza tu hora local."}

        balance = await self.record_balance()
        if not balance.get("success"):
            return {"success": False, "mode": mode, "message": balance.get("message") or "wallet_balance_failed",
                    "raw": balance}
        if isinstance(balance.get("balance"), dict):
            self._apply_balance_to_capital(balance["balance"])

        missing = [sym for sym in self.symbols if await self._get_instrument_rules(sym) is None]
        if missing:
            return {"success": False, "mode": mode, "message": "instrument_rules_failed", "errors": missing[:10]}
        return {"success": True, "mode": mode, "drift_s": drift_s}

    # -------------------------------------------------- instrument rules
    async def _get_instrument_rules(self, symbol: str) -> Optional[InstrumentRules]:
        now = time.time()
        cached = self.instrument_rules.get(symbol)
        if cached is not None and now - self._instrument_rules_ts.get(symbol, 0.0) < self.config.INSTRUMENT_RULES_TTL_S:
            return cached
        try:
            data = await self._public_client().get_instruments_info(self.config.BYBIT_CATEGORY, symbol)
            lst = (data.get("result") or {}).get("list") or [] if data.get("retCode") == 0 else []
            rules = InstrumentRules.from_bybit(lst[0]) if lst else None
        except Exception as e:
            logger.warning(f"⚠️ instruments-info {symbol}: {e}")
            rules = None
        if rules is None or not rules.is_valid():
            return cached  # se conserva la última regla válida si la hubo
        self.instrument_rules[symbol] = rules
        self._instrument_rules_ts[symbol] = now
        return rules

    # --------------------------------------------------------- placement
    def _build_spot_create_body(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        qty_str: str,
        order_link_id: str,
        time_in_force: Optional[str] = None,
        price_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        ot = "Market" if str(order_type or "").strip().lower() == "market" else "Limit"
        tif = "IOC" if ot == "Market" else (time_in_force or self.config.TIME_IN_FORCE)
        body: Dict[str, Any] = {
            "category": self.config.BYBIT_CATEGORY,
            "symbol": symbol,
            "side": "Buy" if str(side).lower() == "buy" else "Sell",
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
        cfg = self.config
        if not cfg.LIVE_TRADING_ENABLED:
            return {"success": False, "message": "LIVE_TRADING_ENABLED deshabilitado"}
        client = self._bybit_client()
        if client is None:
            logger.error("❌ Credenciales de API no configuradas. No se puede colocar la orden.")
            return {"success": False, "message": _NO_CREDS}
        rules = await self._get_instrument_rules(symbol)
        if rules is None:
            return {"success": False, "message": "instrument_rules_unavailable"}

        ot = order_type or cfg.ORDER_TYPE
        body = self._build_spot_create_body(
            symbol=symbol,
            side=action,
            order_type=ot,
            qty_str=format_decimal(rules.qty(quantity, ROUND_DOWN)),
            order_link_id=self._link_id(),
            price_str=format_decimal(rules.price(price, ROUND_HALF_UP)) if ot != "Market" else None,
        )
        attempts = int(cfg.MAX_CHASE_ATTEMPTS)
        async with self._order_slots:
            for attempt in range(attempts):
                try:
                    result = await client.create_order(body)
                except Exception as e:
                    logger.error(f"❌ Error en intento {attempt + 1}/{attempts}: {e}")
                    if attempt < attempts - 1:
                        await asyncio.sleep(cfg.CHASE_INTERVAL * (2 ** attempt))
                        continue
                    return {"success": False, "message": f"Error tras {attempts} intentos: {e}"}

                http_status = result.get("http_status")
                ret_code = result.get("retCode")
                if http_status == 200 and ret_code == 0:
                    order_id = (result.get("result") or {}).get("orderId") or ""
                    logger.info(
                        "Orden colocada: %s %s %s @ %s, TP=%s, SL=%s, OrderID=%s",
                        symbol, body["side"], body["qty"], body.get("price", "Market"), tp, sl, order_id,
                    )
                    self._balance_dirty = True
                    return {"success": True, "order_id": order_id, "order_link_id": body["orderLinkId"],
                            "raw": result}

                if http_status == 429:
                    wait = cfg.CHASE_INTERVAL * (2 ** attempt)
                    logger.warning(f"⚠️ Rate limit alcanzado. Reintentando en {wait}s...")
                    await asyncio.sleep(wait)
                    continue

                # Precio fuera de la banda permitida: reajustar al límite que informa Bybit.
                if body["orderType"] == "Limit" and ret_code in {170193, 170194}:
                    nums = re.findall(r"\d+(?:\.\d+)?", str(result.get("retMsg") or ""))
                    limit_price = _f(nums[-1]) if nums else 0.0
                    if limit_price > 0:
                        current = _f(body.get("price"))
                        new_price = min(current, limit_price) if ret_code == 170193 else max(current, limit_price)
                        body["price"] = format_decimal(rules.price(new_price, ROUND_HALF_UP))
                        body["orderLinkId"] = self._link_id()
                        continue

                logger.error(
                    f"❌ Error al colocar orden (HTTP {http_status}): retCode={ret_code}, retMsg={result.get('retMsg')}"
                )
                return {"success": False, "message": result.get("retMsg", "Error desconocido"), "raw": result}
        return {"success": False, "message": f"Falló tras {attempts} intentos"}

    async def cancel_all_open_orders(self, symbol: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
        if not self.config.LIVE_TRADING_ENABLED:
            return {"success": True, "skipped": True, "mode": "disabled", "seen": 0, "cancelled": 0, "failed": 0,
                    "failures": []}
        client = self._bybit_client()
        if client is None:
            return {"success": False, "message": _NO_CREDS}
        category = self.config.BYBIT_CATEGORY
        try:
            payload = await client.get_open_orders_merged(category=category, symbol=symbol, limit=int(limit))
            if payload.get("retCode") != 0:
                return {"success": False, "message": payload.get("retMsg") or "get_open_orders_failed", "raw": payload}
            orders = [o for o in (payload.get("result") or {}).get("list") or [] if isinstance(o, dict)]
        except Exception as e:
            return {"success": False, "message": str(e)}

        cancelled, failures = 0, []
        for o in orders:
            oid, sym = o.get("orderId"), o.get("symbol")
            if not self.config.is_bot_order_link(o.get("orderLinkId")) or not oid or not sym:
                continue
            try:
                res = await client.cancel_order({"category": category, "symbol": sym, "orderId": oid})
                if res.get("retCode") == 0:
                    cancelled += 1
                else:
                    failures.append({"orderId": oid, "symbol": sym, "retCode": res.get("retCode"),
                                     "retMsg": res.get("retMsg")})
            except Exception as e:
                failures.append({"orderId": oid, "symbol": sym, "error": str(e)})
        return {"success": True, "seen": len(orders), "cancelled": cancelled, "failed": len(failures),
                "failures": failures[:50]}

    async def exchange_open_orders_all(self) -> List[Dict[str, Any]]:
        """Órdenes abiertas en el exchange para todos los símbolos (consultas en paralelo)."""
        client = self._bybit_client()
        if client is None:
            return []
        category = self.config.BYBIT_CATEGORY

        async def _one(sym: str) -> List[Dict[str, Any]]:
            try:
                payload = await client.get_open_orders_merged(category=category, symbol=sym, limit=200)
            except Exception:
                return []
            return list((payload.get("result") or {}).get("list") or []) if payload.get("retCode") == 0 else []

        merged: Dict[str, Dict[str, Any]] = {}
        for rows in await asyncio.gather(*(_one(s) for s in self.symbols)):
            for row in rows:
                oid = row.get("orderId") if isinstance(row, dict) else None
                if isinstance(oid, str) and oid:
                    merged[oid] = row
        return list(merged.values())

    def open_trades(self, db: Session, limit: int = 500) -> List[Trade]:
        from nertz_core.db import OPEN_STATUSES

        return (
            db.query(Trade).filter(Trade.outcome_status.in_(OPEN_STATUSES))
            .order_by(Trade.timestamp.desc()).limit(int(limit)).all()
        )

    # --------------------------------------------------------------- sync
    def _set_order_status(self, order_id: str, symbol: str, status: str, raw: Any = None, **extra: Any) -> None:
        self.order_status[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **({"raw": raw} if raw is not None else {}),
            **extra,
        }

    async def _fetch_bybit_order_for_trade(self, client, symbol: str, trade: Trade) -> Optional[Dict[str, Any]]:
        order_id = str(trade.order_id or "").strip()
        if not order_id:
            return None
        category = self.config.BYBIT_CATEGORY
        link = trade_order_link_id(trade) or None

        def _first_row(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            if payload.get("retCode") != 0:
                return None
            lst = (payload.get("result") or {}).get("list") or []
            return lst[0] if lst and isinstance(lst[0], dict) else None

        try:
            row = _first_row(
                await client.order_history(category=category, symbol=symbol, order_id=order_id, order_link_id=link,
                                           limit=1)
            )
            if row is not None:
                merge_raw(trade, order_history=row)
                return row
            row = _first_row(await client.order_realtime(category=category, symbol=symbol, order_id=order_id))
            if row is not None:
                return row
            ex = await client.execution_list(category=category, symbol=symbol, order_id=order_id, limit=20)
            if ex.get("retCode") == 0:
                qty = val = 0.0
                for r in (ex.get("result") or {}).get("list") or []:
                    q, p = _f(r.get("execQty")), _f(r.get("execPrice"))
                    if q > 0 and p > 0:
                        qty += q
                        val += q * p
                if qty > 0:
                    return {"orderId": order_id, "orderStatus": "Filled", "avgPrice": str(val / qty),
                            "cumExecQty": str(qty), "from_execution_list": True}
        except Exception as e:
            logger.debug(f"fetch order {order_id}: {e}")
        return None

    async def _cancel_stale_tpsl(self, client, sym: str, open_by_id: Dict[str, Dict[str, Any]],
                                 now: datetime, results: Dict[str, int]) -> None:
        max_age = float(self.config.TPSL_CANCEL_AFTER_S)
        if max_age <= 0:
            return
        for oid, o in list(open_by_id.items()):
            order_filter = str(o.get("orderFilter") or "").lower()
            stop_type = str(o.get("stopOrderType") or "").lower()
            if "tpsl" not in order_filter and "tpsl" not in stop_type:
                continue
            if _norm_status(o.get("orderStatus")) in _TERMINAL_EXCHANGE:
                continue
            created = o.get("createdTime") or o.get("updatedTime")
            try:
                age_s = (now - timestamp_to_datetime(int(created))).total_seconds() if created is not None else 0.0
            except (TypeError, ValueError):
                age_s = 0.0
            if age_s < max_age:
                continue
            try:
                res = await client.cancel_order({"category": self.config.BYBIT_CATEGORY, "symbol": sym, "orderId": oid})
                if res.get("retCode") == 0:
                    results["tpsl_cancelled"] += 1
                    open_by_id.pop(oid, None)
                    self._balance_dirty = True
                    self._set_order_status(oid, sym, "cancelled", res)
                else:
                    results["errors"] += 1
            except Exception:
                results["errors"] += 1

    async def sync_open_orders(
        self,
        db: Session,
        symbol: Optional[str] = None,
        timeout_seconds: float = 30.0,
        update_after_seconds: float = 20.0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        if not self.config.LIVE_TRADING_ENABLED:
            return {"success": True, "results": {"skipped": 1, "mode": "disabled"}}
        client = self._bybit_client()
        if client is None:
            return {"success": False, "message": _NO_CREDS}

        now_ts = time.time()
        gap = max(0.5, float(self.config.ORDERS_SYNC_INTERVAL_S))
        if now_ts - self._last_orders_sync_ts < gap:
            return {"success": True, "results": {"skipped": 1, "reason": "sync_interval"}}

        async with self._orders_sync_lock:
            if now_ts - self._last_orders_sync_ts < gap:
                return {"success": True, "results": {"skipped": 1, "reason": "sync_interval"}}
            self._last_orders_sync_ts = now_ts
            now = datetime.now(timezone.utc)
            results = dict.fromkeys(
                ("checked", "updated", "amended", "cancelled", "tpsl_cancelled", "replaced", "imported_orphan",
                 "orphan_open", "no_action", "errors"),
                0,
            )
            changed = False
            symbols = [symbol] if symbol else list(self.symbols)
            for sym in symbols:
                changed |= await self._sync_symbol(client, db, sym, now, float(timeout_seconds),
                                                   float(update_after_seconds), int(limit), results)

            if changed:
                db.commit()
                try:
                    self._refresh_trades_cache()
                    for sym in symbols:
                        await self._save_results(sym, None)
                except Exception as e:
                    logger.warning(f"⚠️ Post-sync refresh falló: {e}")

            self._last_orders_sync_results = {"ts": now.isoformat(), "results": dict(results), "changed": changed}
            return {"success": True, "results": results}

    async def _sync_symbol(self, client, db: Session, sym: str, now: datetime, timeout_s: float,
                           update_after_s: float, limit: int, results: Dict[str, int]) -> bool:
        cfg = self.config
        category = cfg.BYBIT_CATEGORY
        open_by_id: Dict[str, Dict[str, Any]] = {}
        try:
            payload = await client.get_open_orders_merged(category=category, symbol=sym, limit=limit)
            if payload.get("retCode") == 0:
                for o in (payload.get("result") or {}).get("list") or []:
                    oid = o.get("orderId") if isinstance(o, dict) else None
                    if isinstance(oid, str) and oid:
                        open_by_id[oid] = o
                        self._set_order_status(oid, sym, str(o.get("orderStatus") or "").lower(), o)
        except Exception:
            pass

        await self._cancel_stale_tpsl(client, sym, open_by_id, now, results)

        trades = (
            db.query(Trade)
            .filter(Trade.symbol == sym, Trade.order_id.isnot(None), Trade.order_id != "")
            .filter(~Trade.outcome_status.in_(_SYNC_DONE))
            .order_by(Trade.timestamp.desc())
            .limit(300)
            .all()
        )
        changed = False
        tracked_ids = {str(t.order_id) for t in trades}
        tracked_links = {trade_order_link_id(t) for t in trades} - {""}

        for trade in trades:
            order_id = str(trade.order_id)
            order = open_by_id.get(order_id)
            if order is None:
                order = await self._fetch_bybit_order_for_trade(client, sym, trade)
                changed |= order is not None
            if order is None:
                results["no_action"] += 1
                continue

            results["checked"] += 1
            link = order.get("orderLinkId")
            if isinstance(link, str) and link:
                tracked_links.add(link)
            is_bot = cfg.is_bot_order_link(link) or cfg.is_bot_order_link(trade_order_link_id(trade))
            status = _norm_status(order.get("orderStatus"))
            self._set_order_status(order_id, sym, str(order.get("orderStatus") or "").lower(), order)
            elapsed = (now - (utc_aware(trade.timestamp) or now)).total_seconds()
            order_filter = str(order.get("orderFilter") or "").strip().lower()
            is_conditional = bool(order_filter and order_filter != "order")
            can_reprice = is_bot and elapsed >= update_after_s and status in _OPEN_EXCHANGE and not is_conditional

            if can_reprice and str(order.get("orderType") or "").lower() == "limit":
                if await self._amend_to_top_of_book(client, sym, order_id, order, trade):
                    results["amended"] += 1
                    changed = True
                    continue

            if is_bot and elapsed >= timeout_s:
                if status in _TERMINAL_EXCHANGE:
                    if await self._update_trade_from_bybit(trade, order):
                        results["updated"] += 1
                        changed = True
                        self._balance_dirty = True
                    continue
                try:
                    res = await client.cancel_order({"category": category, "symbol": sym, "orderId": order_id})
                except Exception:
                    res = {"retCode": -1}
                if res.get("retCode") == 0:
                    merge_raw(trade, cancel=res)
                    # Lo ejecutado antes de cancelar sigue siendo posición abierta.
                    await self._update_trade_from_bybit(trade, {**order, "orderStatus": "Cancelled"})
                    self._set_order_status(order_id, sym, "cancelled", res)
                    results["cancelled"] += 1
                    changed = True
                    self._balance_dirty = True
                else:
                    results["errors"] += 1
                continue

            if can_reprice:
                rep = await self._replace_order_with_market(sym, order_id, trade, bybit_order=order)
                if rep.get("success"):
                    results["replaced"] += 1
                    changed = True
                    self._balance_dirty = True
                else:
                    results["errors"] += 1
                continue

            if await self._update_trade_from_bybit(trade, order):
                results["updated"] += 1
                changed = True
                self._balance_dirty = True

        for oid, orphan in open_by_id.items():
            link = str(orphan.get("orderLinkId") or "")
            if oid in tracked_ids or (link and link in tracked_links):
                continue
            results["orphan_open"] += 1
            if not cfg.is_bot_order_link(link):
                continue
            try:
                if self._import_orphan(db, sym, oid, orphan, now):
                    await self._update_trade_from_bybit(db.query(Trade).filter(Trade.order_id == oid).first(), orphan)
                    results["imported_orphan"] += 1
                    changed = True
                    self._balance_dirty = True
            except Exception:
                results["errors"] += 1
        return changed

    async def _amend_to_top_of_book(self, client, sym: str, order_id: str, order: Dict[str, Any], trade: Trade) -> bool:
        book = self.orderbook_data.get(sym)
        side = str(order.get("side") or "").lower()
        target = (book.best_bid() if side == "buy" else book.best_ask()) if book is not None else 0.0
        rules = await self._get_instrument_rules(sym)
        if target <= 0 or rules is None:
            return False
        body = {
            "category": self.config.BYBIT_CATEGORY,
            "symbol": sym,
            "orderId": order_id,
            "price": format_decimal(rules.price(target, ROUND_HALF_UP)),
        }
        if trade.tp_price is not None:
            body["takeProfit"] = format_decimal(rules.price(float(trade.tp_price)))
        if trade.sl_price is not None:
            body["stopLoss"] = format_decimal(rules.price(float(trade.sl_price)))
        try:
            res = await client.amend_order(body)
        except Exception:
            return False
        if res.get("retCode") == 0:
            merge_raw(trade, amend=res)
            return True
        return False

    def _import_orphan(self, db: Session, sym: str, oid: str, orphan: Dict[str, Any], now: datetime) -> bool:
        if db.query(Trade.id).filter(Trade.symbol == sym, Trade.order_id == str(oid)).first() is not None:
            return False
        side = str(orphan.get("side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            return False
        try:
            ts = timestamp_to_datetime(int(orphan.get("createdTime")))
        except (TypeError, ValueError):
            ts = now
        entry = _f(orphan.get("price")) or _f(orphan.get("avgPrice"))
        qty = _f(orphan.get("qty")) or _f(orphan.get("leavesQty"))
        tp = _f(orphan.get("takeProfit")) or None
        sl = _f(orphan.get("stopLoss")) or None
        trade = Trade(
            trade_id=self._next_trade_id(db),
            timestamp=ts,
            symbol=sym,
            action=side,
            order_id=str(oid),
            bybit_raw={
                "order_realtime": orphan,
                "order_link_id": str(orphan.get("orderLinkId") or ""),
                "imported_orphan": True,
                "imported_at": now.isoformat(),
            },
            entry_price=entry,
            exit_price=0.0,
            tp_price=tp,
            sl_price=sl,
            quantity=qty,
            profit_loss=0.0,
            outcome_status="pending",
            decision=side,
            risk_reward_ratio=self._risk_reward(),
        )
        db.add(trade)
        db.flush()
        self._set_order_status(str(oid), sym, str(orphan.get("orderStatus") or "pending").lower(), orphan,
                               trade_id=int(trade.trade_id))
        return True

    async def _update_trade_from_bybit(self, trade: Optional[Trade], bybit_order: Dict[str, Any]) -> bool:
        if trade is None:
            return False
        try:
            status = _norm_status(bybit_order.get("orderStatus"))
            avg_price = _f(bybit_order.get("avgPrice"))
            cum_qty = _f(bybit_order.get("cumExecQty"))
            cum_fee = _f(bybit_order.get("cumExecFee"))
            now = datetime.now(timezone.utc)
            raw = merge_raw(trade, order_realtime=bybit_order)

            # Si la orden sustituyó a otra parcialmente ejecutada, se consolida la posición.
            prev_exec = (raw.get("replace") or {}).get("executed_before") if isinstance(raw.get("replace"), dict) else None
            if isinstance(prev_exec, dict) and _f(prev_exec.get("qty")) > 0 and cum_qty > 0:
                pq, pp = _f(prev_exec.get("qty")), _f(prev_exec.get("avg_price"))
                avg_price = (avg_price * cum_qty + pp * pq) / (cum_qty + pq) if avg_price > 0 and pp > 0 else avg_price
                cum_qty += pq

            before = (trade.outcome_status, trade.entry_price, trade.quantity)
            if status in {"new", "created", "active", "untriggered", "triggered"}:
                trade.outcome_status = "pending"
            elif status == "partiallyfilled":
                trade.outcome_status = "partial"
                if avg_price > 0:
                    trade.entry_price = avg_price
                if cum_qty > 0:
                    trade.exit_price = 0.0
                    trade.profit_loss = -cum_fee if cum_fee > 0 else 0.0
            elif status == "filled" or (status in _TERMINAL_EXCHANGE and cum_qty > 0):
                # Terminal con ejecución (incl. PartiallyFilledCanceled / cancelada tras fill parcial):
                # la posición existe y la gestiona el TP/SL virtual.
                trade.outcome_status = "filled"
                trade.outcome_timestamp = now
                if avg_price > 0:
                    trade.entry_price = avg_price
                if cum_qty > 0:
                    trade.quantity = cum_qty
                trade.exit_price = 0.0
                trade.profit_loss = -cum_fee if cum_fee > 0 else 0.0
            elif status in _TERMINAL_EXCHANGE:
                trade.outcome_status = "cancelled"
                trade.outcome_timestamp = now
                trade.exit_price = 0.0
                trade.profit_loss = 0.0
            else:
                trade.outcome_status = status or trade.outcome_status or "pending"
            return before != (trade.outcome_status, trade.entry_price, trade.quantity)
        except Exception as e:
            logger.error(f"❌ Error actualizando trade {getattr(trade, 'trade_id', None)}: {e}")
            return False

    async def _replace_order_with_market(
        self,
        symbol: str,
        order_id: str,
        trade: Trade,
        bybit_order: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = self.config
        try:
            if not cfg.LIVE_TRADING_ENABLED:
                return {"success": False, "message": "LIVE_TRADING_ENABLED deshabilitado"}
            client = self._bybit_client()
            if client is None:
                return {"success": False, "message": _NO_CREDS}

            cancel = await client.cancel_order({"category": cfg.BYBIT_CATEGORY, "symbol": symbol, "orderId": order_id})
            if cancel.get("retCode") != 0:
                return {"success": False, "message": cancel.get("retMsg") or "cancel_failed", "raw": cancel}
            self._balance_dirty = True

            order = bybit_order or ((trade.bybit_raw or {}).get("order_realtime") if isinstance(trade.bybit_raw, dict) else {}) or {}
            executed = _f(order.get("cumExecQty"))
            executed_avg = _f(order.get("avgPrice"))
            remaining = max(0.0, float(trade.quantity or 0.0) - executed)
            replace: Dict[str, Any] = {"cancel": cancel, "create": None}
            if executed > 0:
                replace["executed_before"] = {"qty": executed, "avg_price": executed_avg}

            rules = await self._get_instrument_rules(symbol)
            qty_dec = rules.qty(remaining, ROUND_DOWN) if rules else to_decimal(0)
            skip = None
            if remaining <= 0:
                skip = "nothing_remaining"
            elif rules is None:
                skip = "instrument_rules_unavailable"
            elif qty_dec < rules.qty(rules.min_qty, ROUND_UP):
                skip = "remaining_below_min_qty"
            elif rules.min_notional > 0 and float(qty_dec) * float(trade.entry_price or 0.0) < rules.min_notional:
                skip = "remaining_below_min_notional"
            if skip:
                replace["skipped"] = skip
                merge_raw(trade, replace=replace)
                # Sin remanente operable: queda lo ya ejecutado (o nada).
                await self._update_trade_from_bybit(trade, {**order, "orderStatus": "Cancelled"})
                return {"success": True, "old_order_id": order_id, "new_order_id": "", "raw": replace}

            link = self._link_id()
            body = self._build_spot_create_body(
                symbol=symbol,
                side=str(trade.action),
                order_type="Market",
                qty_str=format_decimal(qty_dec),
                order_link_id=link,
            )
            create = await client.create_order(body)
            if create.get("retCode") != 0:
                return {"success": False, "message": create.get("retMsg") or "create_failed", "raw": create}
            self._balance_dirty = True
            new_id = str((create.get("result") or {}).get("orderId") or "")
            replace["create"] = create
            merge_raw(trade, replace=replace, order_link_id=link)
            trade.order_id = new_id or trade.order_id
            trade.outcome_status = "pending"
            trade.outcome_timestamp = None
            if new_id:
                self._set_order_status(new_id, symbol, "pending", create)
            return {"success": True, "old_order_id": order_id, "new_order_id": new_id, "raw": replace}
        except Exception as e:
            logger.error(f"❌ Error reemplazando {order_id}: {e}")
            return {"success": False, "message": str(e)}

    # ------------------------------------------------------------ outcomes
    async def _finalize_due_outcomes(self, db: Session, symbol: str, exit_price: float) -> Optional[Trade]:
        """Cierra el resultado de trades 'filled' al horizonte (etiqueta mark-to-market).

        En live con AUTO_TPSL activo la posición la cierra el TP/SL virtual con
        una orden real; marcarla 'final' aquí la dejaba abierta en el exchange sin
        gestión, así que en ese modo no se aplica el horizonte.
        """
        cfg = self.config
        if exit_price <= 0 or (cfg.LIVE_TRADING_ENABLED and cfg.AUTO_TPSL_ENABLED):
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=float(cfg.OUTCOME_HORIZON_S))
        q = (
            db.query(Trade)
            .filter(Trade.symbol == symbol, Trade.timestamp <= cutoff)
            .filter(~Trade.outcome_status.in_(["final", "cancelled", "invalid_entry"]))
        )
        if cfg.LIVE_TRADING_ENABLED:
            q = q.filter(Trade.outcome_status == "filled")
        pending = q.order_by(Trade.timestamp.asc()).limit(50).all()
        if not pending:
            return None

        now = datetime.now(timezone.utc)
        last: Optional[Trade] = None
        for t in pending:
            entry, qty = executed_entry(t)
            if entry <= 0 or qty <= 0:
                t.outcome_status = "invalid_entry"
                t.outcome_timestamp = now
                continue
            pnl = trade_pnl(t.action, entry, exit_price, qty, float(cfg.FEE_RATE))
            t.exit_price = float(exit_price)
            t.pnl_gross = pnl.gross
            t.profit_loss = pnl.net
            t.outcome_status = "final"
            t.outcome_timestamp = now
            last = t
        db.commit()
        return last

    # ------------------------------------------------------------- balance
    async def _record_simulated_balance(self, *, account_type: str, coin: Optional[str], reason: str) -> Dict[str, Any]:
        total_equity = float(self.capital or self.config.CAPITAL_USDT or 0.0)
        payload = {"mode": "simulated", "reason": reason, "coin": coin, "accountType": account_type}
        await self._persist_balance(account_type, coin, total_equity, total_equity, payload,
                                    {"mode": "simulated", "reason": reason})
        return {"success": True, "balance": {"total_equity": total_equity, "available_balance": total_equity},
                "raw": payload}

    async def _persist_balance(self, account_type: str, coin: Optional[str], total: float, available: float,
                               raw: Dict[str, Any], extra: Dict[str, Any]) -> None:
        with self.SessionLocal() as db:
            db.add(BalanceSnapshot(timestamp=datetime.now(timezone.utc), account_type=account_type, coin=coin,
                                   total_equity=total, available_balance=available, raw=raw))
            db.commit()
        body = {"account_type": account_type, "coin": coin, "total_equity": total, "available_balance": available,
                **extra}
        await self._record_event({"type": "balance", **body})
        await asyncio.to_thread(self._update_last_balance, body)

    async def record_balance(self, account_type: Optional[str] = None, coin: Optional[str] = None) -> Dict[str, Any]:
        cfg = self.config
        coin = coin or cfg.QUOTE_COIN
        client = self._bybit_client_for_balance()
        if client is None:
            if not cfg.LIVE_TRADING_ENABLED:
                return await self._record_simulated_balance(
                    account_type=account_type or cfg.ACCOUNT_TYPE, coin=coin, reason="no_bybit_credentials"
                )
            return {"success": False, "message": _NO_CREDS}

        order: List[str] = [account_type] if account_type else []
        order += [a for a in cfg.account_types() if a not in order]
        payload: Dict[str, Any] = {}
        parsed: Dict[str, Any] = {"valid": False}
        resolved = order[0]
        for acct in order:
            for c in (coin, None):
                payload = await client.wallet_balance(account_type=acct, coin=c)
                parsed = parse_wallet_balance(payload, coin=coin)
                if parsed.get("valid"):
                    resolved = acct
                    break
            if parsed.get("valid"):
                break
        if not parsed.get("valid"):
            return {"success": False, "message": parsed.get("ret_msg") or "wallet_balance_invalid", "raw": payload}

        total = float(parsed["total_equity"])
        available = float(parsed["available_balance"])
        await self._persist_balance(
            resolved, coin, total, available, payload,
            {"http_status": payload.get("http_status"), "retCode": payload.get("retCode"),
             "retMsg": payload.get("retMsg")},
        )
        return {"success": True, "balance": {"total_equity": total, "available_balance": available}, "raw": payload}
