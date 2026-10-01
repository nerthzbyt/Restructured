"""Persistencia de resultados: snapshots de métricas, eventos, results.json y serialización de trades."""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy.orm import Session

from nertz_core.accounting import capital_view, executed_entry, pnl_summary
from nertz_core.db import OPEN_STATUSES, MetricSnapshot, Trade, latest_valid_balance
from nertz_core.engine.host import EngineHost
from utils import (
    append_metrics_snapshot,
    append_results_event,
    load_results_json,
    maybe_auto_git_commit,
    patch_results,
    save_results,
    update_last_balance,
)

logger = logging.getLogger("NertzMetalEngine")

try:
    from nertz_engine.storage import MetricRow
except ImportError:  # pragma: no cover
    MetricRow = None  # type: ignore[assignment]

THRESHOLD_ENV_KEYS = (
    "EGM_BUY_THRESHOLD",
    "EGM_SELL_THRESHOLD",
    "COMBINED_BUY_THRESHOLD",
    "COMBINED_SELL_THRESHOLD",
    "COMBINED_HOLD_BAND",
)


def persist_values_to_env(env_path: str, values: Dict[str, Any]) -> Dict[str, Any]:
    """Reescribe (o añade) claves en un archivo .env conservando el resto."""
    env_path = os.path.abspath(env_path)
    try:
        if not os.path.exists(env_path):
            return {"success": False, "message": "env_not_found", "path": env_path, "values": values}
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        patterns = {k: re.compile(rf"^\s*{re.escape(k)}\s*=") for k in values}
        found = set()
        out: List[str] = []
        for line in lines:
            key = next((k for k, pat in patterns.items() if pat.match(line)), None)
            if key is None:
                out.append(line)
            else:
                out.append(f"{key}={values[key]}")
                found.add(key)
        out.extend(f"{k}={v}" for k, v in values.items() if k not in found)
        with open(env_path, "w", encoding="utf-8") as f:
            f.write("\n".join(out) + "\n")
        return {"success": True, "path": env_path, "values": values}
    except Exception as e:
        return {"success": False, "message": str(e), "path": env_path, "values": values}


class ReportingMixin(EngineHost):
    # ---------------------------------------------------------- basics
    @property
    def _logs_dir(self) -> str:
        return self.paths.logs_dir

    def _thresholds_payload(self, symbol: Optional[str] = None) -> Dict[str, float]:
        from signal_engine import symmetrize_threshold_values

        buy, sell, hold = self._thresholds_for(symbol)
        sym = symmetrize_threshold_values(buy, sell, hold)
        return {
            "egm_buy_threshold": float(self.config.EGM_BUY_THRESHOLD),
            "egm_sell_threshold": float(self.config.EGM_SELL_THRESHOLD),
            "combined_buy_threshold": float(sym.combined_buy_threshold),
            "combined_sell_threshold": float(sym.combined_sell_threshold),
            "combined_hold_band": float(sym.combined_hold_band),
        }

    def persist_thresholds_to_env(self) -> Dict[str, Any]:
        values = {k: float(getattr(self.config, k)) for k in THRESHOLD_ENV_KEYS}
        return persist_values_to_env(self.paths.env_file, values)

    @staticmethod
    def _serialize_metrics_for_storage(metrics: Any) -> Dict[str, Any]:
        if not isinstance(metrics, dict):
            return {}
        out: Dict[str, Any] = {}
        for k, v in metrics.items():
            if k == "thresholds" or v is None:
                continue
            if isinstance(v, bool):
                out[str(k)] = v
            elif isinstance(v, dict):
                out[str(k)] = v
            else:
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if np.isfinite(fv):
                    out[str(k)] = fv
        return out

    async def _record_event(self, event: Dict[str, Any]) -> None:
        """Evento en results.json sin bloquear el event loop."""
        try:
            await asyncio.to_thread(
                append_results_event, event, self._logs_dir, int(self.config.RESULTS_MAX_EVENTS)
            )
        except Exception as e:
            self._rl_log(f"event:{event.get('type')}", "warning", f"⚠️ No se pudo registrar evento: {e}", interval_s=60.0)

    def _update_last_balance(self, body: Dict[str, Any]) -> None:
        update_last_balance(body, log_dir=self._logs_dir)

    def _persist_thresholds_block(self, update: Dict[str, Any], targets: Dict[str, Any]) -> None:
        try:
            append_results_event({"type": "thresholds", "update": update, "targets": targets},
                                 log_dir=self._logs_dir, max_events=int(self.config.RESULTS_MAX_EVENTS))
            patch_results({"thresholds": {"timestamp": datetime.now(timezone.utc).isoformat(),
                                          "values": self._thresholds_payload(), "update": update,
                                          "targets": targets}}, log_dir=self._logs_dir)
        except Exception as e:
            self._rl_log("thresholds:persist", "warning", f"⚠️ No se pudo persistir thresholds: {e}", interval_s=60.0)
        if self.config.PERSIST_THRESHOLDS_TO_ENV:
            self.persist_thresholds_to_env()

    # ------------------------------------------------ metric snapshots
    async def _record_metrics_snapshot(self, db: Session, symbol: str, last_price: float, metrics: Dict[str, Any],
                                       decision: str) -> None:
        cfg = self.config
        if str(decision).lower() == "warmup" or not metrics.get("data_ok"):
            return
        now_ts = time.time()
        if now_ts - self._last_metrics_snapshot_ts.get(symbol, 0.0) < max(0.0, float(cfg.METRICS_SNAPSHOT_DEDUP_S)):
            return
        self._last_metrics_snapshot_ts[symbol] = now_ts
        now = datetime.now(timezone.utc)
        thresholds = self._thresholds_payload(symbol)
        combined = float(metrics.get("combined", 0.0) or 0.0)
        stored = self._serialize_metrics_for_storage(metrics)

        self._metrics_window.setdefault(symbol, deque(maxlen=cfg.METRICS_WINDOW_BUFFER)).append(
            {"ts": now_ts, "decision": str(decision), "combined": combined}
        )
        core = {k: float(metrics.get(k, 0.0) or 0.0) for k in ("ild", "egm", "rol", "pio", "ogm", "volatility")}
        try:
            db.add(MetricSnapshot(timestamp=now, symbol=symbol, last_price=float(last_price or 0.0),
                                  decision=str(decision), combined=combined, thresholds=thresholds, **core))
            db.commit()
        except Exception as e:
            logger.debug(f"metric_snapshots sqlite skip {symbol}: {e}")
            db.rollback()

        if self._storage is not None and MetricRow is not None:
            await self._storage.enqueue_metric(
                MetricRow(timestamp=now, symbol=symbol, last_price=float(last_price or 0.0), decision=str(decision),
                          combined=combined, thresholds=thresholds, metrics=stored, **core)
            )

        if not cfg.STORAGE_DISABLE_JSONL:
            payload = {"timestamp": now.isoformat(), "ts": now_ts, "symbol": symbol,
                       "last_price": float(last_price or 0.0), "decision": str(decision), "metrics": stored,
                       "thresholds": thresholds}
            await asyncio.to_thread(append_metrics_snapshot, payload, self.paths.data_dir)

        if now_ts - self._last_metrics_json_ts.get(symbol, 0.0) >= max(0.0, float(cfg.METRICS_RESULTS_EVENT_MIN_S)):
            self._last_metrics_json_ts[symbol] = now_ts
            await self._record_event({"type": "metrics", "symbol": symbol, "last_price": float(last_price or 0.0),
                                      "decision": str(decision), "metrics": stored, "thresholds": thresholds})

    # -------------------------------------------------- trade serializer
    @staticmethod
    def _normalize_outcome_status(value: Any) -> str:
        return value if isinstance(value, str) and value.strip() else "legacy"

    def _serialize_trade_for_api(self, t: Trade) -> Dict[str, Any]:
        status = self._normalize_outcome_status(t.outcome_status)
        is_final = status == "final"
        raw = t.bybit_raw if isinstance(t.bybit_raw, dict) else None
        order_info: Dict[str, Any] = {}
        if raw:
            for key in ("order_realtime", "order_history"):
                block = raw.get(key)
                if isinstance(block, dict) and block:
                    order_info = block
                    break

        def _opt(v: Any) -> Optional[float]:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        snapshot_raw = raw.get("metrics_snapshot") if raw else None
        snapshot: Dict[str, Any] = snapshot_raw if isinstance(snapshot_raw, dict) else {}
        # Las métricas viven en snapshot["metrics"] (antes se leían del nivel superior y salían siempre 0).
        metrics_raw = snapshot.get("metrics")
        m: Dict[str, Any] = metrics_raw if isinstance(metrics_raw, dict) else snapshot

        def _m(*keys: str) -> float:
            for k in keys:
                v = m.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        continue
            return 0.0

        entry, qty = executed_entry(t)
        pl = float(t.profit_loss or 0.0)
        return {
            "trade_id": t.trade_id,
            "timestamp": t.timestamp.isoformat(),
            "symbol": t.symbol,
            "action": t.action,
            "order_id": t.order_id,
            "entry_price": entry,
            "exit_price": float(t.exit_price) if is_final else None,
            "tp_price": t.tp_price,
            "sl_price": t.sl_price,
            "quantity": qty,
            "profit_loss": pl if is_final else None,
            "pnl_gross": float(t.pnl_gross or 0.0) if is_final else None,
            "pnl_open": pl if status in {"filled", "partial"} and pl else None,
            "outcome_status": status,
            "outcome_timestamp": t.outcome_timestamp.isoformat() if t.outcome_timestamp else None,
            "exchange_order_status": str(order_info.get("orderStatus") or "").strip().lower() or None,
            "exchange_avg_price": _opt(order_info.get("avgPrice")),
            "exchange_cum_exec_qty": _opt(order_info.get("cumExecQty")),
            "bybit_raw": raw,
            "metrics_snapshot": snapshot,
            "decision": t.decision,
            "combined": float(t.combined or 0.0),
            "ild": float(t.ild or 0.0),
            "egm": float(t.egm or 0.0),
            "rol": float(t.rol or 0.0),
            "pio": float(t.pio or 0.0),
            "ogm": float(t.ogm or 0.0),
            "risk_reward_ratio": float(t.risk_reward_ratio or 0.0),
            # Componentes atómicos para ML (planos para Pandas/XGBoost).
            "egm_pressure": _m("asymmetry"),
            "egm_flow_tfi": _m("tfi", "recent_trades_imbalance_qty_pct"),
            "egm_momentum": _m("mom_raw"),
            "micro_spread_bps": _m("spread_bps"),
            "micro_offset_bps": _m("microprice_offset_bps"),
            "rvol_raw": _m("rvol", "recent_trades_rvol"),
            "imbalance_qty_pct": _m("recent_trades_imbalance_qty_pct"),
            "ild_raw": _m("ild_raw"),
            "rol_raw": _m("rol_raw"),
        }

    def _refresh_trades_cache(self, symbol: Optional[str] = None) -> None:
        with self.SessionLocal() as db:
            for sym in ([symbol] if symbol else list(self.symbols)):
                rows = db.query(Trade).filter_by(symbol=sym).order_by(Trade.timestamp.desc()).all()
                self.trades_cache[sym] = [self._serialize_trade_for_api(t) for t in rows]

    def _load_trades_cache(self):
        self._refresh_trades_cache()

    # --------------------------------------------------------- results
    def build_results(self, trades_all: List[Trade], latest_balance: Any, previous: Dict[str, Any],
                      trade_result: Optional[Trade]) -> Dict[str, Any]:
        cfg = self.config
        precision = int(cfg.RESULTS_PRECISION)
        trades_by_symbol: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.symbols}
        for t in trades_all:
            status = self._normalize_outcome_status(t.outcome_status)
            is_final = status == "final"
            raw = t.bybit_raw if isinstance(t.bybit_raw, dict) else None
            trades_by_symbol.setdefault(t.symbol, []).append({
                "trade_id": t.trade_id,
                "timestamp": t.timestamp.isoformat(),
                "symbol": t.symbol,
                "action": t.action,
                "order_id": t.order_id,
                "entry_price": float(t.entry_price),
                "exit_price": float(t.exit_price) if is_final else None,
                "tp_price": float(t.tp_price) if t.tp_price is not None else None,
                "sl_price": float(t.sl_price) if t.sl_price is not None else None,
                "quantity": float(t.quantity),
                "profit_loss": float(t.profit_loss) if is_final else None,
                "outcome_status": status,
                "outcome_timestamp": t.outcome_timestamp.isoformat() if t.outcome_timestamp else None,
                "bybit_raw": raw,
                "metrics_snapshot": raw.get("metrics_snapshot") if raw else None,
                "decision": t.decision,
                "combined": float(t.combined),
                "ild": float(t.ild),
                "egm": float(t.egm),
                "rol": float(t.rol),
                "pio": float(t.pio),
                "ogm": float(t.ogm),
                "risk_reward_ratio": float(t.risk_reward_ratio),
            })

        summary = pnl_summary(trades_all, list(trades_by_symbol), precision)
        cap = capital_view(previous_results=previous, latest_balance=latest_balance, engine_capital=self.capital,
                           configured_capital=cfg.CAPITAL_USDT)
        metadata: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "capital_inicial": round(cap.capital_inicial, precision),
            "capital_actual": round(cap.capital_actual, precision),
            "capital_final": round(cap.capital_actual, precision),
            "capital_source": cap.capital_source,
            "capital_pnl": round(cap.capital_pnl, precision),
            "total_pnl": summary["net_profit"],
            "total_trades": summary["total_trades"],
            "open_trades": sum(1 for t in trades_all if t.outcome_status in OPEN_STATUSES),
            "iterations": self.iterations,
            "running": self.running,
        }
        results: Dict[str, Any] = {
            "metadata": metadata,
            "summary": {k: summary[k] for k in ("total_profit", "total_loss", "net_profit", "win_rate",
                                                 "avg_profit_per_trade")},
            "by_symbol": summary["by_symbol"],
            "trades": trades_by_symbol,
        }
        if latest_balance is not None:
            block = {
                "timestamp": latest_balance.timestamp.isoformat(),
                "account_type": latest_balance.account_type,
                "coin": latest_balance.coin,
                "total_equity": float(latest_balance.total_equity or 0.0),
                "available_balance": float(latest_balance.available_balance or 0.0),
            }
            results["last_balance"] = block
            if cap.capital_source == "bybit_wallet_balance":
                metadata.update({
                    "balance_timestamp": block["timestamp"],
                    "balance_total_equity": block["total_equity"],
                    "balance_available_balance": block["available_balance"],
                    "balance_account_type": block["account_type"],
                    "balance_coin": block["coin"],
                })
        if trade_result is not None:
            metadata["last_trade_timestamp"] = trade_result.timestamp.isoformat()
            results["last_trade"] = self._serialize_trade_for_api(trade_result)
        return results

    async def _save_results(self, _symbol: Optional[str], trade_result: Optional[Trade]) -> None:
        with self.SessionLocal() as db:
            trades_all = db.query(Trade).order_by(Trade.timestamp.asc()).all()
            latest_balance = latest_valid_balance(db)
        previous = await asyncio.to_thread(load_results_json, self._logs_dir)
        results = self.build_results(trades_all, latest_balance, previous, trade_result)
        await asyncio.to_thread(save_results, results, self._logs_dir)
        maybe_auto_git_commit("results_snapshot")
        logger.info(
            f"📊 Resultados guardados: Total PNL={results['summary']['net_profit']} {self.config.QUOTE_COIN}, "
            f"Capital={results['metadata']['capital_actual']} {self.config.QUOTE_COIN}"
        )

    def profit_report(self, db: Session) -> Dict[str, Any]:
        cfg = self.config
        precision = int(cfg.RESULTS_PRECISION)
        trades_all = db.query(Trade).order_by(Trade.timestamp.asc()).all()
        summary = pnl_summary(trades_all, self.symbols, precision)
        cap = capital_view(previous_results=load_results_json(self._logs_dir),
                           latest_balance=latest_valid_balance(db), engine_capital=self.capital,
                           configured_capital=cfg.CAPITAL_USDT)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "capital_inicial": round(cap.capital_inicial, precision),
            "capital_actual": round(cap.capital_actual, precision),
            "capital_source": cap.capital_source,
            "capital_pnl": round(cap.capital_pnl, precision),
            "total_pnl": summary["net_profit"],
            "total_profit": summary["total_profit"],
            "total_loss": summary["total_loss"],
            "net_profit": summary["net_profit"],
            "win_rate": summary["win_rate"],
            "by_symbol": summary["by_symbol"],
        }

    # ------------------------------------------------------------ resets
    def wipe_database(self, db: Session) -> Dict[str, int]:
        return self.database.wipe(db)

    def reset_results_json(self) -> str:
        fp = os.path.join(self._logs_dir, "results.json")
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except OSError:
            pass
        save_results({"events": [], "metadata": {"timestamp": datetime.now(timezone.utc).isoformat(), "reset": True}},
                     log_dir=self._logs_dir)
        return fp

    def reset_trades(self):
        self.trades_cache = {symbol: [] for symbol in self.symbols}
        with self.SessionLocal() as db:
            db.query(Trade).delete()
            db.commit()
        self.trade_id_counter = self._load_initial_trade_id()
        logger.info("🧹 Trades reseteados")
