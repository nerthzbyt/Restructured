"""Automatismos: agente interno, auto-HFT, auto-calibración de umbrales y ticks de métricas."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from nertz_core.db import ThresholdSnapshot, Trade
from signal_engine import Thresholds, blend_thresholds_symmetric, relax_thresholds_symmetric

logger = logging.getLogger("NertzMetalEngine")


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    v = sorted(values)
    mid = len(v) // 2
    return float(v[mid]) if len(v) % 2 else float((v[mid - 1] + v[mid]) / 2)


class AutomationMixin:
    # ----------------------------------------------------------- helpers
    def _recent_decisions(self, symbols: List[str], window_s: float, limit: int) -> tuple[List[str], List[float]]:
        """Decisiones (y combined) recientes en ventana, del más nuevo al más viejo."""
        cutoff = time.time() - float(window_s)
        decisions: List[str] = []
        combined: List[float] = []
        for sym in symbols:
            q = self._metrics_window.get(sym)
            if not isinstance(q, deque):
                continue
            for row in reversed(q):
                ts = row.get("ts") if isinstance(row, dict) else None
                if ts is None:
                    continue
                if float(ts) < cutoff:
                    break
                d = row.get("decision")
                if isinstance(d, str):
                    decisions.append(d.lower())
                    combined.append(float(row.get("combined") or 0.0))
                if len(decisions) >= limit:
                    return decisions, combined
        return decisions, combined

    def _metrics_window_s(self) -> float:
        return max(60.0, float(self.config.METRICS_WINDOW_MINUTES) * 60.0)

    def _set_thresholds(self, th: Thresholds, *, source: str) -> None:
        self.config.update(
            {
                "COMBINED_BUY_THRESHOLD": th.combined_buy_threshold,
                "COMBINED_SELL_THRESHOLD": th.combined_sell_threshold,
                "COMBINED_HOLD_BAND": th.combined_hold_band,
            },
            source=source,
        )

    def relax_thresholds(self, factor: float, *, source: str) -> Dict[str, Any]:
        before = self._thresholds_payload()
        cfg = self.config
        self._set_thresholds(
            relax_thresholds_symmetric(
                cfg.COMBINED_BUY_THRESHOLD, cfg.COMBINED_SELL_THRESHOLD, cfg.COMBINED_HOLD_BAND,
                factor=float(factor), params=cfg.signal_params,
            ),
            source=source,
        )
        return {"before": before, "after": self._thresholds_payload()}

    # ------------------------------------------------------------- agent
    async def _agent_tick(self, db: Session) -> None:
        cfg = self.config
        now_ts = time.time()
        if now_ts - self._agent_last_tick_ts < float(cfg.AGENT_TICK_MIN_S):
            return
        self._agent_last_tick_ts = now_ts
        actions = self._actions()

        if self.running and (self._start_task is None or self._start_task.done()):
            if self.schedule_start():
                actions.append({"type": "restart_start_task", "ts": datetime.now(timezone.utc).isoformat()})

        if now_ts - self._agent_last_relax_ts >= float(cfg.AGENT_RELAX_INTERVAL_S):
            try:
                await self._agent_maybe_relax(now_ts, actions)
            except Exception as e:
                actions.append({"type": "relax_thresholds_error", "ts": datetime.now(timezone.utc).isoformat(),
                                "message": str(e)})

        if cfg.ML_ENABLED:
            last_train = float(self._ml_last_train_ts.get("__all__", 0.0) or 0.0)
            try:
                final_count = int(db.query(Trade).filter(Trade.outcome_status == "final").count())
            except Exception:
                final_count = None
            fast = final_count is not None and final_count >= int(cfg.ML_MIN_SAMPLES) and "__all__" not in self._ml_models
            due = now_ts - last_train >= float(cfg.AUTO_AGENT_TRAIN_INTERVAL_MIN) * 60.0
            if fast or due:
                res = self.train_ml_model_from_trades(db, symbol=None)
                self._ml_last_train_ts["__all__"] = now_ts
                model = res.get("model") if isinstance(res.get("model"), dict) else {}
                actions.append({
                    "type": "ml_train",
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "success": bool(res.get("success")),
                    "samples": model.get("samples"),
                    "final_trades": final_count,
                    "min_samples": int(cfg.ML_MIN_SAMPLES),
                })

    async def _agent_maybe_relax(self, now_ts: float, actions: deque) -> None:
        cfg = self.config
        recent = [t for t in self.last_trade_time.values() if isinstance(t, datetime)]
        last_trade = max(recent) if recent else None
        age_trade_s = (datetime.now(timezone.utc) - last_trade).total_seconds() if last_trade else None
        window_s = self._metrics_window_s()
        decisions, _ = self._recent_decisions(self.symbols, window_s, int(cfg.AGENT_DECISIONS_MAX))
        total = len(decisions)
        hold_ratio = (sum(1 for d in decisions if d == "hold") / total) if total else 0.0
        idle = age_trade_s is None or age_trade_s >= float(cfg.AGENT_RELAX_IDLE_S)
        if not (total >= int(cfg.AGENT_RELAX_MIN_SNAPSHOTS) and hold_ratio >= float(cfg.AGENT_RELAX_HOLD_RATIO) and idle):
            return
        change = self.relax_thresholds(float(cfg.AGENT_RELAX_FACTOR), source="auto_agent")
        self._agent_last_relax_ts = now_ts
        info = {
            **change,
            "metrics_window_s": window_s,
            "snapshots_seen": total,
            "hold_ratio": hold_ratio,
            "age_trade_s": age_trade_s,
        }
        actions.append({"type": "relax_thresholds", "ts": datetime.now(timezone.utc).isoformat(), **info})
        await self._record_event({"type": "agent_action", "action": "relax_thresholds", **info})

    async def _enable_secondary_systems_if_due(self, db: Session) -> None:
        """Con AUTO_ENABLE_SECONDARY_SYSTEMS=true activa el agente tras el retardo de arranque.

        (Antes se activaba siempre, ignorando la configuración.)
        """
        cfg = self.config
        if not cfg.AUTO_ENABLE_SECONDARY_SYSTEMS or self._secondary_auto_enabled_ts > 0.0:
            return
        now_ts = time.time()
        if now_ts - self._boot_ts < float(cfg.SECONDARY_SYSTEMS_DELAY_S):
            return
        self._secondary_auto_enabled_ts = now_ts
        if cfg.AUTO_AGENT_ENABLED:
            return
        cfg.update({"AUTO_AGENT_ENABLED": True}, source="auto_enable_secondary")
        self._actions().append({"type": "auto_enable", "ts": datetime.now(timezone.utc).isoformat(),
                                "system": "AUTO_AGENT_ENABLED", "enabled": True})
        await self._record_event({"type": "auto_enable", "system": "AUTO_AGENT_ENABLED", "enabled": True})

    # --------------------------------------------------------------- HFT
    async def run_cycles(self, symbol: str, cycles: int, interval_ms: int, collect_only: bool) -> None:
        if cycles < 0:
            return
        remaining = cycles
        while self.running and (remaining > 0 or cycles == 0):
            with self.SessionLocal() as db:
                await self._core_cycle(symbol, db, collect_only=collect_only)
            if cycles != 0:
                remaining -= 1
            await asyncio.sleep(interval_ms / 1000 if interval_ms > 0 else 0)

    def start_hft(self, symbol: str, interval_ms: int = 250, collect_only: bool = False) -> bool:
        if self.is_hft_running(symbol):
            return False
        self.hft_tasks[symbol] = asyncio.create_task(
            self.run_cycles(symbol, cycles=0, interval_ms=interval_ms, collect_only=collect_only)
        )
        self._hft_params[symbol] = {
            "interval_ms": int(interval_ms),
            "collect_only": bool(collect_only),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        return True

    def stop_hft(self, symbol: str) -> bool:
        task = self.hft_tasks.get(symbol)
        if not task:
            return False
        task.cancel()
        self._hft_params[symbol] = {**(self._hft_params.get(symbol) or {}),
                                    "stopped_at": datetime.now(timezone.utc).isoformat()}
        return True

    def is_hft_running(self, symbol: str) -> bool:
        task = self.hft_tasks.get(symbol)
        return bool(task is not None and not task.done())

    def stop_all_hft(self) -> Dict[str, bool]:
        return {sym: self.stop_hft(sym) for sym in list(self.hft_tasks)}

    def _auto_hft_enabled_effective(self) -> bool:
        return bool(self.config.AUTO_HFT_ENABLED) or bool(self._auto_hft_enabled)

    async def _auto_hft_tick(self, db: Session) -> None:
        cfg = self.config
        if not self.running or not self._auto_hft_enabled_effective():
            return
        now_ts = time.time()
        if now_ts - self._auto_hft_last_tick_ts < max(0.25, float(cfg.AUTO_HFT_TICK_S)):
            return
        self._auto_hft_last_tick_ts = now_ts

        min_snaps = max(5, int(cfg.AUTO_HFT_MIN_SNAPSHOTS))
        threshold = float(cfg.AUTO_HFT_COMBINED_ABS_THRESHOLD)
        actions = self._actions()
        for sym in self.symbols:
            st = self._auto_hft_state.setdefault(sym, {"last_change_ts": 0.0})
            if now_ts - float(st.get("last_change_ts") or 0.0) < max(5.0, float(cfg.AUTO_HFT_COOLDOWN_S)):
                continue
            decisions, combined = self._recent_decisions([sym], max(10.0, float(cfg.AUTO_HFT_WINDOW_S)),
                                                         int(cfg.AGENT_DECISIONS_MAX))
            total = len(decisions)
            if total <= 0:
                continue
            ratio = sum(1 for d in decisions if d in {"buy", "sell"}) / total
            abs_avg = sum(abs(v) for v in combined) / len(combined) if combined else 0.0
            running = self.is_hft_running(sym)
            reason = {"total": total, "ratio": ratio, "abs_avg": abs_avg}

            if not running and total >= min_snaps and ratio >= float(cfg.AUTO_HFT_START_RATIO) and abs_avg >= threshold:
                if self.start_hft(sym, interval_ms=max(0, int(cfg.AUTO_HFT_INTERVAL_MS)),
                                  collect_only=bool(cfg.AUTO_HFT_COLLECT_ONLY)):
                    st.update(last_change_ts=now_ts, last_action="start", reason=reason)
                    actions.append({"type": "auto_hft_start", "ts": datetime.now(timezone.utc).isoformat(),
                                    "symbol": sym, "ratio": ratio, "abs_avg": abs_avg})
                    await self._record_event({"type": "auto_hft", "action": "start", "symbol": sym, "ratio": ratio,
                                              "abs_avg": abs_avg, "interval_ms": int(cfg.AUTO_HFT_INTERVAL_MS),
                                              "collect_only": bool(cfg.AUTO_HFT_COLLECT_ONLY)})
            elif running and (total < min_snaps or ratio <= float(cfg.AUTO_HFT_STOP_RATIO) or abs_avg < threshold * 0.75):
                if self.stop_hft(sym):
                    st.update(last_change_ts=now_ts, last_action="stop", reason=reason)
                    actions.append({"type": "auto_hft_stop", "ts": datetime.now(timezone.utc).isoformat(),
                                    "symbol": sym, "ratio": ratio, "abs_avg": abs_avg})
                    await self._record_event({"type": "auto_hft", "action": "stop", "symbol": sym, "ratio": ratio,
                                              "abs_avg": abs_avg})

    # ------------------------------------------------ threshold calibration
    def _compute_threshold_targets(self, trades: List[Trade]) -> Dict[str, float]:
        p = self.config.signal_params
        win_buys = [t for t in trades if t.action == "buy" and t.egm is not None and (t.profit_loss or 0.0) > 0]
        win_sells = [t for t in trades if t.action == "sell" and t.egm is not None and (t.profit_loss or 0.0) > 0]
        targets: Dict[str, float] = {}
        v = _median([float(t.egm) for t in win_buys])
        if v is not None:
            targets["egm_buy_threshold"] = max(0.0, min(1.0, v * 0.8))
        v = _median([float(t.egm) for t in win_sells])
        if v is not None:
            targets["egm_sell_threshold"] = min(0.0, max(-1.0, v * 0.8))
        v = _median([float(t.combined) for t in win_buys])
        if v is not None:
            targets["combined_buy_threshold"] = max(p.threshold_min, min(p.threshold_max, v * 0.9))
        v = _median([float(t.combined) for t in win_sells])
        if v is not None:
            targets["combined_sell_threshold"] = min(-p.threshold_min, max(-p.threshold_max, v * 0.9))
        if "combined_buy_threshold" in targets and "combined_sell_threshold" in targets:
            sym = (targets["combined_buy_threshold"] - targets["combined_sell_threshold"]) / 2.0
            targets["combined_buy_threshold"], targets["combined_sell_threshold"] = sym, -sym
        return targets

    def _apply_threshold_update(self, targets: Dict[str, float], alpha: float = 0.1) -> Dict[str, Any]:
        cfg = self.config
        before = self._thresholds_payload()
        a = float(alpha)
        changes: Dict[str, float] = {}
        if "egm_buy_threshold" in targets:
            changes["EGM_BUY_THRESHOLD"] = (1 - a) * cfg.EGM_BUY_THRESHOLD + a * float(targets["egm_buy_threshold"])
        if "egm_sell_threshold" in targets:
            changes["EGM_SELL_THRESHOLD"] = (1 - a) * cfg.EGM_SELL_THRESHOLD + a * float(targets["egm_sell_threshold"])
        if changes:
            cfg.update(changes, source="threshold_calibration")
        if "combined_buy_threshold" in targets or "combined_sell_threshold" in targets:
            current = Thresholds(cfg.COMBINED_BUY_THRESHOLD, cfg.COMBINED_SELL_THRESHOLD, cfg.COMBINED_HOLD_BAND)
            target = Thresholds(
                float(targets.get("combined_buy_threshold", current.combined_buy_threshold)),
                float(targets.get("combined_sell_threshold", current.combined_sell_threshold)),
                float(targets.get("combined_hold_band", current.combined_hold_band)),
            )
            self._set_thresholds(blend_thresholds_symmetric(current, target, a, cfg.signal_params),
                                 source="threshold_calibration")
        elif "combined_hold_band" in targets:
            cfg.update({"COMBINED_HOLD_BAND": (1 - a) * cfg.COMBINED_HOLD_BAND + a * float(targets["combined_hold_band"])},
                       source="threshold_calibration")
        return {"before": before, "after": self._thresholds_payload()}

    def _calibrate(self, db: Session, trades: List[Trade], alpha: float) -> Optional[Dict[str, Any]]:
        targets = self._compute_threshold_targets(trades)
        if not targets:
            return None
        update = self._apply_threshold_update(targets, alpha=alpha)
        cfg = self.config
        wins = sum(1 for t in trades if (t.profit_loss or 0.0) > 0)
        losses = sum(1 for t in trades if (t.profit_loss or 0.0) < 0)
        db.add(ThresholdSnapshot(
            timestamp=datetime.now(timezone.utc),
            egm_buy_threshold=cfg.EGM_BUY_THRESHOLD,
            egm_sell_threshold=cfg.EGM_SELL_THRESHOLD,
            combined_buy_threshold=cfg.COMBINED_BUY_THRESHOLD,
            combined_sell_threshold=cfg.COMBINED_SELL_THRESHOLD,
            stats={
                "targets": targets,
                "sample_size": len(trades),
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / len(trades)) * 100 if trades else 0.0,
                "combined_hold_band": cfg.COMBINED_HOLD_BAND,
                **update,
            },
        ))
        db.commit()
        self._persist_thresholds_block(update, targets)
        return {"targets": targets, "update": update, "wins": wins, "losses": losses}

    def force_calibrate_thresholds(self, db: Session, sample_size: int = 500, alpha: float = 1.0,
                                   min_trades: int = 20) -> Dict[str, Any]:
        try:
            before = self._thresholds_payload()
            trades = (
                db.query(Trade).filter(Trade.outcome_status == "final")
                .order_by(Trade.timestamp.desc()).limit(max(1, int(sample_size))).all()
            )
            total = len(trades)
            wins = sum(1 for t in trades if (t.profit_loss or 0.0) > 0)
            losses = sum(1 for t in trades if (t.profit_loss or 0.0) < 0)
            base = {"sample_size": total, "wins": wins, "losses": losses,
                    "win_rate": (wins / total) * 100 if total else 0.0}
            if total < int(min_trades):
                return {"success": False, "message": "not_enough_final_trades", **base,
                        "thresholds": {"before": before, "after": before}}
            res = self._calibrate(db, trades, float(alpha))
            if res is None:
                return {"success": False, "message": "no_targets", **base,
                        "thresholds": {"before": before, "after": before}}
            return {"success": True, **base, "targets": res["targets"], "thresholds": res["update"]}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _auto_tune_thresholds_if_due(self) -> None:
        cfg = self.config
        if not cfg.AUTO_TUNE_THRESHOLDS:
            return
        now_ts = time.time()
        if now_ts - self._last_tune_ts < float(cfg.AUTO_TUNE_INTERVAL_S):
            return
        self._last_tune_ts = now_ts
        with self.SessionLocal() as db:
            trades = (
                db.query(Trade).filter(Trade.outcome_status == "final")
                .order_by(Trade.timestamp.desc()).limit(int(cfg.AUTO_TUNE_SAMPLE)).all()
            )
            if len(trades) < int(cfg.AUTO_TUNE_MIN_TRADES):
                return
            try:
                self._calibrate(db, trades, float(cfg.AUTO_TUNE_ALPHA))
            except Exception as e:
                self._rl_log("thresholds:auto_tune", "warning", f"⚠️ Auto-tune falló: {e}", interval_s=60.0)

    # ----------------------------------------------------- metrics ticks
    async def _live_metrics_tick(self, db: Session) -> None:
        refresh_s = max(1.0, float(self.config.METRICS_LIVE_REFRESH_S))
        now_ts = time.time()
        for symbol in self.symbols:
            if now_ts - self._last_live_metrics_ts.get(symbol, 0.0) < refresh_s:
                continue
            self._last_live_metrics_ts[symbol] = now_ts
            try:
                await self._core_cycle(symbol, db, collect_only=True)
            except Exception as e:
                logger.debug(f"live_metrics_tick skip {symbol}: {e}")

    async def _metrics_snapshot_tick(self, db: Session) -> None:
        interval_s = float(self.config.METRICS_SNAPSHOT_INTERVAL_S)
        now_ts = time.time()
        for symbol in self.symbols:
            if now_ts - self._last_metrics_snapshot_ts.get(symbol, 0.0) < interval_s:
                continue
            metrics = dict(self._last_metrics_by_symbol.get(symbol) or {})
            if not metrics.get("data_ok"):
                await self._core_cycle(symbol, db, collect_only=True)
                metrics = dict(self._last_metrics_by_symbol.get(symbol) or {})
            if not metrics.get("data_ok"):
                continue
            last_price = self._last_price(symbol, self.candles.get(symbol))
            if last_price <= 0:
                continue
            await self._record_metrics_snapshot(db, symbol, last_price, metrics,
                                                self._determine_decision(symbol, metrics))
