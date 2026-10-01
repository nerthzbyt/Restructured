"""Estado live del motor Nertzh embebido (mismo proceso que NerT_AI_PRO/main.py)."""
from __future__ import annotations

import json
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
JSONL_PATH = BASE_DIR / "data" / "metrics_snapshots.jsonl"

_MOM_KEYS = (
    "mom",
    "mom_raw",
    "ret5m",
    "ret20m",
    "combined",
    "pio",
    "egm",
    "ild",
    "data_ok",
)


def _tail_jsonl_mom(symbol: str, *, n: int = 8) -> List[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym or not JSONL_PATH.is_file():
        return []
    try:
        lines = JSONL_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    hits: List[Dict[str, Any]] = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol", "")).strip().upper() != sym:
            continue
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        hits.append(
            {
                "ts": row.get("ts") or row.get("timestamp"),
                "decision": row.get("decision"),
                "combined": metrics.get("combined"),
                "mom": metrics.get("mom"),
                "mom_raw": metrics.get("mom_raw"),
                "ret5m": metrics.get("ret5m"),
                "ret20m": metrics.get("ret20m"),
            }
        )
        if len(hits) >= max(1, int(n)):
            break
    hits.reverse()
    return hits


def _pick_metrics(metrics: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in _MOM_KEYS:
        if k in metrics:
            out[k] = metrics[k]
    return out


def bot_live_state_snapshot(*, symbol: str = "BTCUSDT") -> Dict[str, Any]:
    """Lee directamente bot.* — fuente fiel al loop, sin recalcular vía HTTP."""
    try:
        import Nertzh as nertzh

        bot = nertzh.bot
        cfg = nertzh.config
    except Exception as e:
        return {"ok": False, "error": "motor_unavailable", "message": str(e)}

    sym = str(symbol or "BTCUSDT").strip().upper()
    candles_buf = bot.candles.get(sym) or []
    candle_count = len(candles_buf) if isinstance(candles_buf, list) else 0

    metrics = dict(bot._last_metrics_by_symbol.get(sym) or {})
    metrics_pick = _pick_metrics(metrics)

    detail: Dict[str, Any] = {}
    if metrics:
        try:
            detail = bot._decision_detail(sym, metrics)
        except Exception:
            detail = {}

    window_tail: List[Dict[str, Any]] = []
    q = bot._metrics_window.get(sym)
    if isinstance(q, deque):
        for row in list(q)[-15:]:
            if isinstance(row, dict):
                window_tail.append(dict(row))

    start_task = getattr(bot, "_start_task", None) or getattr(bot, "start_task", None)
    support_task = getattr(bot, "support_task", None)

    return {
        "ok": True,
        "source": "motor_interno",
        "entry_point": "python NerT_AI_PRO/main.py (puerto 8787, motor embebido en /api)",
        "symbol": sym,
        "bot_running": bool(getattr(bot, "running", False)),
        "start_task_active": bool(start_task is not None and not start_task.done()),
        "support_loop_active": bool(support_task is not None and not support_task.done()),
        "iterations": int(getattr(bot, "iterations", 0) or 0),
        "candle_count_memory": candle_count,
        "candles_sufficient_for_mom": candle_count >= 21,
        "metrics_loop": metrics_pick,
        "decision_detail": detail,
        "recent_window_decisions": window_tail,
        "jsonl_mom_tail": _tail_jsonl_mom(sym, n=8),
        "thresholds": {
            "combined_buy": float(getattr(cfg, "COMBINED_BUY_THRESHOLD", 0.0) or 0.0),
            "combined_sell": float(getattr(cfg, "COMBINED_SELL_THRESHOLD", 0.0) or 0.0),
            "hold_band": float(getattr(cfg, "COMBINED_HOLD_BAND", 0.0) or 0.0),
        },
        "data_source_priority": [
            "1. bot_live_state (este snapshot, loop en memoria)",
            "2. nertzh_api.decisions (métricas de _last_metrics_by_symbol)",
            "3. nertzh_api.candles/{symbol}/50 (retornos si hace falta)",
            "4. src_read data/metrics_snapshots.jsonl (historial mom)",
            "5. nertzh_api.metrics/combined SOLO si no hay loop activo (recalculan; ahora usan velas en memoria)",
        ],
        "note": (
            "Para análisis en vivo, confía en metrics_loop y decision_detail de este snapshot "
            "o en nertzh_api.decisions — no en métricas recalculadas aisladas."
        ),
        "timestamp": int(time.time() * 1000),
    }