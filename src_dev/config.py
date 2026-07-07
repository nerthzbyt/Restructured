"""Configuración del entorno dev — todo desde .env, sin hardcode de ranking."""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict

from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
DEV_ENV = os.path.join(os.path.dirname(__file__), ".env")

for path in (SRC_DIR, PROJECT_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
load_dotenv(DEV_ENV, override=False)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def private_rest_base_url() -> str:
    env = str(os.getenv("BYBIT_ENV", "mainnet") or "mainnet").strip().lower()
    if env == "demo":
        return "https://api-demo.bybit.com"
    return "https://api.bybit.com"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default)) or str(default)).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _load_combined_weights() -> Dict[str, float]:
    raw = str(os.getenv("DEV_COMBINED_WEIGHTS", "") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return {str(k): float(v) for k, v in parsed.items()}
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    from optimizer import DEFAULT_COMBINED_WEIGHTS

    return DEFAULT_COMBINED_WEIGHTS.as_dict()


def load_trading_thresholds() -> Dict[str, Any]:
    from settings import ConfigSettings

    c = ConfigSettings()
    return {
        "combined_buy": float(c.COMBINED_BUY_THRESHOLD),
        "combined_sell": float(c.COMBINED_SELL_THRESHOLD),
        "combined_hold_band": float(c.COMBINED_HOLD_BAND),
        "tp_pct": float(c.TP_PERCENTAGE),
        "sl_pct": float(c.SL_PERCENTAGE),
        "capital_usdt": float(c.CAPITAL_USDT),
        "min_trade_size": float(c.MIN_TRADE_SIZE),
        "orderbook_lambda": float(c.ORDERBOOK_LAMBDA),
        "orderbook_pct_band": float(c.ORDERBOOK_PCT_BAND),
        "ild_target_move": float(c.ILD_TARGET_MOVE),
        "metrics_window_minutes": float(c.METRICS_WINDOW_MINUTES),
    }


@dataclass(frozen=True)
class BybitEndpoints:
    rest_base: str = "https://api.bybit.com"
    ws_spot_public: str = "wss://stream.bybit.com/v5/public/spot"
    ws_linear_public: str = "wss://stream.bybit.com/v5/public/linear"

    def rest(self, path: str) -> str:
        return f"{self.rest_base.rstrip('/')}{path}"


@dataclass
class DevSettings:
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    orderbook_depth: int = 50
    recent_trades_limit: int = 50
    metrics_window_minutes: float = 15.0
    orderbook_lambda: float = 0.03
    orderbook_pct_band: float = 0.015
    ild_target_move: float = 0.002
    lab_top_n: int = 5
    lab_history_samples: int = 6
    lab_history_interval_s: float = 2.0
    lab_order_history_limit: int = 500
    lab_order_stats_source: str = "exchange"
    lab_old_results_path: str = ""
    lab_min_order_stats_samples: int = 50
    lab_require_credentials: bool = True
    lab_min_calibration_samples: int = 4
    lab_ws_probe_s: float = 10.0
    combined_weights: dict = field(default_factory=dict)
    endpoints: BybitEndpoints = field(default_factory=BybitEndpoints)

    @classmethod
    def from_env(cls) -> "DevSettings":
        sym = str(os.getenv("SYMBOL", "BTCUSDT") or "BTCUSDT").split(",")[0].strip()
        th = load_trading_thresholds()
        return cls(
            symbol=sym,
            timeframe=str(os.getenv("TIMEFRAME", "1m") or "1m").strip(),
            orderbook_depth=int(os.getenv("ORDERBOOK_DEPTH", "50") or 50),
            recent_trades_limit=_env_int("RECENT_TRADES_LIMIT", 50),
            metrics_window_minutes=th["metrics_window_minutes"],
            orderbook_lambda=th["orderbook_lambda"],
            orderbook_pct_band=th["orderbook_pct_band"],
            ild_target_move=th["ild_target_move"],
            lab_top_n=_env_int("DEV_ORDER_LAB_TOP_N", 5),
            lab_history_samples=_env_int("DEV_LAB_HISTORY_SAMPLES", 6),
            lab_history_interval_s=_env_float("DEV_LAB_HISTORY_INTERVAL_S", 2.0),
            lab_order_history_limit=_env_int("DEV_LAB_ORDER_HISTORY_LIMIT", 500),
            lab_order_stats_source=str(
                os.getenv("DEV_LAB_ORDER_STATS_SOURCE", "exchange") or "exchange"
            ).strip().lower(),
            lab_old_results_path=str(
                os.getenv("DEV_LAB_OLD_RESULTS_PATH", "")
                or os.path.join(os.path.dirname(__file__), "old_results.json")
            ),
            lab_min_order_stats_samples=_env_int("DEV_LAB_MIN_ORDER_STATS_SAMPLES", 50),
            lab_require_credentials=_env_bool("DEV_LAB_REQUIRE_CREDENTIALS", True),
            lab_min_calibration_samples=_env_int("DEV_LAB_MIN_CALIBRATION_SAMPLES", 4),
            lab_ws_probe_s=_env_float("DEV_LAB_WS_PROBE_S", 10.0),
            combined_weights=_load_combined_weights(),
        )

    def ranked_output_path(self) -> str:
        return os.path.join(OUTPUT_DIR, "order_lab_ranked.json")

    def top_output_path(self) -> str:
        return os.path.join(OUTPUT_DIR, f"order_lab_top{max(1, int(self.lab_top_n))}.json")

    def debug_output_path(self) -> str:
        return os.path.join(OUTPUT_DIR, "order_lab_debug.json")

    @property
    def sqlite_path(self) -> str:
        return os.path.join(DATA_DIR, "trading.db")

    @property
    def jsonl_path(self) -> str:
        return os.path.join(DATA_DIR, "metrics_snapshots.jsonl")


DEFAULT_SETTINGS = DevSettings.from_env()