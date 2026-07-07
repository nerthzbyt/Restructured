import json
import logging
import os
from typing import Any, Callable

logger = logging.getLogger("NertzMetalEngine")


class ConfigSettings:
    # Class Constants
    VALID_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
    VALID_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]
    VALID_ORDERBOOK_DEPTHS = [1, 5, 10, 25, 50]
    VALID_ORDER_TYPES = ["Limit", "Market", "limit", "market"]
    VALID_TIME_IN_FORCE = ["GTC", "IOC", "FOK", "PostOnly", "GoodTillCancel", "ImmediateOrCancel", "FillOrKill"]
    DEFAULT_ORDERBOOK_DEPTH = 50

    def __init__(self):
        try:
            # Load variables from environment
            self.BYBIT_API_KEY = self._get_env("BYBIT_API_KEY")
            self.BYBIT_API_SECRET = self._get_env("BYBIT_API_SECRET")
            env_raw = str(self._get_env("BYBIT_ENV", "mainnet") or "").strip().lower()
            if env_raw in {"", "mainnet"}:
                self.BYBIT_ENV = "mainnet"
            elif env_raw == "demo":
                self.BYBIT_ENV = "demo"
            else:
                raise ValueError(
                    f"BYBIT_ENV inválido: {env_raw!r}. Valores permitidos: demo, mainnet"
                )
            self.LIVE_TRADING_ENABLED = self._get_env_bool("LIVE_TRADING_ENABLED", default=False)

            # Validated and defaulted attributes
            self.SYMBOL = self._get_env_symbols("SYMBOL", "BTCUSDT", self.__class__.VALID_SYMBOLS)
            self.TIMEFRAME = self._get_env_with_validation("TIMEFRAME", "1m", self.__class__.VALID_TIMEFRAMES)
            self.ORDER_TYPE = self._get_env_with_validation("ORDER_TYPE", "Limit", self.__class__.VALID_ORDER_TYPES)
            self.TIME_IN_FORCE = self._get_env_with_validation("TIME_IN_FORCE", "GTC",
                                                               self.__class__.VALID_TIME_IN_FORCE)
            self.ORDERBOOK_DEPTH = self._get_env_with_validation(
                "ORDERBOOK_DEPTH", self.DEFAULT_ORDERBOOK_DEPTH, self.VALID_ORDERBOOK_DEPTHS, cast_to=int
            )

            # Numeric parameters
            self.MAX_ITERATIONS = self._get_env_int("MAX_ITERATIONS", default=0, positive=True)
            self.DEFAULT_SLEEP_TIME = self._get_env_int("DEFAULT_SLEEP_TIME", default=10, positive=True)
            self.TRADE_COOLDOWN_S = self._get_env_clamped_float(
                "TRADE_COOLDOWN_S",
                default=0.0,
                min_value=0.0,
                max_value=300.0,
            )
            self.COOLDOWN_BYPASS_STRONG_SIGNAL = self._get_env_bool(
                "COOLDOWN_BYPASS_STRONG_SIGNAL", default=True
            )
            self.COOLDOWN_BYPASS_MULT = self._get_env_clamped_float(
                "COOLDOWN_BYPASS_MULT",
                default=1.25,
                min_value=1.0,
                max_value=5.0,
            )
            self.MAX_CONCURRENT_ORDERS = self._get_env_int(
                "MAX_CONCURRENT_ORDERS", default=20, positive=True
            )
            self.ALLOW_MULTIPLE_ACTIVE_TRADES = self._get_env_bool(
                "ALLOW_MULTIPLE_ACTIVE_TRADES", default=True
            )
            self.OUTCOME_HORIZON_S = self._get_env_clamped_float(
                "OUTCOME_HORIZON_S",
                default=15.0,
                min_value=5.0,
                max_value=3600.0,
            )
            self.SUPPORT_LOOP_INTERVAL_S = self._get_env_clamped_float(
                "SUPPORT_LOOP_INTERVAL_S",
                default=1.0,
                min_value=0.25,
                max_value=30.0,
            )
            self.ORDERS_SYNC_INTERVAL_S = self._get_env_clamped_float(
                "ORDERS_SYNC_INTERVAL_S",
                default=5.0,
                min_value=0.5,
                max_value=300.0,
            )
            self.ORDERS_SYNC_UPDATE_AFTER_S = self._get_env_clamped_float(
                "ORDERS_SYNC_UPDATE_AFTER_S",
                default=5.0,
                min_value=0.5,
                max_value=300.0,
            )
            self.ORDERS_SYNC_TIMEOUT_S = self._get_env_clamped_float(
                "ORDERS_SYNC_TIMEOUT_S",
                default=15.0,
                min_value=1.0,
                max_value=600.0,
            )
            self.ORDERS_SYNC_LIMIT = self._get_env_int("ORDERS_SYNC_LIMIT", default=100, positive=True)
            self.CAPITAL_USDT = self._get_env_float("CAPITAL_USDT", default=2000.0, positive=True)
            self.VOLUME_THRESHOLD = self._get_env_float("VOLUME_THRESHOLD", default=1.0, positive=True)
            self.RISK_FACTOR = self._get_env_clamped_float("RISK_FACTOR", default=0.01, min_value=0.0, max_value=1.0)
            self.MAX_TRADE_SIZE = self._get_env_clamped_float("MAX_TRADE_SIZE", default=0.05, min_value=0.0,
                                                              max_value=1.0)
            self.MIN_TRADE_SIZE = self._get_env_clamped_float("MIN_TRADE_SIZE", default=0.0001, min_value=0.0,
                                                              max_value=1.0)
            self.FEE_RATE = self._get_env_clamped_float("FEE_RATE", default=0.002, min_value=0.0, max_value=0.1)
            self.TP_PERCENTAGE = self._get_env_float("TP_PERCENTAGE", default=1.5, positive=True)
            self.SL_PERCENTAGE = self._get_env_float("SL_PERCENTAGE", default=0.5, positive=True)
            self.PRICE_SHIFT_FACTOR = self._get_env_clamped_float("PRICE_SHIFT_FACTOR", default=0.003, min_value=0.0,
                                                                  max_value=0.1)
            self.RSI_UPPER_THRESHOLD = self._get_env_clamped_float("RSI_UPPER_THRESHOLD", default=80.0, min_value=0.0,
                                                                   max_value=100.0)
            self.RSI_LOWER_THRESHOLD = self._get_env_clamped_float("RSI_LOWER_THRESHOLD", default=20.0, min_value=0.0,
                                                                   max_value=100.0)
            self.RATE_LIMIT_DELAY = self._get_env_int("RATE_LIMIT_DELAY", default=50, positive=True)
            self.PIO_THRESHOLD = self._get_env_float("PIO_THRESHOLD", default=0.0)
            self.EGM_BUY_THRESHOLD = self._get_env_float("EGM_BUY_THRESHOLD", default=0.02)
            self.EGM_SELL_THRESHOLD = self._get_env_float("EGM_SELL_THRESHOLD", default=-0.02)
            self.COMBINED_BUY_THRESHOLD = self._get_env_float("COMBINED_BUY_THRESHOLD", default=4.5)
            self.COMBINED_SELL_THRESHOLD = self._get_env_float("COMBINED_SELL_THRESHOLD", default=-4.5)
            self.COMBINED_HOLD_BAND = self._get_env_float("COMBINED_HOLD_BAND", default=3.0, positive=True)
            self.AVG_SPREAD_BPS = self._get_env_clamped_float(
                "AVG_SPREAD_BPS", default=1.5, min_value=0.1, max_value=50.0
            )
            self.AUTO_GIT_COMMIT = self._get_env_bool("AUTO_GIT_COMMIT", default=False)
            self.ORDERBOOK_LAMBDA = self._get_env_float("ORDERBOOK_LAMBDA", default=0.03, positive=True)
            self.ORDERBOOK_PCT_BAND = self._get_env_clamped_float("ORDERBOOK_PCT_BAND", default=0.015, min_value=0.0,
                                                                  max_value=0.25)
            self.ILD_TARGET_MOVE = self._get_env_clamped_float("ILD_TARGET_MOVE", default=0.002, min_value=0.0001,
                                                               max_value=0.05)
            self.METRICS_WINDOW_MINUTES = self._get_env_clamped_float("METRICS_WINDOW_MINUTES", default=15.0,
                                                                      min_value=1.0, max_value=120.0)
            self.AUTO_TUNE_THRESHOLDS = self._get_env_bool("AUTO_TUNE_THRESHOLDS", default=False)
            self.PERSIST_THRESHOLDS_TO_ENV = self._get_env_bool("PERSIST_THRESHOLDS_TO_ENV", default=False)
            self.FORMULAS = self._get_env_json_dict("FORMULAS_JSON", default={})
            default_formulas = {
                "basis": "(mark_price - index_price) / (index_price + 1e-12)",
                "obi": "(bid_notional_sum_k - ask_notional_sum_k) / (bid_notional_sum_k + ask_notional_sum_k + 1e-12)",
                "tfi": "(taker_buy_qty - taker_sell_qty) / (taker_buy_qty + taker_sell_qty + 1e-12)",
                "spread_rel": "(best_ask - best_bid) / (mid_price + 1e-12)",
                "microprice_offset": "(microprice - mid_price) / (mid_price + 1e-12)",
                "rvol": "RecentTrades:rvol",
                "spread_bps": "SpreadRel * 10000",
                "microprice_offset_bps": "((MicroPrice - MidPrice) / (MidPrice + 1e-12)) * 10000",
            }
            if not isinstance(self.FORMULAS, dict):
                self.FORMULAS = {}
            for k, v in default_formulas.items():
                if k not in self.FORMULAS:
                    self.FORMULAS[k] = v
            self.ML_ENABLED = self._get_env_bool("ML_ENABLED", default=False)
            self.ML_MIN_SAMPLES = self._get_env_int("ML_MIN_SAMPLES", default=50, positive=True)
            self.ML_PROB_THRESHOLD = self._get_env_clamped_float("ML_PROB_THRESHOLD", default=0.6, min_value=0.5,
                                                                 max_value=0.99)
            self.AUTO_AGENT_ENABLED = self._get_env_bool("AUTO_AGENT_ENABLED", default=False)
            self.AUTO_ENABLE_SECONDARY_SYSTEMS = self._get_env_bool("AUTO_ENABLE_SECONDARY_SYSTEMS", default=False)
            self.AUTO_AGENT_TRAIN_INTERVAL_MIN = self._get_env_clamped_float("AUTO_AGENT_TRAIN_INTERVAL_MIN",
                                                                             default=5.0, min_value=1.0,
                                                                             max_value=1440.0)
            self.AUTO_TPSL_ENABLED = self._get_env_bool("AUTO_TPSL_ENABLED", default=True)
            self.AUTO_TPSL_INTERVAL_S = self._get_env_clamped_float("AUTO_TPSL_INTERVAL_S", default=3.0, min_value=0.25,
                                                                    max_value=60.0)
            self.AUTO_TPSL_MIN_TP_MOVE_TICKS = self._get_env_int("AUTO_TPSL_MIN_TP_MOVE_TICKS", default=1,
                                                                 positive=True)
            self.AUTO_TPSL_MIN_SL_MOVE_TICKS = self._get_env_int("AUTO_TPSL_MIN_SL_MOVE_TICKS", default=1,
                                                                 positive=True)
            self.AUTO_TPSL_TRAIL_GAP_MULT = self._get_env_clamped_float("AUTO_TPSL_TRAIL_GAP_MULT", default=1.2,
                                                                        min_value=0.0, max_value=10.0)
            self.AUTO_TPSL_TRAIL_GAP_MIN = self._get_env_clamped_float("AUTO_TPSL_TRAIL_GAP_MIN", default=0.001,
                                                                       min_value=0.0, max_value=0.2)
            self.AUTO_TPSL_TP_EXT_MULT = self._get_env_clamped_float("AUTO_TPSL_TP_EXT_MULT", default=1.25,
                                                                     min_value=1.0, max_value=5.0)
            self.AUTO_TPSL_ML_TP_BOOST = self._get_env_clamped_float("AUTO_TPSL_ML_TP_BOOST", default=1.0,
                                                                     min_value=0.0, max_value=10.0)
            self.TPSL_CANCEL_AFTER_S = self._get_env_clamped_float("TPSL_CANCEL_AFTER_S", default=90.0, min_value=5.0,
                                                                   max_value=3600.0)
            self.FULL_RESET_ON_BOOT = self._get_env_bool("FULL_RESET_ON_BOOT", default=False)

            # Storage (DuckDB batch writer for HF time-series)
            self.STORAGE_BACKEND = self._get_env_with_validation(
                "STORAGE_BACKEND",
                "duckdb",
                ["duckdb", "duck", "sqlite_legacy", "sqlite", "legacy"],
            )
            self.STORAGE_PATH = str(
                self._get_env("STORAGE_PATH", "") or ""
            ).strip() or "data/nertz.duckdb"
            self.STORAGE_BATCH_INTERVAL_MS = self._get_env_clamped_float(
                "STORAGE_BATCH_INTERVAL_MS",
                default=50.0,
                min_value=10.0,
                max_value=5000.0,
            )
            self.ORDERBOOK_PERSIST_INTERVAL_MS = self._get_env_clamped_float(
                "ORDERBOOK_PERSIST_INTERVAL_MS",
                default=200.0,
                min_value=50.0,
                max_value=10000.0,
            )
            self.TICKER_PERSIST_INTERVAL_MS = self._get_env_clamped_float(
                "TICKER_PERSIST_INTERVAL_MS",
                default=200.0,
                min_value=50.0,
                max_value=10000.0,
            )
            self.STORAGE_DISABLE_JSONL = self._get_env_bool(
                "STORAGE_DISABLE_JSONL",
                default=self.STORAGE_BACKEND in {"duckdb", "duck"},
            )
            self.STORAGE_SQLITE_MIRROR = self._get_env_bool(
                "STORAGE_SQLITE_MIRROR",
                default=True,
            )

            # Price Chasing configuration
            self.MAX_CHASE_ATTEMPTS = self._get_env_int("MAX_CHASE_ATTEMPTS", default=3, positive=True)
            self.CHASE_INTERVAL = self._get_env_float("CHASE_INTERVAL", default=2.0, positive=True)

            # Log configuration
            self._log_config()
        except ValueError as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    # Validation and Environment Query Helpers
    def _get_env(self, key: str, default: Any = None):
        """Get an environment variable or its default."""
        return os.getenv(key, default)

    def _get_env_bool(self, key: str, default: bool) -> bool:
        """Get a boolean environment variable."""
        return self._get_env(key, str(default)).lower() == "true"

    def _get_env_symbols(self, key: str, default: str, valid_values: list) -> str:
        value = self._get_env(key, default)
        if not isinstance(value, str) or not value.strip():
            return default
        symbols = [s.strip() for s in value.split(",") if s.strip()]
        if not symbols:
            return default
        if any(s not in valid_values for s in symbols):
            logger.warning(f"Invalid {key}: {value}. Using default '{default}'.")
            return default
        return ",".join(symbols)

    def _get_env_with_validation(self, key: str, default: Any, valid_values: list, cast_to: Callable = str):
        """Validate and cast environment variable."""
        value = self._get_env(key, default)
        try:
            value_cast = cast_to(value)
            if value_cast not in valid_values:
                raise ValueError(f"Value '{value}' for {key} is invalid. Valid values are: {valid_values}")
            return value_cast
        except Exception as e:
            logger.warning(f"Invalid {key}: {e}. Using default '{default}'.")
            return default

    def _get_env_int(self, key: str, default: int, positive: bool = False) -> int:
        """Validate an integer environment variable."""
        value = int(self._get_env(key, default))
        if positive and value < 0:
            raise ValueError(f"{key} must be a positive integer.")
        return value

    def _get_env_float(self, key: str, default: float, positive: bool = False) -> float:
        """Validate a float environment variable."""
        value = float(self._get_env(key, default))
        if positive and value < 0.0:
            raise ValueError(f"{key} must be a positive float.")
        return value

    def _get_env_clamped_float(self, key: str, default: float, min_value: float, max_value: float) -> float:
        """Clamp a float environment variable to a specific range."""
        value = self._get_env_float(key, default)
        if not min_value <= value <= max_value:
            raise ValueError(f"{key} must be between {min_value} and {max_value}.")
        return value

    def _get_env_json_dict(self, key: str, default: dict) -> dict:
        value = self._get_env(key, None)
        if value is None:
            return default if isinstance(default, dict) else {}
        if isinstance(value, dict):
            return value
        if not isinstance(value, str) or not value.strip():
            return default if isinstance(default, dict) else {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else (default if isinstance(default, dict) else {})
        except Exception:
            logger.warning(f"Invalid {key}: JSON inválido. Usando default.")
            return default if isinstance(default, dict) else {}

    # Logging
    def _log_config(self) -> None:
        """Log the loaded configuration."""
        logger.info("Configuration loaded:")
        logger.info(f"  - SYMBOL: {self.SYMBOL}, TIMEFRAME: {self.TIMEFRAME}, ORDER_TYPE: {self.ORDER_TYPE}")
        api_key_status = "SET" if self.BYBIT_API_KEY else "NOT_SET"
        logger.info(
            f"  - BYBIT_ENV: {self.BYBIT_ENV}, LIVE_TRADING_ENABLED: {self.LIVE_TRADING_ENABLED}, BYBIT_API_KEY: {api_key_status}"
        )
        logger.info(
            f"  - STORAGE: {self.STORAGE_BACKEND} @ {self.STORAGE_PATH}, batch_ms={self.STORAGE_BATCH_INTERVAL_MS}"
        )
