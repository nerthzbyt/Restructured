"""
NerT Quant Engine — punto de entrada y fachada del motor.

Arquitectura (monolito modular, un solo proceso):

    settings.py            registro único de configuración (.env + cambios en caliente)
    signal_engine.py       decisión: umbrales, estados de mercado, vetos y gates
    utils.py               métricas de microestructura y persistencia results.json
    bybit_v5.py            cliente REST Bybit v5 (firmado + público)
    optimizer.py           búsqueda de umbrales/pesos sobre trades históricos
    nertz_core/
        db.py              modelos SQLAlchemy + Database
        market.py          libro incremental, velas, parsers WS/REST
        history.py         historia columnar para z-scores
        sizing.py          reglas de instrumento, cuantización, tamaño y TP/SL
        accounting.py      PnL con comisiones, wallet, resúmenes
        ml.py              filtro logístico
        engine/            NertzMetalEngine = core + market_data + orders + tpsl + automation + reporting
        api/               routers FastAPI (market, trading, admin)
        cli.py             servidor standalone y lanzador

Este módulo conserva los nombres que usan NerT_AI_PRO, tests y scripts
(``config``, ``bot``, ``app``, ``SessionLocal``, modelos, ``BASE_URL``...).
"""
import asyncio
import logging
import os
import sys

_SRC = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SRC, ".."))
for _p in (_SRC, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NertzMetalEngine")

from bybit_v5 import BybitV5Client  # noqa: E402
from nertz_core.api import create_app  # noqa: E402
from nertz_core.db import (  # noqa: E402
    BalanceSnapshot,
    Base,
    MarketData,
    MarketTicker,
    MetricSnapshot,
    Orderbook,
    ThresholdSnapshot,
    Trade,
)
from nertz_core.engine import NertzMetalEngine  # noqa: E402
from nertz_core.engine.reporting import _THRESHOLD_ENV_KEYS, persist_values_to_env  # noqa: E402
from nertz_core.runtime import RuntimePaths, default_config, default_database  # noqa: E402
from settings import TIMEFRAME_TO_BYBIT, ConfigSettings  # noqa: E402

config: ConfigSettings = default_config()
database = default_database()
paths = RuntimePaths.from_config(config)

engine = database.engine
SessionLocal = database.SessionLocal
get_db = database.get_db
DATABASE_DIR = paths.data_dir
DATABASE_URL = paths.sqlite_path

_endpoints = config.endpoints()
BASE_URL = _endpoints.rest_public
WS_URL = _endpoints.ws_public


def timeframe_to_bybit_interval(timeframe: str) -> str:
    return TIMEFRAME_TO_BYBIT.get(timeframe, timeframe.replace("m", ""))


def _persist_thresholds_to_env(env_path: str):
    return persist_values_to_env(env_path, {k: float(getattr(config, k)) for k in _THRESHOLD_ENV_KEYS})


bot = NertzMetalEngine(config, database, paths=paths)
app = create_app(bot)


async def main() -> None:
    from nertz_core.cli import serve

    await serve(bot, app)


__all__ = [
    "BalanceSnapshot", "Base", "BASE_URL", "BybitV5Client", "ConfigSettings", "DATABASE_DIR", "DATABASE_URL",
    "MarketData", "MarketTicker", "MetricSnapshot", "NertzMetalEngine", "Orderbook", "SessionLocal",
    "ThresholdSnapshot", "Trade", "WS_URL", "app", "bot", "config", "database", "engine", "get_db", "logger",
    "main", "paths", "timeframe_to_bybit_interval",
]


if __name__ == "__main__":
    asyncio.run(main())
