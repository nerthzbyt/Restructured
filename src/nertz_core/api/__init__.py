"""API HTTP del motor (FastAPI). Un único proceso: los routers comparten el mismo ``bot``."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from nertz_core.api import admin, market, trading

logger = logging.getLogger("NertzMetalEngine")


def build_routers(bot):
    return [market.build(bot), trading.build(bot), admin.build(bot)]


def create_app(bot) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            preflight = await bot.preflight()
        except Exception as e:
            preflight = {"success": False, "message": str(e)}
        if preflight.get("success"):
            await bot.save_results(symbol=None, trade_result=None)
            await bot.start_storage()
            if bot.start_on_boot:
                bot.schedule_start()
        else:
            logger.error(f"❌ Preflight falló en startup: {preflight.get('message') or 'error'}")
        try:
            yield
        finally:
            await bot.stop_storage()
            bot.stop()

    app = FastAPI(lifespan=lifespan, title="NerT Quant Engine")
    for router in build_routers(bot):
        app.include_router(router)
    return app
