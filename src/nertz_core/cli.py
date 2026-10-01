"""Servidor standalone y lanzador interactivo del motor."""
from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import logging
import os

import uvicorn

logger = logging.getLogger("NertzMetalEngine")


async def launcher_loop(bot, server: uvicorn.Server) -> None:
    cfg = bot.config
    expected = os.getenv("NERTZ_LAUNCHER_PASSWORD") or ""
    if expected:
        pw = await asyncio.to_thread(getpass.getpass, "Password: ")
        if pw != expected:
            print("Login failed.")
            await server.shutdown()
            return

    def _status() -> None:
        print(json.dumps({
            "running": bot.running,
            "mode": bot.mode,
            "support_loop_running": bool(bot.support_task is not None and not bot.support_task.done()),
            "auto_hft_enabled": bot.auto_hft_enabled_effective(),
            "hft": {s: {"running": bot.is_hft_running(s), "params": bot.hft_params.get(s) or {}} for s in bot.symbols},
        }, indent=2))

    def _start_all_hft() -> None:
        bot.mode = "hft"
        for s in bot.symbols:
            bot.start_hft(s, interval_ms=cfg.AUTO_HFT_INTERVAL_MS, collect_only=cfg.AUTO_HFT_COLLECT_ONLY)

    def _set_mode(mode: str) -> None:
        bot.mode = mode
        bot.stop_all_hft()

    def _auto_hft(enabled: bool) -> None:
        cfg.update({"AUTO_HFT_ENABLED": enabled}, source="launcher")
        bot.auto_hft_enabled = enabled
        if not enabled:
            bot.stop_all_hft()

    menu = {
        "1": ("Status", _status),
        "2": ("Start bot", lambda: bot.schedule_start()),
        "3": ("Stop bot", bot.stop),
        "4": ("Mode normal", lambda: _set_mode("normal")),
        "5": ("Mode full", lambda: _set_mode("full")),
        "6": ("Start HFT (all symbols)", _start_all_hft),
        "7": ("Stop HFT (all symbols)", bot.stop_all_hft),
        "8": ("Enable auto-HFT", lambda: _auto_hft(True)),
        "9": ("Disable auto-HFT", lambda: _auto_hft(False)),
    }
    while True:
        print("")
        for key, (label, _) in menu.items():
            print(f"{key}) {label}")
        print("0) Exit")
        choice = (await asyncio.to_thread(input, "> ")).strip()
        if choice == "0":
            await server.shutdown()
            bot.stop()
            return
        action = menu.get(choice)
        if action:
            action[1]()


async def serve(bot, app) -> None:
    cfg = bot.config
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=cfg.API_HOST)
    parser.add_argument("--port", type=int, default=cfg.API_PORT)
    parser.add_argument("--api-only", action="store_true")
    parser.add_argument("--launcher", action="store_true")
    parser.add_argument("--auto-hft", action="store_true")
    args = parser.parse_args()

    bot.start_on_boot = not (args.api_only or args.launcher)
    if args.auto_hft:
        cfg.update({"AUTO_HFT_ENABLED": True}, source="cli")
        bot.auto_hft_enabled = True

    server = uvicorn.Server(uvicorn.Config(app, host=str(args.host), port=int(args.port)))
    try:
        logger.info("🚀 Iniciando servidor API...")
        if args.launcher:
            serve_task = asyncio.create_task(server.serve())
            await asyncio.sleep(0.25)
            await launcher_loop(bot, server)
            if not serve_task.done():
                serve_task.cancel()
            return
        await server.serve()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupción del usuario detectada.")
        await server.shutdown()
        bot.stop()
    except Exception as e:
        logger.error(f"❌ Error crítico en main(): {e}")
        await server.shutdown()
