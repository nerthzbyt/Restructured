# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/).

## [5.1.0-linux] — 2026-07-07

### Added

- Soporte Linux para `qwen_desktop`: JWT desde Firefox, Chromium y snap paths.
- Endpoint `GET /agent/chat/history` y restauración de historial en Agent Console UI.
- Montaje estático `GET /project-docs/` (documentación v5 embebida en el servidor).
- Helper `_live_metrics_for_symbol()` — fuente fiel al loop del motor.
- `.env.example` con plantilla de arranque único (`main.py` puerto 8787).
- Scripts `scripts/validate_system.py` y `scripts/benchmark_qwen.py`.
- Documentación `docs/v5/linux-platform.md`.

### Fixed

- **L0 0% bug**: `/agent/prediction-level` y `/agent/context` leían `ticker_data.metrics` vacío; ahora usan `_last_metrics_by_symbol`.
- **UI Buy/Sell TH**: umbrales sincronizados desde `bot_live_state.thresholds`.
- **Enlace Documentación**: footer apuntaba a GitHub Pages sin deploy; fallback local `/project-docs/`.
- Carga de `.env` en `main.py` al iniciar el agente.

### Changed

- `react_agent.py`: mejoras en trazabilidad ReAct y contexto del agente.
- `docs/v5/agent-api.md` y `docs/index.html`: endpoints actualizados.

### Deployment

- Push a `main` con cambios en `docs/**` dispara `.github/workflows/pages.yml` → GitHub Pages en `https://nerthzbyt.github.io/Restructured/`.