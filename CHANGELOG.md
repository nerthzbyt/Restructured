# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/).

## [Unreleased]

### Fixed
- Hallazgos de Qodana / Inspect Code (PyCharm): `done` posiblemente sin asignar en el writer de DuckDB, `_convert` tratado como no invocable, código inalcanzable en `quantize_to_step` (un `step` NaN ahora devuelve el valor sin cuantizar) y formatos `:,.2f` sobre tipos no numéricos en `analyze_all.py`.
- `except Exception` silenciosos acotados al tipo de error real; los que deben seguir amplios (llamadas al exchange, fórmulas configurables) ahora registran el error en el log.

### Changed
- Los routers API, `cli.py` y el migrador usan métodos/propiedades públicos del motor y de `DuckDBBackend` en lugar de miembros `_privados`.
- Nuevo `nertz_core/engine/host.py`: declara el estado compartido que usan los mixins (solo anotaciones, sin efecto en runtime).
- Diccionario de proyecto compartido para el corrector del IDE en `.idea/dictionaries/project.xml`.

## [5.2.0] — 2026-09-30

Reestructuración de `src/` como monolito modular. Mapa completo en `docs/v5/architecture.md`.

### Changed
- `src/Nertzh.py` (6.186 líneas) queda como fachada; el motor vive en `src/nertz_core/` (engine en mixins por responsabilidad y routers FastAPI por dominio). Mismo proceso, mismas rutas API.
- `src/settings.py`: registro único y tipado de configuración (~150 claves) con validación fail-fast, `update()` en caliente, overrides por símbolo (`SYMBOL_OVERRIDES_JSON`) y generación de `.env.example`. Se eliminan la lista blanca de símbolos, las URLs fijas y los `getattr(config, "X", default)`.
- `signal_engine.SignalParams`: umbrales de señal configurables vía `SIGNAL_PARAMS_JSON` (defaults idénticos).
- Tamaño de orden por nocional (`MAX_POSITION_NOTIONAL_PCT`, 10 % por defecto); `MAX_TRADE_SIZE`/`MIN_TRADE_SIZE` pasan a ser topes opcionales (0 = sin tope).
- Soporte `BYBIT_ENV=testnet` y overrides de endpoints.

### Added
- `GET /config/schema`, `POST /config/update`, `POST /symbols/add`.
- Tests: `tests/test_core_units.py`, `tests/test_engine_offline.py` (ciclo completo con un exchange simulado).

### Fixed
- PnL: las comisiones se restan sobre el nocional de entrada y de salida (antes reducían las pérdidas).
- TP/SL virtual: ahora dispara y cierra la posición con una orden real; antes nunca disparaba y guardaba un % como PnL.
- En live con TP/SL activo, el horizonte ya no marca `final` posiciones que siguen abiertas en el exchange.
- Fills parciales cancelados conservan la posición ejecutada.
- TFI entraba en el combined siempre como 0 (la historia no guardaba `tfi_raw`).
- Un peso 0 explícito ya no se sustituye por el default; los pesos optimizados ya no se pierden con el siguiente ticker.
- TP/SL cuantizados al tick real (antes `round(x, 2)`).
- `AUTO_ENABLE_SECONDARY_SYSTEMS` ahora se respeta; `MAX_CONCURRENT_ORDERS` ahora se aplica.
- DuckDB: `flush()` persiste toda la cola y se corrige una carrera que perdía registros.
- `nertz status` ya no imprime la API secret.

### Performance
- Z-scores con historia columnar (≈6× más rápido por ciclo), libro de órdenes incremental, IO de results.json fuera del event loop, sync de órdenes solo para trades no ejecutados.

## [5.1.1] — 2026-07-07

### Documentación
- GitHub Pages: `.nojekyll`, secciones live-verify y benchmark Qwen en `docs/index.html`.
- Nuevos: `docs/v5/live-verification.md`, `docs/v5/qwen-benchmark.md`, `docs/v5/qwen_benchmark_advanced.json`.
- Catálogo agente: `live_verification`, `qwen_benchmark`, `docs_public_url` en `/agent/catalog`.

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