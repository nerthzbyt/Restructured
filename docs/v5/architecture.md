# Mapa del sistema — NerT Quant Engine v5

Este documento describe la arquitectura real de `src/` tras la reestructuración de 2026-09:
qué hace cada pieza, cómo fluyen los datos, dónde se persiste cada cosa y cómo extender el
sistema sin tocar código fijo.

**Principio de diseño: monolito modular.** Todo corre en **un solo proceso y un solo event
loop**. Los módulos están desacoplados por responsabilidad, pero no hay microservicios, colas
externas ni llamadas por red entre componentes. El motor, la API y el agente comparten el mismo
objeto `bot` en memoria.

---

## 1. Vista general

```
                    ┌───────────────────────────────────────────────┐
                    │ NerT_AI_PRO/main.py  (FastAPI :8787)          │
                    │ agente ReAct · Qwen · MCP · UI                │
                    │   monta el motor en /api  ─────────────┐      │
                    └────────────────────────────────────────┼──────┘
                                                             │ mismo proceso
┌────────────────────────────────────────────────────────────▼──────────────────┐
│ src/Nertzh.py — fachada: config · database · bot · app                        │
│                                                                               │
│  settings.py ─── registro único de configuración (.env + cambios en caliente) │
│       │                                                                       │
│  nertz_core/engine  NertzMetalEngine                                          │
│   ├─ core.py         estado por símbolo · ciclo de decisión · apertura        │
│   ├─ market_data.py  REST inicial · WebSocket · persistencia tick/libro       │
│   ├─ orders.py       cliente Bybit · preflight · órdenes · sync · balance     │
│   ├─ tpsl.py         TP/SL virtual (trailing + cierre real)                   │
│   ├─ automation.py   agente interno · auto-HFT · auto-tune · ticks métricas   │
│   └─ reporting.py    snapshots · eventos · results.json · serialización       │
│                                                                               │
│  signal_engine.py  utils.py(métricas)  optimizer.py  bybit_v5.py              │
│  nertz_core/{market, history, sizing, accounting, ml, db, runtime}.py         │
│  nertz_core/api/{market, trading, admin}.py  → routers FastAPI                │
└───────────────┬────────────────────────────┬──────────────────────────────────┘
                │                            │
       Bybit v5 REST/WS            SQLite (data/trading.db) · DuckDB (data/nertz.duckdb)
                                   logs/results.json · data/metrics_snapshots.jsonl
```

---

## 2. Módulos de `src/`

| Módulo | Responsabilidad | Depende de |
|---|---|---|
| `settings.py` | **Registro único** `SETTINGS`: clave, tipo, default, rango, descripción. Carga `.env`, valida (fail-fast), `update()` en caliente, `for_symbol()`, `endpoints()`, `schema()`, `.env.example` | `signal_engine` (pesos/params) |
| `signal_engine.py` | Decisión pura: `SignalParams`, umbrales simétricos escalados por volatilidad, estados de mercado (optimal/chop/toxic/breakout), vetos (spoof, microprice, TFI), gates de ejecución | numpy |
| `utils.py` | Métricas de microestructura (`calculate_metrics`: PIO, ILD, EGM, ROL, OGM, MOM, TFI, combined), fórmulas TSM, discovery (soportes/resistencias), persistencia `results.json`/JSONL | `signal_engine`, `nertz_core.history` |
| `bybit_v5.py` | Cliente async Bybit v5: firma HMAC, reintentos con backoff, endpoints públicos (kline, orderbook, tickers, instruments) y privados (órdenes, wallet) | aiohttp |
| `optimizer.py` | Búsqueda aleatoria + local de umbrales y pesos sobre trades finalizados | `signal_engine` |
| `Nertzh.py` | Fachada y entrypoint: expone `config`, `bot`, `app`, `SessionLocal`, modelos, `BASE_URL`/`WS_URL` | todo |
| `nertz_core/runtime.py` | Singletons de proceso (config, base de datos) y rutas derivadas de la config | `settings` |
| `nertz_core/db.py` | Modelos SQLAlchemy, `Database` (engine + sesiones + migración aditiva), helpers | SQLAlchemy |
| `nertz_core/market.py` | `OrderBook` incremental (snapshot + deltas), `Candle`, parsers de ticker y trades | — |
| `nertz_core/history.py` | `MetricHistory` columnar con desalojo por tiempo y `z_score` vectorizado | numpy |
| `nertz_core/sizing.py` | `InstrumentRules` (tick/lot/mínimos del exchange), cuantización Decimal, `size_order`, `protective_levels` | — |
| `nertz_core/accounting.py` | PnL con comisiones de entrada y salida, parser de wallet, capital inicial, `pnl_summary` | — |
| `nertz_core/ml.py` | Regresión logística numpy (features, entrenamiento, predicción) | numpy |
| `nertz_core/api/*` | Routers FastAPI: `market` (lectura de mercado/métricas), `trading` (trades, órdenes, HFT, decisiones), `admin` (ciclo de vida, config, agente, storage, validación) | engine |
| `nertz_core/cli.py` | Servidor standalone (`python src/Nertzh.py`) y lanzador interactivo | uvicorn |

Fuera de `src/`, el motor usa `nertz_engine/storage` (DuckDB con escritura por lotes) y
`nertz_engine/engine/symbols.py` (`OperationManager`: cooldown por símbolo).

---

## 3. Flujo de datos (camino caliente)

```
Bybit WS público ──► _on_message ──┬─ orderbook  → OrderBook.apply_delta  (sin reparsear el libro)
                                   ├─ tickers    → ticker_data[sym].update (conserva combined_weights)
                                   ├─ publicTrade→ recent_trades (deque)
                                   └─ kline      → candles[sym]; persiste al abrir/confirmar
                                                     │ confirm=true
                                                     ▼
                                         _core_cycle(symbol)   (lock por símbolo)
   1. _maybe_sync_balance          ← BALANCE_SYNC_INTERVAL_S / balance "sucio"
   2. compute_metrics(record=True) ← calculate_metrics + MetricHistory (z-scores en ventana)
   3. evaluate_signal              ← umbrales (for_symbol) + SignalParams
   4. _finalize_due_outcomes       ← solo si no hay TP/SL virtual gestionando la posición
   5. snapshot de métricas         → SQLite / DuckDB / JSONL / results.json (IO fuera del loop)
   6. filtro ML (opcional)
   7. check_execution_gates        ← spread, rvol, antigüedad del último trade, spoof
   8. cooldown / modo un-trade
   9. _open_position:
        reglas de instrumento (cache TTL) → size_order → protective_levels
        → _place_order (semáforo MAX_CONCURRENT_ORDERS) → Trade(pending)
```

### Bucle de soporte (`SUPPORT_LOOP_INTERVAL_S`)

```
_enable_secondary_systems_if_due → sync_open_orders → _agent_tick (si AUTO_AGENT_ENABLED)
→ _auto_hft_tick → _auto_tpsl_tick (si AUTO_TPSL_ENABLED) → _live_metrics_tick → _metrics_snapshot_tick
```

### Ciclo de vida de un trade

```
pending ──(fill parcial)──► partial ──► filled ──(TP/SL virtual tocado → orden real de cierre)──► final
   │                           │           ▲
   │                           └─ cancelada con ejecución ─┘  (la posición ejecutada se conserva)
   └─(cancelada/rechazada sin ejecución)──► cancelled
filled ──(sin TP/SL virtual: horizonte OUTCOME_HORIZON_S, mark-to-market)──► final
entrada inválida ──► invalid_entry
```

`sync_open_orders` solo consulta en el exchange los trades aún no ejecutados; los `filled`
pasan a manos del gestor TP/SL.

---

## 4. Persistencia

| Almacén | Contenido | Escritor |
|---|---|---|
| `data/trading.db` (SQLite) | `trades`, `metric_snapshots`, `balance_snapshots`, `threshold_snapshots`, espejo de `market_data`/`market_ticker`/`orderbook` | engine (sesión por operación) |
| `data/nertz.duckdb` | series HF: `market_ticks`, `orderbook_snapshots`, `metric_snapshots` (con JSON completo de métricas), `engine_events` | `DuckDBBackend` por lotes; también rehidrata la historia de z-scores al arrancar |
| `logs/results.json` | resumen, capital, trades, eventos (máx. `RESULTS_MAX_EVENTS`) | `utils.save_results`/`append_results_event` vía `asyncio.to_thread` |
| `data/metrics_snapshots.jsonl` | espejo legible de snapshots (desactivado por defecto con DuckDB) | reporting |

---

## 5. Configuración: cómo evoluciona el sistema sin tocar código

* **Un único lugar**: `src/settings.py → SETTINGS`. Añadir un parámetro es añadir un `Setting`;
  el resto del código lo lee como `config.CLAVE`.
* **Validación**: un valor inválido en `.env` detiene el arranque con un mensaje claro. En
  caliente, `config.update()` es atómico (o se aplican todos los cambios o ninguno) y la API
  responde `422`.
* **En caliente**: `POST /config/update {"CLAVE": valor}`; esquema y cambios recientes en
  `GET /config/schema`.
* **Por símbolo**: `SYMBOL_OVERRIDES_JSON={"XRPUSDT": {"RISK_FACTOR": 0.02, "MAX_TRADE_SIZE": 500}}`.
  Sirve para riesgo, umbrales `COMBINED_*`, TP/SL, cooldown, spread de referencia y parámetros
  del libro.
* **Cualquier par**: `SYMBOL` acepta cualquier símbolo con formato válido (ya no hay lista
  blanca). En caliente: `POST /symbols/add?symbol=SOLUSDT` carga datos, reglas y suscripción WS.
* **Entornos**: `BYBIT_ENV = mainnet | demo | testnet`, con overrides `BYBIT_REST_URL`,
  `BYBIT_PUBLIC_REST_URL` y `BYBIT_WS_PUBLIC_URL`.
* **Señal**: `SIGNAL_PARAMS_JSON` sobreescribe cualquier campo de `signal_engine.SignalParams`;
  `COMBINED_WEIGHTS_JSON` fija los pesos por defecto; `FORMULAS_JSON` añade métricas derivadas
  (TSM) sin tocar código.
* **Precisión**: tick, paso de cantidad y mínimos se leen del exchange (`instruments-info`); no
  hay decimales fijos.
* `.env.example` se genera desde el registro (`python src/settings.py --env-example`) y CI
  comprueba que esté sincronizado.

---

## 6. API (montada en `/api` por NerT_AI_PRO)

| Router | Rutas |
|---|---|
| `market` | `/market_data/{s}`, `/ticker/{s}`, `/metrics/{s}`, `/combined/{s}`, `/ild|rol|pio|egm|ogm/{s}`, `/discovery/metrics/{s}`, `/orderbook/{s}`, `/candles/{s}/{n}` |
| `trading` | `/profit`, `/trades/{s}`, `/last_trade/{s}`, `/ml/dataset/trades`, `/execute_trade/{s}`, `/hft/*`, `/mode/*`, `/balance`, `/orders/status`, `/orders/sync`, `/order_status/{id}`, `/exchange/open_orders/{s}`, `/decisions/{s}`, `/operations/status` |
| `admin` | `/start`, `/stop`, `/status`, `/health`, `/symbols/add`, `/config`, `/config/schema`, `/config/update`, `/config/update_all`, `/config/update_thresholds`, `/settings`, `/ml/*`, `/admin/agent/*`, `/admin/tpsl/*`, `/admin/auto_hft/*`, `/admin/optimize/system`, `/admin/full_reset`, `/storage/*`, `/validation` |

Rutas y parámetros son los mismos de antes (comprobado comparando el OpenAPI). Las rutas nuevas
son `/config/schema`, `/config/update` y `/symbols/add`.

---

## 7. Correcciones de comportamiento incluidas en la reestructuración

| Área | Antes | Ahora |
|---|---|---|
| PnL | `pnl × (1 − fee)`: las comisiones *reducían* las pérdidas | `bruto − fee × (nocional entrada + salida)`; `pnl_gross` guardado aparte |
| TP/SL virtual | disparaba contra niveles ya re-ajustados al precio actual → **nunca cerraba**; además guardaba un % como PnL en USDT con estado `closed` | dispara contra los niveles vigentes, cierra con orden real (Market por defecto) y registra `final` con PnL neto |
| Horizonte vs TP/SL (live) | a los 15 s marcaba `final` y la posición quedaba abierta en el exchange sin gestión | con `AUTO_TPSL_ENABLED` la cierra el gestor TP/SL; el horizonte solo aplica sin TP/SL |
| Fills parciales | `PartiallyFilledCanceled` o cancelada con ejecución → estado huérfano o `cancelled` (posición perdida) | pasa a `filled` con la cantidad y el precio medio ejecutados |
| TFI en combined | la historia no guardaba `tfi_raw` → **z(TFI) siempre 0** | se registra y participa con su peso (`COMBINED_WEIGHTS_JSON` con `"tfi": 0` recupera el comportamiento previo) |
| Pesos 0 | `peso or default`: un peso 0 volvía al default | un 0 explícito se respeta |
| Pesos optimizados | se guardaban en `ticker_data` y el siguiente ticker WS los borraba | el ticker se actualiza en sitio; los pesos persisten |
| TP/SL | `round(x, 2)`: roto en pares de precio bajo (XRP) | cuantizado al `tickSize` real |
| Tamaño | `MAX_TRADE_SIZE=0.05` en unidades base (pensado para BTC) | tope de nocional `MAX_POSITION_NOTIONAL_PCT` (10 % del capital) + topes opcionales por símbolo |
| Sistemas secundarios | el agente se activaba siempre a los 20 s, ignorando la config | solo si `AUTO_ENABLE_SECONDARY_SYSTEMS=true` |
| `MAX_CONCURRENT_ORDERS` | declarado pero sin usar | semáforo real alrededor de la colocación |
| Features ML atómicas | leídas del nivel equivocado del snapshot → siempre 0 | leídas de `metrics_snapshot["metrics"]` |
| Historia tras reinicio | solo desde JSONL (desactivado por defecto con DuckDB) | desde DuckDB, con fallback a JSONL |
| DuckDB | `flush()` no vaciaba la cola y había una carrera que perdía registros (8 de 50 guardados) | `flush()` persiste todo lo encolado |
| `nertz status` | imprimía la API secret en claro | enmascara credenciales |
| Reentreno ML | con muestras insuficientes reintentaba en cada tick (0,5 s) | una vez por intervalo |

## 8. Rendimiento

* Z-scores: historia columnar + numpy. Con 3000 muestras en ventana, **7,9 ms → 1,3 ms por
  ciclo** (medido con `calculate_metrics` sin cambiar resultados).
* Libro: estado `{precio: cantidad}` con la vista cacheada; antes cada delta reconstruía y
  reparseaba el libro completo.
* WebSocket: solo se abre sesión SQLite cuando hay que escribir (antes, una por mensaje); las
  velas se persisten al abrir y al confirmar, no en cada update.
* IO de `results.json`/JSONL fuera del event loop (`asyncio.to_thread`).
* Sync de órdenes: solo los trades no ejecutados (antes consultaba al exchange cada 5 s por cada
  trade `filled`); las órdenes abiertas por filtro se piden en paralelo.
* Endpoints que calculaban las métricas dos veces por request (`/combined`, `/ild`, `/rol`) ahora
  las calculan una vez.

## 9. Pruebas

```bash
python -m unittest tests.test_signal_engine tests.test_dev_signal_lab tests.test_system \
    tests.test_core_units tests.test_engine_offline -v
```

`tests/test_engine_offline.py` ejecuta el ciclo completo contra un exchange simulado (sin red):
carga inicial → WS → métricas → orden → fill → TP/SL virtual → cierre con PnL neto.
