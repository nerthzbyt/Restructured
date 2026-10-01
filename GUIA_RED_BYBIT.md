# Guía completa de configuración de red y uso de la API (Bybit v5)

## Inicio rápido del servicio

Ejecuta el host local (API + chat) desde la raíz del proyecto:

```bash
python NerT_AI_PRO/main.py run --host 127.0.0.1 --port 8787
```

La API de trading se monta en:

```
http://127.0.0.1:8787/api
```

Si cambias host o puerto, usa ese mismo valor en los ejemplos.

## Selección de red: demo y mainnet

La red se controla con `BYBIT_ENV` en el `.env`:

- `BYBIT_ENV=demo` — órdenes autenticadas en `https://api-demo.bybit.com` (demo trading de mainnet).
- `BYBIT_ENV=mainnet` (o vacío) — órdenes autenticadas en `https://api.bybit.com`.
- Datos públicos (REST + WebSocket) siempre usan mainnet: `api.bybit.com` / `stream.bybit.com`.
- `LIVE_TRADING_ENABLED=true` habilita órdenes y balance real; `false` deshabilita trading live.

## Variables .env (todas las configuraciones disponibles)

Estas variables se leen desde el archivo .env y tienen validación interna.

| Variable | Default | Descripción y validación |
| --- | --- | --- |
| BYBIT_API_KEY | (vacío) | API key de Bybit para autenticación. |
| BYBIT_API_SECRET | (vacío) | API secret de Bybit para autenticación. |
| BYBIT_ENV | mainnet | `demo` o `mainnet`. Controla la API autenticada (demo vs mainnet). |
| LIVE_TRADING_ENABLED | false | true/false. Habilita órdenes reales y balance real. |
| SYMBOL | BTCUSDT | Valores válidos: BTCUSDT, ETHUSDT, XRPUSDT. Admite lista separada por comas. |
| TIMEFRAME | 1m | Valores válidos: 1m, 5m, 15m, 1h, 4h, 1d. |
| ORDER_TYPE | Limit | Valores válidos: Limit, Market (case-insensitive). |
| TIME_IN_FORCE | GTC | Valores válidos: GTC, IOC, FOK, PostOnly, GoodTillCancel, ImmediateOrCancel, FillOrKill. |
| ORDERBOOK_DEPTH | 50 | Valores válidos: 1, 5, 10, 25, 50. |
| MAX_ITERATIONS | 0 | Entero positivo o 0. |
| DEFAULT_SLEEP_TIME | 10 | Entero positivo. |
| CAPITAL_USDT | 2000.0 | Float positivo. Capital simulado cuando LIVE_TRADING_ENABLED=false. |
| VOLUME_THRESHOLD | 1.0 | Float positivo. |
| RISK_FACTOR | 0.01 | Float entre 0.0 y 1.0. |
| MAX_TRADE_SIZE | 0.05 | Float entre 0.0 y 1.0. |
| MIN_TRADE_SIZE | 0.0001 | Float entre 0.0 y 1.0. |
| FEE_RATE | 0.002 | Float entre 0.0 y 0.1. |
| TP_PERCENTAGE | 1.5 | Float positivo. |
| SL_PERCENTAGE | 0.5 | Float positivo. |
| PRICE_SHIFT_FACTOR | 0.003 | Float entre 0.0 y 0.1. |
| RSI_UPPER_THRESHOLD | 80.0 | Float entre 0.0 y 100.0. |
| RSI_LOWER_THRESHOLD | 20.0 | Float entre 0.0 y 100.0. |
| RATE_LIMIT_DELAY | 50 | Entero positivo. |
| PIO_THRESHOLD | 0.0 | Float. |
| EGM_BUY_THRESHOLD | 0.02 | Float. |
| EGM_SELL_THRESHOLD | -0.02 | Float. |
| COMBINED_BUY_THRESHOLD | 6.5 | Float. |
| COMBINED_SELL_THRESHOLD | -6.5 | Float. |
| COMBINED_HOLD_BAND | 1.5 | Float positivo. |
| ORDERBOOK_LAMBDA | 0.03 | Float positivo. |
| ORDERBOOK_PCT_BAND | 0.015 | Float entre 0.0 y 0.25. |
| ILD_TARGET_MOVE | 0.002 | Float entre 0.0001 y 0.05. |
| METRICS_WINDOW_MINUTES | 15.0 | Float entre 1.0 y 120.0. |
| AUTO_TUNE_THRESHOLDS | false | true/false. |
| PERSIST_THRESHOLDS_TO_ENV | false | true/false. |
| FORMULAS_JSON | {} | JSON dict con fórmulas personalizadas. |
| ML_ENABLED | false | true/false. |
| ML_MIN_SAMPLES | 50 | Entero positivo. |
| ML_PROB_THRESHOLD | 0.6 | Float entre 0.5 y 0.99. |
| AUTO_AGENT_ENABLED | false | true/false. |
| AUTO_ENABLE_SECONDARY_SYSTEMS | false | true/false. |
| AUTO_AGENT_TRAIN_INTERVAL_MIN | 5.0 | Float entre 1.0 y 1440.0. |
| AUTO_TPSL_ENABLED | true | true/false. |
| AUTO_TPSL_INTERVAL_S | 3.0 | Float entre 0.25 y 60.0. |
| AUTO_TPSL_MIN_TP_MOVE_TICKS | 1 | Entero positivo. |
| AUTO_TPSL_MIN_SL_MOVE_TICKS | 1 | Entero positivo. |
| AUTO_TPSL_TRAIL_GAP_MULT | 1.2 | Float entre 0.0 y 10.0. |
| AUTO_TPSL_TRAIL_GAP_MIN | 0.001 | Float entre 0.0 y 0.2. |
| AUTO_TPSL_TP_EXT_MULT | 1.25 | Float entre 1.0 y 5.0. |
| AUTO_TPSL_ML_TP_BOOST | 1.0 | Float entre 0.0 y 10.0. |
| MAX_CHASE_ATTEMPTS | 3 | Entero positivo. |
| CHASE_INTERVAL | 2.0 | Float positivo. |

## Balance: total, USDT y BTC

Endpoint:

```
GET /api/balance?account_type=UNIFIED&coin=USDT
```

Ejemplos:

```bash
curl "http://127.0.0.1:8787/api/balance?account_type=UNIFIED&coin=USDT"
curl "http://127.0.0.1:8787/api/balance?account_type=UNIFIED&coin=BTC"
```

Respuesta clave:

- balance.total_equity
- balance.available_balance

Si LIVE_TRADING_ENABLED=false o no hay credenciales, el balance es simulado y se calcula desde el capital interno.

## Precios de mercado y datos

Precio actual (ticker):

```
GET /api/ticker/{symbol}
```

Velas (últimas 5):

```
GET /api/market_data/{symbol}
```

Métricas internas:

```
GET /api/metrics/{symbol}
```

Combinado (velas, orderbook, ticker y trades recientes):

```
GET /api/combined/{symbol}
```

Métricas de descubrimiento:

```
GET /api/discovery/metrics/{symbol}
GET /api/ild/{symbol}
GET /api/rol/{symbol}
```

Ejemplo:

```bash
curl "http://127.0.0.1:8787/api/ticker/BTCUSDT"
```

## Órdenes y ejecución

Ejecutar un ciclo de trading para un símbolo:

```
POST /api/execute_trade/{symbol}?collect_only=false&force_trade=false
```

Estado de órdenes abiertas y vinculación con la base de datos:

```
GET /api/orders/status
```

Sincronizar órdenes abiertas con la base:

```
POST /api/orders/sync
```

Consultar estado de una orden por ID:

```
GET /api/order_status/{order_id}
```

Órdenes abiertas directo desde Bybit:

```
GET /api/exchange/open_orders/{symbol}?limit=200
```

Historial de trades en memoria y últimos trades:

```
GET /api/trades/{symbol}
GET /api/last_trade/{symbol}
```

Ejemplo:

```bash
curl -X POST "http://127.0.0.1:8787/api/execute_trade/BTCUSDT?collect_only=false&force_trade=false"
```

## Control del bot

Estado general:

```
GET /api/status
```

Iniciar y detener:

```
POST /api/start
POST /api/stop
```

## Configuración actual en runtime

Consultar configuración efectiva:

```
GET /api/config
```

Consultar settings por símbolo:

```
GET /api/settings
```

## Uso en otros proyectos

Esta guía se puede reutilizar en otros proyectos siempre que:

- Exista un servicio con la misma API (rutas /api/* indicadas aquí).
- Se configuren las mismas variables de entorno en el .env.
- Se ajuste host/puerto/base_url en los ejemplos según el proyecto.

Si el proyecto monta la API en otra ruta, reemplaza /api por el prefijo correcto.
