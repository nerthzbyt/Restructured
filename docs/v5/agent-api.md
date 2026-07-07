# API Agent — NerT AI PRO

Base URL: `http://127.0.0.1:8787`

OpenAPI interactivo: `http://127.0.0.1:8787/docs`

## Health

```http
GET /health
```

## Catálogo de inteligencia

```http
GET /agent/catalog
```

Retorna indicadores, niveles L0-L4, perfiles validados, backends Qwen y resumen del sweep.

## Contexto en vivo

```http
GET /agent/context?symbol=BTCUSDT
```

Incluye: `bot_live_state`, `metrics_live`, `prediction_level`, `order_profiles_validated`.

## Predicción

```http
GET /agent/prediction-level/{symbol}
POST /predict/{symbol}
```

## Agente autónomo

```http
POST /agent/chat
Content-Type: application/json

{
  "message": "analiza el estado completo del sistema",
  "symbol": "BTCUSDT",
  "limit": 2000,
  "iterations": 900,
  "apply": false,
  "use_react": true
}
```

## Optimización

```http
POST /agent/optimize
{
  "symbol": "BTCUSDT",
  "limit": 2000,
  "iterations": 900,
  "apply": true
}
```

## LLM / Qwen

```http
GET  /agent/llm/status
POST /agent/llm/chat
```

## Herramientas del agente

```http
GET /agent/tools
GET /agent/tools?query=orderbook
GET /agent/tools?full=true
```

## Validación de cableado

```http
POST /agent/validate
```

Verifica: `project_context`, `bot_live_state`, `nertzh_api.*`, `mcp_bybit.*`, `market_ticker`.

## Memoria del agente

```http
GET  /agent/memory/stats
GET  /agent/memory/recent?limit=50
POST /agent/memory/clear
GET  /agent/chat/history?limit=30
```

`chat/history` empareja `chat_in` + `chat_out` por `session_id` para restaurar el feed de la UI.

## Documentación embebida

```http
GET /project-docs/
```

Sirve `docs/` (misma referencia que GitHub Pages). Útil cuando Pages no está desplegado.

Sitio público (tras deploy): `https://nerthzbyt.github.io/Restructured/`

## Mercado público

```http
GET  /market/ticker/{symbol}
GET  /market/orderbook/{symbol}?depth=50
WS   /ws/ticker/{symbol}
WS   /ws/orderbook/{symbol}
```