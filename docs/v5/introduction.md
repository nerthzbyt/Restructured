# Introducción — NerT API v5

## Visión general

**Restructured** es un motor de trading cuantitativo para Bybit Spot que combina:

1. **Motor `src/`** — Cálculo de métricas de microestructura en tiempo real, ejecución de órdenes v5 y optimización de umbrales.
2. **Agente `NerT_AI_PRO/`** — FastAPI con agente ReAct, integración Qwen LLM, MCP Bybit y consola web profesional.
3. **Laboratorio `src_dev/`** — Validación de perfiles de orden contra historial real del exchange.

## Principios de diseño

- **Datos del exchange primero**: las estadísticas de perfiles de orden provienen de `exchange_api_order_history`, no de DB local.
- **Sin pesos mágicos**: el scorer usa frecuencia observada + métricas live, calibración Welford y factores explícitos.
- **Niveles de predicción graduales**: L0–L4 evitan operar sin calibración o con señal débil.
- **LLM como copiloto**: Qwen propone estrategias; la ejecución sigue reglas cuantitativas validadas.

## Componentes

| Componente | Ruta | Puerto |
|------------|------|--------|
| Motor Nertzh | `src/Nertzh.py` | 8000 (API interna) |
| Agente PRO | `NerT_AI_PRO/main.py` | 8787 |
| Config | `config/settings.py` | — |
| Tests | `tests/` | — |

## Entornos Bybit

| `BYBIT_ENV` | API Base | Uso |
|-------------|----------|-----|
| `demo` | `api-demo.bybit.com` | Paper trading, validación |
| `mainnet` | `api.bybit.com` | Producción |

## Siguiente lectura

- [Indicadores](indicators.md)
- [Niveles de predicción](prediction-levels.md)
- [API Agent](agent-api.md)