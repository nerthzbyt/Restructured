# src_dev — Resumen de hallazgos y validación de métricas

Entorno independiente para validar que `src/utils.py::calculate_metrics` produce números correctos con datos reales de Bybit.

## Estructura

```
src_dev/
├── config.py              # Rutas, endpoints, parámetros (lee .env del proyecto)
├── bybit/
│   ├── endpoints.py       # Rutas REST v5
│   ├── rest.py            # Cliente aiohttp (snapshot completo)
│   └── ws.py              # Colector WebSocket spot (orderbook/ticker/trades/kline)
├── models/
│   └── market.py          # MarketSnapshot, MetricValidationReport
├── collectors/
│   ├── snapshot_builder.py # REST / WS / DB híbrido → snapshot unificado
│   └── db_sources.py      # SQLite trading.db + JSONL history
├── analysis/
│   ├── reference_metrics.py # Recálculo independiente pio_raw/ild_raw/ogm_raw/rol_raw
│   └── orderbook_stats.py   # Spread, microprice, imbalance estructural
├── validators/
│   └── metrics_validator.py # utils vs referencia vs JSONL del bot
├── run_validate.py          # CLI validación puntual
├── run_realtime.py          # Loop cada N segundos
└── output/                  # validation_log.jsonl (generado al usar --save)
```

## Cómo ejecutar

Desde la raíz del proyecto:

```bash
# Validación REST (recomendado, más estable)
python -m src_dev.run_validate --source rest --symbol BTCUSDT --save

# WebSocket 12s + fallback REST
python -m src_dev.run_validate --source ws --ws-seconds 12

# Híbrido: velas/orderbook de SQLite + REST para trades/ticker
python -m src_dev.run_validate --source db

# Loop realtime 12 muestras cada 5s
python -m src_dev.run_realtime --interval 5 --iterations 12 --save
```

## Hallazgos técnicos (auditoría)

### 1. Capas de métricas en `utils.py`

| Capa | Campos | Dependencia |
|------|--------|-------------|
| **Raw físico** | `pio_raw`, `ild_raw`, `ogm_raw`, `rol_raw` | Orderbook + velas + (rol: ciclo anterior) |
| **Z-score** | `pio`, `ild`, `egm`, `rol`, `ogm`, `mom` | Ventana `metric_history` (≥4 muestras) |
| **Combinado** | `combined`, `combined_z` | Pesos `combined_weights` × z-scores |

Los raw son **verificables** con fórmulas cerradas (implementadas en `analysis/reference_metrics.py`).
Los z-scores **no son verificables en el primer ciclo** sin historial — por diseño.

### 2. Por qué el primer snapshot tras reinicio tenía combined=0

- Antes: `WelfordState` global en memoria → se perdía al reiniciar.
- Fix en producción: z-scores desde ventana rolling + restaurar JSONL + no persistir si `metrics_calibrated=False`.
- En `src_dev`: el validador reporta `metrics_calibrated` y compara raw (siempre deben coincidir).

### 3. Fuentes de datos Bybit

| Dato | REST | WS spot | SQLite bot |
|------|------|---------|------------|
| Klines | ✅ `/v5/market/kline` | ✅ `kline.1.SYM` | ✅ `market_data` |
| Orderbook | ✅ limit 50 | ✅ `orderbook.50.SYM` | ⚠️ puede estar stale |
| Ticker | ✅ `/v5/market/tickers` | ✅ `tickers.SYM` | ✅ `market_ticker` |
| Recent trades | ✅ `/v5/market/recent-trade` | ✅ `publicTrade.SYM` | ❌ no persistido |
| Instrument rules | ✅ `instruments-info` | — | ❌ |
| Open interest | ✅ linear only | linear WS | ❌ |

**Spot no tiene open interest** — solo `category=linear`. El validador lo obtiene como referencia opcional.

### 4. Endpoints y URLs (mainnet público)

- REST: `https://api.bybit.com`
- WS spot: `wss://stream.bybit.com/v5/public/spot`
- Demo/privado (`api-demo.bybit.com`) no se usa aquí — solo datos públicos de mercado.

### 5. Validación cruzada

El validador comprueba:

1. **Raw tolerance** (default 0.5% relativo): `pio_raw`, `ild_raw`, `ogm_raw`, `rol_raw`, `weighted_liquidity`
2. **JSONL del bot** (si existe): compara última línea de `data/metrics_snapshots.jsonl`
3. **Orderbook stats**: spread_bps, microprice, depth imbalance

`rol_raw` suele ser 0 en snapshot aislado (necesita `prev_weighted_liquidity` + `rol_dt_s` del ciclo anterior).

### 6. Diferencias esperadas vs JSONL

- Si el bot y `src_dev` corren en momentos distintos → `combined` puede diferir (mercado mueve).
- Raw (`pio_raw`, `ild_raw`) deben coincidir **si el orderbook es el mismo**.
- JSONL >120s de antigüedad → comparación solo orientativa.

### 7. Recomendaciones para producción

1. Usar **REST** para bootstrap inicial (50 velas) y **WS** para streaming.
2. Persistir `recent_trades` en SQLite si se quiere reproducibilidad offline.
3. Pasar `prev_weighted_liquidity` entre ciclos para `rol_raw` válido.
4. Mantener `metrics_snapshots.jsonl` para restaurar ventana z-score al reiniciar.

## Resultados de validación

> Ejecutar `python -m src_dev.run_validate --source rest --save` y revisar `output/validation_log.jsonl`.
> Los resultados se actualizan en cada ejecución local con datos live de Bybit.

## Dependencias

Reutiliza del proyecto: `aiohttp`, `numpy`, `websockets`, `python-dotenv`, `src/utils.py`.

No modifica `src/` ni `Nertzh.py` — entorno aislado.