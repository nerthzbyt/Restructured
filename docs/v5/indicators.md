# Indicadores y métricas

Referencia de los indicadores propios calculados en `src/utils.py` → `calculate_metrics()`.

## Normalización

Todos los indicadores raw pasan por **Welford z-score** con historial en `ticker_data.metric_history`. Mínimo 5 muestras para z completo; antes se aplica factor `count/min_count`.

## Catálogo

### PIO — Pressure Imbalance Orderbook

| Campo | Valor |
|-------|-------|
| ID | `pio` |
| Peso default | 0.25 |
| Fórmula raw | `Σ bid_qty·e^(-λ·dist) - Σ ask_qty·e^(-λ·dist)` |
| Interpretación | Positivo = presión compradora en el libro |

### EGM — Enhanced Gravity Metric

| Campo | Valor |
|-------|-------|
| ID | `egm` |
| Peso default | 0.30 |
| Fórmula | `pio_z × (1 + |asymmetry|) + bonus_rol` |
| Filtro | Veto en `determine_decision` si contradice el lado |

### ILD — Implied Liquidity Depth

| Campo | Valor |
|-------|-------|
| ID | `ild` |
| Peso default | **-0.15** (negativo) |
| Fórmula | Notional para alcanzar `mid ± target_move` |
| Interpretación | Alta liquidez → menor urgencia de market order |

### ROL — Rate of Liquidity

| Campo | Valor |
|-------|-------|
| ID | `rol` |
| Peso default | 0.10 |
| Fórmula | `(liquidez_actual - liquidez_prev) / dt_s` |

### OGM — Order Gap Metric

| Campo | Valor |
|-------|-------|
| ID | `ogm` |
| Peso default | 0.05 |
| Fórmula | `(ask_large_gap - ask_med) - (bid_large_gap - bid_med)` |

### TFI — Trade Flow Imbalance

| Campo | Valor |
|-------|-------|
| ID | `tfi` |
| Peso default | 0.25 |
| Ventana | Últimos 10 trades |
| Fuente | `recent_trades` del WS público |

### MOM — Momentum Composite

| Campo | Valor |
|-------|-------|
| ID | `mom` |
| Peso default | 0.16 |
| Inputs | EMA5, EMA20, ret1m, ret5m, ret20m, IGD, volatilidad |

## Combined

```
combined = scale × Σ(w_i × z_i)
```

Pesos default (`optimizer.py`):

```python
pio=0.25, egm=0.30, ild=-0.15, rol=0.10, ogm=0.05, mom=0.16, tfi=0.25, scale=10.0
```

## API

```http
GET /agent/catalog
GET /api/metrics/{symbol}    # vía motor Nertzh
GET /agent/prediction-level/BTCUSDT
```