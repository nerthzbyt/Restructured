# Niveles de predicción L0–L4

Sistema de gradación de confianza basado en métricas calibradas y validación en exchange (sweep 2026-07-04, BTCUSDT).

## Tabla de niveles

| Nivel | Nombre | Confianza | Condiciones | Acción |
|-------|--------|-----------|-------------|--------|
| **L0** | Sin señal | 0% | `data_ok=false` o no calibrado | No operar |
| **L1** | Observación | 25% | Calibrado, `\|combined\| < hold_band` | Monitorear, Limit |
| **L2** | Direccional débil | 50% | Señal parcial, veto EGM/ML | Preparar, no ejecutar |
| **L3** | Accionable | 75% | Umbrales + EGM coherente + score ≥80 | Market+IOC validado |
| **L4** | Alta convicción | 92% | L3 + composite ≥85.6 + execution ≥90 | Ejecución prioritaria |

## Base de validación

### Sweep completo (2026-07-04)

- **29,184** combinaciones evaluadas
- Fuente: `bybit_exchange_only` + `exchange_api_order_history`
- Top composite: **85.62** (Market+IOC Sell, cb=6.0, tp=0.2%, sl=0.15%)

### Live verify demo

- Perfil ejecutado: `Market|IOC`
- Slippage: **2.16 bps**
- Fill: **true**
- Latencia: ~2032 ms

## Cálculo en runtime

Implementado en `NerT_AI_PRO/intelligence_catalog.py` → `compute_prediction_level()`.

```python
# Umbrales default validados
buy_th = 6.0
sell_th = -6.0
hold_band = 3.0

# ML puede degradar L3/L4 → L2 si p < 0.55
```

## Endpoint

```http
GET /agent/prediction-level/BTCUSDT
```

Respuesta:

```json
{
  "ok": true,
  "symbol": "BTCUSDT",
  "prediction": {
    "level": "L3",
    "name": "Accionable",
    "confidence_pct": 75,
    "combined": 7.42,
    "egm": 1.23,
    "metrics_calibrated": true,
    "recommended_profiles": [
      "Market|IOC|baseCoin|Order",
      "Limit|GTC|Order"
    ]
  }
}
```