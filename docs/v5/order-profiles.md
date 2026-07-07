# Perfiles de orden

Perfiles spot validados contra historial real de Bybit (500 órdenes, BTCUSDT, 2026-07-04).

## Distribución observada en exchange

| Perfil | % observado | Score lab | Uso recomendado |
|--------|-------------|-----------|-----------------|
| `Market\|IOC\|Order` | **86.8%** | 84.03 | Señales L3-L4, ejecución inmediata |
| `Limit\|GTC\|Order` | 12.8% | 85.78 | Rangos L1-L2, maker |
| `Limit\|PostOnly\|Order` | 0.2% | 78.0 | Spread tight, fee rebate |

## Scoring (src_dev/orders/scorer.py)

Factores del score compuesto (0–100):

| Factor | Descripción |
|--------|-------------|
| `calibration` | Ratio muestras / min_calibration_samples |
| `signal_type_fit` | Ajuste Market vs Limit según signal_strength |
| `spread_fit` | Penalización por spread_bps alto |
| `vol_regime` | Régimen de volatilidad vs tipo de orden |
| `microstructure` | PIO + depth_imb + TFI alineados con side |
| `exchange_observed` | Frecuencia histórica del perfil en exchange |

## Top recomendación validada

```json
{
  "order_type": "Market",
  "time_in_force": "IOC",
  "market_unit": "baseCoin",
  "system_params": {
    "combined_buy": 6.0,
    "combined_sell": -6.0,
    "combined_hold_band": 3.0,
    "tp_pct": 0.2,
    "sl_pct": 0.15,
    "risk_reward": 1.333
  },
  "composite_score": 85.62,
  "execution_score": 90.0
}
```

## API

```http
GET /agent/order-profiles
```

## Referencia Bybit

Crear orden: [Bybit v5 Create Order](https://bybit-exchange.github.io/docs/v5/order/create-order)

Campos mapeados: `category`, `symbol`, `side`, `orderType`, `qty`, `timeInForce`, `marketUnit`, `orderLinkId`.