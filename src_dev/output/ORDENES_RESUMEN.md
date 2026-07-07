# Laboratorio de órdenes spot — resumen (exchange only)

Generado: 2026-07-04T02:19:53.592825+00:00
Símbolo: **BTCUSDT**
Fuente: Bybit REST/WS/Private API — **sin DB local**
Docs API: https://bybit-exchange.github.io/docs/v5/order/create-order
Estado: **OK**

## Métricas live (laboratorio utils)

- combined: `17.626136738947295`
- calibrated: `True`
- last_price: `62472.7`
- muestras historial: `6`
- notional (CAPITAL_USDT): `54803.03`
- stats órdenes fuente: `exchange_api_order_history`
- órdenes muestreadas (exchange): `500`
- distribución tipos (exchange):
  - `Market|IOC|Order`: 86.2%
  - `Limit|GTC|Order`: 13.8%

## Top 10 perfiles de orden

### #1 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev0|-|none|-|Buy`

### #2 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev0|-|none|TickSize|Buy`

### #3 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev0|-|none|Percent|Buy`

### #4 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev1|-|none|-|Buy`

### #5 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev1|-|none|TickSize|Buy`

### #6 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `baseCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|baseCoin|Order|lev1|-|none|Percent|Buy`

### #7 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `quoteCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|quoteCoin|Order|lev0|-|none|-|Buy`

### #8 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `quoteCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|quoteCoin|Order|lev0|-|none|TickSize|Buy`

### #9 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `quoteCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|quoteCoin|Order|lev0|-|none|Percent|Buy`

### #10 — score 87.4586
- **Tipo:** `Market` + `IOC`
- **Filter:** `Order` | TP/SL: `none`
- **marketUnit:** `quoteCoin`
- **Side hint:** `Buy`
- **ID:** `Market|IOC|quoteCoin|Order|lev1|-|none|-|Buy`

## Archivos en output/
- `order_lab_top10.json` — reporte top 10
- `order_lab_ranked.json` — ranking completo
- `order_lab_debug.json` — debug conexiones + errores
- `exchange_catalog.json` — instrument rules + órdenes exchange
- `LAB_RULES.md` — política del laboratorio
