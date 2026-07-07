# Verificación live — gate old_results vs exchange

Generado: 2026-07-04T02:20:36.578622+00:00
Símbolo: **BTCUSDT** | Env: **demo**

## Gate de estadísticas

### old_results (histórico)
- órdenes muestreadas: **293**
  - `Limit|GTC|Order`: 100.0%

### exchange API (antes)
- órdenes muestreadas: **500**
  - `Market|IOC|Order`: 86.2%
  - `Limit|GTC|Order`: 13.8%

### exchange API (después ops live)
- órdenes muestreadas: **500**
  - `Market|IOC|Order`: 86.4%
  - `Limit|GTC|Order`: 13.6%

## Top por fuente (score laboratorio)

### old_results
- #1 score=85.7801 | Limit+GTC | obs=1.0
- #2 score=85.7801 | Limit+GTC | obs=1.0
- #3 score=85.7801 | Limit+GTC | obs=1.0
- #4 score=85.7801 | Limit+GTC | obs=1.0
- #5 score=85.2117 | Limit+GTC | obs=1.0

### exchange (post-ops)
- #1 score=82.5119 | Market+IOC | obs=0.864
- #2 score=82.5119 | Market+IOC | obs=0.864
- #3 score=82.5119 | Market+IOC | obs=0.864
- #4 score=82.5119 | Market+IOC | obs=0.864
- #5 score=82.5119 | Market+IOC | obs=0.864

## True top por ejecución real (demo)

- live #1 | lab #1 score=82.4869 | Market+IOC | filled=True slippage_bps=2.1609 latency_ms=2032.6

## Balance demo
- antes: equity=73530.20587988 available=73530.20587988
- después: equity=73518.20852948 available=73518.20852948
