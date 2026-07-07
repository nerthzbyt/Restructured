# Verificación live — gate old_results vs exchange

Generado: 2026-07-07T02:29:59.175599+00:00
Símbolo: **BTCUSDT** | Env: **demo**

## Gate de estadísticas

### old_results (histórico)
- órdenes muestreadas: **293**
  - `Limit|GTC|Order`: 100.0%

### exchange API (antes)
- órdenes muestreadas: **500**
  - `Market|IOC|Order`: 55.0%
  - `Limit|GTC|Order`: 44.8%
  - `Limit|PostOnly|Order`: 0.2%

### exchange API (después ops live)
- órdenes muestreadas: **500**
  - `Market|IOC|Order`: 54.8%
  - `Limit|GTC|Order`: 45.0%
  - `Limit|PostOnly|Order`: 0.2%

## Top por fuente (score laboratorio)

### old_results
- #1 score=86.2943 | Limit+GTC | obs=1.0
- #2 score=86.2943 | Limit+GTC | obs=1.0
- #3 score=85.5165 | Limit+GTC | obs=1.0
- #4 score=85.5165 | Limit+GTC | obs=1.0
- #5 score=83.5165 | Limit+GTC | obs=1.0

### exchange (post-ops)
- #1 score=80.1832 | Limit+GTC | obs=0.45
- #2 score=80.1832 | Limit+GTC | obs=0.45
- #3 score=79.4054 | Limit+GTC | obs=0.45
- #4 score=79.4054 | Limit+GTC | obs=0.45
- #5 score=78.4125 | Market+IOC | obs=0.548

## True top por ejecución real (demo)

- live #1 | lab #5 score=78.4347 | Market+IOC | filled=True slippage_bps=1.0533 latency_ms=4015.1
- live #2 | lab #3 score=79.3832 | Limit+GTC | filled=False slippage_bps=0.0 latency_ms=5029.5
- live #3 | lab #1 score=80.1609 | Limit+GTC | filled=False slippage_bps=0.0 latency_ms=2375.9

## Balance demo
- antes: equity=80653.61723139 available=80653.61723139
- después: equity=80665.56293852 available=80665.56293852
