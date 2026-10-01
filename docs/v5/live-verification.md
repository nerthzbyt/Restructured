# Verificación live — gate old_results vs exchange

**Generado:** 2026-07-07T02:29:59 UTC  
**Símbolo:** BTCUSDT · **Entorno:** Bybit demo

## Resumen ejecutivo

Validación post-migración Linux que compara el laboratorio histórico (`old_results`) contra la API real del exchange antes y después de operaciones live en demo. El gate confirma que el mix de órdenes del exchange (≈55% Market+IOC / 45% Limit+GTC) difiere del histórico interno (100% Limit+GTC), y calibra los perfiles con ejecución real.

## Gate de estadísticas

### old_results (histórico interno)

| Métrica | Valor |
|---------|-------|
| Órdenes muestreadas | 293 |
| Limit\|GTC\|Order | 100.0% |

### exchange API (antes de ops live)

| Métrica | Valor |
|---------|-------|
| Órdenes muestreadas | 500 |
| Market\|IOC\|Order | 55.0% |
| Limit\|GTC\|Order | 44.8% |
| Limit\|PostOnly\|Order | 0.2% |

### exchange API (después de ops live)

| Métrica | Valor |
|---------|-------|
| Órdenes muestreadas | 500 |
| Market\|IOC\|Order | 54.8% |
| Limit\|GTC\|Order | 45.0% |
| Limit\|PostOnly\|Order | 0.2% |

El mix se mantiene estable tras operar en demo; no hay deriva significativa en el reparto Market vs Limit.

## Top por fuente (score laboratorio)

### old_results

| Rank | Score | Perfil | Observación |
|------|-------|--------|-------------|
| #1 | 86.2943 | Limit+GTC | obs=1.0 |
| #2 | 86.2943 | Limit+GTC | obs=1.0 |
| #3 | 85.5165 | Limit+GTC | obs=1.0 |
| #4 | 85.5165 | Limit+GTC | obs=1.0 |
| #5 | 83.5165 | Limit+GTC | obs=1.0 |

### exchange (post-ops)

| Rank | Score | Perfil | Observación |
|------|-------|--------|-------------|
| #1 | 80.1832 | Limit+GTC | obs=0.45 |
| #2 | 80.1832 | Limit+GTC | obs=0.45 |
| #3 | 79.4054 | Limit+GTC | obs=0.45 |
| #4 | 79.4054 | Limit+GTC | obs=0.45 |
| #5 | 78.4125 | Market+IOC | obs=0.548 |

**Interpretación:** el histórico sobrepondera Limit+GTC; el exchange favorece Market+IOC en L3–L4. Los scores exchange son ~6 pts menores que old_results en el top — esperado al alinear con ejecución real.

## True top por ejecución real (demo)

| Live | Lab rank | Score | Perfil | Fill | Slippage (bps) | Latencia (ms) |
|------|----------|-------|--------|------|----------------|---------------|
| #1 | #5 | 78.4347 | Market+IOC | ✅ | 1.0533 | 4015.1 |
| #2 | #3 | 79.3832 | Limit+GTC | ❌ | 0.0 | 5029.5 |
| #3 | #1 | 80.1609 | Limit+GTC | ❌ | 0.0 | 2375.9 |

- **Market+IOC** es el único fill exitoso en la tanda live; slippage < 1.1 bps.
- **Limit+GTC** puntúa alto en laboratorio pero no fill en demo (latencia 2.4–5.0 s).

## Balance demo

| Momento | Equity | Available |
|---------|--------|-----------|
| Antes | 80,653.62 USDT | 80,653.62 USDT |
| Después | 80,665.56 USDT | 80,665.56 USDT |

Delta: **+11.95 USDT** tras la verificación (demo, sin riesgo real).

## Implicaciones para el agente

1. Priorizar **Market+IOC** cuando `prediction_level` ≥ L3 y se requiere fill garantizado.
2. Usar **Limit+GTC** en L1–L2 o cuando el coste maker compensa el riesgo de no-fill.
3. No confiar en `old_results` como única fuente de ranking — siempre cruzar con `exchange API` y ejecución live.

## Referencias

- Resumen generado: `src_dev/output/LIVE_VERIFY_RESUMEN.md`
- Script: `scripts/validate_system.py`
- API: `GET /agent/order-profiles`, `POST /agent/validate`