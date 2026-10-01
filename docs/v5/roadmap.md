# Roadmap y usos futuros

## Q3 2026 — En progreso

### Factorización del motor

Reducir `Nertzh.py` (~5500 LOC) en módulos `nertz_engine/`:

| Módulo | Reemplaza | Reducción est. |
|--------|-----------|----------------|
| `orders.placement` | `_place_order` | 12% |
| `signals.decision` | `_determine_decision` | 8% |
| `orders.tpsl` | AUTO_TPSL block | 15% |
| `exchange.client` | `_bybit_client` | 10% |
| `metrics.loop` | main cycle | 20% |
| `admin.api` | FastAPI routes | 15% |

**Total estimado: ~80% reducción** reutilizando `src_dev/orders/*`.

### API v5 unificada

- REST público documentado (estilo Bybit)
- WebSocket unificado ticker + orderbook + señales
- GitHub Pages como portal de documentación

## Q4 2026 — Planificado

- **ML ensemble**: XGBoost + calibración isotónica por símbolo
- **Multi-exchange**: normalización de perfiles Bybit/Binance
- **Backtest en vivo**: replay de métricas desde DuckDB storage

## 2027 — Investigación

- Agente autónomo con memoria episódica (SQLite → vector store)
- Auto-evolución de pesos con guardrails de riesgo
- Integración MCP ampliada (portfolio, earn, convert)

## Métricas de éxito

| Métrica | Actual | Objetivo Q4 |
|---------|--------|-------------|
| Composite score top | 85.62 | ≥ 88 |
| Slippage live (bps) | 2.16 | < 2.0 |
| Perfiles validados | 304 | 500+ |
| LOC Nertzh.py | ~5500 | < 1500 |