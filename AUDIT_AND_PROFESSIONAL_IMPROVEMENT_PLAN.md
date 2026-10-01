# Auditoría Completa + Plan de Mejora Profesional

**Repositorio:** `nerthzbyt/Restructured` (NerT Quant Engine v5)
**Fecha:** 2026-07-05
**Analista:** Grok (auditoría solicitada por el mantenedor)
**Commit analizado:** `66054e860da3444fe1c684c19bb606dfe588d75e` (main)

## Resumen Ejecutivo

Este es un sistema **altamente funcional y ambicioso** de trading cuantitativo para Bybit Spot que combina:
- Métricas de microestructura propietarias (PIO, EGM, ROL, OGM, Combined, ILD, TFI, microprice, spoof detection).
- Motor de ejecución con virtual TP/SL, auto-HFT, auto-TPSL, auto-relax de umbrales y filtrado ML.
- Agente AI (NerT_AI_PRO) con ReAct + Qwen para control conversacional, optimización y diagnóstico.
- Alta observabilidad (FastAPI con decenas de endpoints de inspección en tiempo real).
- Persistencia híbrida (SQLite + DuckDB + JSONL).

**Fortalezas destacadas:**
- Lógica de señales en `src/signal_engine.py` es limpia, testeable y profesional (dataclasses, MarketState enum, detección de spoofing, veto por microprice, gates de ejecución).
- Integración Bybit V5 muy robusta (orderLinkId tracking, preflight con drift de reloj, sync de órdenes, import de órdenes huérfanas, amend/replace logic).
- Sistema de auto-calibración y adaptación dinámica de thresholds.
- Diseño orientado a investigación cuantitativa + control total (valoras ownership de fórmulas y baja telemetría).

**Problemas críticos (deuda técnica severa):**
- **Monolitos masivos**: `src/Nertzh.py` tiene **272.278 líneas** y `NerT_AI_PRO/main.py` **~116k líneas**. Esto viola todos los principios de ingeniería de software profesional y es el mayor riesgo para mantenimiento, testing, depuración y evolución segura.
- `src/utils.py` tiene **72.869 líneas** (cálculos de métricas + posiblemente datos o implementaciones muy verbosas).
- Métodos de miles de líneas con lógica anidada compleja (_core_cycle, _auto_tpsl_tick, sync_open_orders, _serialize_trade_for_api).
- Configuración dinámica excesiva (`getattr(config, "KEY", default)` repetido cientos de veces).
- Baja separación de responsabilidades (el engine contiene FastAPI, WS, persistencia, ejecución, ML, auto-sistemas todo junto).

El sistema está **funcional en demo/mainnet** y tiene validaciones reales Bybit (86.8% Market|IOC en sweeps), pero no está listo para operación profesional a escala sin refactor.

## Análisis por Áreas

### 1. Arquitectura y Diseño
- **Actual**: Monolítico + FastAPI todo-en-uno + clase gigante `NertzMetalEngine`.
- **Problema**: Alta coupling, difícil de testear de forma aislada, diffs de git inutilizables, onboarding lento.
- **Recomendación fuerte**: Migrar hacia arquitectura modular inspirada en event-driven (como NautilusTrader que has evaluado positivamente, pero manteniendo control total de tus métricas).
  - Componentes sugeridos: `MarketDataBus`, `SignalEngine` (ya separado), `ExecutionEngine`, `RiskManager`, `VirtualTPSLManager`, `OrderLifecycleManager`, `PersistenceLayer`.
  - Usar colas asyncio o un message bus ligero para desacoplar handlers de WS del ciclo de decisión.

### 2. Calidad de Código
- Buen uso de type hints en partes nuevas y async/await correcto.
- `signal_engine.py`: **Excelente** (limpio, documentado, dataclasses frozen, lógica de umbrales simétricos bien encapsulada).
- El resto: deuda alta. Falta de docstrings en funciones críticas, magic numbers, paths de error complejos.
- Mix de español/inglés en comentarios.
- Potenciales race conditions a pesar de locks (lógica de _auto_tpsl_tick y sync es muy intrincada).

### 3. Lógica de Trading y Riesgo
- **Positivo**: Sizing por volatilidad + risk_factor, gates de ejecución (spread, rvol, trade age, spoof), confirmaciones múltiples (classic + v2 + TFI alignment + microprice), ML como filtro secundario.
- **Virtual TPSL**: Creativo y necesario para Spot, pero la lógica de actualización DB + posible close de mercado necesita más tests de edge cases (partial fills, latency, price jumps).
- **Auto-HFT y Auto-Agent**: Interesantes para adaptación dinámica, pero requieren backtesting riguroso para evitar overtrading o relajación excesiva de thresholds.
- **Recomendación**: Añadir circuit breakers a nivel de portfolio (max daily loss, max concurrent notional exposure) y kill-switch global.

### 4. Integración Bybit V5
- Muy sólida. Buen manejo de rate limits, retries, orderLinkId para tracking, merge de order_history/realtime/execution_list, import de órdenes huérfanas, preflight con chequeo de deriva de reloj.
- `bybit_v5.py` parece limpio (no auditado línea por línea en esta pasada).

### 5. Observabilidad y Persistencia
- **Excelente**. Endpoints como `/decisions/{symbol}`, `/validation`, `/orders/status`, `/discovery/metrics/{symbol}`, `/admin/agent/status` son de nivel profesional para debugging en vivo.
- Persistencia híbrida bien pensada (DuckDB para analítica pesada + SQLite mirror + JSONL para legibilidad).
- Snapshots de métricas, thresholds y balances frecuentes.

### 6. NerT_AI_PRO (Agente)
- Diseño potente: ReAct + Qwen (desktop/dashscope/ollama), tool registry, mcp_bridge, react_agent.
- Permite control conversacional del bot, optimización Optuna, consulta de catálogo de indicadores/niveles/perfiles.
- **Riesgo**: Tool calling con LLM puede ser no-determinístico. Recomendación: robustecer con validación estricta de outputs de tools y circuit breakers en acciones críticas (place_order, relax_thresholds).

### 7. Testing y Validación
- Tests básicos presentes (`tests/test_signal_engine.py`, `test_system.py`, `validate_demo_bybit.py`).
- **Gap crítico**: Falta de tests unitarios exhaustivos para lógica de decisión, virtual TPSL, order replace, y edge cases de métricas (volatilidad=0, orderbook vacío, datos stale).
- Validación real Bybit mencionada en README (sweep 29k combinaciones) es buena práctica.

## Plan de Mejora Profesional (Priorizado y Realista)

### Fase 1: Estabilización Inmediata (1-2 semanas) — **Crítica**
1. **Refactor de Configuración** (alta prioridad)
   - Reemplazar `ConfigSettings` dinámico por `pydantic-settings` + `BaseSettings` con validación tipada y defaults.
   - Eliminar ~200+ `getattr(config, "KEY", default)`.
2. **Extracción de Módulos Pequeños**
   - Extraer `VirtualTPSLManager` y `OrderLifecycleManager` de Nertzh.py (los métodos más largos y críticos).
   - Mover lógica de `_serialize_trade_for_api` y atomic_features a un serializer dedicado.
3. **Mejora de Testing**
   - Añadir tests parametrizados para `evaluate_signal` y `check_execution_gates` con fixtures de datos reales Bybit.
   - Test de propiedad para lógica de virtual close (hypothesis).
4. **Documentación Interna**
   - Añadir este informe al repo + diagrama de arquitectura actual vs propuesta.

### Fase 2: Modularización Estructural (3-4 semanas)
- Estructura objetivo:
  ```
  src/nertzh/
    __init__.py
    engine.py                 # Fachada ligera que orquesta
    config.py                 # Pydantic Settings
    data/
      market_bus.py           # WS + REST ingestion + replay
      bybit_client.py         # wrapper
    signals/
      engine.py               # Ya existe, promover
      metrics.py              # Mover cálculos pesados de utils.py
    execution/
      order_manager.py
      tpsl_manager.py
      risk_manager.py
    persistence/
      hybrid_store.py
    api/
      app.py
      routes/
        trading.py
        diagnostics.py
        admin.py
        agent.py
  ```
- Mantener compatibilidad total con la API actual (los endpoints /api/* deben seguir funcionando idénticos para NerT_AI_PRO y usuarios).
- Usar dependency injection (FastAPI Depends) para testabilidad.

### Fase 3: Profesionalización y Confiabilidad (continuo)
- **Backtesting con Parity**: Implementar replay engine que use exactamente el mismo `evaluate_signal` + gates + virtual TPSL logic sobre datos históricos del storage. Validar que live y backtest den mismos resultados en las mismas condiciones.
- **Circuit Breakers y Safeguards**: Max daily loss, max open notional por símbolo, cooldown global, kill-switch vía endpoint + archivo flag.
- **Observabilidad Avanzada**: Exportador Prometheus + dashboards Grafana (latencia de decisión, fill rate, slippage real vs esperado, tasa de relajación de thresholds).
- **Perfilado**: cProfile / py-spy en hot paths de `calculate_metrics` (utils.py). Considerar numba para partes numéricas críticas si es bottleneck.
- **Robustez del Agente AI**: Validación estricta de tool outputs, timeouts, y "human-in-the-loop" para acciones de trading reales.

### Fase 4: Escalabilidad y Visión a Largo Plazo
- Soporte nativo para 50-100+ símbolos concurrentes con mejor modelo de concurrencia.
- Evaluar hybrid: tus métricas custom + NautilusTrader (o similar) para la capa de ejecución y portfolio management (event-driven con guaranteed backtest/live parity).
- Formalizar "Nertz Quant Engine" como framework interno versionado con plugins para nuevas estrategias/métricas.

## Mejoras Aplicadas en Esta Auditoría

1. **Creación de este informe**: Archivo `AUDIT_AND_PROFESSIONAL_IMPROVEMENT_PLAN.md` agregado al repositorio para referencia permanente y onboarding.
2. **Recomendación inmediata ejecutable**: 
   - Crear rama `refactor/config-pydantic` y migrar `config/settings.py` + `src/settings.py` a Pydantic v2.
   - Esto es un cambio de alto impacto / bajo riesgo que reduce drásticamente la fragilidad.

**Próximos pasos sugeridos (a confirmar contigo):**
- ¿Quieres que implemente el refactor de Config a Pydantic ahora?
- ¿Extraigo primero el VirtualTPSLManager como módulo separado?
- ¿Prefieres que genere un esqueleto de la nueva estructura de paquetes (`src/nertzh/`) con imports compatibles?
- ¿Audit más profundo de `utils.py` o `NerT_AI_PRO/` en la siguiente iteración?

Este sistema tiene **potencial excelente** para convertirse en un motor de trading cuantitativo profesional de clase mundial si se resuelve la deuda técnica de monolitismo. Tus métricas y enfoque en control/observabilidad son diferenciadores fuertes.

---

*Fin del informe. Listo para iterar en mejoras concretas.*