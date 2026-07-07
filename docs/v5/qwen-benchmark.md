# Benchmark Qwen — NerT v2.0 Advanced

**Fecha:** 2026-07-07T02:33:14 UTC  
**Modelo solicitado:** `qwen-max-latest` → normalizado `qwen3.7-max`  
**Plataforma:** Linux · JWT desde Firefox (`chat.qwen.ai`)

## Resumen

| Métrica | Valor |
|---------|-------|
| Casos | 12 |
| Tasa de éxito API | 100% |
| Score medio | 0.467 |
| Latencia media | 23.85 s |
| **Grado global** | **C** |
| Tier estimado | Mid-tier (composite 0.627) |

## Por categoría

| Categoría | Score | Notas |
|-----------|-------|-------|
| **math** | 0.85 | Fuerte — integración gaussiana, Bayes |
| **code** | 0.75 | LRU cache OK; optimización two-sum falló output |
| **reasoning** | 0.467 | Einstein OK; multi-step y uncertainty débiles |
| **agent** | 0.40 | Workflow parseado pero faltan campos schema |
| **trading** | 0.20 | Kelly, portfolio, microestructura — JSON no alinea schema |
| **analysis** | 0.00 | Long context — claves q1–q5 ausentes |

## Por dificultad

| Dificultad | Score |
|------------|-------|
| expert | 0.425 |
| hard | 0.55 |

## Casos destacados

### ✅ Perfectos (1.0)

- `logic_einstein_riddle` — respuesta: **Alemán**
- `math_probability_bayesian` — P=0.0875576
- `code_lru_cache` — TESTS_PASSED

### ⚠️ Parciales

- `math_integration_rigorous` — 0.7 (falta √π explícito)
- `code_algorithm_optimization` — 0.5 (wrong_output)
- `trading_kelly_advanced` — 0.2 (schema: kelly_basic, position_size…)
- `agent_complex_workflow` — 0.4 (faltan steps, success_criteria)

### ❌ Cero

- `long_context_analysis` — 0.0 (claves q1–q5)
- `trading_microstructure_deep` — 0.0 (schema español vs inglés esperado)

## Asimetría del modelo

- **Fortaleza:** matemáticas (0.85)
- **Debilidad:** trading cuant + análisis documental largo
- **Asymmetry score:** 0.85 — modelo muy desbalanceado; usar Qwen para math/code, no para JSON trading estricto sin post-proceso

## Peers estimados

GPT-4o mini · Mistral Medium · Qwen3.5-Flash

## Estado sesión Qwen (Linux)

```json
{
  "session_found": true,
  "api_ok": true,
  "token_source": "~/.mozilla/firefox/.../https+++chat.qwen.ai/ls"
}
```

## Recomendaciones para el agente NerT

1. **ReAct + herramientas** para trading (no confiar en JSON libre del LLM).
2. **Post-validación schema** en respuestas de Kelly, portfolio y microestructura.
3. **Prompts con claves explícitas** (`q1`…`q5`, `directional_bias`, etc.) para análisis largo.
4. Re-ejecutar: `python scripts/benchmark_qwen.py --model qwen-max-latest`

## Datos completos

JSON completo: [qwen_benchmark_advanced.json](qwen_benchmark_advanced.json)