# Integración Qwen LLM

NerT AI PRO soporta múltiples backends LLM con preferencia por **Qwen Desktop** para desarrollo y **DashScope** para producción.

## Backends

| Backend | Variable | Modelos | Autenticación |
|---------|----------|---------|---------------|
| `qwen_desktop` | `LLM_BACKEND=qwen_desktop` | qwen3.7-plus, qwen3.7-max, qwen3.5-flash | JWT desde Qwen Desktop |
| `openai_compat` | `LLM_BACKEND=openai_compat` | qwen-plus-latest, qwen-max-latest | `DASHSCOPE_API_KEY` |
| `ollama` | `LLM_BACKEND=ollama` | qwen2.5-coder:latest | Local, sin key |

## Qwen Desktop

1. Instalar [Qwen Desktop](https://chat.qwen.ai)
2. Iniciar sesión (Google)
3. El token JWT se lee de `%APPDATA%/Qwen/Local Storage/leveldb`
4. Verificar: `GET /agent/llm/status` → `qwen_desktop.token_present: true`

Implementación: `NerT_AI_PRO/qwen_desktop.py`

### Mapeo de modelos

| Alias API | Modelo real |
|-----------|-------------|
| qwen-plus-latest | qwen3.7-plus |
| qwen-max-latest | qwen3.7-max |
| qwen-turbo-latest | qwen3.5-flash |

## Uso en el agente

- **ReAct loop** (`react_agent.py`): razonamiento + tool calling
- **Propuesta de estrategia** (`_llm_propose_strategy`): JSON con thresholds + weights
- **Síntesis de diagnósticos**: análisis post-ejecución de tools

## Variables de entorno

```env
LLM_BACKEND=qwen_desktop
LLM_MODEL=qwen-plus-latest
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
LLM_API_KEY=sk-...
LLM_TEMPERATURE=0.2
LLM_TIMEOUT_S=120
```

## Seguridad

- El LLM **no ejecuta órdenes directamente**; propone parámetros que pasan por `optimizer` y gates del motor.
- En demo, `mcp_bybit.getWalletBalance` redirige a `nertzh_api.balance`.