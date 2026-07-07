# Plataforma Linux — NerT AI PRO

Variante operativa validada en **Ubuntu/Linux** (julio 2026). Extiende el upstream Restructured v5 con soporte nativo para entorno desktop Linux sin depender de Qwen Desktop Windows.

## Diferencias respecto al upstream

| Área | Upstream | Linux platform |
|------|----------|----------------|
| JWT Qwen | Qwen Desktop (Windows `%APPDATA%`) | Firefox / Chromium / snap `qwen-desktop` |
| UI Agent Console | Sin historial persistente | Restaura chat desde `agent_memory.sqlite` |
| Nivel L0–L4 | Leía `ticker_data.metrics` (vacío) | Lee `_last_metrics_by_symbol` del loop |
| Documentación UI | Enlace GitHub Pages (404 si no deploy) | `/project-docs/` embebido + GitHub Pages |
| Umbrales en UI | Buy/Sell TH en blanco | Sincronizados desde `bot_live_state.thresholds` |

## Inicio rápido (Linux)

```bash
git clone https://github.com/nerthzbyt/Restructured.git
cd Restructured
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Editar .env con claves Bybit demo

python NerT_AI_PRO/main.py run --host 127.0.0.1 --port 8787
```

- UI: `http://127.0.0.1:8787`
- Docs locales: `http://127.0.0.1:8787/project-docs/`
- OpenAPI: `http://127.0.0.1:8787/docs`

## Qwen en Linux (sin API de pago)

1. Iniciar sesión en [chat.qwen.ai](https://chat.qwen.ai) con **Firefox** (o app snap).
2. El backend `qwen_desktop` extrae el JWT de LevelDB local.
3. Verificar: `GET /agent/llm/status` → `session_found: true`.

Rutas escaneadas automáticamente:

- `~/.mozilla/firefox/*/storage/default/https+++chat.qwen.ai/ls`
- `~/snap/firefox/common/.mozilla/firefox/...`
- `~/snap/qwen-desktop/.../.config/Qwen/Local Storage/leveldb`
- Override manual: `LLM_QWEN_DESKTOP_LEVELDB=/ruta/al/leveldb`

## Endpoints nuevos (Agent)

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/agent/chat/history` | Historial UI (chat_in + chat_out) |
| `GET` | `/project-docs/` | Documentación v5 servida por FastAPI |

## Producción LLM (recomendado)

Para despliegue 24/7 usar API oficial vía `openai_compat` (DashScope, xAI, Groq, DeepSeek):

```env
LLM_BACKEND=openai_compat
LLM_BASE_URL=https://api.x.ai/v1
LLM_API_KEY=sk-...
LLM_MODEL=grok-3-mini
```

`qwen_desktop` queda como backend de desarrollo gratuito en workstation Linux.

## Validación en Linux (2026-07-07)

- Monitor 60 min: 104 ticks, 0 incidentes
- Signal Lab: 81.7% match decisiones motor vs horizontes dev
- Fix L0: niveles L1–L4 visibles tras calibración Welford
- Memoria agente: 16+ eventos en SQLite, restauración UI OK
- **Live verify:** gate old_results vs exchange — ver [live-verification.md](live-verification.md)
- **Benchmark Qwen:** grado C, math 0.85 — ver [qwen-benchmark.md](qwen-benchmark.md)

## Documentación pública (GitHub Pages)

| URL | Uso |
|-----|-----|
| `http://127.0.0.1:8787/project-docs/` | Siempre disponible con el servidor local |
| `https://nerthzbyt.github.io/Restructured/` | Sitio estático tras deploy Actions |

El footer del Agent Console apunta a `/project-docs/` (local). Tras push a `main` con cambios en `docs/**`, el workflow `.github/workflows/pages.yml` publica el sitio.

**Si GitHub Pages muestra 404:** activar en repo → Settings → Pages → Source: **GitHub Actions**.
- **Live verify:** gate old_results vs exchange — ver [live-verification.md](live-verification.md)
- **Benchmark Qwen:** grado C, math 0.85 — ver [qwen-benchmark.md](qwen-benchmark.md)

## Documentación pública (GitHub Pages)

| URL | Uso |
|-----|-----|
| `http://127.0.0.1:8787/project-docs/` | Siempre disponible con el servidor local |
| `https://nerthzbyt.github.io/Restructured/` | Sitio estático tras deploy Actions |

El footer del Agent Console apunta a `/project-docs/` (local). Tras push a `main` con cambios en `docs/**`, el workflow `.github/workflows/pages.yml` publica el sitio.

**Si GitHub Pages muestra 404:** activar en repo → Settings → Pages → Source: **GitHub Actions**.