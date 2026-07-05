# Restructured — NerT Quant Engine v5

Motor de trading cuantitativo para **Bybit Spot** con agente autónomo **NerT AI PRO**, integración **Qwen LLM**, métricas de microestructura validadas en exchange y laboratorio de perfiles de orden.

[![API Docs](https://img.shields.io/badge/docs-v5%20API-f7a600?style=flat-square)](https://nerthzbyt.github.io/Restructured/)
[![Python](https://img.shields.io/badge/python-3.10%2B-20b26c?style=flat-square)](requirements.txt)
[![Bybit](https://img.shields.io/badge/exchange-Bybit%20v5-000?style=flat-square)](https://bybit-exchange.github.io/docs/v5/intro)

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│  NerT_AI_PRO (FastAPI Agent)                                    │
│  ReAct · Qwen Desktop/DashScope · MCP Bybit · Memoria SQLite    │
├─────────────────────────────────────────────────────────────────┤
│  src/ — Motor principal                                         │
│  Nertzh.py · utils.py (métricas) · optimizer.py · bybit_v5.py   │
├─────────────────────────────────────────────────────────────────┤
│  Bybit V5 REST + WebSocket (Spot)                               │
└─────────────────────────────────────────────────────────────────┘
```

## Inicio rápido

```bash
# Clonar e instalar
git clone https://github.com/nerthzbyt/Restructured.git
cd Restructured
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

# Configurar (.env)
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_ENV=demo                  # demo | mainnet
LLM_BACKEND=qwen_desktop        # qwen_desktop | openai_compat | ollama

# Motor de trading
python src/Nertzh.py

# Agente AI PRO (puerto 8787)
python NerT_AI_PRO/main.py
```

Abre `http://127.0.0.1:8787` para la consola del agente (tema negro profesional).

## Documentación

| Sección | Descripción |
|---------|-------------|
| [Introducción v5](docs/v5/introduction.md) | Visión general del sistema |
| [Indicadores](docs/v5/indicators.md) | PIO, EGM, ILD, ROL, OGM, TFI, MOM, Combined |
| [Niveles de predicción](docs/v5/prediction-levels.md) | L0–L4 con datos validados |
| [Perfiles de orden](docs/v5/order-profiles.md) | Market+IOC, Limit+GTC, scoring |
| [API Agent](docs/v5/agent-api.md) | Endpoints REST del agente |
| [Integración Qwen](docs/v5/qwen-integration.md) | LLM backends y modelos |
| [Roadmap](docs/v5/roadmap.md) | Usos futuros |

**Sitio web:** [nerthzbyt.github.io/Restructured](https://nerthzbyt.github.io/Restructured/)

## API Agent — endpoints principales

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/agent/catalog` | Catálogo completo: indicadores, niveles, perfiles |
| `GET` | `/agent/prediction-level/{symbol}` | Nivel L0–L4 en tiempo real |
| `GET` | `/agent/order-profiles` | Perfiles validados vs exchange |
| `GET` | `/agent/context` | Estado bot + métricas + predicción |
| `POST` | `/agent/chat` | Agente autónomo ReAct |
| `POST` | `/agent/optimize` | Optimización Optuna thresholds/pesos |
| `GET` | `/agent/llm/status` | Estado Qwen / LLM |
| `POST` | `/predict/{symbol}` | Decisión + probabilidades ML |

## Validación con datos reales

Sweep completo **2026-07-04** sobre **BTCUSDT** (fuente: Bybit exchange API):

- **29,184** combinaciones evaluadas (304 perfiles × 48 params × 2 lados)
- **86.8%** de órdenes históricas = `Market|IOC|Order`
- **Top score compuesto:** 85.62 (execution 90.0, fit producción 0.85)
- **Live demo:** fill OK, slippage 2.16 bps, latencia ~2s

## Estructura del repositorio

```
Restructured/
├── src/                 # Motor Nertzh + métricas + Bybit v5
├── NerT_AI_PRO/         # Agente FastAPI + UI + Qwen
├── config/              # Configuración centralizada
├── tests/               # Tests del sistema
├── docs/                # Documentación v5 + GitHub Pages
└── requirements.txt
```

## Licencia

Proyecto privado de investigación cuantitativa. Uso bajo responsabilidad del operador.