#!/usr/bin/env python3
"""Benchmark Qwen Advanced v2.0 — lab NerT/Qwen (no leaderboard público).

Valida techo del modelo para el agente Restructured. Un score alto aquí + /agent/readiness OK
implica agente completo (ReAct con tools, anti-fable). No sustituye smoke diario del motor.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import math

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "NerT_AI_PRO"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from path_safety import safe_write_text  # noqa: E402

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"), override=False)

try:
    from qwen_desktop import normalize_model, qwen_desktop_chat, qwen_desktop_status
except ImportError:
    async def qwen_desktop_chat(messages, model, timeout_s):
        return {"ok": False, "error": "client_not_available"}
    async def qwen_desktop_status():
        return {"session_found": False}
    def normalize_model(m):
        return m


@dataclass
class BenchCase:
    id: str
    category: str
    difficulty: str
    prompt: str
    scorer: Callable[[str], Dict[str, Any]]
    timeout_s: float = 180.0
    requires_execution: bool = False


@dataclass
class BenchResult:
    case_id: str
    category: str
    difficulty: str
    ok: bool
    score: float
    latency_s: float
    chars: int
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    preview: str = ""


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    """Extrae JSON de texto, manejando markdown y texto adicional."""
    s = str(text or "").strip()
    if not s:
        return None

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, re.DOTALL)
    if json_match:
        try:
            obj = json.loads(json_match.group(1))
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

    lb, rb = s.find("{"), s.rfind("}")
    if lb >= 0 and rb > lb:
        try:
            obj = json.loads(s[lb:rb + 1])
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

    return None


def _extract_code_block(text: str) -> str:
    """Extrae bloque de código Python."""
    s = str(text or "").strip()

    match = re.search(r"```python\s*(.*?)\s*```", s, re.DOTALL)
    if match:
        return match.group(1).strip()

    match = re.search(r"```\s*(.*?)\s*```", s, re.DOTALL)
    if match:
        return match.group(1).strip()

    return s


def _execute_python(code: str, timeout_s: float = 5.0) -> Tuple[bool, str, str]:
    """Ejecuta código Python en sandbox."""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            f.flush()

            result = subprocess.run(
                ["python3", f.name],
                capture_output=True,
                text=True,
                timeout=timeout_s,
                cwd=tempfile.gettempdir()
            )

            os.unlink(f.name)

            return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
    except subprocess.TimeoutExpired:
        return False, "", "timeout"
    except Exception as e:
        return False, "", str(e)


def _build_cases(seed: int) -> List[BenchCase]:
    rng = random.Random(seed)

    def score_json_structure(required_keys: List[str], value_types: Optional[Dict[str, type]] = None):
        def _fn(text: str) -> Dict[str, Any]:
            obj = _extract_json(text)
            if not obj:
                return {"parsed": False, "score": 0.0, "reason": "no_json"}

            missing = [k for k in required_keys if k not in obj]
            if missing:
                return {
                    "parsed": True,
                    "score": max(0.0, 1.0 - 0.2 * len(missing)),
                    "missing": missing
                }

            type_errors = []
            if value_types:
                for key, expected_type in value_types.items():
                    if key in obj and not isinstance(obj[key], expected_type):
                        type_errors.append(f"{key}: expected {expected_type.__name__}, got {type(obj[key]).__name__}")

            if type_errors:
                return {"parsed": True, "score": 0.6, "type_errors": type_errors}

            return {"parsed": True, "score": 1.0}

        return _fn

    def score_code_execution(test_code: str, expected_output: str):
        def _fn(text: str) -> Dict[str, Any]:
            code = _extract_code_block(text)
            if not code:
                return {"score": 0.0, "reason": "no_code"}

            full_code = f"{code}\n\n{test_code}"
            success, stdout, stderr = _execute_python(full_code, timeout_s=5.0)

            if not success:
                return {"score": 0.3, "reason": "execution_failed", "stderr": stderr[:500]}

            if expected_output in stdout:
                return {"score": 1.0, "output": stdout[:200]}
            else:
                return {"score": 0.5, "reason": "wrong_output", "expected": expected_output, "got": stdout[:200]}

        return _fn

    def score_math_rigorous(expected_answer: float, tolerance: float = 0.01, require_steps: bool = True):
        def _fn(text: str) -> Dict[str, Any]:
            nums = [float(x) for x in re.findall(r"-?\d+\.?\d*", text)]

            if not nums:
                return {"score": 0.0, "reason": "no_answer"}

            best = min(nums, key=lambda n: abs(n - expected_answer))
            error = abs(best - expected_answer) / max(abs(expected_answer), 1e-9)

            if error > tolerance:
                return {"score": max(0.0, 1.0 - error * 5), "expected": expected_answer, "got": best, "error_pct": round(error * 100, 2)}

            if require_steps:
                reasoning_markers = ["paso", "step", "therefore", "entonces", "así que", "so", "because", "porque"]
                has_reasoning = any(marker in text.lower() for marker in reasoning_markers)
                if not has_reasoning:
                    return {"score": 0.8, "answer": best, "reason": "correct_but_no_reasoning"}

            return {"score": 1.0, "answer": best}

        return _fn

    cases: List[BenchCase] = [
        # RAZONAMIENTO LÓGICO DURO
        BenchCase(
            "logic_einstein_riddle", "reasoning", "expert",
            "Resuelve el acertijo de Einstein:\nHay 5 casas de diferentes colores, con dueños de diferentes nacionalidades, bebidas, cigarrillos y mascotas.\n- El británico vive en la casa roja\n- El sueco tiene perros\n- El danés toma té\n- La casa verde está a la izquierda de la blanca\n- El dueño de la casa verde toma café\n- La persona que fuma Pall Mall cría pájaros\n- El dueño de la casa amarilla fuma Dunhill\n- El que vive en la casa del centro toma leche\n- El noruego vive en la primera casa\n- La persona que fuma Blend vive junto a la que tiene gatos\n- El que cría caballos vive junto al que fuma Dunhill\n- El que fuma Bluemasters toma cerveza\n- El alemán fuma Prince\n- El noruego vive junto a la casa azul\n- El que fuma Blend tiene un vecino que toma agua\n\n¿Quién tiene el pez? Responde SOLO el nombre de la nacionalidad.",
            lambda t: {"score": 1.0 if "alemán" in t.lower() or "german" in t.lower() else 0.0, "answer": t[:100]},
            timeout_s=300
        ),

        # MATEMÁTICAS AVANZADAS
        BenchCase(
            "math_integration_rigorous", "math", "expert",
            "Demuestra paso a paso que ∫₀^∞ e^(-x²) dx = √π/2.\nMuestra todos los pasos del razonamiento, incluyendo:\n1) La técnica del cuadrado de la integral\n2) Cambio a coordenadas polares\n3) Evaluación de límites\n4) Resultado final con justificación",
            lambda t: {"score": 1.0 if all(x in t for x in ["polares", "√π", "límite"]) and "π/2" in t else 0.7 if "polares" in t or "polar" in t.lower() else 0.3, "has_polar": "polares" in t or "polar" in t.lower(), "has_sqrt_pi": "√π" in t or "sqrt(pi)" in t.lower()},
            timeout_s=300
        ),

        BenchCase(
            "math_probability_bayesian", "math", "hard",
            "Problema de probabilidad bayesiana:\nUna enfermedad afecta al 1% de la población. Un test tiene 95% de sensibilidad y 90% de especificidad.\nSi una persona da positivo, ¿cuál es la probabilidad de que realmente tenga la enfermedad?\nCalcula paso a paso usando el teorema de Bayes y da la respuesta con 4 decimales.",
            score_math_rigorous(expected_answer=(0.01 * 0.95) / (0.01 * 0.95 + 0.99 * 0.10), tolerance=0.01, require_steps=True),
            timeout_s=180
        ),

        # PROGRAMACIÓN CON EJECUCIÓN
        BenchCase(
            "code_lru_cache", "code", "hard",
            "Implementa una clase LRUCache en Python con estos métodos:\n- __init__(capacity): inicializa con capacidad máxima\n- get(key): retorna valor o -1 si no existe, actualiza como más reciente\n- put(key, value): inserta/actualiza, elimina el menos reciente si excede capacidad\n\nIncluye manejo de edge cases.",
            score_code_execution(
                test_code='\ncache = LRUCache(2)\ncache.put(1, 1)\ncache.put(2, 2)\nassert cache.get(1) == 1\ncache.put(3, 3)\nassert cache.get(2) == -1\ncache.put(4, 4)\nassert cache.get(1) == -1\nassert cache.get(3) == 3\nassert cache.get(4) == 4\nprint("TESTS_PASSED")',
                expected_output="TESTS_PASSED"
            ),
            requires_execution=True, timeout_s=120
        ),

        BenchCase(
            "code_algorithm_optimization", "code", "expert",
            "Dado un array de enteros y un target, encuentra los índices de dos números que sumen el target.\nRequisitos:\n1) Complejidad O(n) tiempo\n2) O(n) espacio máximo\n3) Maneja casos donde no hay solución\n4) Incluye tests unitarios\n\nEscribe código Python completo y ejecutable.",
            score_code_execution(
                test_code='\nassert two_sum([2, 7, 11, 15], 9) == [0, 1]\nassert two_sum([3, 2, 4], 6) == [1, 2]\nassert two_sum([3, 3], 6) == [0, 1]\nassert two_sum([1, 2, 3], 7) is None or two_sum([1, 2, 3], 7) == []\nprint("TESTS_PASSED")',
                expected_output="TESTS_PASSED"
            ),
            requires_execution=True, timeout_s=180
        ),

        # TRADING CUANTITATIVO AVANZADO
        BenchCase(
            "trading_kelly_advanced", "trading", "hard",
            "Calcula el Kelly fraction óptimo para estas condiciones:\n- Win rate: 58%\n- Average win: $150\n- Average loss: $100\n- Correlación entre trades: 0.15\n- Número máximo de posiciones simultáneas: 3\n\nCalcula:\n1) Kelly fraction básico\n2) Kelly fraccional (half-Kelly) para reducir volatilidad\n3) Ajuste por correlación\n4) Tamaño de posición recomendado para cuenta de $100,000\n\nResponde en JSON con todos los cálculos paso a paso.",
            score_json_structure(["kelly_basic", "kelly_fractional", "kelly_adjusted", "position_size"], {"kelly_basic": float, "kelly_fractional": float, "kelly_adjusted": float, "position_size": float}),
            timeout_s=180
        ),

        BenchCase(
            "trading_portfolio_optimization", "trading", "expert",
            "Optimiza un portfolio con 3 activos:\n\nActivos:\n- A: retorno esperado 12%, volatilidad 22%\n- B: retorno esperado 8%, volatilidad 18%\n- C: retorno esperado 15%, volatilidad 30%\n\nMatriz de correlación:\n      A    B    C\nA   1.0  0.3  0.5\nB   0.3  1.0  0.2\nC   0.5  0.2  1.0\n\nRestricciones:\n- Peso mínimo por activo: 10%\n- Peso máximo por activo: 50%\n- Retorno mínimo del portfolio: 10%\n\nEncuentra los pesos que maximizan el Sharpe ratio. Responde en JSON con los pesos óptimos, retorno esperado, volatilidad y Sharpe ratio.",
            score_json_structure(["weights", "expected_return", "volatility", "sharpe_ratio"], {"weights": dict, "expected_return": float, "volatility": float, "sharpe_ratio": float}),
            timeout_s=300
        ),

        # RAZONAMIENTO MULTI-TURNO
        BenchCase(
            "reasoning_multi_step", "reasoning", "expert",
            "Problema de razonamiento multi-paso:\n\nContexto inicial:\n- 4 traders: Alice, Bob, Charlie, Diana\n- Capital inicial: $100,000 cada uno\n- Reglas de trading:\n  1) Si combined > 5, comprar\n  2) Si combined < -5, vender\n  3) Si -5 <= combined <= 5, hold\n  4) Después de cada trade, actualizar capital\n\nSecuencia de señales combined: [6, -3, 8, -7, 2, 9, -6]\n\nCada trader usa diferente estrategia:\n- Alice: sigue las reglas estrictamente\n- Bob: usa half-Kelly sizing\n- Charlie: arriesga 2% del capital por trade\n- Diana: usa position sizing fijo de $10,000\n\nSimula los 7 trades para cada trader y reporta:\n1) Capital final de cada uno\n2) Número de trades ganadores vs perdedores\n3) Drawdown máximo\n4) Sharpe ratio estimado\n\nResponde en JSON con todos los cálculos.",
            score_json_structure(["alice", "bob", "charlie", "diana"], {"alice": dict, "bob": dict, "charlie": dict, "diana": dict}),
            timeout_s=300
        ),

        # COMPRENSIÓN DE CONTEXTO LARGO
        BenchCase(
            "long_context_analysis", "analysis", "hard",
            "Analiza este documento técnico largo y responde preguntas específicas:\n\n[DOCUMENTO TÉCNICO - Sistema de Trading Algorítmico NerT AI PRO v3.7]\n\nArquitectura:\nEl sistema utiliza una arquitectura de microservicios con 7 componentes principales:\n1) Data Ingestion Layer: procesa 50,000 ticks/segundo de múltiples exchanges\n2) Feature Engineering Pipeline: calcula 247 features técnicos en tiempo real\n3) Signal Generation Engine: ejecuta 12 estrategias paralelas\n4) Risk Management Module: monitorea VaR, CVaR y exposure en tiempo real\n5) Order Execution System: smart order routing con 8 venues diferentes\n6) Position Monitoring: tracking de PnL con latencia < 5ms\n7) Performance Analytics: dashboards y reporting en tiempo real\n\nMétricas de performance (últimos 12 meses):\n- Sharpe Ratio: 2.84\n- Sortino Ratio: 4.12\n- Max Drawdown: -8.7%\n- Win Rate: 61.3%\n- Average Trade: $847\n- Total Trades: 15,847\n- Profit Factor: 1.92\n\nLimitaciones conocidas:\n- El sistema asume liquidez constante, lo cual no es cierto en mercados de baja capitalización\n- El modelo de ejecución no considera market impact para órdenes > 1% del volumen diario\n- El risk management usa VaR paramétrico que subestima fat tails\n- La latencia de red puede causar slippage en condiciones de alta volatilidad\n- El sistema no maneja correctamente gaps overnight en mercados 24/5\n\nMejoras planificadas v3.8:\n- Implementar ejecución con market impact model de Almgren-Chriss\n- Agregar detector de régimen de mercado basado en HMM\n- Migrar risk management a Historical Simulation VaR\n- Añadir módulo de correlación dinámica DCC-GARCH\n- Implementar circuit breakers automáticos basados en volatility regime\n\nPreguntas a responder:\n1) ¿Cuál es la principal limitación del sistema actual respecto a ejecución de órdenes grandes?\n2) ¿Qué técnica estadística se usará para mejorar el risk management en v3.8?\n3) ¿Cuál es el Profit Factor del sistema y qué indica?\n4) Nombra 3 de las 5 limitaciones conocidas del sistema\n5) ¿Qué modelo se implementará para mejorar la ejecución en v3.8?\n\nResponde en JSON con las 5 respuestas.",
            score_json_structure(["q1", "q2", "q3", "q4", "q5"]),
            timeout_s=180
        ),

        # ANÁLISIS DE MICROESTRUCTURA
        BenchCase(
            "trading_microstructure_deep", "trading", "expert",
            "Analiza esta situación de microestructura de mercado:\n\nOrder Book BTCUSDT:\nBID: \n- Level 1: $67,234.50 - 2.34 BTC\n- Level 2: $67,234.00 - 1.87 BTC\n- Level 3: $67,233.50 - 3.12 BTC\n- Level 4: $67,233.00 - 0.95 BTC\n- Level 5: $67,232.50 - 4.21 BTC\n\nASK:\n- Level 1: $67,235.50 - 1.23 BTC\n- Level 2: $67,236.00 - 2.89 BTC\n- Level 3: $67,236.50 - 1.45 BTC\n- Level 4: $67,237.00 - 3.67 BTC\n- Level 5: $67,237.50 - 2.11 BTC\n\nMétricas adicionales:\n- Spread: $1.00 (0.00149%)\n- Bid-ask imbalance: +0.34 (más presión compradora)\n- Order flow imbalance (últimos 10 min): +2.45 BTC net buy\n- Large order detection: 3 iceberg orders detectadas en bid side\n- Spoofing score: 0.23 (bajo)\n- Market impact estimate: 0.5 bps por 1 BTC\n\nAnaliza:\n1) Sesgo direccional basado en order book imbalance\n2) Calidad de la liquidez en ambos lados\n3) Riesgo de spoofing y manipulación\n4) Recomendación de entrada con justificación\n5) Tamaño óptimo de orden para minimizar market impact\n\nResponde en JSON estructurado.",
            score_json_structure(["directional_bias", "liquidity_quality", "spoofing_risk", "recommendation", "optimal_size"], {"directional_bias": str, "liquidity_quality": dict, "spoofing_risk": str, "recommendation": str, "optimal_size": float}),
            timeout_s=240
        ),

        # AGENT TOOL-USE AVANZADO
        BenchCase(
            "agent_complex_workflow", "agent", "expert",
            "Eres un agente de trading con acceso a estas herramientas:\n1) get_market_data(symbol, timeframe): obtiene datos OHLCV\n2) calculate_indicators(data, indicators): calcula indicadores técnicos\n3) generate_signals(strategy_name, params): genera señales de trading\n4) validate_signal(signal, risk_params): valida señal contra reglas de riesgo\n5) execute_trade(signal, size): ejecuta trade\n6) get_portfolio_state(): obtiene estado actual del portfolio\n7) log_decision(reasoning, action): registra decisión\n\nContexto:\n- Symbol: ETHUSDT\n- Timeframe: 1h\n- Capital disponible: $50,000\n- Risk por trade: 1%\n- Portfolio actual: 40% ETH, 60% USDT\n- Máximo 3 posiciones abiertas\n\nPlanifica un workflow completo de trading que:\n1) Obtiene datos de mercado\n2) Calcula RSI, MACD y Bollinger Bands\n3) Genera señal usando estrategia \"momentum_reversal\"\n4) Valida señal contra límites de riesgo\n5) Ejecuta trade si es válida\n6) Registra la decisión\n\nDevuelve el plan en JSON con cada paso, herramienta a usar, parámetros y condición de éxito.",
            score_json_structure(["steps", "total_steps", "success_criteria"], {"steps": list, "total_steps": int, "success_criteria": list}),
            timeout_s=240
        ),

        # ASIMETRÍA: RAZONAMIENTO CON INCERTIDUMBRE
        BenchCase(
            "reasoning_uncertainty", "reasoning", "expert",
            "Problema con información incompleta:\n\nUn trader tiene 3 estrategias: A, B, C.\n- Estrategia A: funciona bien en mercados alcistas (70% win rate)\n- Estrategia B: funciona bien en mercados bajistas (65% win rate)  \n- Estrategia C: funciona bien en mercados laterales (60% win rate)\n\nInformación disponible:\n- Últimos 30 días: 60% alcista, 20% bajista, 20% lateral\n- Próximos 30 días: predicción incierta, estimación: 40% alcista, 30% bajista, 30% lateral\n- Correlación entre estrategias: A-B = -0.3, A-C = 0.2, B-C = -0.4\n- Capital: $100,000\n\nDecide:\n1) ¿Qué combinación de estrategias usar?\n2) ¿Qué porcentaje de capital asignar a cada una?\n3) ¿Cómo ajustar si el régimen de mercado cambia?\n4) ¿Cuál es el Sharpe ratio esperado de la combinación?\n\nJustifica tu decisión considerando la incertidumbre en la predicción del régimen.\nResponde en JSON con tu análisis completo.",
            score_json_structure(["strategy_mix", "capital_allocation", "adjustment_plan", "expected_sharpe"], {"strategy_mix": list, "capital_allocation": dict, "adjustment_plan": str, "expected_sharpe": float}),
            timeout_s=300
        ),
    ]

    return cases


async def run_benchmark(model: str, seed: int, cases: Optional[List[BenchCase]] = None) -> Dict[str, Any]:
    model_norm = normalize_model(model)
    status = await qwen_desktop_status()

    if not status.get("session_found"):
        return {"ok": False, "error": "qwen_session_missing", "status": status}

    suite = cases or _build_cases(seed)
    results: List[BenchResult] = []

    for case in suite:
        t0 = time.perf_counter()
        res = await qwen_desktop_chat(messages=[{"role": "user", "content": case.prompt}], model=model_norm, timeout_s=case.timeout_s)
        latency = time.perf_counter() - t0

        if not res.get("ok"):
            results.append(BenchResult(case_id=case.id, category=case.category, difficulty=case.difficulty, ok=False, score=0.0, latency_s=latency, chars=0, error=str(res.get("error") or res.get("message") or "unknown")))
            await asyncio.sleep(1.5)
            continue

        content = str(res.get("content") or "")
        scored = case.scorer(content)
        score = float(scored.get("score", 0.0))

        results.append(BenchResult(case_id=case.id, category=case.category, difficulty=case.difficulty, ok=True, score=score, latency_s=latency, chars=len(content), details=scored, preview=content[:300].replace("\n", " ")))
        await asyncio.sleep(2.0)

    ok_results = [r for r in results if r.ok]
    avg_score = sum(r.score for r in results) / max(1, len(results))
    avg_latency = sum(r.latency_s for r in ok_results) / max(1, len(ok_results))
    success_rate = len(ok_results) / max(1, len(results))

    by_cat: Dict[str, List[float]] = {}
    by_difficulty: Dict[str, List[float]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r.score)
        by_difficulty.setdefault(r.difficulty, []).append(r.score)

    return {
        "ok": True,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "model_requested": model,
        "model_normalized": model_norm,
        "benchmark_version": "2.0_advanced",
        "qwen_status": {"session_found": status.get("session_found"), "api_ok": status.get("api_ok"), "token_source": status.get("token_source")},
        "summary": {
            "cases": len(results),
            "success_rate": round(success_rate, 3),
            "avg_score": round(avg_score, 3),
            "avg_latency_s": round(avg_latency, 2),
            "grade": _grade(avg_score, success_rate),
        },
        "by_category": {k: round(sum(v) / len(v), 3) for k, v in by_cat.items()},
        "by_difficulty": {k: round(sum(v) / len(v), 3) for k, v in by_difficulty.items()},
        "results": [
            {"id": r.case_id, "category": r.category, "difficulty": r.difficulty, "ok": r.ok, "score": round(r.score, 3), "latency_s": round(r.latency_s, 2), "chars": r.chars, "error": r.error, "details": r.details, "preview": r.preview}
            for r in results
        ],
        "market_tier_estimate": _market_tier(avg_score, success_rate),
        "asymmetry_analysis": _analyze_asymmetries(results),
    }


def _grade(avg_score: float, success_rate: float) -> str:
    composite = 0.7 * avg_score + 0.3 * success_rate
    if composite >= 0.90:
        return "A+"
    if composite >= 0.85:
        return "A"
    if composite >= 0.80:
        return "B+"
    if composite >= 0.75:
        return "B"
    if composite >= 0.70:
        return "B-"
    if composite >= 0.65:
        return "C+"
    if composite >= 0.60:
        return "C"
    return "D"


def _market_tier(avg_score: float, success_rate: float) -> Dict[str, Any]:
    composite = 0.7 * avg_score + 0.3 * success_rate
    tiers = [
        ("Frontier Elite", 0.92, ["GPT-5.2 Ultra", "Claude Opus 4.6 Pro", "Gemini 3.1 Ultra"]),
        ("Frontier", 0.85, ["GPT-5.2", "Claude Opus 4.6", "Gemini 3.1 Pro", "Qwen3.7-Max"]),
        ("Near-frontier", 0.78, ["GPT-5 mini", "Claude Sonnet 4.6", "Qwen3.7-Plus"]),
        ("Strong", 0.70, ["DeepSeek V3.2", "Llama 4 Maverick", "Qwen3.7-Standard"]),
        ("Mid-tier", 0.60, ["GPT-4o mini", "Mistral Medium", "Qwen3.5-Flash"]),
        ("Lightweight", 0.0, ["Small local models"]),
    ]
    for tier_name, threshold, peers in tiers:
        if composite >= threshold:
            return {"composite": round(composite, 3), "estimated_tier": tier_name, "peer_models": peers, "note": "Estimación basada en benchmark avanzado NerT v2.0"}
    return {"composite": round(composite, 3), "estimated_tier": "Lightweight", "peer_models": []}


def _analyze_asymmetries(results: List[BenchResult]) -> Dict[str, Any]:
    by_cat: Dict[str, List[BenchResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)

    analysis = {"strongest_areas": [], "weakest_areas": [], "asymmetry_score": 0.0}
    cat_scores = {cat: sum(r.score for r in res) / len(res) for cat, res in by_cat.items()}

    if cat_scores:
        max_score = max(cat_scores.values())
        min_score = min(cat_scores.values())
        analysis["asymmetry_score"] = round(max_score - min_score, 3)

        analysis["strongest_areas"] = [{"category": cat, "avg_score": round(score, 3)} for cat, score in cat_scores.items() if score >= 0.8]
        analysis["weakest_areas"] = [{"category": cat, "avg_score": round(score, 3)} for cat, score in cat_scores.items() if score < 0.6]

    return analysis


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Qwen Advanced v2.0")
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", "qwen-max-latest"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default=os.path.join(ROOT, "logs", "qwen_benchmark_advanced.json"))
    args = parser.parse_args()

    report = asyncio.run(run_benchmark(args.model, args.seed))

    out_path = safe_write_text(args.out, json.dumps(report, indent=2, ensure_ascii=False))
    args.out = str(out_path)

    if not report.get("ok"):
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 1

    s = report["summary"]
    tier = report["market_tier_estimate"]
    asym = report["asymmetry_analysis"]

    print("=" * 70)
    print(f"BENCHMARK QWEN ADVANCED v2.0")
    print("=" * 70)
    print(f"Model: {report['model_normalized']}")
    print(f"Cases: {s['cases']} | Success: {s['success_rate']*100:.0f}% | Score: {s['avg_score']:.2f} | Grade: {s['grade']}")
    print(f"Latency avg: {s['avg_latency_s']:.1f}s")
    print(f"\nTier estimado: {tier.get('estimated_tier')} (composite={tier.get('composite')})")
    print(f"Peers: {', '.join(tier.get('peer_models') or [])}")

    print(f"\n{'='*70}")
    print("ANÁLISIS POR CATEGORÍA:")
    print(f"{'='*70}")
    for cat, score in sorted(report["by_category"].items()):
        print(f"  {cat:20} {score:.3f}")

    print(f"\n{'='*70}")
    print("ANÁLISIS POR DIFICULTAD:")
    print(f"{'='*70}")
    for diff, score in sorted(report["by_difficulty"].items()):
        print(f"  {diff:20} {score:.3f}")

    print(f"\n{'='*70}")
    print("ANÁLISIS DE ASIMETRÍAS:")
    print(f"{'='*70}")
    print(f"  Asymmetry Score: {asym['asymmetry_score']:.3f}")
    if asym["strongest_areas"]:
        print(f"  Strongest areas:")
        for area in asym["strongest_areas"]:
            print(f"    - {area['category']}: {area['avg_score']:.3f}")
    if asym["weakest_areas"]:
        print(f"  Weakest areas:")
        for area in asym["weakest_areas"]:
            print(f"    - {area['category']}: {area['avg_score']:.3f}")

    print(f"\n{'='*70}")
    print("DETALLE DE RESULTADOS:")
    print(f"{'='*70}")
    for r in report["results"]:
        mark = "✓" if r["ok"] and r["score"] >= 0.8 else ("⚠" if r["ok"] else "✗")
        print(f"  [{mark}] {r['id']:30} score={r['score']:.2f} lat={r['latency_s']:.1f}s")
        if r.get("error"):
            print(f"       Error: {r['error']}")

    print(f"\n{'='*70}")
    print(f"Reporte guardado en: {args.out}")
    print(f"{'='*70}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())