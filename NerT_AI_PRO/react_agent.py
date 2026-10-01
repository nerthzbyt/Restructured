"""Agente ReAct: razonamiento iterativo con herramientas nativas + MCP + /src.

Anti-fable: el agente debe ejecutar herramientas reales (datos live) y no narrar
como modo Mythos/Fable de Claude (historia sin grounding).
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from mcp_bridge import bybit_env_info, call_bybit_mcp
from bot_state_bridge import bot_live_state_snapshot
from src_bridge import project_context_snapshot
from tool_registry import build_prompt_catalog, search_tools

ExecuteFn = Callable[[str, Dict[str, Any], Dict[str, Any]], Awaitable[Dict[str, Any]]]

_ACTION_RE = re.compile(
    r'\{[^{}]*"type"\s*:\s*"(?:action|final)"[^{}]*\}',
    re.IGNORECASE | re.DOTALL,
)

_FABLE_MARKERS = (
    "había una vez",
    "en un mundo",
    "imaginemos",
    "fable",
    "mythos",
    "leyenda",
    "cuento",
    "metáfora poética",
)

_DATA_GOAL_HINTS = (
    "precio",
    "ticker",
    "balance",
    "métrica",
    "metric",
    "orden",
    "order",
    "mercado",
    "market",
    "btc",
    "eth",
    "analiza",
    "analyze",
    "estado",
    "status",
    "combined",
    "señal",
    "signal",
    "portfolio",
    "trading",
)


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def _extract_action_json(text: str) -> Optional[Dict[str, Any]]:
    s = str(text or "").strip()
    if not s:
        return None
    try:
        j = json.loads(s)
        if isinstance(j, dict) and j.get("type") in {"action", "final"}:
            return j
    except Exception:
        pass

    md = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, re.DOTALL | re.IGNORECASE)
    if md:
        try:
            j = json.loads(md.group(1))
            if isinstance(j, dict) and j.get("type") in {"action", "final"}:
                return j
        except Exception:
            pass

    for m in _ACTION_RE.finditer(s):
        chunk = m.group(0)
        try:
            j = json.loads(chunk)
            if isinstance(j, dict) and j.get("type") in {"action", "final"}:
                return j
        except Exception:
            continue
    lb = s.find("{")
    rb = s.rfind("}")
    if lb >= 0 and rb > lb:
        try:
            j = json.loads(s[lb : rb + 1])
            if isinstance(j, dict) and j.get("type") in {"action", "final"}:
                return j
        except Exception:
            pass
    return None


def _goal_needs_live_data(goal: str) -> bool:
    g = str(goal or "").lower()
    return any(h in g for h in _DATA_GOAL_HINTS)


def _looks_like_fable(text: str) -> bool:
    low = str(text or "").lower()
    return any(m in low for m in _FABLE_MARKERS)


def _tool_steps_in_trace(trace: List[Dict[str, Any]]) -> int:
    return sum(1 for t in trace if t.get("type") == "action" and t.get("tool"))


def _tools_prompt(catalog: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for t in catalog:
        name = t.get("name")
        kind = t.get("kind", "")
        desc = str(t.get("description") or "").strip()
        if len(desc) > 160:
            desc = desc[:157] + "..."
        args = t.get("args") or t.get("input_schema") or {}
        lines.append(f"- [{kind}] {name}: {desc}")
    return "\n".join(lines)


def _system_prompt(catalog: List[Dict[str, Any]], ctx: Dict[str, Any]) -> str:
    env = bybit_env_info()
    demo_block = ""
    if env.get("is_demo"):
        demo_block = (
            "\nMODO DEMO TRADING (BYBIT_ENV=demo):\n"
            "- API privada del motor: api-demo.bybit.com (vía nertzh_api.*)\n"
            "- Datos públicos: api.bybit.com (market_ticker, mcp_bybit públicos)\n"
            "- NO uses mcp_bybit para wallet/órdenes privadas → usa nertzh_api.balance, orders_status\n"
        )
    return (
        "Eres NerT AI PRO, agente cuantitativo AUTÓNOMO con acceso a herramientas reales.\n"
        "NO eres un narrador literario (prohibido modo fable/mythos/storytelling).\n"
        "Bucle ReAct: piensas → eliges UNA herramienta → observas → repites hasta concluir.\n\n"
        "FORMATO OBLIGATORIO (SOLO JSON, sin markdown):\n"
        'Actuar: {"type":"action","thought":"...","tool":"nombre","args":{...}}\n'
        'Final: {"type":"final","thought":"...","answer":"respuesta completa en español"}\n\n'
        "REGLAS ANTI-FABLE (críticas):\n"
        "- Toda cifra de mercado/balance/orden DEBE venir de una observación de herramienta.\n"
        "- Prohibido inventar precios, PnL, métricas o estados del bot sin tool previa.\n"
        "- Si el usuario pide análisis live: mínimo bot_live_state o nertzh_api.ticker antes del final.\n"
        "- No metáforas, cuentos ni respuestas genéricas de manual.\n"
        "- Usa tool_search si necesitas una herramienta que no ves listada.\n"
        "- Entrada única: python NerT_AI_PRO/main.py — Nertzh es motor embebido en /api, no servicio aparte.\n"
        "- Para análisis EN VIVO prioriza bot_live_state y nertzh_api.decisions (métricas del loop).\n"
        "- nertzh_api.metrics/combined son secundarios; historial mom → src_read data/metrics_snapshots.jsonl.\n"
        "- Usa project_context y src_read/src_grep para entender gates y código en /src.\n"
        "- Factoriza: bot_live_state/decisions → mercado → balance/órdenes → conclusión.\n"
        "- category=linear para perps USDT. No inventes datos.\n"
        "- Mutaciones (órdenes) solo si el usuario pidió apply=true.\n"
        f"{demo_block}\n"
        "CONTEXTO PROYECTO:\n"
        f"{json.dumps(ctx, ensure_ascii=False, indent=2)}\n\n"
        "HERRAMIENTAS CORE (usa tool_search para más):\n"
        f"{_tools_prompt(catalog)}"
    )


async def run_react_agent(
    *,
    goal: str,
    symbol: Optional[str],
    execute_tool: ExecuteFn,
    llm_chat_fn: Callable[..., Awaitable[Dict[str, Any]]],
    max_steps: int = 10,
    include_mcp: bool = True,
    allow_mutations: bool = False,
    session_id: str = "",
) -> Dict[str, Any]:
    proj = project_context_snapshot()
    ctx = proj.get("context") if isinstance(proj.get("context"), dict) else {}
    sym_boot = str(symbol or "BTCUSDT").strip().upper()
    live = bot_live_state_snapshot(symbol=sym_boot)
    ctx = {
        **ctx,
        "bot_live_state": live if live.get("ok") else {"ok": False, "note": live.get("message") or live.get("error")},
    }
    catalog = build_prompt_catalog(include_mcp=include_mcp)
    system = _system_prompt(catalog, ctx)
    sym = str(symbol or "BTCUSDT").strip().upper()
    run_ctx = {
        "symbol": sym,
        "goal": goal,
        "allow_mutations": bool(allow_mutations),
        "bybit_env": ctx.get("bybit_env"),
    }
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": (
                f"Objetivo: {goal}\nSímbolo: {sym}\n"
                "Analiza de forma autónoma. Para estado del motor usa bot_live_state o nertzh_api.decisions primero."
            ),
        },
    ]
    trace: List[Dict[str, Any]] = []
    final_answer: Optional[str] = None
    steps = max(1, min(20, int(max_steps)))
    last_res: Dict[str, Any] = {}
    parse_failures = 0
    max_parse_failures = _env_int("AGENT_MAX_PARSE_RETRIES", 4, 1, 8)
    min_tool_steps = _env_int("AGENT_MIN_TOOL_STEPS", 1, 0, 5)
    needs_live = _goal_needs_live_data(goal)
    if needs_live:
        min_tool_steps = max(min_tool_steps, _env_int("AGENT_MIN_TOOL_STEPS_LIVE", 2, 1, 6))

    for step in range(steps):
        res = await llm_chat_fn(messages)
        last_res = res if isinstance(res, dict) else {}
        if not bool(res.get("ok")):
            return {
                "ok": False,
                "mode": "react",
                "error": res.get("error"),
                "backend": res.get("backend"),
                "model": res.get("model"),
                "trace": trace,
                "session_id": session_id,
                "timestamp": int(time.time() * 1000),
            }
        content = str(res.get("content") or "").strip()
        action = _extract_action_json(content)
        if action is None:
            parse_failures += 1
            example = (
                '{"type":"action","thought":"consultar mercado","tool":"nertzh_api.ticker",'
                '"args":{"symbol":"BTCUSDT"}}'
            )
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Formato inválido ({parse_failures}/{max_parse_failures}). "
                        f"Responde SOLO JSON sin markdown. Ejemplo: {example}"
                    ),
                }
            )
            trace.append({"step": step + 1, "parse_error": True, "raw": content[:500]})
            if parse_failures >= max_parse_failures:
                return {
                    "ok": False,
                    "mode": "react",
                    "error": "react_json_parse_exhausted",
                    "message": "El LLM no emitió JSON ReAct válido tras varios intentos.",
                    "trace": trace,
                    "backend": last_res.get("backend"),
                    "model": last_res.get("model"),
                    "session_id": session_id,
                    "timestamp": int(time.time() * 1000),
                }
            continue

        parse_failures = 0

        if action.get("type") == "final":
            answer = str(action.get("answer") or action.get("thought") or "").strip()
            tool_steps = _tool_steps_in_trace(trace)
            if needs_live and tool_steps < min_tool_steps:
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Rechazado: faltan datos live ({tool_steps}/{min_tool_steps} tools). "
                            "Ejecuta herramientas antes del final."
                        ),
                    }
                )
                trace.append(
                    {
                        "step": step + 1,
                        "type": "final_rejected",
                        "reason": "insufficient_tool_steps",
                        "tool_steps": tool_steps,
                        "min_required": min_tool_steps,
                    }
                )
                continue
            if _looks_like_fable(answer):
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Rechazado: respuesta narrativa/fable. "
                            "Usa datos de herramientas y responde técnico."
                        ),
                    }
                )
                trace.append({"step": step + 1, "type": "final_rejected", "reason": "fable_detected"})
                continue
            final_answer = answer
            trace.append({"step": step + 1, "type": "final", "thought": action.get("thought"), "answer": final_answer})
            break

        tool = str(action.get("tool") or "").strip()
        args = action.get("args") if isinstance(action.get("args"), dict) else {}
        thought = str(action.get("thought") or "").strip()
        if not tool:
            messages.append({"role": "user", "content": "Falta 'tool'. Usa tool_search si no sabes cuál elegir."})
            continue

        if tool.startswith("mcp_bybit."):
            obs = await call_bybit_mcp(tool, args, allow_mutations=bool(allow_mutations))
        else:
            obs = await execute_tool(tool, args, run_ctx)

        trace.append({"step": step + 1, "type": "action", "thought": thought, "tool": tool, "args": args, "observation": obs})
        obs_text = json.dumps(obs, ensure_ascii=False)
        if len(obs_text) > 6000:
            obs_text = obs_text[:6000] + "...(truncado)"
        messages.append({"role": "assistant", "content": content})
        messages.append(
            {"role": "user", "content": f"Observación de {tool}:\n{obs_text}\n\nContinúa o emite type=final."}
        )

    if not final_answer:
        messages.append(
            {
                "role": "user",
                "content": (
                    'Emite SOLO {"type":"final","thought":"...","answer":"..."} '
                    "basado en observaciones de herramientas (sin inventar datos)."
                ),
            }
        )
        res = await llm_chat_fn(messages)
        last_res = res if isinstance(res, dict) else {}
        if bool(res.get("ok")):
            action = _extract_action_json(str(res.get("content") or ""))
            if isinstance(action, dict) and action.get("type") == "final":
                answer = str(action.get("answer") or "").strip()
                if (not needs_live or _tool_steps_in_trace(trace) >= min_tool_steps) and not _looks_like_fable(answer):
                    final_answer = answer
            elif not _looks_like_fable(str(res.get("content") or "")):
                final_answer = str(res.get("content") or "").strip()

    from tool_registry import registry_stats

    stats = registry_stats()
    tool_steps = _tool_steps_in_trace(trace)
    grounded = tool_steps >= min_tool_steps
    complete = bool(final_answer) and (grounded or not needs_live)
    return {
        "ok": complete,
        "mode": "react",
        "answer": final_answer,
        "trace": trace,
        "steps_used": len(trace),
        "tool_steps": tool_steps,
        "min_tool_steps": min_tool_steps,
        "grounded": bool(grounded),
        "anti_fable": True,
        "tools_in_prompt": len(catalog),
        "tools_total": stats.get("total"),
        "tools_by_kind": stats.get("by_kind"),
        "bybit_env": stats.get("bybit_env"),
        "backend": last_res.get("backend"),
        "model": last_res.get("model"),
        "session_id": session_id,
        "timestamp": int(time.time() * 1000),
    }


def tool_search_for_agent(query: str, limit: int = 15) -> Dict[str, Any]:
    hits = search_tools(query, limit=limit)
    return {"ok": True, "query": query, "count": len(hits), "tools": hits}