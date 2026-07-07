"""Agente ReAct: razonamiento iterativo con herramientas nativas + MCP + /src."""
from __future__ import annotations

import json
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
        "Bucle ReAct: piensas → eliges UNA herramienta → observas → repites hasta concluir.\n\n"
        "FORMATO OBLIGATORIO (SOLO JSON, sin markdown):\n"
        'Actuar: {"type":"action","thought":"...","tool":"nombre","args":{...}}\n'
        'Final: {"type":"final","thought":"...","answer":"respuesta completa en español"}\n\n'
        "REGLAS:\n"
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
            messages.append(
                {
                    "role": "user",
                    "content": 'Formato inválido. Responde SOLO JSON {"type":"action",...} o {"type":"final",...}',
                }
            )
            trace.append({"step": step + 1, "parse_error": True, "raw": content[:500]})
            continue

        if action.get("type") == "final":
            final_answer = str(action.get("answer") or action.get("thought") or "").strip()
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
            {"role": "user", "content": 'Emite SOLO {"type":"final","thought":"...","answer":"..."} con análisis completo.'}
        )
        res = await llm_chat_fn(messages)
        last_res = res if isinstance(res, dict) else {}
        if bool(res.get("ok")):
            action = _extract_action_json(str(res.get("content") or ""))
            if isinstance(action, dict) and action.get("type") == "final":
                final_answer = str(action.get("answer") or "").strip()
            else:
                final_answer = str(res.get("content") or "").strip()

    from tool_registry import registry_stats

    stats = registry_stats()
    return {
        "ok": bool(final_answer),
        "mode": "react",
        "answer": final_answer,
        "trace": trace,
        "steps_used": len(trace),
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