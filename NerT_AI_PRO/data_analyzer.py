"""Wrapper del analizador de datos para el agente NerT (archivos grandes)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS = BASE_DIR / "scripts" / "analyze_trading_data.py"


def analyze_trading_data(
    *,
    results_path: str = "logs/results.json",
    jsonl_path: str = "data/metrics_snapshots.jsonl",
    max_output_chars: int = 120_000,
) -> Dict[str, Any]:
    """Ejecuta análisis streaming; devuelve dict (no carga archivos en el LLM)."""
    if not SCRIPTS.is_file():
        return {"ok": False, "error": "analyzer_script_missing", "path": str(SCRIPTS)}

    # Import dinámico para evitar duplicar lógica
    sys.path.insert(0, str(BASE_DIR / "scripts"))
    try:
        import analyze_trading_data as mod  # type: ignore

        report = mod.run_analysis(
            BASE_DIR / results_path.replace("\\", "/"),
            BASE_DIR / jsonl_path.replace("\\", "/"),
        )
    except Exception as e:
        return {"ok": False, "error": "analyze_failed", "message": str(e)}
    finally:
        if str(BASE_DIR / "scripts") in sys.path:
            sys.path.remove(str(BASE_DIR / "scripts"))

    text = json.dumps(report, ensure_ascii=False)
    truncated = len(text) > max_output_chars
    if truncated:
        # Resumen compacto si el reporte excede límite de agente
        compact = {
            "ok": True,
            "truncated": True,
            "hint": "Usa --out en scripts/analyze_trading_data.py para reporte completo en disco",
            "results_summary": {
                "metadata": report.get("results", {}).get("metadata"),
                "verified_math": report.get("results", {}).get("verified_math"),
                "admin_checks": report.get("results", {}).get("admin_checks"),
                "patterns": report.get("results", {}).get("patterns"),
                "by_symbol": report.get("results", {}).get("by_symbol"),
            },
            "metrics_summary": {
                sym: {
                    "records": v.get("records"),
                    "decisions": v.get("decisions"),
                    "combined_formula_audit": v.get("combined_formula_audit"),
                    "indicators_combined": (v.get("indicators") or {}).get("combined"),
                }
                for sym, v in (report.get("metrics_snapshots", {}).get("symbols") or {}).items()
            },
            "cross_file": report.get("cross_file"),
        }
        return {"ok": True, "report": compact, "full_size_chars": len(text)}

    return {"ok": True, "report": report, "truncated": False}