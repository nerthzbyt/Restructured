"""Historia columnar de métricas crudas para z-scores en ventana temporal.

Sustituye a la lista de dicts que se copiaba entera en cada ciclo: aquí cada
métrica vive en su propio ``deque`` y el cálculo de media/desviación se hace
con numpy sobre la columna, sin reconstruir diccionarios.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Any, Deque, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

import numpy as np

# Claves crudas que alimentan los z-scores de calculate_metrics.
RAW_KEYS: Tuple[str, ...] = (
    "pio",
    "ild",
    "egm",
    "rol",
    "ogm",
    "mom_raw",
    "tfi_raw",
    "asymmetry",
    "spread_pct",
)

# Mapeo métrica calculada -> clave cruda en la historia.
METRIC_TO_RAW: Dict[str, str] = {
    "pio_raw": "pio",
    "ild_raw": "ild",
    "egm_raw": "egm",
    "rol_raw": "rol",
    "ogm_raw": "ogm",
    "mom_raw": "mom_raw",
    "recent_trades_imbalance_qty_pct": "tfi_raw",
    "asymmetry": "asymmetry",
    "spread_pct": "spread_pct",
}


def raw_sample_from_metrics(metrics: Mapping[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for src, dst in METRIC_TO_RAW.items():
        v = metrics.get(src)
        try:
            fv = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            fv = 0.0
        out[dst] = fv if math.isfinite(fv) else 0.0
    return out


class MetricHistory:
    """Ventana deslizante por tiempo, almacenada por columnas."""

    __slots__ = ("_ts", "_cols")

    def __init__(self, rows: Optional[Iterable[Mapping[str, Any]]] = None) -> None:
        self._ts: Deque[float] = deque()
        self._cols: Dict[str, Deque[float]] = {k: deque() for k in RAW_KEYS}
        for row in rows or ():
            self.append(float(row.get("ts", 0.0) or 0.0), row)

    def __len__(self) -> int:
        return len(self._ts)

    def __bool__(self) -> bool:
        return bool(self._ts)

    def __iter__(self) -> Iterator[Dict[str, float]]:
        """Compatibilidad: iterar devuelve filas dict (incluye ``ts``)."""
        keys = list(self._cols)
        cols = [self._cols[k] for k in keys]
        for i, ts in enumerate(self._ts):
            row = {"ts": ts}
            for k, col in zip(keys, cols):
                row[k] = col[i]
            yield row

    def clear(self) -> None:
        self._ts.clear()
        for col in self._cols.values():
            col.clear()

    def append(self, ts: float, sample: Mapping[str, Any]) -> None:
        self._ts.append(float(ts))
        for k, col in self._cols.items():
            v = sample.get(k)
            try:
                fv = float(v) if v is not None else math.nan
            except (TypeError, ValueError):
                fv = math.nan
            col.append(fv)

    def evict_older_than(self, cutoff_ts: float) -> None:
        ts = self._ts
        cols = list(self._cols.values())
        while ts and ts[0] < cutoff_ts:
            ts.popleft()
            for col in cols:
                col.popleft()

    def column(self, key: str) -> np.ndarray:
        col = self._cols.get(key)
        if not col:
            return np.empty(0, dtype=np.float64)
        arr = np.fromiter(col, dtype=np.float64, count=len(col))
        return arr[np.isfinite(arr)]

    def tail(self, key: str, n: int) -> List[float]:
        col = self._cols.get(key)
        if not col:
            return []
        start = max(0, len(col) - int(n))
        return [v for i, v in enumerate(col) if i >= start and math.isfinite(v)]

    def last_ts(self) -> Optional[float]:
        return self._ts[-1] if self._ts else None


def z_score(window: np.ndarray, current: float, *, min_count: int = 5) -> float:
    """z de ``current`` contra la ventana (misma semántica que WelfordState.z_from_window)."""
    try:
        cur = float(current)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(cur):
        return 0.0
    count = int(window.size)
    if count < 2:
        return 0.0
    mean = float(window.mean())
    sd = float(math.sqrt(float(((window - mean) ** 2).sum()) / count))
    if sd <= 1e-12:
        return 0.0
    z = (cur - mean) / sd
    min_c = int(min_count) if int(min_count) > 0 else 5
    if count < min_c:
        z *= float(count) / float(min_c)
    return float(z)
