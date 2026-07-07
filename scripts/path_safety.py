"""Validación de rutas bajo el root del proyecto (mitiga path injection Sonar)."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_ALLOWED_ROOTS = (
    PROJECT_ROOT,
    PROJECT_ROOT / "logs",
    PROJECT_ROOT / "data",
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "src_dev" / "output",
    PROJECT_ROOT / "nertz_engine",
    PROJECT_ROOT / "scripts",
)


def safe_path_under_project(path: Path | str, *, must_exist: bool = False) -> Path:
    raw = Path(path).expanduser()
    candidate = (PROJECT_ROOT / raw).resolve() if not raw.is_absolute() else raw.resolve()
    for root in _ALLOWED_ROOTS:
        try:
            candidate.relative_to(root.resolve())
            if must_exist and not candidate.exists():
                raise ValueError(f"path does not exist: {candidate}")
            return candidate
        except ValueError:
            continue
    raise ValueError(f"path outside allowed project directories: {path}")