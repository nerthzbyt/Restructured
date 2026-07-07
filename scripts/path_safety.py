"""Validación de rutas bajo el root del proyecto (mitiga path injection Sonar)."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, TextIO

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_ALLOWED_ROOTS = (
    PROJECT_ROOT,
    PROJECT_ROOT / "logs",
    PROJECT_ROOT / "data",
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "src_dev",
    PROJECT_ROOT / "src_dev" / "output",
    PROJECT_ROOT / "nertz_engine",
    PROJECT_ROOT / "NerT_AI_PRO",
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


def safe_read_text(path: Path | str, *, encoding: str = "utf-8", must_exist: bool = True) -> str:
    safe = safe_path_under_project(path, must_exist=must_exist)
    return safe.read_text(encoding=encoding)


def safe_write_text(path: Path | str, text: str, *, encoding: str = "utf-8") -> Path:
    safe = safe_path_under_project(path)
    safe.parent.mkdir(parents=True, exist_ok=True)
    safe.write_text(text, encoding=encoding)
    return safe


def safe_open(
    path: Path | str,
    mode: str = "r",
    *,
    encoding: str | None = "utf-8",
    must_exist: bool | None = None,
) -> TextIO:
    read_only = "r" in mode and "+" not in mode and "w" not in mode and "a" not in mode
    exists = must_exist if must_exist is not None else read_only
    safe = safe_path_under_project(path, must_exist=exists)
    if "a" in mode or "w" in mode:
        safe.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {}
    if encoding is not None and "b" not in mode:
        kwargs["encoding"] = encoding
    return safe.open(mode, **kwargs)


def safe_lines(path: Path | str, *, encoding: str = "utf-8") -> Iterator[str]:
    with safe_open(path, "r", encoding=encoding, must_exist=True) as handle:
        yield from handle