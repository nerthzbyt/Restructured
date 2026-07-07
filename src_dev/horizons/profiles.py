"""Perfiles de horizonte desde env — sin números mágicos en código."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from itertools import product
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src_dev.config import DevSettings


def _parse_int_list(raw: str, default: List[int]) -> List[int]:
    text = str(raw or "").strip()
    if not text:
        return list(default)
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [int(x) for x in parsed]
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return [int(x.strip()) for x in text.split(",") if x.strip()]


def _parse_float_list(raw: str, default: List[float]) -> List[float]:
    text = str(raw or "").strip()
    if not text:
        return list(default)
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [float(x) for x in parsed]
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return [float(x.strip()) for x in text.split(",") if x.strip()]


@dataclass(frozen=True)
class HorizonProfile:
    name: str
    orderbook_depth: int
    tfi_window: int
    candle_limit: int
    history_window_min: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "orderbook_depth": self.orderbook_depth,
            "tfi_window": self.tfi_window,
            "candle_limit": self.candle_limit,
            "history_window_min": self.history_window_min,
        }


def load_horizon_profiles(settings: Optional["DevSettings"] = None) -> List[HorizonProfile]:
    """
    Carga perfiles desde DEV_HORIZON_PROFILES (JSON array) o producto cartesiano
    de DEV_HORIZON_DEPTHS × TFI_WINDOWS × CANDLE_LIMITS × HISTORY_MINS.
    """
    custom = str(os.getenv("DEV_HORIZON_PROFILES", "") or "").strip()
    if custom:
        try:
            rows = json.loads(custom)
            if isinstance(rows, list) and rows:
                out: List[HorizonProfile] = []
                for i, row in enumerate(rows):
                    if not isinstance(row, dict):
                        continue
                    out.append(
                        HorizonProfile(
                            name=str(row.get("name") or f"custom_{i}"),
                            orderbook_depth=int(row.get("orderbook_depth") or 50),
                            tfi_window=int(row.get("tfi_window") or 10),
                            candle_limit=int(row.get("candle_limit") or 50),
                            history_window_min=float(row.get("history_window_min") or 15.0),
                        )
                    )
                if out:
                    return out
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    depths = _parse_int_list(
        os.getenv("DEV_HORIZON_DEPTHS", ""),
        [int(getattr(settings, "orderbook_depth", 50) or 50)],
    )
    tfi_windows = _parse_int_list(os.getenv("DEV_HORIZON_TFI_WINDOWS", ""), [5, 10, 20])
    candle_limits = _parse_int_list(os.getenv("DEV_HORIZON_CANDLE_LIMITS", ""), [21, 50])
    history_mins = _parse_float_list(
        os.getenv("DEV_HORIZON_HISTORY_MINS", ""),
        [float(getattr(settings, "metrics_window_minutes", 15.0) or 15.0)],
    )

    profiles: List[HorizonProfile] = []
    for d, tw, cl, hm in product(depths, tfi_windows, candle_limits, history_mins):
        profiles.append(
            HorizonProfile(
                name=f"d{d}_tfi{tw}_c{cl}_h{hm:g}",
                orderbook_depth=max(1, min(int(d), 50)),
                tfi_window=max(1, int(tw)),
                candle_limit=max(2, int(cl)),
                history_window_min=max(1.0, float(hm)),
            )
        )
    return profiles