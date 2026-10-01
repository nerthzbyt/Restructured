"""Modelo ML dev: regresión logística numpy (misma familia que Nertzh.py)."""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

FEATURE_KEYS = (
    "combined",
    "combined_z",
    "pio",
    "egm",
    "ild",
    "rol",
    "ogm",
    "mom",
    "tfi",
    "volatility",
    "spread_bps",
    "rvol",
    "horizon_combined_mean",
    "horizon_combined_std",
    "horizon_level_L3_count",
    "horizon_level_L4_count",
)


def _sigmoid(z: np.ndarray) -> np.ndarray:
    zc = np.clip(z, -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-zc))


class DevMLModel:
    def __init__(self, *, feature_keys: Sequence[str] = FEATURE_KEYS):
        self.feature_keys = tuple(feature_keys)
        self.mu: Optional[np.ndarray] = None
        self.sigma: Optional[np.ndarray] = None
        self.w: Optional[np.ndarray] = None
        self.samples = 0
        self.accuracy_train = 0.0
        self.trained_at: Optional[str] = None

    @staticmethod
    def features_from_observation(obs: Dict[str, Any]) -> np.ndarray:
        prod = obs.get("production") or {}
        metrics = prod.get("metrics") or {}
        horizons = obs.get("horizons") or {}

        combined_vals: List[float] = []
        l3 = 0
        l4 = 0
        for row in horizons.values():
            if not isinstance(row, dict):
                continue
            try:
                combined_vals.append(float(row.get("combined") or 0.0))
            except (TypeError, ValueError):
                pass
            lvl = str(row.get("level") or "")
            if lvl == "L3":
                l3 += 1
            elif lvl == "L4":
                l4 += 1

        h_mean = float(np.mean(combined_vals)) if combined_vals else 0.0
        h_std = float(np.std(combined_vals)) if len(combined_vals) > 1 else 0.0

        raw = {
            "combined": metrics.get("combined"),
            "combined_z": metrics.get("combined_z"),
            "pio": metrics.get("pio"),
            "egm": metrics.get("egm"),
            "ild": metrics.get("ild"),
            "rol": metrics.get("rol"),
            "ogm": metrics.get("ogm"),
            "mom": metrics.get("mom"),
            "tfi": metrics.get("tfi"),
            "volatility": metrics.get("volatility"),
            "spread_bps": metrics.get("spread_bps"),
            "rvol": metrics.get("rvol"),
            "horizon_combined_mean": h_mean,
            "horizon_combined_std": h_std,
            "horizon_level_L3_count": float(l3),
            "horizon_level_L4_count": float(l4),
        }
        vec = []
        for k in FEATURE_KEYS:
            try:
                vec.append(float(raw.get(k) or 0.0))
            except (TypeError, ValueError):
                vec.append(0.0)
        return np.array(vec, dtype=np.float64)

    def predict_proba(self, obs: Dict[str, Any]) -> Optional[float]:
        if self.mu is None or self.sigma is None or self.w is None:
            return None
        x = self.features_from_observation(obs)
        if x.size != self.mu.size:
            return None
        xn = (x - self.mu) / np.where(self.sigma > 1e-9, self.sigma, 1.0)
        xb = np.concatenate([np.ones((1,), dtype=np.float64), xn], axis=0)
        if xb.size != self.w.size:
            return None
        p = float(_sigmoid(xb @ self.w))
        return p if math.isfinite(p) else None

    def train_from_labeled_observations(
        self,
        rows: List[Dict[str, Any]],
        *,
        min_samples: int = 20,
        epochs: int = 300,
        lr: float = 0.12,
        l2: float = 0.02,
    ) -> Dict[str, Any]:
        feats: List[np.ndarray] = []
        labels: List[float] = []
        for row in rows:
            label = row.get("forward_label")
            if label not in (0, 1, 0.0, 1.0):
                continue
            x = self.features_from_observation(row)
            if not np.all(np.isfinite(x)):
                continue
            feats.append(x)
            labels.append(float(label))

        if len(feats) < int(min_samples):
            return {
                "success": False,
                "message": "insufficient_labeled_samples",
                "samples": len(feats),
                "min_samples": int(min_samples),
            }

        X = np.vstack(feats)
        yv = np.array(labels, dtype=np.float64)
        mu = X.mean(axis=0)
        sigma = X.std(axis=0)
        sigma = np.where(sigma > 1e-9, sigma, 1.0)
        Xn = (X - mu) / sigma
        Xb = np.concatenate([np.ones((Xn.shape[0], 1), dtype=np.float64), Xn], axis=1)

        w = np.zeros((Xb.shape[1],), dtype=np.float64)
        n = float(Xb.shape[0])
        for _ in range(int(max(10, epochs))):
            p = _sigmoid(Xb @ w)
            grad = (Xb.T @ (p - yv)) / n
            grad[1:] = grad[1:] + float(l2) * w[1:]
            w = w - float(lr) * grad

        p_final = _sigmoid(Xb @ w)
        pred = (p_final >= 0.5).astype(np.float64)
        acc = float((pred == yv).mean()) if yv.size else 0.0

        self.mu = mu
        self.sigma = sigma
        self.w = w
        self.samples = int(Xb.shape[0])
        self.accuracy_train = acc
        self.trained_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        return {
            "success": True,
            "samples": self.samples,
            "accuracy_train": acc,
            "trained_at": self.trained_at,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "feature_keys": list(self.feature_keys),
            "mu": self.mu.tolist() if self.mu is not None else [],
            "sigma": self.sigma.tolist() if self.sigma is not None else [],
            "w": self.w.tolist() if self.w is not None else [],
            "samples": self.samples,
            "accuracy_train": self.accuracy_train,
            "trained_at": self.trained_at,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def load(self, path: Path) -> bool:
        if not path.is_file():
            return False
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.mu = np.array(payload.get("mu") or [], dtype=np.float64)
            self.sigma = np.array(payload.get("sigma") or [], dtype=np.float64)
            self.w = np.array(payload.get("w") or [], dtype=np.float64)
            self.samples = int(payload.get("samples") or 0)
            self.accuracy_train = float(payload.get("accuracy_train") or 0.0)
            self.trained_at = payload.get("trained_at")
            return self.mu.size > 0 and self.w.size > 0
        except Exception:
            return False