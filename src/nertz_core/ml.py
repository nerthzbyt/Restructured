"""Filtro ML: regresión logística ligera (numpy) sobre trades finalizados."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np

FEATURE_NAMES: List[str] = ["action_sign", "combined", "ild", "egm", "rol", "pio", "ogm", "risk_reward_ratio"]


def sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -50.0, 50.0)))


def action_sign(action: str) -> float:
    a = (action or "").lower()
    return 1.0 if a == "buy" else (-1.0 if a == "sell" else 0.0)


def _num(v: Any) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f


def features_from_metrics(action: str, metrics: Mapping[str, Any], default_rr: float) -> np.ndarray:
    rr = metrics.get("risk_reward_ratio")
    return np.array(
        [
            action_sign(action),
            _num(metrics.get("combined")),
            _num(metrics.get("ild")),
            _num(metrics.get("egm")),
            _num(metrics.get("rol")),
            _num(metrics.get("pio")),
            _num(metrics.get("ogm")),
            _num(rr) if rr is not None else float(default_rr),
        ],
        dtype=np.float64,
    )


def features_from_trade(trade: Any) -> np.ndarray:
    return np.array(
        [
            action_sign(getattr(trade, "action", "")),
            _num(getattr(trade, "combined", 0.0)),
            _num(getattr(trade, "ild", 0.0)),
            _num(getattr(trade, "egm", 0.0)),
            _num(getattr(trade, "rol", 0.0)),
            _num(getattr(trade, "pio", 0.0)),
            _num(getattr(trade, "ogm", 0.0)),
            _num(getattr(trade, "risk_reward_ratio", 0.0)),
        ],
        dtype=np.float64,
    )


def train_logistic(
    trades: Sequence[Any],
    *,
    min_samples: int,
    epochs: int,
    lr: float,
    l2: float,
) -> Dict[str, Any]:
    feats: List[np.ndarray] = []
    labels: List[float] = []
    for t in trades:
        x = features_from_trade(t)
        if not np.all(np.isfinite(x)):
            continue
        feats.append(x)
        labels.append(1.0 if _num(getattr(t, "profit_loss", 0.0)) > 0 else 0.0)

    if len(feats) < int(min_samples):
        return {"success": False, "message": "insufficient_clean_samples", "samples": len(feats)}

    X = np.vstack(feats)
    y = np.array(labels, dtype=np.float64)
    mu = X.mean(axis=0)
    sigma = X.std(axis=0)
    sigma = np.where(sigma > 1e-9, sigma, 1.0)
    Xb = np.concatenate([np.ones((X.shape[0], 1)), (X - mu) / sigma], axis=1)

    w = np.zeros(Xb.shape[1], dtype=np.float64)
    n = float(Xb.shape[0])
    for _ in range(int(max(10, epochs))):
        grad = (Xb.T @ (sigmoid(Xb @ w) - y)) / n
        grad[1:] += float(l2) * w[1:]
        w -= float(lr) * grad

    acc = float(((sigmoid(Xb @ w) >= 0.5).astype(np.float64) == y).mean()) if y.size else 0.0
    return {
        "success": True,
        "model": {
            "features": list(FEATURE_NAMES),
            "mu": mu.tolist(),
            "sigma": sigma.tolist(),
            "w": w.tolist(),
            "samples": int(Xb.shape[0]),
            "accuracy_train": acc,
            "trained_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def predict_proba(model: Optional[Mapping[str, Any]], x: np.ndarray) -> Optional[float]:
    if not isinstance(model, Mapping):
        return None
    try:
        mu = np.array(model.get("mu") or [], dtype=np.float64)
        sigma = np.array(model.get("sigma") or [], dtype=np.float64)
        w = np.array(model.get("w") or [], dtype=np.float64)
        if mu.size == 0 or x.size != mu.size or w.size != mu.size + 1:
            return None
        xn = (x - mu) / np.where(sigma > 1e-9, sigma, 1.0)
        p = float(sigmoid(np.concatenate([[1.0], xn]) @ w))
        return p if np.isfinite(p) else None
    except Exception:
        return None
