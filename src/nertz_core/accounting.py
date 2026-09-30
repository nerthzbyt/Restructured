"""Contabilidad: PnL con comisiones, balance de wallet y resumen de resultados."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class PnL:
    gross: float
    fees: float
    net: float


def trade_pnl(action: str, entry: float, exit_price: float, qty: float, fee_rate: float) -> PnL:
    """PnL en moneda de cotización con comisión sobre el nocional de entrada y salida.

    (La versión anterior multiplicaba el PnL bruto por ``1 - fee``, lo que
    *reducía* las pérdidas en lugar de sumarles la comisión.)
    """
    entry = float(entry)
    exit_price = float(exit_price)
    qty = float(qty)
    if str(action).lower() == "buy":
        gross = (exit_price - entry) * qty
    else:
        gross = (entry - exit_price) * qty
    fees = float(fee_rate) * (abs(entry * qty) + abs(exit_price * qty))
    return PnL(gross=gross, fees=fees, net=gross - fees)


def executed_entry(trade: Any) -> tuple[float, float]:
    """(precio, cantidad) ejecutados según el exchange, con fallback a lo registrado."""
    entry = float(getattr(trade, "entry_price", 0.0) or 0.0)
    qty = float(getattr(trade, "quantity", 0.0) or 0.0)
    raw = getattr(trade, "bybit_raw", None)
    if isinstance(raw, dict):
        info = raw.get("order_realtime") or raw.get("order_history") or {}
        if isinstance(info, dict):
            avg = _f(info.get("avgPrice"))
            cum = _f(info.get("cumExecQty"))
            if avg > 0:
                entry = avg
            if cum > 0:
                qty = cum
    return entry, qty


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------- wallet
def parse_wallet_balance(payload: Mapping[str, Any], coin: Optional[str] = None) -> Dict[str, Any]:
    ret_code = payload.get("retCode")
    if ret_code not in (0, "0"):
        return {
            "total_equity": 0.0,
            "available_balance": 0.0,
            "valid": False,
            "ret_code": ret_code,
            "ret_msg": payload.get("retMsg"),
        }

    result = payload.get("result") or {}
    lst = result.get("list") or []
    row = lst[0] if isinstance(lst, list) and lst else {}

    total_equity = _f(row.get("totalEquity") or row.get("totalWalletBalance") or 0.0)
    available = _f(row.get("totalAvailableBalance") or row.get("totalAvailableToWithdraw") or 0.0)

    coin_key = str(coin or "").strip().upper()
    if coin_key and (total_equity <= 0.0 or available <= 0.0):
        for coin_row in row.get("coin") or []:
            if not isinstance(coin_row, dict) or str(coin_row.get("coin") or "").strip().upper() != coin_key:
                continue
            if total_equity <= 0.0:
                total_equity = _f(
                    coin_row.get("equity") or coin_row.get("walletBalance") or coin_row.get("usdValue") or 0.0
                )
            if available <= 0.0:
                available = _f(
                    coin_row.get("availableToWithdraw")
                    or coin_row.get("availableBalance")
                    or coin_row.get("free")
                    or 0.0
                )
            break

    return {
        "total_equity": total_equity,
        "available_balance": available,
        "valid": total_equity > 0.0 or available > 0.0,
        "ret_code": ret_code,
        "ret_msg": payload.get("retMsg"),
    }


def first_live_balance_equity(events: Any) -> Optional[float]:
    if not isinstance(events, list):
        return None
    for ev in events:
        if not isinstance(ev, dict) or ev.get("type") != "balance":
            continue
        if str(ev.get("mode") or "").lower() in {"disabled", "simulated"}:
            continue
        if ev.get("retCode") not in (None, 0, "0"):
            continue
        te = _f(ev.get("total_equity"))
        if te > 0.0:
            return te
    return None


def resolve_capital_inicial(
    prev_initial: Any,
    prev_source: Any,
    capital_source: str,
    capital_actual: float,
    configured_capital: float,
) -> float:
    capital_actual_f = _f(capital_actual)
    prev_initial_f = _f(prev_initial)
    prev_src = str(prev_source or "").strip().lower()
    cfg_capital = _f(configured_capital)

    if capital_source == "bybit_wallet_balance" and capital_actual_f > 0:
        if prev_initial_f > 0 and prev_src == "bybit_wallet_balance":
            return prev_initial_f
        # Un capital inicial igual al simulado de config no es un baseline real.
        if prev_initial_f > 0 and cfg_capital > 0 and abs(prev_initial_f - cfg_capital) < 1e-6:
            return capital_actual_f
        if prev_initial_f > 0:
            return prev_initial_f
        return capital_actual_f

    if cfg_capital > 0:
        return cfg_capital
    if prev_initial_f > 0:
        return prev_initial_f
    return 0.0


@dataclass(frozen=True)
class CapitalView:
    capital_inicial: float
    capital_actual: float
    capital_source: str
    capital_pnl: float


def capital_view(
    *,
    previous_results: Mapping[str, Any],
    latest_balance: Any,
    engine_capital: float,
    configured_capital: float,
) -> CapitalView:
    prev_meta = previous_results.get("metadata") or {}
    prev_initial = prev_meta.get("capital_inicial")
    prev_source = prev_meta.get("capital_source")

    capital_source = "simulated"
    capital_actual = float(engine_capital)
    if latest_balance is not None and float(getattr(latest_balance, "total_equity", 0.0) or 0.0) > 0:
        capital_source = "bybit_wallet_balance"
        capital_actual = float(latest_balance.total_equity)

    if capital_source == "bybit_wallet_balance" and (
        not prev_initial or str(prev_source or "").lower() in {"simulated", "disabled", ""}
    ):
        baseline = first_live_balance_equity(previous_results.get("events"))
        if baseline and baseline > 0:
            prev_initial = baseline
            prev_source = "bybit_wallet_balance"

    inicial = resolve_capital_inicial(prev_initial, prev_source, capital_source, capital_actual, configured_capital)
    return CapitalView(inicial, capital_actual, capital_source, capital_actual - inicial)


# ---------------------------------------------------------------- summary
def pnl_summary(trades: Iterable[Any], symbols: Iterable[str], precision: int = 6) -> Dict[str, Any]:
    """Resumen de trades finalizados (global y por símbolo). Única fuente para results.json y /profit."""
    finals: List[Any] = [t for t in trades if getattr(t, "outcome_status", None) == "final"]

    def _agg(rows: List[Any]) -> Dict[str, float]:
        profit = sum(float(t.profit_loss or 0.0) for t in rows if float(t.profit_loss or 0.0) > 0)
        loss = sum(float(t.profit_loss or 0.0) for t in rows if float(t.profit_loss or 0.0) < 0)
        wins = sum(1 for t in rows if float(t.profit_loss or 0.0) > 0)
        n = len(rows)
        return {
            "profit": profit,
            "loss": loss,
            "net": profit + loss,
            "count": n,
            "win_rate": (wins / n) * 100 if n else 0.0,
        }

    total = _agg(finals)
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        a = _agg([t for t in finals if t.symbol == sym])
        by_symbol[sym] = {
            "profit": round(a["profit"], precision),
            "loss": round(a["loss"], precision),
            "net_profit": round(a["net"], precision),
            "trade_count": a["count"],
        }
    return {
        "total_profit": round(total["profit"], precision),
        "total_loss": round(total["loss"], precision),
        "net_profit": round(total["net"], precision),
        "total_trades": total["count"],
        "win_rate": round(total["win_rate"], 2),
        "avg_profit_per_trade": round(total["net"] / total["count"], precision) if total["count"] else 0.0,
        "by_symbol": by_symbol,
    }
