"""Reglas de instrumento, cuantización Decimal y plan de orden (tamaño, precio, TP/SL).

Todo se deriva de las reglas reales del exchange (tick, lot, mínimos): no hay
precisiones ni tamaños fijos pensados para un par concreto.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def format_decimal(value: Decimal) -> str:
    s = format(value, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def quantize_to_step(value: float, step: Optional[float], rounding: str) -> Decimal:
    dv = to_decimal(value)
    ds = to_decimal(step)
    if not ds.is_finite() or ds <= 0:
        return dv
    return (dv / ds).to_integral_value(rounding=rounding) * ds


@dataclass(frozen=True)
class InstrumentRules:
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional: float
    raw: Dict[str, Any] = field(default_factory=dict, compare=False)

    @classmethod
    def from_bybit(cls, row: Mapping[str, Any]) -> "InstrumentRules":
        price_filter = row.get("priceFilter") or {}
        lot = row.get("lotSizeFilter") or {}

        def _f(*vals: Any) -> float:
            for v in vals:
                if v is None or v == "":
                    continue
                try:
                    return float(v)
                except (TypeError, ValueError):
                    continue
            return 0.0

        return cls(
            tick_size=_f(price_filter.get("tickSize")),
            qty_step=_f(lot.get("qtyStep"), lot.get("basePrecision")),
            min_qty=_f(lot.get("minOrderQty")),
            min_notional=_f(lot.get("minNotionalValue"), lot.get("minOrderAmt")),
            raw=dict(row),
        )

    def is_valid(self) -> bool:
        return self.tick_size > 0 and self.qty_step > 0

    def as_dict(self) -> Dict[str, float]:
        return {
            "tick_size": self.tick_size,
            "qty_step": self.qty_step,
            "min_qty": self.min_qty,
            "min_notional": self.min_notional,
        }

    # compat: el motor antiguo usaba dicts
    def get(self, key: str, default: Any = None) -> Any:
        return self.as_dict().get(key, default)

    def __getitem__(self, key: str) -> float:
        return self.as_dict()[key]

    def price(self, value: float, rounding: str = ROUND_HALF_UP) -> Decimal:
        return quantize_to_step(value, self.tick_size, rounding)

    def qty(self, value: float, rounding: str = ROUND_DOWN) -> Decimal:
        return quantize_to_step(value, self.qty_step, rounding)


@dataclass(frozen=True)
class SizingInputs:
    capital: float
    risk_factor: float
    volatility: float
    last_price: float
    entry_price: float
    max_position_notional_pct: float
    min_notional_buffer: float
    max_trade_size: float = 0.0
    min_trade_size: float = 0.0


@dataclass(frozen=True)
class SizingResult:
    ok: bool
    quantity: Decimal = Decimal("0")
    reason: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return float(self.detail.get("notional", 0.0))


def size_order(inp: SizingInputs, rules: InstrumentRules) -> SizingResult:
    """Tamaño = riesgo / (volatilidad × precio), acotado por nocional máximo y mínimos del exchange."""
    price = float(inp.entry_price)
    if price <= 0:
        return SizingResult(False, reason="precio_invalido")
    capital = max(0.0, float(inp.capital))
    min_notional = max(0.0, float(rules.min_notional))

    risk_budget = max(capital * float(inp.risk_factor), min_notional * float(inp.min_notional_buffer))
    qty = risk_budget / (float(inp.volatility) * price)

    caps = {"risk_qty": qty}
    if inp.max_position_notional_pct > 0 and capital > 0:
        cap_qty = capital * float(inp.max_position_notional_pct) / price
        caps["notional_cap_qty"] = cap_qty
        qty = min(qty, cap_qty)
    if inp.max_trade_size > 0:
        caps["max_trade_size"] = float(inp.max_trade_size)
        qty = min(qty, float(inp.max_trade_size))

    floor_qty = max(float(rules.min_qty), float(inp.min_trade_size))
    qty_dec = rules.qty(qty, ROUND_DOWN)
    min_qty_dec = rules.qty(floor_qty, ROUND_UP) if floor_qty > 0 else Decimal("0")
    if qty_dec < min_qty_dec:
        qty_dec = min_qty_dec
    if min_notional > 0 and float(qty_dec) * price < min_notional:
        qty_dec = rules.qty(float(to_decimal(min_notional) / to_decimal(price)), ROUND_UP)

    notional = float(qty_dec) * price
    detail = {**caps, "notional": notional, "min_notional": min_notional, "risk_budget": risk_budget}
    if qty_dec <= 0:
        return SizingResult(False, qty_dec, "cantidad_cero", detail)
    if 0 < capital < notional:
        return SizingResult(False, qty_dec, "capital_insuficiente_para_minimo_exchange", detail)
    return SizingResult(True, qty_dec, "", detail)


def protective_levels(
    action: str,
    entry: float,
    volatility: float,
    tp_mult: float,
    sl_mult: float,
    rules: InstrumentRules,
) -> tuple[Decimal, Decimal]:
    """TP/SL por volatilidad, cuantizados al tick y garantizados al lado correcto de la entrada."""
    rng = float(volatility) * float(entry)
    if action == "buy":
        tp, sl = entry + rng * tp_mult, entry - rng * sl_mult
    else:
        tp, sl = entry - rng * tp_mult, entry + rng * sl_mult
    tp_dec = rules.price(tp)
    sl_dec = rules.price(sl)
    entry_dec = to_decimal(entry)
    tick = to_decimal(rules.tick_size)
    if action == "buy":
        if tp_dec <= entry_dec:
            tp_dec = entry_dec + tick
        if sl_dec >= entry_dec:
            sl_dec = entry_dec - tick
    else:
        if tp_dec >= entry_dec:
            tp_dec = entry_dec - tick
        if sl_dec <= entry_dec:
            sl_dec = entry_dec + tick
    return tp_dec, sl_dec


def price_decimals(rules: Optional[InstrumentRules]) -> int:
    """Decimales de precio para logs, derivados del tick."""
    if rules is None or rules.tick_size <= 0:
        return 8
    exp = to_decimal(rules.tick_size).normalize().as_tuple().exponent
    return max(0, -int(exp)) if isinstance(exp, int) else 8
