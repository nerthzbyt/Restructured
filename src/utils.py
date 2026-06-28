import asyncio
import itertools
import hashlib
import hmac
import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque

import numpy as np

logger = logging.getLogger("NertzMetalEngine")
_RESULTS_JSON_LOCK = threading.Lock()
_JSONL_LOCK = threading.Lock()


# === TSM Formula Parser ===

# NOTA: Este ajuste es previo al análisis anterior (trades 1-68 y trades 77-90) y corrige problemas como el sesgo en 'egm' y la ineficiencia de operaciones cortas, optimizando para tiempo real.


@dataclass(frozen=True)
class _TSMToken:
    t: str
    v: Any


class _TSMFormulaError(Exception):
    pass


def _tsm_is_valid_number(x: Any) -> bool:
    if x is None:
        return False
    try:
        v = float(x)
    except Exception:
        return False
    return math.isfinite(v)


def _tsm_to_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def _tsm_round_half_away_from_zero(x: float) -> float:
    if x >= 0:
        return float(math.floor(x + 0.5))
    return float(-math.floor(abs(x) + 0.5))


_TSM_NUMBER_RE = re.compile(r"(?:(?:\d+\.\d+)|(?:\d+\.?)|(?:\.\d+))(?:[eE][+-]?\d+)?")
_TSM_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_:\.]*")


def _tsm_tokenize(s: str) -> List[_TSMToken]:
    if not isinstance(s, str):
        raise _TSMFormulaError("formula_not_string")
    i = 0
    n = len(s)
    out: List[_TSMToken] = []
    while i < n:
        ch = s[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "+-*/":
            out.append(_TSMToken("op", ch))
            i += 1
            continue
        if ch == "(":
            out.append(_TSMToken("lparen", ch))
            i += 1
            continue
        if ch == ")":
            out.append(_TSMToken("rparen", ch))
            i += 1
            continue
        if ch == ",":
            out.append(_TSMToken("comma", ch))
            i += 1
            continue
        mnum = _TSM_NUMBER_RE.match(s, i)
        if mnum:
            out.append(_TSMToken("num", float(mnum.group(0))))
            i = mnum.end()
            continue
        mid = _TSM_IDENT_RE.match(s, i)
        if mid:
            out.append(_TSMToken("ident", mid.group(0)))
            i = mid.end()
            continue
        raise _TSMFormulaError(f"unexpected_char:{ch}")
    out.append(_TSMToken("eof", None))
    return out


@dataclass(frozen=True)
class _TSMAst:
    k: str
    v: Any = None
    a: Any = None
    b: Any = None


class _TSMParser:
    def __init__(self, tokens: List[_TSMToken]):
        self._toks = tokens
        self._i = 0

    def _peek(self) -> _TSMToken:
        return self._toks[self._i]

    def _pop(self) -> _TSMToken:
        t = self._toks[self._i]
        self._i += 1
        return t

    def _expect(self, ttype: str) -> _TSMToken:
        t = self._peek()
        if t.t != ttype:
            raise _TSMFormulaError(f"expected:{ttype}")
        return self._pop()

    def parse(self) -> _TSMAst:
        expr = self._expr(0)
        if self._peek().t != "eof":
            raise _TSMFormulaError("trailing_tokens")
        return expr

    def _lbp(self, tok: _TSMToken) -> int:
        if tok.t != "op":
            return 0
        if tok.v in ("+", "-"):
            return 10
        if tok.v in ("*", "/"):
            return 20
        return 0

    def _expr(self, rbp: int) -> _TSMAst:
        t = self._pop()
        left = self._nud(t)
        while rbp < self._lbp(self._peek()):
            t2 = self._pop()
            left = self._led(t2, left)
        return left

    def _nud(self, tok: _TSMToken) -> _TSMAst:
        if tok.t == "num":
            return _TSMAst("num", tok.v)
        if tok.t == "ident":
            name = str(tok.v)
            if self._peek().t == "lparen":
                self._pop()
                args: List[_TSMAst] = []
                if self._peek().t != "rparen":
                    while True:
                        args.append(self._expr(0))
                        if self._peek().t == "comma":
                            self._pop()
                            continue
                        break
                self._expect("rparen")
                return _TSMAst("call", name, args)
            return _TSMAst("var", name)
        if tok.t == "op" and tok.v == "-":
            right = self._expr(30)
            return _TSMAst("neg", None, right)
        if tok.t == "lparen":
            e = self._expr(0)
            self._expect("rparen")
            return e
        raise _TSMFormulaError("unexpected_token")

    def _led(self, tok: _TSMToken, left: _TSMAst) -> _TSMAst:
        if tok.t != "op":
            raise _TSMFormulaError("unexpected_led")
        op = str(tok.v)
        rbp = 10 if op in ("+", "-") else 20
        right = self._expr(rbp)
        return _TSMAst("bin", op, left, right)


def compile_tsm_formula(formula: str) -> _TSMAst:
    toks = _tsm_tokenize(formula)
    return _TSMParser(toks).parse()


def _tsm_collect(ast: _TSMAst) -> Tuple[List[str], List[str]]:
    vars_used: List[str] = []
    funcs_used: List[str] = []

    def _walk(n: _TSMAst) -> None:
        if n.k == "var":
            vars_used.append(str(n.v))
            return
        if n.k == "call":
            funcs_used.append(str(n.v).lower())
            for x in n.a or []:
                _walk(x)
            return
        if n.k in {"neg"}:
            _walk(n.a)
            return
        if n.k == "bin":
            _walk(n.a)
            _walk(n.b)
            return

    _walk(ast)
    return sorted(set(vars_used)), sorted(set(funcs_used))


def tsm_formula_features(formula: str) -> Dict[str, Any]:
    ast = compile_tsm_formula(formula)
    vars_used, funcs_used = _tsm_collect(ast)
    return {"variables": vars_used, "functions": funcs_used}


def eval_tsm_formula(
    formula: str,
    context: Dict[str, Any],
    *,
    functions: Optional[Dict[str, Callable[..., Any]]] = None,
) -> Optional[float]:
    ast = compile_tsm_formula(formula)
    return eval_tsm_ast(ast, context, functions=functions)


def eval_tsm_ast(
    ast: _TSMAst,
    context: Dict[str, Any],
    *,
    functions: Optional[Dict[str, Callable[..., Any]]] = None,
) -> Optional[float]:
    ctx = context if isinstance(context, dict) else {}
    fn = functions if isinstance(functions, dict) else {}

    def _fn(name: str) -> Optional[Callable[..., Any]]:
        if name in fn:
            return fn[name]
        key = name.lower()
        if key in fn:
            return fn[key]
        return None

    def _as_args(xs: List[_TSMAst]) -> List[Optional[float]]:
        return [_eval(x) for x in xs]

    def _minmax(xs: List[Optional[float]], is_min: bool) -> Optional[float]:
        vals: List[float] = []
        for x in xs:
            v = _tsm_to_number(x)
            if v is not None:
                vals.append(v)
        if not vals:
            return None
        return float(min(vals) if is_min else max(vals))

    def _first(xs: List[Optional[float]]) -> Optional[float]:
        for x in xs:
            v = _tsm_to_number(x)
            if v is not None:
                return float(v)
        return None

    def _avg(xs: List[Optional[float]]) -> Optional[float]:
        vals: List[float] = []
        for x in xs:
            v = _tsm_to_number(x)
            if v is not None:
                vals.append(v)
        if not vals:
            return None
        return float(sum(vals) / float(len(vals)))

    def _abs(x: Optional[float]) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None:
            return None
        return float(abs(float(xv)))

    def _sign(x: Optional[float]) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None:
            return None
        if xv > 0:
            return 1.0
        if xv < 0:
            return -1.0
        return 0.0

    def _log(x: Optional[float], base: Optional[float]) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None or xv <= 0:
            return None
        bv = _tsm_to_number(base)
        if bv is None:
            return float(math.log(float(xv)))
        if bv <= 0 or abs(float(bv) - 1.0) <= 1e-12:
            return None
        return float(math.log(float(xv), float(bv)))

    def _pow(a: Optional[float], b: Optional[float]) -> Optional[float]:
        av = _tsm_to_number(a)
        bv = _tsm_to_number(b)
        if av is None or bv is None:
            return None
        try:
            v = float(math.pow(float(av), float(bv)))
        except Exception:
            return None
        return v if math.isfinite(v) else None

    def _sqrt(x: Optional[float]) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None or xv < 0:
            return None
        v = float(math.sqrt(float(xv)))
        return v if math.isfinite(v) else None

    def _if(a: Optional[float], b: Optional[float], c: Optional[float]) -> Optional[float]:
        av = _tsm_to_number(a)
        if av is None:
            return None
        if float(av) > 0:
            bv = _tsm_to_number(b)
            return float(bv) if bv is not None else None
        cv = _tsm_to_number(c)
        return float(cv) if cv is not None else None

    def _round_std(x: Optional[float], y: Optional[float]) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None:
            return None
        yv = _tsm_to_number(y)
        if yv is None:
            return float(_tsm_round_half_away_from_zero(float(xv)))
        if yv == 0:
            return None
        scaled = float(xv) / float(yv)
        return float(_tsm_round_half_away_from_zero(scaled) * float(yv))

    def _round_updown(
        x: Optional[float], y: Optional[float], up: bool
    ) -> Optional[float]:
        xv = _tsm_to_number(x)
        if xv is None:
            return None
        yv = _tsm_to_number(y)
        if yv is None:
            return float(math.ceil(float(xv)) if up else math.floor(float(xv)))
        if yv == 0:
            return None
        scaled = float(xv) / float(yv)
        return float((math.ceil(scaled) if up else math.floor(scaled)) * float(yv))

    def _ifcmp(
        a: Optional[float],
        b: Optional[float],
        x: Optional[float],
        y: Optional[float],
        op: str,
    ) -> Optional[float]:
        av = _tsm_to_number(a)
        bv = _tsm_to_number(b)
        if av is None or bv is None:
            yv = _tsm_to_number(y)
            return float(yv) if yv is not None else None
        avf = float(av)
        bvf = float(bv)
        ok = False
        if op == "gt":
            ok = avf > bvf
        elif op == "gte":
            ok = avf >= bvf
        elif op == "lt":
            ok = avf < bvf
        elif op == "lte":
            ok = avf <= bvf
        elif op == "eq":
            ok = avf == bvf
        xv = _tsm_to_number(x)
        yv = _tsm_to_number(y)
        return (
            float(xv)
            if ok and xv is not None
            else (float(yv) if yv is not None else None)
        )

    def _check(
        a: Optional[float], b: Optional[float], c: Optional[float]
    ) -> Optional[float]:
        av = _tsm_to_number(a)
        if av is not None and float(av) > 0:
            bv = _tsm_to_number(b)
            return float(bv) if bv is not None else None
        cv = _tsm_to_number(c)
        return float(cv) if cv is not None else None

    def _convert(p: Optional[float]) -> Optional[float]:
        pv = _tsm_to_number(p)
        if pv is None:
            return None
        conv = ctx.get("_convert")
        if callable(conv):
            try:
                return _tsm_to_number(conv(float(pv)))
            except Exception:
                return float(pv)
        return float(pv)

    def _eval(n: _TSMAst) -> Optional[float]:
        if n.k == "num":
            return _tsm_to_number(n.v)
        if n.k == "var":
            if str(n.v) in ctx:
                return _tsm_to_number(ctx.get(str(n.v)))
            return None
        if n.k == "neg":
            x = _eval(n.a)
            xv = _tsm_to_number(x)
            return None if xv is None else float(-float(xv))
        if n.k == "bin":
            op = str(n.v)
            left = _eval(n.a)
            right = _eval(n.b)
            av = _tsm_to_number(left)
            bv = _tsm_to_number(right)
            if av is None or bv is None:
                return None
            a = float(av)
            b = float(bv)
            if op == "+":
                return a + b
            if op == "-":
                return a - b
            if op == "*":
                return a * b
            if op == "/":
                if abs(b) <= 1e-12:
                    return None
                return a / b
            return None
        if n.k == "call":
            name = str(n.v)
            f = _fn(name)
            if f is not None:
                try:
                    return _tsm_to_number(f(*_as_args(n.a or [])))
                except Exception:
                    return None
            lname = name.lower()
            args = _as_args(n.a or [])
            if lname == "min":
                return _minmax(args, True)
            if lname == "max":
                return _minmax(args, False)
            if lname == "first":
                return _first(args)
            if lname == "avg":
                return _avg(args)
            if lname == "abs":
                x = args[0] if len(args) > 0 else None
                return _abs(x)
            if lname == "sign":
                x = args[0] if len(args) > 0 else None
                return _sign(x)
            if lname == "log":
                x = args[0] if len(args) > 0 else None
                base = args[1] if len(args) > 1 else None
                return _log(x, base)
            if lname == "pow":
                a = args[0] if len(args) > 0 else None
                b = args[1] if len(args) > 1 else None
                return _pow(a, b)
            if lname == "sqrt":
                x = args[0] if len(args) > 0 else None
                return _sqrt(x)
            if lname == "check":
                a = args[0] if len(args) > 0 else None
                b = args[1] if len(args) > 1 else None
                c = args[2] if len(args) > 2 else None
                return _check(a, b, c)
            if lname == "if":
                a = args[0] if len(args) > 0 else None
                b = args[1] if len(args) > 1 else None
                c = args[2] if len(args) > 2 else None
                return _if(a, b, c)
            if lname in {"ifgt", "ifgte", "iflt", "iflte", "ifeq"}:
                a = args[0] if len(args) > 0 else None
                b = args[1] if len(args) > 1 else None
                x = args[2] if len(args) > 2 else None
                y = args[3] if len(args) > 3 else None
                op = lname.replace("if", "")
                return _ifcmp(a, b, x, y, op)
            if lname == "round":
                x = args[0] if len(args) > 0 else None
                y = args[1] if len(args) > 1 else None
                return _round_std(x, y)
            if lname == "roundup":
                x = args[0] if len(args) > 0 else None
                y = args[1] if len(args) > 1 else None
                return _round_updown(x, y, True)
            if lname == "rounddown":
                x = args[0] if len(args) > 0 else None
                y = args[1] if len(args) > 1 else None
                return _round_updown(x, y, False)
            if lname == "convert":
                p = args[0] if len(args) > 0 else None
                return _convert(p)
            return None
        return None

    return _eval(ast)


def eval_tsm_formulas(
    formulas: Dict[str, Any], context: Dict[str, Any]
) -> Dict[str, float]:
    if not isinstance(formulas, dict):
        return {}
    out: Dict[str, float] = {}
    for k, v in formulas.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if not isinstance(v, str) or not v.strip():
            continue
        try:
            res = eval_tsm_formula(v, context)
        except Exception:
            res = None
        vnum = _tsm_to_number(res)
        if vnum is not None:
            out[k] = float(vnum)
    return out


# === Logging & Persistence ===


def append_jsonl_record(file_path: str, record: Dict[str, Any]) -> None:
    file_path = os.path.abspath(str(file_path))
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    payload = record if isinstance(record, dict) else {"value": record}
    if "timestamp" not in payload:
        payload = {**payload, "timestamp": datetime.now(timezone.utc).isoformat()}
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _JSONL_LOCK:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def append_metrics_snapshot(record: Dict[str, Any], data_dir: str) -> str:
    base = os.path.abspath(str(data_dir))
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, "metrics_snapshots.jsonl")
    append_jsonl_record(path, record if isinstance(record, dict) else {"value": record})
    return path


async def append_metrics_snapshot_async(record: Dict[str, Any], data_dir: str) -> str:
    return await asyncio.to_thread(append_metrics_snapshot, record, data_dir)


# === Metrics Calculation ===


def compute_recent_trades_metrics(
    recent_trades: Optional[List[Dict[str, Any]]], *, window_n: int = 10
) -> Dict[str, Any]:
    rows = list(recent_trades or [])
    if not rows:
        return {
            "trade_count": 0,
            "buy_qty": 0.0,
            "sell_qty": 0.0,
            "total_qty": 0.0,
            "buy_notional": 0.0,
            "sell_notional": 0.0,
            "total_notional": 0.0,
            "vwap": 0.0,
            "imbalance_qty_pct": 0.0,
            "last_trade_age_s": None,
        }
    rows = rows[-max(1, int(window_n)) :]
    now_s = time.time()
    buy_qty = 0.0
    sell_qty = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    prices: List[float] = []
    last_ts = None
    for t in rows:
        if not isinstance(t, dict):
            continue
        qty = _safe_float(t.get("qty"), 0.0)
        price = _safe_float(t.get("price"), 0.0)
        side = str(t.get("side") or "").lower()
        ts = t.get("ts")
        try:
            if ts is not None:
                last_ts = float(ts)
        except Exception:
            pass
        if qty <= 0 or price <= 0:
            continue
        prices.append(float(price))
        notion = qty * price
        if side in {"buy", "b"}:
            buy_qty += qty
            buy_notional += notion
        elif side in {"sell", "s"}:
            sell_qty += qty
            sell_notional += notion
        else:
            buy_qty += qty * 0.5
            sell_qty += qty * 0.5
            buy_notional += notion * 0.5
            sell_notional += notion * 0.5

    total_qty = buy_qty + sell_qty
    total_notional = buy_notional + sell_notional
    vwap = (total_notional / total_qty) if total_qty > 0 else 0.0
    imbalance = ((buy_qty - sell_qty) / (total_qty + 1e-12)) if total_qty > 0 else 0.0
    rvol = 0.0
    try:
        if len(prices) >= 3:
            lr: List[float] = []
            for i in range(1, len(prices)):
                p0 = float(prices[i - 1])
                p1 = float(prices[i])
                if p0 > 0 and p1 > 0:
                    lr.append(float(math.log(p1 / p0)))
            if len(lr) >= 2:
                rvol = float(np.std(np.asarray(lr, dtype=np.float64)))
    except Exception:
        rvol = 0.0
    last_age = None
    if last_ts is not None:
        try:
            last_age = max(0.0, float(now_s) - float(last_ts))
        except Exception:
            last_age = None

    return {
        "trade_count": int(len(rows)),
        "buy_qty": float(buy_qty),
        "sell_qty": float(sell_qty),
        "total_qty": float(total_qty),
        "buy_notional": float(buy_notional),
        "sell_notional": float(sell_notional),
        "total_notional": float(total_notional),
        "vwap": float(vwap),
        "imbalance_qty_pct": float(imbalance),
        "rvol": float(rvol),
        "last_trade_age_s": float(last_age)
        if isinstance(last_age, (int, float))
        else None,
    }


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return v if math.isfinite(v) else float(default)


def _parse_book_side(rows: Any, limit: int) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    if not isinstance(rows, list):
        return out
    for r in rows[: max(1, int(limit))]:
        if not isinstance(r, (list, tuple)) or len(r) < 2:
            continue
        p = _safe_float(r[0], 0.0)
        q = _safe_float(r[1], 0.0)
        if p > 0 and q > 0:
            out.append((p, q))
    return out


def _pivot_levels_from_candles(
    candle_data: List[Dict[str, float]],
    *,
    lookback: int,
    window: int,
    tol_pct: float,
    max_levels: int,
) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    if not candle_data:
        return [], []
    series = list(reversed(candle_data[: max(1, int(lookback))]))
    highs = [_safe_float(c.get("high"), 0.0) for c in series]
    lows = [_safe_float(c.get("low"), 0.0) for c in series]
    vols = [_safe_float(c.get("volume"), 0.0) for c in series]
    n = len(series)
    w = max(1, int(window))
    sup_raw: List[Tuple[float, float]] = []
    res_raw: List[Tuple[float, float]] = []
    for i in range(w, n - w):
        lo = lows[i]
        hi = highs[i]
        if lo > 0 and lo == min(lows[i - w : i + w + 1]):
            sup_raw.append((lo, vols[i]))
        if hi > 0 and hi == max(highs[i - w : i + w + 1]):
            res_raw.append((hi, vols[i]))

    def _cluster(xs: List[Tuple[float, float]], side: str) -> List[Dict[str, float]]:
        if not xs:
            return []
        xs_sorted = sorted(xs, key=lambda t: t[0])
        clusters: List[Dict[str, float]] = []
        for price, weight in xs_sorted:
            if price <= 0:
                continue
            if not clusters:
                clusters.append(
                    {
                        "price": float(price),
                        "strength": float(weight),
                        "touches": 1.0,
                    }
                )
                continue
            last = clusters[-1]
            last_price = float(last["price"])
            base = last_price if last_price > 0 else float(price)
            if abs(price - base) / base <= float(tol_pct):
                s = float(last["strength"]) + float(weight)
                t = float(last["touches"]) + 1.0
                last_touches = float(last["touches"])
                weighted_sum = last_price * last_touches
                weighted_sum += float(price)
                last["price"] = float(weighted_sum / t)
                last["strength"] = s
                last["touches"] = t
            else:
                clusters.append(
                    {
                        "price": float(price),
                        "strength": float(weight),
                        "touches": 1.0,
                    }
                )
        clusters_sorted = sorted(
            clusters, key=lambda d: float(d.get("strength", 0.0)), reverse=True
        )
        keep = clusters_sorted[: max(1, int(max_levels))]
        if side == "support":
            keep = sorted(keep, key=lambda d: float(d["price"]), reverse=True)
        else:
            keep = sorted(keep, key=lambda d: float(d["price"]))
        return keep

    return _cluster(sup_raw, "support"), _cluster(res_raw, "resistance")


def _orderbook_walls(
    orderbook_data: Dict[str, Any],
    *,
    last_price: float,
    band_pct: float,
    depth: int,
    max_levels: int,
) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    bids = _parse_book_side((orderbook_data or {}).get("bids"), depth)
    asks = _parse_book_side((orderbook_data or {}).get("asks"), depth)
    if last_price <= 0:
        last_price = bids[0][0] if bids else (asks[0][0] if asks else 0.0)
    if last_price <= 0:
        return [], []
    band = max(0.0, float(band_pct))
    lo = last_price * (1.0 - band)
    hi = last_price * (1.0 + band)
    bids_band = [b for b in bids if lo <= b[0] <= hi]
    asks_band = [a for a in asks if lo <= a[0] <= hi]
    top_bids = sorted(bids_band or bids, key=lambda t: t[1], reverse=True)[
        : max(1, int(max_levels))
    ]
    top_asks = sorted(asks_band or asks, key=lambda t: t[1], reverse=True)[
        : max(1, int(max_levels))
    ]
    supports = [
        {"price": float(p), "qty": float(q), "notional": float(p * q)}
        for p, q in top_bids
    ]
    resistances = [
        {"price": float(p), "qty": float(q), "notional": float(p * q)}
        for p, q in top_asks
    ]
    supports = sorted(supports, key=lambda d: float(d["price"]), reverse=True)
    resistances = sorted(resistances, key=lambda d: float(d["price"]))
    return supports, resistances


def calculate_discovery_metrics(
    candle_data: List[Dict[str, float]],
    orderbook_data: Dict[str, Any],
    ticker_data: Dict[str, Any],
    recent_trades: Optional[List[Dict[str, Any]]] = None,
    *,
    candles_n: int = 5,
    book_levels_n: int = 10,
    pe_band_pct: float = 0.001,
    ar_trades_n: int = 10,
    sr_lookback: int = 200,
    sr_window: int = 2,
    sr_tol_pct: float = 0.0015,
    sr_levels_n: int = 6,
) -> Dict[str, Any]:
    last_price = _safe_float((ticker_data or {}).get("last_price"), 0.0)
    if isinstance(candle_data, list):
        candles_used = candle_data[: max(1, int(candles_n))]
    else:
        candles_used = []
    if not last_price and candles_used:
        last_price = _safe_float(candles_used[0].get("close"), 0.0)

    vols = [_safe_float(c.get("volume"), 0.0) for c in candles_used]
    cv = float(sum(vols) / float(len(vols))) if vols else 0.0
    ranges = [
        max(
            0.0,
            _safe_float(c.get("high"), 0.0) - _safe_float(c.get("low"), 0.0),
        )
        for c in candles_used
    ]
    avg_range = float(sum(ranges) / float(len(ranges))) if ranges else 0.0
    cvo = float(avg_range / last_price) if last_price > 0 else 0.0

    bids = _parse_book_side((orderbook_data or {}).get("bids"), book_levels_n)
    asks = _parse_book_side((orderbook_data or {}).get("asks"), book_levels_n)
    cp = float(sum(q for _, q in bids) + sum(q for _, q in asks))

    ild = float((cv * 0.4 + cp * 0.4 + cvo * 0.2) * 100.0)

    band = max(0.0, float(pe_band_pct))
    pe = 0.0
    if last_price > 0 and (bids or asks):
        lo = last_price * (1.0 - band)
        hi = last_price * (1.0 + band)
        pe = float(
            sum(q for p, q in bids if lo <= p <= hi)
            + sum(q for p, q in asks if lo <= p <= hi)
        )

    ar = 0.0
    rt = recent_trades if isinstance(recent_trades, list) else []
    if rt:
        qtys: List[float] = []
        n_trades = max(1, int(ar_trades_n))
        for t in rt[-n_trades:]:
            if not isinstance(t, dict):
                continue
            qty = t.get("qty") or t.get("size") or t.get("q")
            qtys.append(_safe_float(qty, 0.0))
        ar = float(sum(qtys))

    vr = float(cv)
    rol = float((vr * 0.3 + pe * 0.4 + ar * 0.3) * 100.0)
    if last_price > 0:
        if float(avg_range / last_price) > 0.005:
            rol *= 0.8

    rop_min = None
    rop_max = None
    if last_price > 0 and pe > 0 and (bids or asks):
        lo = last_price * (1.0 - band)
        hi = last_price * (1.0 + band)
        levels = [(p, q) for p, q in (bids + asks) if lo <= p <= hi]
        if levels:
            levels_sorted = sorted(
                levels,
                key=lambda t: abs(t[0] - last_price),
            )
            acc = 0.0
            thr = pe * 0.5
            for p, q in levels_sorted:
                acc += q
                if rop_min is None or p < rop_min:
                    rop_min = float(p)
                if rop_max is None or p > rop_max:
                    rop_max = float(p)
                if acc >= thr:
                    break

    pivot_supports, pivot_resistances = _pivot_levels_from_candles(
        candle_data,
        lookback=int(sr_lookback),
        window=int(sr_window),
        tol_pct=float(sr_tol_pct),
        max_levels=int(sr_levels_n),
    )
    wall_supports, wall_resistances = _orderbook_walls(
        orderbook_data,
        last_price=float(last_price),
        band_pct=float(max(0.005, band)),
        depth=max(20, int(book_levels_n) * 5),
        max_levels=int(sr_levels_n),
    )

    def _merge_levels(
        a: List[Dict[str, float]], b: List[Dict[str, float]], tol: float
    ) -> List[Dict[str, float]]:
        out: List[Dict[str, float]] = []
        for src in a or []:
            if not isinstance(src, dict):
                continue
            price_val = _safe_float(src.get("price"), 0.0)
            if price_val <= 0:
                continue
            out.append(dict(src))
        for src in b or []:
            p = _safe_float((src or {}).get("price"), 0.0)
            if p <= 0:
                continue
            merged = False
            for d in out:
                base = _safe_float(d.get("price"), 0.0)
                if base > 0 and abs(p - base) / base <= tol:
                    d["price"] = float((base + p) / 2.0)
                    d["strength"] = float(
                        _safe_float(d.get("strength"), 0.0)
                        + _safe_float(
                            src.get("qty")
                            or src.get("notional")
                            or src.get("strength"),
                            0.0,
                        )
                    )
                    touches = _safe_float(d.get("touches"), 1.0)
                    d["touches"] = float(touches + 1.0)
                    merged = True
                    break
            if not merged:
                out.append(
                    {
                        "price": float(p),
                        "strength": float(
                            _safe_float(
                                src.get("qty")
                                or src.get("notional")
                                or src.get("strength"),
                                0.0,
                            )
                        ),
                        "touches": float(_safe_float(src.get("touches"), 1.0)),
                    }
                )
        return out

    supports_all = _merge_levels(
        pivot_supports,
        wall_supports,
        float(sr_tol_pct),
    )
    resistances_all = _merge_levels(
        pivot_resistances, wall_resistances, float(sr_tol_pct)
    )
    supports_all = sorted(
        supports_all, key=lambda d: float(d.get("price", 0.0)), reverse=True
    )[: max(1, int(sr_levels_n))]
    resistances_all = sorted(
        resistances_all,
        key=lambda d: float(d.get("price", 0.0)),
    )[: max(1, int(sr_levels_n))]

    nearest_support = None
    nearest_resistance = None
    if last_price > 0:
        below = [
            d for d in supports_all if float(d.get("price", 0.0)) < last_price
        ]
        above = []
        for d in resistances_all:
            if float(d.get("price", 0.0)) > last_price:
                above.append(d)
        if below:
            nearest_support = float(
                max(below, key=lambda d: float(d["price"]))["price"]
            )
        if above:
            nearest_resistance = float(
                min(above, key=lambda d: float(d["price"]))["price"]
            )

    support_dist_pct = (
        float((last_price - nearest_support) / last_price)
        if last_price > 0 and nearest_support
        else None
    )
    resistance_dist_pct = (
        float((nearest_resistance - last_price) / last_price)
        if last_price > 0 and nearest_resistance
        else None
    )

    return {
        "combined": {
            "last_price": float(last_price),
            "candles_n": int(len(candles_used)),
            "cv": float(cv),
            "cp": float(cp),
            "cvo": float(cvo),
            "vr": float(vr),
            "pe": float(pe),
            "ar": float(ar),
            "rop_min": float(rop_min) if rop_min is not None else None,
            "rop_max": float(rop_max) if rop_max is not None else None,
        },
        "ild": float(ild),
        "rol": float(rol),
        "supports": supports_all,
        "resistances": resistances_all,
        "nearest_support": float(nearest_support)
        if nearest_support is not None
        else None,
        "nearest_resistance": float(nearest_resistance)
        if nearest_resistance is not None
        else None,
        "support_dist_pct": float(support_dist_pct)
        if support_dist_pct is not None
        else None,
        "resistance_dist_pct": float(resistance_dist_pct)
        if resistance_dist_pct is not None
        else None,
    }


def calculate_metrics(
    candle_data: List[Dict[str, Any]],
    orderbook_data: Dict[str, Any],
    ticker_data: Dict[str, Any],
    depth: int = 5,
    *,
    recent_trades: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    last_price_seed = _safe_float((ticker_data or {}).get("last_price"), 0.0)
    if last_price_seed <= 0 and candle_data:
        last_price_seed = _safe_float((candle_data[0] or {}).get("close"), 0.0)

    if not candle_data or len(candle_data) < 2 or last_price_seed <= 0:
        logger.warning(
            "Datos insuficientes para métricas, "
            "devolviendo valores por defecto"
        )
        return {
            "data_ok": False,
            "combined": 0.0,
            "ild": 0.0,
            "egm": 0.0,
            "rol": 0.0,
            "pio": 0.0,
            "ogm": 0.0,
            "volatility": 0.0,
        }

    try:
        last_price: float = float(last_price_seed)
        mark_price = last_price
        index_price = last_price
        try:
            mp = ticker_data.get("mark_price")
            if mp is None:
                mp = ticker_data.get("markPrice")
            ip = ticker_data.get("index_price")
            if ip is None:
                ip = ticker_data.get("indexPrice")
            mpv = _tsm_to_number(mp)
            if mpv is not None:
                mark_price = float(mpv)
            ipv = _tsm_to_number(ip)
            if ipv is not None:
                index_price = float(ipv)
        except Exception:
            mark_price = last_price
            index_price = last_price

        highs: np.ndarray = np.array(
            [
                float(c.get("high", 0))
                for c in candle_data[:20]
                if c.get("high") is not None
            ],
            dtype=np.float64,
        )
        lows: np.ndarray = np.array(
            [
                float(c.get("low", 0))
                for c in candle_data[:20]
                if c.get("low") is not None
            ],
            dtype=np.float64,
        )
        if len(highs) == 0 or len(lows) == 0 or last_price <= 0:
            return {
                "data_ok": False,
                "combined": 0.0,
                "ild": 0.0,
                "egm": 0.0,
                "rol": 0.0,
                "pio": 0.0,
                "ogm": 0.0,
                "volatility": 0.0,
            }

        volatility: float = float((highs.max() - lows.min()) / last_price)

        depth_i = int(depth) if isinstance(depth, int) and depth > 0 else 50
        bids_raw = orderbook_data.get("bids") or []
        asks_raw = orderbook_data.get("asks") or []
        bids_in: List[Tuple[float, float]] = []
        asks_in: List[Tuple[float, float]] = []
        for row in bids_raw[:depth_i]:
            try:
                p = float(row[0])
                q = float(row[1])
                if p > 0 and q > 0:
                    bids_in.append((p, q))
            except Exception:
                continue
        for row in asks_raw[:depth_i]:
            try:
                p = float(row[0])
                q = float(row[1])
                if p > 0 and q > 0:
                    asks_in.append((p, q))
            except Exception:
                continue

        if not bids_in or not asks_in:
            return {
                "data_ok": False,
                "combined": 0.0,
                "ild": 0.0,
                "egm": 0.0,
                "rol": 0.0,
                "pio": 0.0,
                "ogm": 0.0,
                "volatility": float(volatility),
            }

        best_bid = float(max(p for p, _ in bids_in))
        best_ask = float(min(p for p, _ in asks_in))
        if best_bid <= 0 or best_ask <= 0:
            return {
                "data_ok": False,
                "combined": 0.0,
                "ild": 0.0,
                "egm": 0.0,
                "rol": 0.0,
                "pio": 0.0,
                "ogm": 0.0,
                "volatility": float(volatility),
            }

        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return {
                "data_ok": False,
                "combined": 0.0,
                "ild": 0.0,
                "egm": 0.0,
                "rol": 0.0,
                "pio": 0.0,
                "ogm": 0.0,
                "volatility": float(volatility),
            }

        best_bid_qty = 0.0
        best_ask_qty = 0.0
        for p, q in bids_in:
            if p == best_bid:
                best_bid_qty += q
        for p, q in asks_in:
            if p == best_ask:
                best_ask_qty += q
        microprice = mid
        denom = best_bid_qty + best_ask_qty
        if denom > 0:
            microprice = (
                (best_ask * best_bid_qty + best_bid * best_ask_qty) / denom
            )

        k_book = int(min(len(bids_in), len(asks_in), max(1, depth_i)))
        bid_notional_sum_k = 0.0
        ask_notional_sum_k = 0.0
        for p, q in bids_in[:k_book]:
            bid_notional_sum_k += float(p) * float(q)
        for p, q in asks_in[:k_book]:
            ask_notional_sum_k += float(p) * float(q)
        obi_notional = (bid_notional_sum_k - ask_notional_sum_k) / (
            bid_notional_sum_k + ask_notional_sum_k + 1e-12
        )

        lambda_ = float(ticker_data.get("orderbook_lambda", 0.03) or 0.03)
        pct_band = float(ticker_data.get("orderbook_pct_band", 0.015) or 0.015)
        target_move = float(ticker_data.get("ild_target_move", 0.002) or 0.002)

        bids: List[Tuple[float, float]] = []
        asks: List[Tuple[float, float]] = []
        for p, q in bids_in:
            if abs(p - mid) / mid <= pct_band:
                bids.append((p, q))
        for p, q in asks_in:
            if abs(p - mid) / mid <= pct_band:
                asks.append((p, q))

        if not bids or not asks:
            bids = bids_in
            asks = asks_in

        bid_w_sum = 0.0
        ask_w_sum = 0.0
        for p, q in bids:
            dist = max(0.0, mid - p)
            bid_w_sum += q * float(np.exp(-lambda_ * dist))
        for p, q in asks:
            dist = max(0.0, p - mid)
            ask_w_sum += q * float(np.exp(-lambda_ * dist))

        pio_raw = bid_w_sum - ask_w_sum
        weighted_liquidity = bid_w_sum + ask_w_sum
        asymmetry = (bid_w_sum - ask_w_sum) / (weighted_liquidity + 1e-12)

        up_target = mid * (1.0 + target_move)
        down_target = mid * (1.0 - target_move)

        asks_sorted = sorted(asks, key=lambda x: x[0])
        bids_sorted = sorted(bids, key=lambda x: x[0], reverse=True)

        up_notional = 0.0
        for p, q in asks_sorted:
            up_notional += p * q
            if p >= up_target:
                break

        down_notional = 0.0
        for p, q in bids_sorted:
            down_notional += p * q
            if p <= down_target:
                break

        ild_raw = (up_notional + down_notional) / 2.0

        def _gap_stats(
            levels: List[Tuple[float, float]], ascending: bool
        ) -> Tuple[float, float]:
            if len(levels) < 3:
                return 0.0, 0.0
            levels_sorted = sorted(
                levels,
                key=lambda x: x[0],
                reverse=not ascending,
            )
            prices = np.array([p for p, _ in levels_sorted], dtype=np.float64)
            qtys = np.array([q for _, q in levels_sorted], dtype=np.float64)
            if len(prices) < 3:
                return 0.0, 0.0
            gaps = np.abs(np.diff(prices))
            if len(gaps) == 0:
                return 0.0, 0.0
            med_gap = float(np.median(gaps))
            q_thr = float(np.quantile(qtys, 0.9)) if len(qtys) > 0 else 0.0
            large_idx = np.where(qtys[:-1] >= q_thr)[0]
            if len(large_idx) == 0:
                return med_gap, med_gap
            large_gaps = gaps[large_idx]
            if len(large_gaps):
                return med_gap, float(np.mean(large_gaps))
            return med_gap, med_gap

        ask_med_gap, ask_large_gap = _gap_stats(asks, ascending=True)
        bid_med_gap, bid_large_gap = _gap_stats(bids, ascending=False)
        ogm_raw = (ask_large_gap - ask_med_gap) - (bid_large_gap - bid_med_gap)

        prev_weighted_liq = ticker_data.get("prev_weighted_liquidity")
        dt_s = ticker_data.get("rol_dt_s")
        rol_raw = 0.0
        try:
            if prev_weighted_liq is not None and dt_s is not None:
                dt_s_f = float(dt_s)
                prev_liq_f = float(prev_weighted_liq)
                if dt_s_f > 0:
                    rol_raw = (weighted_liquidity - prev_liq_f) / dt_s_f
        except Exception:
            rol_raw = 0.0

        history = ticker_data.get("metric_history") or []
        if not isinstance(history, (list, deque)):
            history = []

        def _z(current: float, key: str) -> float:
            xs: List[float] = []
            for h in history:
                if isinstance(h, dict):
                    v = h.get(key)
                    if v is not None:
                        try:
                            xs.append(float(v))
                        except Exception:
                            pass
            xs.append(float(current))
            
            n = len(xs)
            if n < 2:
                return 0.0
                
            arr = np.array(xs, dtype=np.float64)
            mu = float(np.mean(arr))
            sd = float(np.std(arr))
            
            if sd <= 1e-12:
                return 0.0
                
            z = (float(current) - mu) / sd
            
            if n < 5:
                z *= (n / 5.0)
                
            return z

        pio_z = _z(pio_raw, "pio")
        ild_z = _z(ild_raw, "ild")
        rol_z = _z(rol_raw, "rol")
        ogm_z = _z(ogm_raw, "ogm")

        bonus = 0.0
        if abs(rol_z) >= 1.5 and abs(pio_z) > 0:
            bonus = float(np.sign(pio_z)) * min(1.5, abs(rol_z) - 1.5)
        egm_raw = (pio_z * (1.0 + abs(asymmetry))) + bonus
        egm_z = _z(egm_raw, "egm")

        cw = (
            ticker_data.get("combined_weights")
            if isinstance(ticker_data, dict)
            else None
        )
        if not isinstance(cw, dict):
            cw = {}
        w_pio = float(cw.get("pio", 0.45) or 0.45)
        w_egm = float(cw.get("egm", 0.30) or 0.30)
        w_ild = float(cw.get("ild", -0.15) or -0.15)
        w_rol = float(cw.get("rol", 0.10) or 0.10)
        w_ogm = float(cw.get("ogm", 0.05) or 0.05)
        w_mom = float(cw.get("mom", 0.16) or 0.16)
        w_scale = float(cw.get("scale", 10.0) or 10.0)
        weights = [w_pio, w_egm, w_ild, w_rol, w_ogm, w_mom, w_scale]
        if not np.isfinite(weights).all():
            w_pio, w_egm, w_ild, w_rol, w_ogm, w_mom, w_scale = (
                0.36,
                0.24,
                -0.12,
                0.08,
                0.04,
                0.16,
                10.0,
            )

        combined_z_micro = (
            w_pio * pio_z
            + w_egm * egm_z
            + w_ild * ild_z
            + w_rol * rol_z
            + w_ogm * ogm_z
        )

        closes: List[float] = []
        for c in candle_data or []:
            if not isinstance(c, dict):
                continue
            v = c.get("close")
            vnum = _tsm_to_number(v)
            if vnum is not None:
                closes.append(float(vnum))

        ret1m = 0.0
        ret5m = 0.0
        ret20m = 0.0
        if len(closes) >= 2 and closes[0] > 0 and closes[1] > 0:
            ret1m = (closes[0] - closes[1]) / closes[1]
        if len(closes) >= 6 and closes[0] > 0 and closes[5] > 0:
            ret5m = (closes[0] - closes[5]) / closes[5]
        if len(closes) >= 21 and closes[0] > 0 and closes[20] > 0:
            ret20m = (closes[0] - closes[20]) / closes[20]

        igd_n5_n20 = float(ret5m - ret20m)

        closes_chrono = list(reversed(closes))

        def _ema(values: List[float], span: int) -> float:
            if span <= 1 or len(values) < 2:
                return float(values[-1]) if values else 0.0
            alpha = 2.0 / (float(span) + 1.0)
            e = float(values[0])
            for x in values[1:]:
                e = alpha * float(x) + (1.0 - alpha) * e
            return float(e)

        ema5 = _ema(closes_chrono, 5) if closes_chrono else 0.0
        ema20 = _ema(closes_chrono, 20) if closes_chrono else 0.0
        if last_price > 0:
            ema_diff_rel = float((ema5 - ema20) / last_price)
        else:
            ema_diff_rel = 0.0

        vol_safe = max(1e-9, float(volatility))
        mom_raw = float(
            0.55 * (float(ret5m) / vol_safe)
            + 0.25 * (float(ret20m) / vol_safe)
            + 0.20 * (float(ema_diff_rel) / vol_safe)
        )
        mom_z = _z(mom_raw, "mom_raw")

        combined_z = float(combined_z_micro) + float(w_mom) * float(mom_z)
        combined = float(combined_z * float(w_scale))

        imb_vals: List[float] = []
        if isinstance(history, deque):
            start = max(0, len(history) - 20)
            history_tail = itertools.islice(history, start, None)
        elif isinstance(history, list):
            history_tail = history[-20:]
        else:
            history_tail = []
        for h in history_tail:
            if not isinstance(h, dict):
                continue
            v = h.get("asymmetry")
            vnum = _tsm_to_number(v)
            if vnum is not None:
                imb_vals.append(float(vnum))
        imb_vals.append(float(asymmetry))
        imbalance20 = (
            float(np.mean(np.array(imb_vals, dtype=np.float64)))
            if imb_vals
            else float(asymmetry)
        )

        ret1m_sign = 0.0
        if ret1m > 0:
            ret1m_sign = 1.0
        elif ret1m < 0:
            ret1m_sign = -1.0
        cbd_n20 = float(ret1m_sign * imbalance20)

        spread_rel = float((best_ask - best_bid) / mid) if mid > 0 else 0.0

        formulas = ticker_data.get("formulas") or {}
        if isinstance(formulas, str) and formulas.strip():
            try:
                formulas = json.loads(formulas)
            except Exception:
                formulas = {}
        ctx: Dict[str, Any] = {
            **{
                "last_price": float(last_price),
                "LastPrice": float(last_price),
                "DBMarket": float(last_price),
                "MidPrice": float(mid),
                "BestBid": float(best_bid),
                "BestAsk": float(best_ask),
                "Spread": float(best_ask - best_bid),
                "SpreadPct": float(spread_rel),
                "SpreadRel": float(spread_rel),
                "MicroPrice": float(microprice),
                "DBMinBuyout": float(best_ask),
                "DBMaxBuyout": float(best_bid),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
                "mid_price": float(mid),
                "microprice": float(microprice),
                "mark_price": float(mark_price),
                "index_price": float(index_price),
            },
            "K": float(k_book),
            "bid_notional_sum_k": float(bid_notional_sum_k),
            "ask_notional_sum_k": float(ask_notional_sum_k),
            "obi_notional": float(obi_notional),
            "combined": float(combined),
            "ild": float(ild_z),
            "egm": float(egm_z),
            "rol": float(rol_z),
            "pio": float(pio_z),
            "ogm": float(ogm_z),
            "volatility": float(volatility),
            "pio_raw": float(pio_raw),
            "ild_raw": float(ild_raw),
            "egm_raw": float(egm_raw),
            "rol_raw": float(rol_raw),
            "ogm_raw": float(ogm_raw),
            "weighted_liquidity": float(weighted_liquidity),
            "asymmetry": float(asymmetry),
            "imbalance20": float(imbalance20),
            "ret1m": float(ret1m),
            "ret5m": float(ret5m),
            "ret20m": float(ret20m),
            "igd_n5_n20": float(igd_n5_n20),
            "ema5": float(ema5),
            "ema20": float(ema20),
            "ema_diff_rel": float(ema_diff_rel),
            "mom": float(mom_z),
            "mom_raw": float(mom_raw),
            "cbd_n20": float(cbd_n20),
        }
        for i, c in enumerate((candle_data or [])[:5], start=1):
            if not isinstance(c, dict):
                continue
            for k_src, k_dst in (
                ("open", "Open"),
                ("high", "High"),
                ("low", "Low"),
                ("close", "Close"),
                ("volume", "Volume"),
            ):
                v = c.get(k_src)
                vnum = _tsm_to_number(v)
                if vnum is not None:
                    ctx[f"Candle{i}{k_dst}"] = float(vnum)

        trades_metrics = compute_recent_trades_metrics(
            recent_trades,
            window_n=10,
        )
        if isinstance(trades_metrics, dict):
            for k, v in trades_metrics.items():
                if v is None:
                    continue
                vnum = _tsm_to_number(v)
                if vnum is not None:
                    ctx[f"RecentTrades:{k}"] = float(vnum)
            buy_qty = trades_metrics.get("buy_qty")
            sell_qty = trades_metrics.get("sell_qty")
            buy_v = _tsm_to_number(buy_qty)
            if buy_v is not None:
                ctx["taker_buy_qty"] = float(buy_v)
            sell_v = _tsm_to_number(sell_qty)
            if sell_v is not None:
                ctx["taker_sell_qty"] = float(sell_v)

        try:
            discovery = calculate_discovery_metrics(
                candle_data,
                orderbook_data,
                ticker_data,
                recent_trades=recent_trades,
            )
            supports = (
                discovery.get("supports")
                if isinstance(discovery, dict)
                else None
            )
            resistances = (
                discovery.get("resistances")
                if isinstance(discovery, dict)
                else None
            )
            if isinstance(supports, list):
                for i, d in enumerate(supports[:10], start=1):
                    if not isinstance(d, dict):
                        continue
                    pv = _tsm_to_number(d.get("price"))
                    if pv is not None:
                        ctx[f"Support{i}"] = float(pv)
            if isinstance(resistances, list):
                for i, d in enumerate(resistances[:10], start=1):
                    if not isinstance(d, dict):
                        continue
                    pv = _tsm_to_number(d.get("price"))
                    if pv is not None:
                        ctx[f"Resistance{i}"] = float(pv)
            if isinstance(discovery, dict):
                for k in (
                    "nearest_support",
                    "nearest_resistance",
                    "support_dist_pct",
                    "resistance_dist_pct",
                    "ild",
                    "rol",
                ):
                    v = discovery.get(k)
                    vnum = _tsm_to_number(v)
                    if vnum is not None:
                        ctx[k] = float(vnum)
        except Exception:
            pass
        formulas_dict = formulas if isinstance(formulas, dict) else {}
        derived = eval_tsm_formulas(formulas_dict, ctx)

        logger.debug(
            "Métricas: combined=%.2f, ild=%.4f, egm=%.4f, rol=%.4f, "
            "pio=%.4f, ogm=%.4f, volatility=%.4f",
            combined,
            ild_z,
            egm_z,
            rol_z,
            pio_z,
            ogm_z,
            volatility,
        )
        return {
            "data_ok": True,
            "last_price": float(last_price),
            "combined": float(combined),
            "ild": float(ild_z),
            "egm": float(egm_z),
            "rol": float(rol_z),
            "pio": float(pio_z),
            "ogm": float(ogm_z),
            "combined_z_micro": float(combined_z_micro),
            "combined_z": float(combined_z),
            "combined_weights": {
                "pio": float(w_pio),
                "egm": float(w_egm),
                "ild": float(w_ild),
                "rol": float(w_rol),
                "ogm": float(w_ogm),
                "mom": float(w_mom),
                "scale": float(w_scale),
            },
            "combined_components": {
                "pio": float(w_pio) * float(pio_z),
                "egm": float(w_egm) * float(egm_z),
                "ild": float(w_ild) * float(ild_z),
                "rol": float(w_rol) * float(rol_z),
                "ogm": float(w_ogm) * float(ogm_z),
                "mom": float(w_mom) * float(mom_z),
                "sum_z": float(combined_z),
            },
            "volatility": float(volatility),
            "best_bid": float(best_bid),
            "best_ask": float(best_ask),
            "mid_price": float(mid),
            "spread": float(best_ask - best_bid),
            "spread_pct": float(spread_rel),
            "spread_rel": float(spread_rel),
            "microprice": float(microprice),
            "pio_raw": float(pio_raw),
            "ild_raw": float(ild_raw),
            "egm_raw": float(egm_raw),
            "rol_raw": float(rol_raw),
            "ogm_raw": float(ogm_raw),
            "weighted_liquidity": float(weighted_liquidity),
            "asymmetry": float(asymmetry),
            "imbalance20": float(imbalance20),
            "ret1m": float(ret1m),
            "ret5m": float(ret5m),
            "ret20m": float(ret20m),
            "igd_n5_n20": float(igd_n5_n20),
            "ema5": float(ema5),
            "ema20": float(ema20),
            "ema_diff_rel": float(ema_diff_rel),
            "mom": float(mom_z),
            "mom_raw": float(mom_raw),
            "cbd_n20": float(cbd_n20),
            **(
                {
                    f"recent_trades_{k}": v
                    for k, v in (trades_metrics or {}).items()
                }
                if isinstance(trades_metrics, dict)
                else {}
            ),
            **derived,
        }
    except Exception as e:
        logger.error(f"Error en calculate_metrics: {e}", exc_info=True)
        return {
            "data_ok": False,
            "combined": 0.0,
            "ild": 0.0,
            "egm": 0.0,
            "rol": 0.0,
            "pio": 0.0,
            "ogm": 0.0,
            "volatility": 0.0,
        }


# === Math & Statistics ===


def calculate_rolling_volatility(prices: List[float], window: int) -> float:
    """
    Calcula la volatilidad móvil de una lista de precios.

    :param prices: Lista de precios.
    :param window: Tamaño de la ventana para el cálculo.
    :return: Volatilidad móvil.
    """
    if len(prices) < window:
        return 0.0
    recent_prices = np.array(prices[-window:])
    prev_prices = np.array(prices[-window - 1:-1])
    log_returns = np.log(recent_prices / prev_prices)
    return np.std(log_returns) * np.sqrt(window)


def _safe_float_opt(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not np.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _robust_center_scale(values: np.ndarray) -> Tuple[float, float]:
    if values.size <= 0:
        return 0.0, 1.0
    med = float(np.median(values))
    mad = float(np.median(np.abs(values - med)))
    scale = 1.4826 * mad
    if not np.isfinite(scale) or scale <= 1e-12:
        std = float(np.std(values))
        scale = std if np.isfinite(std) and std > 1e-12 else 1.0
    return med, float(scale)


def normalize_metric_value(
    value: Any,
    *,
    center: float,
    scale: float,
    cap_z: float = 4.0,
    squash: str = "tanh",
) -> Optional[float]:
    v = _safe_float_opt(value)
    if v is None:
        return None
    z = (v - float(center)) / max(1e-12, float(scale))
    z = float(np.clip(z, -float(cap_z), float(cap_z)))
    if squash == "none":
        return z
    if squash == "tanh":
        return float(np.tanh(z))
    if squash == "logistic":
        return float(2.0 / (1.0 + np.exp(-z)) - 1.0)
    return float(np.tanh(z))


def normalize_series_robust(
    series: List[Any],
    *,
    window: int = 500,
    cap_z: float = 4.0,
    squash: str = "tanh",
) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    buf: List[float] = []
    w = int(max(10, window))
    for x in series:
        v = _safe_float_opt(x)
        if v is not None:
            buf.append(v)
        lookback = buf[-w:] if len(buf) >= 1 else []
        arr = np.asarray(lookback, dtype=np.float64)
        arr = arr[np.isfinite(arr)]
        center, scale = _robust_center_scale(arr)
        out.append(
            normalize_metric_value(
                x, center=center, scale=scale, cap_z=cap_z, squash=squash
            )
        )
    return out


def _spearman_corr(x: np.ndarray, y: np.ndarray) -> Optional[float]:
    if x.size < 5 or y.size < 5:
        return None
    xr = x.argsort().argsort().astype(np.float64)
    yr = y.argsort().argsort().astype(np.float64)
    if float(np.std(xr)) <= 1e-12 or float(np.std(yr)) <= 1e-12:
        return None
    return float(np.corrcoef(xr, yr)[0, 1])


# === Results Validation ===


def validate_metric_predictiveness_from_results(
    *,
    results_path: str,
    metric_key: str,
    horizon_steps: int = 5,
    window: int = 500,
    cap_z: float = 4.0,
    squash: str = "tanh",
) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = []
    try:
        path = os.path.abspath(str(results_path))
        if path.lower().endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = str(line or "").strip()
                    if not s:
                        continue
                    try:
                        rec = json.loads(s)
                    except Exception:
                        continue
                    if not isinstance(rec, dict):
                        continue
                    if rec.get("type") == "metrics":
                        events.append(rec)
                        continue
                    m = rec.get("metrics")
                    lp = rec.get("last_price")
                    if isinstance(m, dict) and lp is not None:
                        events.append(
                            {
                                "type": "metrics",
                                "timestamp": rec.get("timestamp"),
                                "last_price": lp,
                                "metrics": m,
                            }
                        )
        else:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                evs = (payload or {}).get("events")
            else:
                evs = None
            if isinstance(evs, list):
                for ev in evs:
                    if isinstance(ev, dict):
                        events.append(ev)
    except Exception as e:
        return {"ok": False, "error": str(e), "path": results_path}

    if not events:
        return {"ok": False, "error": "events_missing", "path": results_path}

    prices: List[float] = []
    metrics: List[float] = []
    ts: List[Any] = []
    for ev in events:
        if not isinstance(ev, dict) or ev.get("type") != "metrics":
            continue
        price = _safe_float_opt(ev.get("last_price"))
        m = ev.get("metrics")
        val = None
        if isinstance(m, dict):
            val = _safe_float_opt(m.get(metric_key))
        if price is None or val is None:
            continue
        prices.append(price)
        metrics.append(val)
        ts.append(ev.get("timestamp"))

    h = int(max(1, horizon_steps))
    if len(prices) <= h + 10:
        return {
            "ok": False,
            "error": "insufficient_samples",
            "samples": int(len(prices)),
            "horizon_steps": h,
        }

    x_raw = np.asarray([float(v) for v in metrics], dtype=np.float64)
    p = np.asarray([float(v) for v in prices], dtype=np.float64)
    fut_ret = (p[h:] - p[:-h]) / np.maximum(1e-12, p[:-h])
    x_raw = x_raw[:-h]

    x_norm_list = normalize_series_robust(
        x_raw.tolist(), window=window, cap_z=cap_z, squash=squash
    )
    x_norm_values = [v for v in x_norm_list if v is not None]
    x_norm = np.asarray(x_norm_values, dtype=np.float64)
    idx = np.asarray(
        [i for i, v in enumerate(x_norm_list) if v is not None], dtype=np.int64
    )
    y = fut_ret[idx]

    if x_norm.size < 10:
        return {
            "ok": False,
            "error": "insufficient_valid_samples",
            "samples": int(x_norm.size),
        }

    pearson = None
    try:
        if float(np.std(x_norm)) > 1e-12 and float(np.std(y)) > 1e-12:
            pearson = float(np.corrcoef(x_norm, y)[0, 1])
    except Exception:
        pearson = None

    spear = None
    try:
        spear = _spearman_corr(x_norm, y)
    except Exception:
        spear = None

    directional = float(
        np.mean(
            (np.sign(x_norm) == np.sign(y)).astype(np.float64)
        )
    )

    return {
        "ok": True,
        "metric_key": metric_key,
        "horizon_steps": h,
        "samples": int(x_norm.size),
        "pearson": pearson,
        "spearman": spear,
        "directional_accuracy": directional,
        "first_ts": ts[0] if ts else None,
        "last_ts": ts[-1] if ts else None,
    }


# === API Helpers ===


def generate_signature(api_secret: str, prehash: str) -> str:
    """Genera la firma HMAC SHA256 para la API V5 de Bybit."""
    return hmac.new(
        api_secret.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


# === Results Persistence ===


def save_results(results: dict, log_dir: str = "logs") -> None:
    """
    Guarda los resultados en un archivo JSON en el directorio especificado.

    :param results: Los resultados a guardar.
    :param log_dir: El directorio donde se guardará el archivo.
    """
    log_dir = os.path.abspath(log_dir)
    os.makedirs(log_dir, exist_ok=True)
    filepath = os.path.join(log_dir, "results.json")
    with _RESULTS_JSON_LOCK:
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if isinstance(existing, dict):
                    keys = (
                        "events",
                        "last_metrics",
                        "thresholds",
                        "last_balance",
                    )
                    for key in keys:
                        if key in existing and key not in results:
                            results[key] = existing[key]
            except Exception:
                pass
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4)
    logger.info(f"📈 Resultados guardados en {filepath}")


def load_results_json(log_dir: str = "logs") -> dict:
    log_dir = os.path.abspath(log_dir)
    filepath = os.path.join(log_dir, "results.json")
    with _RESULTS_JSON_LOCK:
        if not os.path.exists(filepath):
            return {}
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


async def load_results_json_async(log_dir: str = "logs") -> dict:
    return await asyncio.to_thread(load_results_json, log_dir)


def append_results_event(
    event: dict, log_dir: str = "logs", max_events: int = 2000
) -> None:
    log_dir = os.path.abspath(log_dir)
    os.makedirs(log_dir, exist_ok=True)
    filepath = os.path.join(log_dir, "results.json")

    with _RESULTS_JSON_LOCK:
        payload: dict = {}
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if isinstance(existing, dict):
                    payload = existing
            except Exception:
                payload = {}

        events = payload.get("events")
        if not isinstance(events, list):
            events = []
        if isinstance(event, dict) and "timestamp" not in event:
            event = {
                **event,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        events.append(event)
        if max_events > 0 and len(events) > max_events:
            events = events[-max_events:]
        payload["events"] = events

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4)


async def save_results_async(results: dict, log_dir: str = "logs") -> None:
    await asyncio.to_thread(save_results, results, log_dir)


async def append_results_event_async(
    event: dict, log_dir: str = "logs", max_events: int = 2000
) -> None:
    await asyncio.to_thread(append_results_event, event, log_dir, max_events)


def timestamp_to_datetime(timestamp_ms: int) -> datetime:
    """
    Convierte un timestamp en milisegundos a un objeto datetime.

    :param timestamp_ms: El timestamp en milisegundos.
    :return: El objeto datetime correspondiente.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def calculate_tp_sl(
    price: float,
    volatility: float,
    action: str,
    tp_factor: float = 1.5,
    sl_factor: float = 1.0,
) -> Tuple[float, float]:
    """Calcula Take Profit y Stop Loss dinámicos basados en volatilidad."""
    try:
        price_range = volatility * price
        if action.lower() == "buy":
            tp = price + (price_range * tp_factor)
            sl = price - (price_range * sl_factor)
        else:  # sell
            tp = price - (price_range * tp_factor)
            sl = price + (price_range * sl_factor)
        return round(tp, 2), round(sl, 2)
    except Exception as e:
        logger.error(f"❌ Error en calculate_tp_sl: {e}")
        return 0.0, 0.0
