"""Aplica parches de factorización/cooldown/TP-SL a Nertzh.py y symbols.py."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from path_safety import safe_path_under_project  # noqa: E402

NERTZH = safe_path_under_project(ROOT / "src" / "Nertzh.py")
SYMBOLS = safe_path_under_project(ROOT / "nertz_engine" / "engine" / "symbols.py")


def patch_symbols():
    text = SYMBOLS.read_text(encoding="utf-8")
    old = """    def can_trade(self, now: Optional[float] = None) -> bool:
        ts = float(now if now is not None else time.time())
        return (ts - float(self.last_trade_ts)) >= float(self.cooldown_s)"""
    new = """    def can_trade(self, now: Optional[float] = None) -> bool:
        if float(self.cooldown_s) <= 0.0:
            return True
        ts = float(now if now is not None else time.time())
        return (ts - float(self.last_trade_ts)) >= float(self.cooldown_s)"""
    if old in text and new not in text:
        SYMBOLS.write_text(text.replace(old, new), encoding="utf-8")
        print("patched symbols.py")


def patch_nertzh():
    text = NERTZH.read_text(encoding="utf-8")

    if "from nertz_orders import" not in text:
        text = text.replace(
            "from bybit_v5 import BybitV5Client\n",
            "from bybit_v5 import BybitV5Client\n"
            "from nertz_orders import build_spot_create_body, strip_exchange_tpsl\n",
        )

    # Cooldown autónomo
    old_cd = """                cooldown = timedelta (seconds=float (getattr (config, "TRADE_COOLDOWN_S", config.DEFAULT_SLEEP_TIME)))
                last_trade_time = self.last_trade_time.get (symbol, datetime.min.replace (tzinfo=timezone.utc))
                in_cooldown = False"""
    new_cd = """                cooldown_s = float (getattr (config, "TRADE_COOLDOWN_S", 0.0) or 0.0)
                last_trade_time = self.last_trade_time.get (symbol, datetime.min.replace (tzinfo=timezone.utc))
                in_cooldown = False
                if cooldown_s > 0:
                    elapsed = (current_time - last_trade_time).total_seconds ()
                    in_cooldown = elapsed < cooldown_s
                    if in_cooldown and bool (getattr (config, "COOLDOWN_BYPASS_STRONG_SIGNAL", True)):
                        try:
                            comb = float ((metrics or {}).get ("combined") or 0.0)
                            buy_th = float (getattr (config, "COMBINED_BUY_THRESHOLD", 1.5) or 1.5)
                            mult = float (getattr (config, "COOLDOWN_BYPASS_MULT", 1.25) or 1.25)
                            if abs (comb) >= buy_th * mult:
                                in_cooldown = False
                        except Exception:
                            pass"""
    if old_cd in text:
        text = text.replace(old_cd, new_cd)

    text = text.replace(
        "                ctx = self.operations.get (symbol)\n"
        "                if False and not ctx.can_trade ():\n"
        "                    return\n",
        "                ctx = self.operations.get (symbol)\n"
        "                ctx.cooldown_s = float (getattr (config, \"TRADE_COOLDOWN_S\", 0.0) or 0.0)\n"
        "                if not ctx.can_trade ():\n"
        "                    return\n",
    )

    # _replace_order_with_market: sin TP/SL en Market
    old_mkt = """            tp_val = getattr (trade, "tp_price", None)
            sl_val = getattr (trade, "sl_price", None)
            tp_str = None
            sl_str = None
            if tp_val is not None and sl_val is not None:
                try:
                    tp_str = self._format_decimal (self._quantize_to_step (float (tp_val), tick_size, ROUND_HALF_UP))
                    sl_str = self._format_decimal (self._quantize_to_step (float (sl_val), tick_size, ROUND_HALF_UP))
                except Exception:
                    tp_str = None
                    sl_str = None
            if tp_str and sl_str:
                create_body["takeProfit"] = tp_str
                create_body["stopLoss"] = sl_str
                create_body["tpOrderType"] = "Market"
                create_body["slOrderType"] = "Market"

            create_result = await client.create_order (create_body)
            if create_result.get ("retCode") != 0 and ("takeProfit" in create_body or "stopLoss" in create_body):
                create_body.pop ("takeProfit", None)
                create_body.pop ("stopLoss", None)
                create_body.pop ("tpOrderType", None)
                create_body.pop ("slOrderType", None)
                create_result = await client.create_order (create_body)"""
    new_mkt = """            # Spot Market: sin TP/SL nativo — virtual TPSL vía AUTO_TPSL/outcomes
            create_result = await client.create_order (create_body)"""
    if old_mkt in text:
        text = text.replace(old_mkt, new_mkt)

    # _place_order body via nertz_orders
    old_place_start = """                order_type_raw = config.ORDER_TYPE or "Limit"
                order_type = {
                    "limit": "Limit",
                    "Limit": "Limit",
                    "market": "Market",
                    "Market": "Market",
                }.get (order_type_raw, "Limit")

                tif_raw = config.TIME_IN_FORCE or "GTC"
                time_in_force = {
                    "GoodTillCancel": "GTC",
                    "GTC": "GTC",
                    "ImmediateOrCancel": "IOC",
                    "IOC": "IOC",
                    "FillOrKill": "FOK",
                    "FOK": "FOK",
                    "PostOnly": "PostOnly",
                }.get (tif_raw, "GTC")
                if order_type == "Market":
                    time_in_force = "IOC"

                qty_str = self._format_decimal (self._quantize_to_step (quantity, qty_step, ROUND_DOWN))
                tp_str = self._format_decimal (self._quantize_to_step (tp, tick_size, ROUND_HALF_UP))
                sl_str = self._format_decimal (self._quantize_to_step (sl, tick_size, ROUND_HALF_UP))
                order_link_id = f"nertzh-{uuid.uuid4 ().hex[:20]}"

                body_params = {
                    "category": "spot",
                    "symbol": symbol,
                    "side": side,
                    "orderType": order_type,
                    "qty": qty_str,
                    "timeInForce": time_in_force,
                    "orderLinkId": order_link_id,
                }
                # Spot API no soporta takeProfit/stopLoss nativo en el payload de creación
                # Esto es manejado por el francotirador virtual o condicionales posteriores
                if order_type == "Limit":
                    price_str = self._format_decimal (self._quantize_to_step (price, tick_size, ROUND_HALF_UP))
                    body_params["price"] = price_str
                if order_type == "Market":
                    body_params["marketUnit"] = "baseCoin"
                result = await client.create_order (body_params)"""
    new_place_start = """                qty_str = self._format_decimal (self._quantize_to_step (quantity, qty_step, ROUND_DOWN))
                order_link_id = f"nertzh-{uuid.uuid4 ().hex[:20]}"
                price_str = None
                if (config.ORDER_TYPE or "Limit").lower () != "market":
                    price_str = self._format_decimal (self._quantize_to_step (price, tick_size, ROUND_HALF_UP))
                body_params = build_spot_create_body (
                    symbol=symbol,
                    side=side,
                    order_type=config.ORDER_TYPE or "Limit",
                    qty_str=qty_str,
                    order_link_id=order_link_id,
                    time_in_force=config.TIME_IN_FORCE or "GTC",
                    price_str=price_str,
                    attach_exchange_tpsl=False,
                )
                result = await client.create_order (body_params)"""
    if old_place_start in text:
        text = text.replace(old_place_start, new_place_start)

    NERTZH.write_text(text, encoding="utf-8")
    print("patched Nertzh.py")


if __name__ == "__main__":
    patch_symbols()
    patch_nertzh()