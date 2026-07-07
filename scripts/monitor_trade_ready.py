"""Poll Nertzh decisions endpoint and alert when a trade is imminent."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "http://127.0.0.1:8787"
SYMBOL = "BTCUSDT"
INTERVAL_S = 12.0
LOG_PATH = Path(__file__).resolve().parents[1] / "logs" / "trade_ready_monitor.log"
RESULTS_PATH = Path(__file__).resolve().parents[1] / "logs" / "results.json"


def _fetch_json(url: str) -> dict | None:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
        print(f"[ERR] {url} {exc}", flush=True)
        return None


def _fetch() -> dict | None:
    return _fetch_json(f"{BASE_URL}/api/decisions/{SYMBOL}")


def _fetch_ticker(symbol: str) -> float | None:
    data = _fetch_json(f"{BASE_URL}/api/ticker/{symbol}")
    if not isinstance(data, dict):
        return None
    try:
        return float(data.get("last_price") or 0.0)
    except (TypeError, ValueError):
        return None


def _read_last_trade() -> dict | None:
    try:
        raw = RESULTS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        lt = data.get("last_trade")
        return lt if isinstance(lt, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _log(line: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def _notify_windows(title: str, message: str) -> None:
    safe_title = title.replace("'", "''").replace('"', '`"')
    safe_msg = message.replace("'", "''").replace('"', '`"')
    ps = (
        "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, "
        "ContentType = WindowsRuntime] | Out-Null; "
        "$t = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent("
        "[Windows.UI.Notifications.ToastTemplateType]::ToastText02); "
        "$nodes = $t.GetElementsByTagName('text'); "
        f"$nodes.Item(0).AppendChild($t.CreateTextNode('{safe_title}')) | Out-Null; "
        f"$nodes.Item(1).AppendChild($t.CreateTextNode('{safe_msg}')) | Out-Null; "
        "$toast = [Windows.UI.Notifications.ToastNotification]::new($t); "
        "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Nertzh').Show($toast)"
    )
    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", ps],
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        try:
            import winsound

            winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        except Exception:
            pass


def main() -> int:
    symbol = sys.argv[1] if len(sys.argv) > 1 else SYMBOL
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else INTERVAL_S
    prev_decision = ""
    prev_would_trade = False
    tracked_order_id: str | None = None
    tracked_status = ""

    _log(f"=== monitor start {datetime.now(timezone.utc).isoformat()} symbol={symbol} ===")

    while True:
        data = _fetch()
        if not isinstance(data, dict):
            time.sleep(interval)
            continue

        detail = data.get("decision_detail") if isinstance(data.get("decision_detail"), dict) else {}
        gates = data.get("execution_gates") if isinstance(data.get("execution_gates"), dict) else {}
        decision = str(detail.get("decision") or "unknown")
        combined = float(detail.get("combined") or 0.0)
        mom = float(detail.get("mom") or 0.0)
        would_trade = bool(data.get("would_trade_on_next_kline"))
        blocked_by = gates.get("blocked_by")
        th = detail.get("thresholds_effective") if isinstance(detail.get("thresholds_effective"), dict) else {}
        buy_th = float(th.get("buy") or 0.0)
        conf = detail.get("confirmations") if isinstance(detail.get("confirmations"), dict) else {}
        blockers = detail.get("blockers_if_not_trading") or []

        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = (
            f"[{ts}] {decision.upper():4} combined={combined:.3f} mom={mom:.3f} "
            f"buy_th={buy_th:.2f} would_trade={would_trade} blocked={blocked_by}"
        )

        changed = decision != prev_decision or would_trade != prev_would_trade
        if changed or would_trade or decision in {"buy", "sell"}:
            _log(line)
            if blockers:
                _log(f"       blockers={blockers} ok_v2_buy={conf.get('ok_v2_buy')}")
        else:
            print(line, flush=True)

        if would_trade and not prev_would_trade:
            msg = "Trade inminente en cierre de vela 1m"
            _log(f"*** ALERTA: would_trade_on_next_kline=TRUE — {msg} ***")
            _notify_windows("Nertzh — Trade listo", f"{symbol}: {msg}")

        if decision in {"buy", "sell"} and decision != prev_decision:
            _log(f"*** ALERTA: decision={decision.upper()} detectada ***")
            _notify_windows(
                f"Nertzh — {decision.upper()}",
                f"{symbol} combined={combined:.2f} mom={mom:.2f}",
            )

        last_trade = _read_last_trade()
        if isinstance(last_trade, dict) and str(last_trade.get("symbol") or "") == symbol:
            oid = str(last_trade.get("order_id") or "")
            status = str(last_trade.get("outcome_status") or "unknown")
            entry = float(last_trade.get("entry_price") or 0.0)
            tp = float(last_trade.get("tp_price") or 0.0)
            sl = float(last_trade.get("sl_price") or 0.0)
            price = _fetch_ticker(symbol)

            if status in {"pending", "partial", "filled"} and oid:
                if tracked_order_id != oid:
                    tracked_order_id = oid
                    tracked_status = status
                    _log(
                        f"       TRADE ABIERTO id={oid} entry={entry:.1f} "
                        f"TP={tp:.1f} SL={sl:.1f}"
                    )
                if price and price > 0 and tp > 0 and sl > 0:
                    dist_tp = tp - price
                    dist_sl = price - sl
                    print(
                        f"       price={price:.1f} dist_TP={dist_tp:+.1f} dist_SL={dist_sl:+.1f}",
                        flush=True,
                    )

            if (
                tracked_order_id
                and oid == tracked_order_id
                and status == "final"
                and tracked_status != "final"
            ):
                pl = float(last_trade.get("profit_loss") or 0.0)
                exit_px = float(last_trade.get("exit_price") or 0.0)
                action = str(last_trade.get("action") or "").upper()
                win = "WIN" if pl > 0 else "LOSS" if pl < 0 else "FLAT"
                _log(
                    f"*** TRADE CERRADO {win} {action} pl={pl:+.4f} USDT "
                    f"entry={entry:.1f} exit={exit_px:.1f} ***"
                )
                _notify_windows(
                    f"Nertzh — Trade {win}",
                    f"{symbol} PNL={pl:+.2f} USDT (entry {entry:.0f} → exit {exit_px:.0f})",
                )
                tracked_status = "final"
                tracked_order_id = None

        prev_decision = decision
        prev_would_trade = would_trade
        time.sleep(interval)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        _log("=== monitor stop ===")
        raise SystemExit(0)