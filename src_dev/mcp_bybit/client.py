"""Cliente MCP stdio mínimo para bybit-official-trading-server."""
from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional


def _resolve_cmd(command: List[str]) -> List[str]:
    if not command:
        return command
    exe = shutil.which(command[0])
    if exe:
        return [exe, *command[1:]]
    return command


class McpBybitClient:
    def __init__(
        self,
        *,
        command: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        startup_timeout_s: float = 90.0,
    ):
        self._cmd = _resolve_cmd(command or ["npx", "-y", "bybit-official-trading-server@latest"])
        self._env = {**os.environ, **(env or {})}
        self._startup_timeout_s = startup_timeout_s
        self._proc: Optional[subprocess.Popen[str]] = None
        self._rx: queue.Queue[Dict[str, Any]] = queue.Queue()
        self._reader: Optional[threading.Thread] = None
        self._next_id = 1

    def start(self) -> None:
        if self._proc is not None:
            return
        self._proc = subprocess.Popen(
            self._cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._env,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert self._proc.stdout is not None
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()
        self._request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "src_dev-mcp-validator", "version": "1.0.0"},
            },
            timeout_s=self._startup_timeout_s,
        )
        self._notify("notifications/initialized", {})

    def close(self) -> None:
        if self._proc is None:
            return
        try:
            if self._proc.stdin:
                self._proc.stdin.close()
        except Exception:
            pass
        try:
            self._proc.terminate()
            self._proc.wait(timeout=5)
        except Exception:
            try:
                self._proc.kill()
            except Exception:
                pass
        self._proc = None

    def list_tools(self) -> List[Dict[str, Any]]:
        result = self._request("tools/list", {}, timeout_s=60.0)
        tools = result.get("tools") or []
        if not isinstance(tools, list):
            raise RuntimeError(f"tools/list unexpected payload: {result!r}")
        return tools

    def call_tool(self, name: str, arguments: Dict[str, Any], *, timeout_s: float = 30.0) -> Dict[str, Any]:
        return self._request("tools/call", {"name": name, "arguments": arguments}, timeout_s=timeout_s)

    def _read_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for line in self._proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, dict):
                self._rx.put(msg)

    def _notify(self, method: str, params: Dict[str, Any]) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def _request(self, method: str, params: Dict[str, Any], *, timeout_s: float) -> Dict[str, Any]:
        req_id = self._next_id
        self._next_id += 1
        self._send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                msg = self._rx.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                continue
            if msg.get("id") == req_id:
                if "error" in msg:
                    err = msg["error"]
                    raise RuntimeError(f"MCP error {method}: {err}")
                result = msg.get("result")
                if isinstance(result, dict):
                    return result
                return {"value": result}
        raise TimeoutError(f"MCP timeout waiting for {method}")

    def _send(self, payload: Dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("MCP process not started")
        self._proc.stdin.write(json.dumps(payload) + "\n")
        self._proc.stdin.flush()