"""Sole ``websockets`` import — transport seam (doc 22a §4.4)."""

from __future__ import annotations

from typing import Protocol

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed as _WsConnectionClosed


class ConnectionClosed(Exception):
    """Transport-level close signal for the state machine."""

    def __init__(self, code: int = 1000, reason: str = "") -> None:
        super().__init__(reason or f"connection closed ({code})")
        self.code = code
        self.reason = reason


class Transport(Protocol):
    async def send(self, data: str) -> None: ...

    async def recv(self) -> str: ...

    async def close(self, code: int = 1000) -> None: ...


class _WebsocketsTransport:
    def __init__(self, ws: WebSocketClientProtocol) -> None:
        self._ws = ws

    async def send(self, data: str) -> None:
        await self._ws.send(data)

    async def recv(self) -> str:
        try:
            raw = await self._ws.recv()
        except _WsConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd is not None else 1000
            reason = exc.rcvd.reason if exc.rcvd is not None else ""
            raise ConnectionClosed(code, reason) from exc
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        return raw

    async def close(self, code: int = 1000) -> None:
        await self._ws.close(code=code)


async def open_transport(url: str, headers: dict[str, str]) -> Transport:
    # websockets>=12,<14 uses ``extra_headers`` (renamed in v14).
    ws = await websockets.connect(url, extra_headers=headers)
    return _WebsocketsTransport(ws)


__all__ = ["ConnectionClosed", "Transport", "open_transport"]
