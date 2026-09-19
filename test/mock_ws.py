"""In-memory mock WS transport for SDK tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from anaxer.ws._transport import ConnectionClosed

_CLOSE = object()


class MockTransport:
    def __init__(self, server: MockWsServer) -> None:
        self._server = server
        self._closed = False
        self._inbox: asyncio.Queue[object] = asyncio.Queue()
        server._attach(self)

    async def send(self, data: str) -> None:
        if self._closed:
            raise ConnectionClosed(1000, "closed")
        await self._server._on_client_message(data)

    async def recv(self) -> str:
        item = await self._inbox.get()
        if item is _CLOSE:
            raise ConnectionClosed(1000, "closed")
        assert isinstance(item, str)
        return item

    async def close(self, code: int = 1000) -> None:
        if self._closed:
            return
        self._closed = True
        self._inbox.put_nowait(_CLOSE)
        await self._server._on_client_close(code)

    def _push(self, data: str) -> None:
        if not self._closed:
            self._inbox.put_nowait(data)


Handler = Callable[["MockWsServer", dict[str, Any]], Awaitable[None] | None]


class MockWsServer:
    def __init__(
        self,
        *,
        on_connect: Callable[[MockWsServer], Awaitable[None] | None] | None = None,
        auto_connected: bool = True,
        limits: dict[str, Any] | None = None,
    ) -> None:
        self.received: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}
        self._transport: MockTransport | None = None
        self._on_connect = on_connect
        self._auto_connected = auto_connected
        self._limits = limits or {
            "connectionsPerStream": 2,
            "tradesRequiresMints": False,
            "tradesMintCap": 10,
        }
        self._message_handlers: list[Handler] = []
        self.closed_codes: list[int] = []
        self.open_count = 0

    def on_message(self, handler: Handler) -> None:
        self._message_handlers.append(handler)

    def _attach(self, transport: MockTransport) -> None:
        self._transport = transport

    async def open_transport(self, url: str, headers: dict[str, str]) -> MockTransport:
        self.headers = dict(headers)
        self.open_count += 1
        transport = MockTransport(self)
        if self._on_connect is not None:
            result = self._on_connect(self)
            if asyncio.iscoroutine(result):
                await result
        elif self._auto_connected:
            await self.send_connected()
        return transport

    async def send_connected(self, *, malformed: bool = False) -> None:
        if malformed:
            await self.send({"v": 1, "type": "connected", "ts": 1, "limits": {}})
            return
        await self.send(
            {
                "v": 1,
                "type": "connected",
                "ts": 1_700_000_000_000,
                "limits": self._limits,
            }
        )

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._transport is not None
        self._transport._push(json.dumps(payload))

    async def send_raw(self, raw: str) -> None:
        assert self._transport is not None
        self._transport._push(raw)

    async def _on_client_message(self, data: str) -> None:
        msg = json.loads(data)
        self.received.append(msg)
        for handler in self._message_handlers:
            result = handler(self, msg)
            if asyncio.iscoroutine(result):
                await result

    async def _on_client_close(self, code: int) -> None:
        self.closed_codes.append(code)

    async def kill(self) -> None:
        if self._transport is not None and not self._transport._closed:
            self._transport._closed = True
            self._transport._inbox.put_nowait(_CLOSE)
