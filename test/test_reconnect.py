"""Reconnect / heartbeat / slow_consumer side-channel behavior."""

from __future__ import annotations

import asyncio

import pytest

import anaxer
from anaxer import AnaxerError
from anaxer._config import Reconnect
from mock_ws import MockWsServer


async def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timeout waiting for condition")


@pytest.mark.asyncio
async def test_reconnect_resends_active_subs() -> None:
    servers: list[MockWsServer] = []
    open_n = {"n": 0}

    async def open_transport(url: str, headers: dict[str, str]):
        open_n["n"] += 1
        server = MockWsServer()
        servers.append(server)

        async def on_msg(s: MockWsServer, msg: dict) -> None:
            if msg.get("type") == "subscribe":
                await s.send(
                    {
                        "v": 1,
                        "type": "subscribed",
                        "id": msg["id"],
                        "channel": msg["channel"],
                        "filters": msg.get("filters", {}),
                        "ts": 1,
                    }
                )

        server.on_message(on_msg)
        return await server.open_transport(url, headers)

    async with anaxer.connect(
        "key",
        reconnect=Reconnect(base_delay=0.05, max_delay=0.1),
        heartbeat_interval=0,
        _open_transport_fn=open_transport,
    ) as client:
        sub = client.stream("creations", exclude_mayhem=True)
        await _wait_for(lambda: open_n["n"] >= 1 and any(
            m.get("type") == "subscribe" for m in servers[0].received
        ))
        first_sub = next(m for m in servers[0].received if m.get("type") == "subscribe")
        await servers[0].kill()
        await _wait_for(lambda: open_n["n"] >= 2)
        await _wait_for(
            lambda: any(m.get("type") == "subscribe" for m in servers[-1].received)
        )
        resent = next(m for m in servers[-1].received if m.get("type") == "subscribe")
        assert resent["id"] == first_sub["id"]
        assert resent["filters"].get("excludeMayhem") is True
        # data channel uninterrupted — push event on new connection
        await servers[-1].send(
            {
                "v": 1,
                "channel": "creations",
                "type": "created",
                "sub": resent["id"],
                "ts": 1,
                "data": {
                    "type": "created",
                    "source": "pump_fun",
                    "mint": "Mint111111111111111111111111111111111111111",
                    "name": "X",
                    "symbol": "X",
                    "creator": None,
                    "uri": None,
                    "mayhemMode": False,
                    "socials": {"website": None, "twitter": None, "telegram": None},
                    "signature": "s",
                    "slot": 1,
                    "timestamp": 1,
                },
            }
        )
        event = await asyncio.wait_for(sub.__anext__(), timeout=1)
        assert event.symbol == "X"
        await sub.aclose()


@pytest.mark.asyncio
async def test_reconnect_false_does_not_reconnect() -> None:
    server = MockWsServer()
    async with anaxer.connect(
        "key",
        reconnect=False,
        heartbeat_interval=0,
        _open_transport_fn=server.open_transport,
    ) as client:
        sub = client.stream("creations")
        await _wait_for(lambda: any(m.get("type") == "subscribe" for m in server.received))
        await server.kill()
        with pytest.raises(AnaxerError) as ei:
            await sub.__anext__()
        assert ei.value.code == "connection_closed"
        await asyncio.sleep(0.1)
        assert server.open_count == 1


@pytest.mark.asyncio
async def test_slow_consumer_side_channel_before_reconnect() -> None:
    order: list[str] = []
    servers: list[MockWsServer] = []
    open_n = {"n": 0}

    async def open_transport(url: str, headers: dict[str, str]):
        open_n["n"] += 1
        server = MockWsServer()
        servers.append(server)

        async def on_msg(s: MockWsServer, msg: dict) -> None:
            if msg.get("type") == "subscribe":
                await s.send(
                    {
                        "v": 1,
                        "type": "subscribed",
                        "id": msg["id"],
                        "channel": msg["channel"],
                        "filters": {},
                        "ts": 1,
                    }
                )

        server.on_message(on_msg)
        return await server.open_transport(url, headers)

    async with anaxer.connect(
        "key",
        reconnect=Reconnect(base_delay=0.05, max_delay=0.1),
        heartbeat_interval=0,
        _open_transport_fn=open_transport,
    ) as client:
        client.on_error(lambda e: order.append(f"error:{e.code}"))
        sub = client.stream("creations")
        await _wait_for(lambda: open_n["n"] >= 1 and servers[0].received)
        await servers[0].send(
            {
                "v": 1,
                "type": "error",
                "code": "slow_consumer",
                "message": "too slow",
                "ts": 1,
            }
        )
        await servers[0].kill()
        await _wait_for(lambda: open_n["n"] >= 2)
        assert order[0] == "error:slow_consumer"
        # data channel still works after reconnect
        await _wait_for(
            lambda: any(m.get("type") == "subscribe" for m in servers[-1].received)
        )
        sub_id = next(m["id"] for m in servers[-1].received if m.get("type") == "subscribe")
        await servers[-1].send(
            {
                "v": 1,
                "channel": "creations",
                "type": "created",
                "sub": sub_id,
                "ts": 1,
                "data": {
                    "type": "created",
                    "source": "pump_fun",
                    "mint": "Mint111111111111111111111111111111111111111",
                    "name": "X",
                    "symbol": "X",
                    "creator": None,
                    "uri": None,
                    "mayhemMode": False,
                    "socials": {"website": None, "twitter": None, "telegram": None},
                    "signature": "s",
                    "slot": 1,
                    "timestamp": 1,
                },
            }
        )
        event = await asyncio.wait_for(sub.__anext__(), timeout=1)
        assert event.mint is not None
        await sub.aclose()


@pytest.mark.asyncio
async def test_heartbeat_timeout_forces_reconnect() -> None:
    open_n = {"n": 0}
    servers: list[MockWsServer] = []

    async def open_transport(url: str, headers: dict[str, str]):
        open_n["n"] += 1
        server = MockWsServer()
        servers.append(server)
        return await server.open_transport(url, headers)

    async with anaxer.connect(
        "key",
        reconnect=Reconnect(base_delay=0.05, max_delay=0.1),
        heartbeat_interval=0.05,
        _open_transport_fn=open_transport,
    ) as client:
        _ = client  # entered
        await _wait_for(lambda: open_n["n"] >= 2, timeout=2.0)


@pytest.mark.asyncio
async def test_subscription_fatal_not_resubscribed_after_reconnect() -> None:
    """R2: invalid_filters must leave the sub dead across an unrelated reconnect."""
    servers: list[MockWsServer] = []
    open_n = {"n": 0}

    async def open_transport(url: str, headers: dict[str, str]):
        open_n["n"] += 1
        server = MockWsServer()
        servers.append(server)

        async def on_msg(s: MockWsServer, msg: dict) -> None:
            if msg.get("type") == "subscribe":
                await s.send(
                    {
                        "v": 1,
                        "type": "subscribed",
                        "id": msg["id"],
                        "channel": msg["channel"],
                        "filters": msg.get("filters", {}),
                        "ts": 1,
                    }
                )

        server.on_message(on_msg)
        return await server.open_transport(url, headers)

    async with anaxer.connect(
        "key",
        reconnect=Reconnect(base_delay=0.05, max_delay=0.1),
        heartbeat_interval=0,
        _open_transport_fn=open_transport,
    ) as client:
        bad = client.stream("trades")
        keep = client.stream("creations")
        await _wait_for(lambda: open_n["n"] >= 1)
        await _wait_for(
            lambda: len([m for m in servers[0].received if m.get("type") == "subscribe"]) >= 2
        )
        bad_id = next(
            m["id"]
            for m in servers[0].received
            if m.get("type") == "subscribe" and m.get("channel") == "trades"
        )
        keep_id = next(
            m["id"]
            for m in servers[0].received
            if m.get("type") == "subscribe" and m.get("channel") == "creations"
        )
        await servers[0].send(
            {
                "v": 1,
                "type": "error",
                "code": "invalid_filters",
                "message": "bad filters",
                "id": bad_id,
                "ts": 1,
            }
        )
        with pytest.raises(AnaxerError) as ei:
            await bad.__anext__()
        assert ei.value.code == "invalid_filters"
        assert bad.closed is True

        await servers[0].kill()
        await _wait_for(lambda: open_n["n"] >= 2)
        await _wait_for(
            lambda: any(m.get("type") == "subscribe" for m in servers[-1].received)
        )
        resent_ids = [
            m["id"] for m in servers[-1].received if m.get("type") == "subscribe"
        ]
        assert bad_id not in resent_ids
        assert keep_id in resent_ids
        await keep.aclose()
