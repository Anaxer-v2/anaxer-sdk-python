"""WebSocket client behavior against an in-memory mock transport."""

from __future__ import annotations

import asyncio

import pytest

import anaxer
from anaxer import AnaxerError, CreatedV1, SwapV1
from anaxer._config import Reconnect
from mock_ws import MockWsServer

SAMPLE_SWAP = {
    "type": "swap",
    "source": "pump_fun",
    "wallet": "Wallet1111111111111111111111111111111111111",
    "volumeUsd": 12.5,
    "swap": {
        "from": {
            "mint": "So11111111111111111111111111111111111111112",
            "amount": "1000000000",
            "uiAmount": 1.0,
            "decimals": 9,
        },
        "to": {
            "mint": "3PFaeFMXiRVYueDCnHHSKndKDDpKZhbhFjQ13JXYpump",
            "amount": "1000",
            "uiAmount": 1000.0,
            "decimals": 6,
        },
    },
    "signature": "sig",
    "slot": 1,
    "timestamp": 1700000000,
}


async def _wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timeout waiting for condition")


@pytest.mark.asyncio
async def test_handshake_connected_info_and_bearer() -> None:
    server = MockWsServer()
    async with anaxer.connect(
        "key",
        base_url="http://localhost:3010",
        reconnect=False,
        _open_transport_fn=server.open_transport,
    ) as client:
        assert client.connected.connections_per_stream == 2
        assert client.connected.trades_requires_mints is False
        assert client.connected.trades_mint_cap == 10
        assert server.headers.get("Authorization") == "Bearer key"


@pytest.mark.asyncio
async def test_malformed_connected_raises_from_aenter() -> None:
    server = MockWsServer(auto_connected=False)

    async def on_connect(s: MockWsServer) -> None:
        await s.send_connected(malformed=True)

    server._on_connect = on_connect
    with pytest.raises(AnaxerError) as ei:
        async with anaxer.connect(
            "key",
            reconnect=False,
            _open_transport_fn=server.open_transport,
        ):
            pass
    assert ei.value.code == "internal_error"


@pytest.mark.asyncio
async def test_stream_trades_sol_only_and_typed_events() -> None:
    server = MockWsServer()

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
            await s.send(
                {
                    "v": 1,
                    "channel": "trades",
                    "type": "swap",
                    "sub": msg["id"],
                    "ts": 1,
                    "data": SAMPLE_SWAP,
                }
            )
            await s.send(
                {
                    "v": 1,
                    "channel": "trades",
                    "type": "swap",
                    "sub": "other-sub",
                    "ts": 1,
                    "data": SAMPLE_SWAP,
                }
            )

    server.on_message(on_msg)

    async with anaxer.connect(
        "key",
        reconnect=False,
        _open_transport_fn=server.open_transport,
    ) as client:
        sub = client.stream("trades", sol_only=True)
        await _wait_for(lambda: any(m.get("type") == "subscribe" for m in server.received))
        subscribe = next(m for m in server.received if m.get("type") == "subscribe")
        assert subscribe["filters"].get("solOnly") is True
        event = await sub.__anext__()
        assert isinstance(event, SwapV1)
        assert event.swap.from_.mint.startswith("So1111")
        # Different sub must not surface
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.__anext__(), timeout=0.1)
        await sub.aclose()


@pytest.mark.asyncio
async def test_subscription_fatal_isolates_other_streams() -> None:
    server = MockWsServer()

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

    async with anaxer.connect(
        "key",
        reconnect=False,
        _open_transport_fn=server.open_transport,
    ) as client:
        bad = client.stream("trades")
        good = client.stream("creations")
        await _wait_for(lambda: len([m for m in server.received if m.get("type") == "subscribe"]) >= 2)
        bad_id = next(
            m["id"]
            for m in server.received
            if m.get("type") == "subscribe" and m.get("channel") == "trades"
        )
        await server.send(
            {
                "v": 1,
                "type": "error",
                "code": "invalid_filters",
                "message": "bad",
                "id": bad_id,
                "ts": 1,
            }
        )
        with pytest.raises(AnaxerError) as ei:
            await bad.__anext__()
        assert ei.value.code == "invalid_filters"
        assert bad.closed is True
        # good subscription still open — push a creation
        good_id = next(
            m["id"]
            for m in server.received
            if m.get("type") == "subscribe" and m.get("channel") == "creations"
        )
        await server.send(
            {
                "v": 1,
                "channel": "creations",
                "type": "created",
                "sub": good_id,
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
        created = await asyncio.wait_for(good.__anext__(), timeout=1)
        assert isinstance(created, CreatedV1)
        await good.aclose()


@pytest.mark.asyncio
async def test_unauthorized_terminal() -> None:
    server = MockWsServer()
    errors: list[AnaxerError] = []

    async with anaxer.connect(
        "bad",
        reconnect=Reconnect(max_retries=0),
        _open_transport_fn=server.open_transport,
    ) as client:
        client.on_error(errors.append)
        sub = client.stream("creations")
        await server.send(
            {
                "v": 1,
                "type": "error",
                "code": "unauthorized",
                "message": "bad key",
                "ts": 1,
            }
        )
        with pytest.raises(AnaxerError) as ei:
            await sub.__anext__()
        assert ei.value.code == "unauthorized"
        await _wait_for(lambda: any(e.code == "unauthorized" for e in errors))
        # no reconnect
        await asyncio.sleep(0.05)
        assert server.open_count == 1


@pytest.mark.asyncio
async def test_aclose_and_break_send_unsubscribe() -> None:
    server = MockWsServer()

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
            await s.send(
                {
                    "v": 1,
                    "channel": "creations",
                    "type": "created",
                    "sub": msg["id"],
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

    server.on_message(on_msg)

    async with anaxer.connect(
        "key",
        reconnect=False,
        _open_transport_fn=server.open_transport,
    ) as client:
        async with client.stream("creations") as sub:
            async for _event in sub:
                break
        await _wait_for(lambda: any(m.get("type") == "unsubscribe" for m in server.received))


@pytest.mark.asyncio
async def test_invalid_payload_side_channel() -> None:
    server = MockWsServer()
    seen: list[AnaxerError] = []

    async with anaxer.connect(
        "key",
        reconnect=False,
        _open_transport_fn=server.open_transport,
    ) as client:
        client.on_error(seen.append)
        sub = client.stream("trades")
        await _wait_for(lambda: any(m.get("type") == "subscribe" for m in server.received))
        sub_id = next(m["id"] for m in server.received if m.get("type") == "subscribe")
        await server.send_raw("not-json")
        await server.send(
            {
                "v": 1,
                "channel": "trades",
                "type": "swap",
                "sub": sub_id,
                "ts": 1,
                "data": {"type": "swap", "broken": True},
            }
        )
        await _wait_for(lambda: any(e.code == "invalid_payload" for e in seen))
        assert sub.closed is False
        await sub.aclose()
