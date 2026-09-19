"""REST client tests with httpx.MockTransport."""

from __future__ import annotations

import httpx
import pytest

from anaxer import AnaxerError, Client, connect


def _router(request: httpx.Request) -> httpx.Response:
    assert request.headers.get("Authorization") == "Bearer test-key"
    path = request.url.path
    if path.endswith("/missing"):
        return httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "gone"}},
        )
    if path == "/v1/tokens/batch":
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "mint": "Mint111111111111111111111111111111111111111",
                        "name": "A",
                        "symbol": "A",
                        "supply": "1",
                        "socials": {"website": None, "twitter": None, "telegram": None},
                    }
                ]
            },
        )
    if path.endswith("/price") and "/batch/" not in path:
        return httpx.Response(
            200,
            json={
                "type": "price",
                "source": "pump_fun",
                "mint": "Mint111111111111111111111111111111111111111",
                "price": {"sol": 1.0, "usd": 150.0},
                "marketCapUsd": 100.0,
                "slot": 1,
                "timestamp": 1,
            },
        )
    if "/trades" in path:
        params = dict(request.url.params)
        assert params.get("solOnly") == "true"
        cursor = params.get("cursor")
        if cursor == "page2":
            return httpx.Response(
                200,
                json={
                    "data": [],
                    "next": None,
                    "window": {"from": 1, "to": 2},
                },
            )
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "type": "swap",
                        "source": "pump_fun",
                        "wallet": "Wallet1111111111111111111111111111111111111",
                        "volumeUsd": 1.0,
                        "swap": {
                            "from": {
                                "mint": "So11111111111111111111111111111111111111112",
                                "amount": "1",
                                "uiAmount": 1.0,
                                "decimals": 9,
                            },
                            "to": {
                                "mint": "Mint111111111111111111111111111111111111111",
                                "amount": "1",
                                "uiAmount": 1.0,
                                "decimals": 6,
                            },
                        },
                        "signature": "s",
                        "slot": 1,
                        "timestamp": 1,
                    }
                ],
                "next": "page2",
                "window": {"from": 1, "to": 2},
            },
        )
    if path == "/v1/creations":
        params = dict(request.url.params)
        if params.get("cursor") == "c2":
            return httpx.Response(
                200,
                json={"data": [], "next": None, "window": {"from": 1, "to": 2}},
            )
        return httpx.Response(
            200,
            json={
                "data": [
                    {
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
                    }
                ],
                "next": "c2",
                "window": {"from": 1, "to": 2},
            },
        )
    if path == "/v1/rate":
        return httpx.Response(
            429,
            headers={"Retry-After": "0"},
            json={"error": {"code": "rate_limited", "message": "slow down"}},
        )
    if path == "/v1/boom":
        return httpx.Response(
            500,
            json={"error": {"code": "internal", "message": "boom"}},
        )
    if path == "/v1/bad":
        return httpx.Response(
            400,
            json={"error": {"code": "invalid_request", "message": "nope"}},
        )
    return httpx.Response(404, json={"error": {"code": "not_found", "message": path}})


def test_sync_client_auth_and_price() -> None:
    transport = httpx.MockTransport(_router)
    client = Client("test-key", http_transport=transport)
    price = client.tokens.price("Mint111111111111111111111111111111111111111")
    assert price.price.usd == 150.0
    with pytest.raises(AnaxerError) as ei:
        client.tokens.get("missing")
    assert ei.value.code == "not_found"
    assert ei.value.status == 404
    client.close()


def test_sync_iter_creations_and_trades_sol_only() -> None:
    transport = httpx.MockTransport(_router)
    client = Client("test-key", http_transport=transport)
    items = list(client.iter_creations())
    assert len(items) == 1
    trades = list(
        client.iter_trades("Mint111111111111111111111111111111111111111", sol_only=True)
    )
    assert len(trades) == 1
    client.close()


def test_sync_retries_429_then_raises() -> None:
    calls = {"n": 0}

    def router(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            429,
            headers={"Retry-After": "0"},
            json={"error": {"code": "rate_limited", "message": "slow"}},
        )

    client = Client("test-key", rest_max_retries=2, http_transport=httpx.MockTransport(router))
    with pytest.raises(AnaxerError) as ei:
        client.tokens.get("x")
    assert ei.value.code == "rate_limited"
    assert calls["n"] == 3  # initial + 2 retries
    client.close()


def test_sync_no_retry_on_400() -> None:
    calls = {"n": 0}

    def router(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            400,
            json={"error": {"code": "invalid_request", "message": "nope"}},
        )

    client = Client("test-key", rest_max_retries=2, http_transport=httpx.MockTransport(router))
    with pytest.raises(AnaxerError) as ei:
        client.tokens.get("x")
    assert ei.value.code == "invalid_request"
    assert calls["n"] == 1
    client.close()


@pytest.mark.asyncio
async def test_async_client_rest_bearer() -> None:
    # Async REST without needing a real WS: use a no-op open that still "connects"
    from mock_ws import MockWsServer

    server = MockWsServer()
    transport = httpx.MockTransport(_router)
    async with connect(
        "test-key",
        reconnect=False,
        _http_transport=transport,
        _open_transport_fn=server.open_transport,
    ) as client:
        price = await client.tokens.price("Mint111111111111111111111111111111111111111")
        assert price.price.sol == 1.0
        page = await client.tokens.trades(
            "Mint111111111111111111111111111111111111111", sol_only=True
        )
        assert len(page.data) == 1
        batch = await client.tokens.batch(["Mint111111111111111111111111111111111111111"])
        assert len(batch) == 1
