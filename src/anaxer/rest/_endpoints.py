"""REST /v1 endpoint methods for sync and async clients."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

from anaxer.models import (
    CreatedV1,
    GraduatedV1,
    LaunchpadStatsV1,
    PriceUpdateV1,
    SwapV1,
    TokenMetadataV1,
)
from anaxer.rest._http import AsyncHttpClient, QueryValue, SyncHttpClient
from anaxer.rest._pagination import Page, async_iter_pages, page_from_envelope, sync_iter_pages

TimeInput = int | str


class TokensAsyncApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def get(self, mint: str) -> TokenMetadataV1:
        body = await self._http.get_json(f"/v1/tokens/{mint}")
        return TokenMetadataV1.model_validate(body)

    async def batch(self, mints: list[str]) -> list[TokenMetadataV1]:
        body = await self._http.get_json("/v1/tokens/batch", {"mints": ",".join(mints)})
        return [TokenMetadataV1.model_validate(x) for x in body["data"]]

    async def price(self, mint: str) -> PriceUpdateV1:
        body = await self._http.get_json(f"/v1/tokens/{mint}/price")
        return PriceUpdateV1.model_validate(body)

    async def batch_price(self, mints: list[str]) -> list[PriceUpdateV1]:
        body = await self._http.get_json("/v1/tokens/batch/price", {"mints": ",".join(mints)})
        return [PriceUpdateV1.model_validate(x) for x in body["data"]]

    async def trades(
        self,
        mint: str,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        source: str | None = None,
        sol_only: bool | None = None,
    ) -> Page[SwapV1]:
        return await _async_list_page(
            self._http,
            f"/v1/tokens/{mint}/trades",
            SwapV1,
            {
                "limit": limit,
                "cursor": cursor,
                "from": from_,
                "to": to,
                "source": source,
                "solOnly": sol_only,
            },
        )


class LaunchpadsAsyncApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def stats(self, window_hours: int | None = None) -> LaunchpadStatsV1:
        body = await self._http.get_json(
            "/v1/launchpads/stats", {"windowHours": window_hours}
        )
        return LaunchpadStatsV1.model_validate(body)


class TokensApi:
    def __init__(self, http: SyncHttpClient) -> None:
        self._http = http

    def get(self, mint: str) -> TokenMetadataV1:
        body = self._http.get_json(f"/v1/tokens/{mint}")
        return TokenMetadataV1.model_validate(body)

    def batch(self, mints: list[str]) -> list[TokenMetadataV1]:
        body = self._http.get_json("/v1/tokens/batch", {"mints": ",".join(mints)})
        return [TokenMetadataV1.model_validate(x) for x in body["data"]]

    def price(self, mint: str) -> PriceUpdateV1:
        body = self._http.get_json(f"/v1/tokens/{mint}/price")
        return PriceUpdateV1.model_validate(body)

    def batch_price(self, mints: list[str]) -> list[PriceUpdateV1]:
        body = self._http.get_json("/v1/tokens/batch/price", {"mints": ",".join(mints)})
        return [PriceUpdateV1.model_validate(x) for x in body["data"]]

    def trades(
        self,
        mint: str,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        source: str | None = None,
        sol_only: bool | None = None,
    ) -> Page[SwapV1]:
        return _sync_list_page(
            self._http,
            f"/v1/tokens/{mint}/trades",
            SwapV1,
            {
                "limit": limit,
                "cursor": cursor,
                "from": from_,
                "to": to,
                "source": source,
                "solOnly": sol_only,
            },
        )


class LaunchpadsApi:
    def __init__(self, http: SyncHttpClient) -> None:
        self._http = http

    def stats(self, window_hours: int | None = None) -> LaunchpadStatsV1:
        body = self._http.get_json("/v1/launchpads/stats", {"windowHours": window_hours})
        return LaunchpadStatsV1.model_validate(body)


async def _async_list_page(
    http: AsyncHttpClient,
    path: str,
    item_model: Any,
    base_query: dict[str, QueryValue],
) -> Page[Any]:
    body = await http.get_json(path, base_query)
    return page_from_envelope(body, item_model)


def _sync_list_page(
    http: SyncHttpClient,
    path: str,
    item_model: Any,
    base_query: dict[str, QueryValue],
) -> Page[Any]:
    body = http.get_json(path, base_query)
    return page_from_envelope(body, item_model)


async def async_creations(
    http: AsyncHttpClient,
    *,
    limit: int | None = None,
    cursor: str | None = None,
    from_: TimeInput | None = None,
    to: TimeInput | None = None,
    source: str | None = None,
    exclude_mayhem: bool | None = None,
) -> Page[CreatedV1]:
    return await _async_list_page(
        http,
        "/v1/creations",
        CreatedV1,
        {
            "limit": limit,
            "cursor": cursor,
            "from": from_,
            "to": to,
            "source": source,
            "excludeMayhem": exclude_mayhem,
        },
    )


async def async_graduations(
    http: AsyncHttpClient,
    *,
    limit: int | None = None,
    cursor: str | None = None,
    from_: TimeInput | None = None,
    to: TimeInput | None = None,
    source: str | None = None,
    exclude_mayhem: bool | None = None,
    min_liquidity_sol: float | None = None,
) -> Page[GraduatedV1]:
    return await _async_list_page(
        http,
        "/v1/graduations",
        GraduatedV1,
        {
            "limit": limit,
            "cursor": cursor,
            "from": from_,
            "to": to,
            "source": source,
            "excludeMayhem": exclude_mayhem,
            "minLiquiditySol": min_liquidity_sol,
        },
    )


def sync_creations(http: SyncHttpClient, **opts: Any) -> Page[CreatedV1]:
    return _sync_list_page(
        http,
        "/v1/creations",
        CreatedV1,
        {
            "limit": opts.get("limit"),
            "cursor": opts.get("cursor"),
            "from": opts.get("from_"),
            "to": opts.get("to"),
            "source": opts.get("source"),
            "excludeMayhem": opts.get("exclude_mayhem"),
        },
    )


def sync_graduations(http: SyncHttpClient, **opts: Any) -> Page[GraduatedV1]:
    return _sync_list_page(
        http,
        "/v1/graduations",
        GraduatedV1,
        {
            "limit": opts.get("limit"),
            "cursor": opts.get("cursor"),
            "from": opts.get("from_"),
            "to": opts.get("to"),
            "source": opts.get("source"),
            "excludeMayhem": opts.get("exclude_mayhem"),
            "minLiquiditySol": opts.get("min_liquidity_sol"),
        },
    )


async def iter_creations_async(http: AsyncHttpClient, **opts: Any) -> AsyncIterator[CreatedV1]:
    first = await async_creations(http, **opts)
    base = {k: v for k, v in opts.items()}

    async def fetch(cursor: str) -> Page[CreatedV1]:
        return await async_creations(http, **{**base, "cursor": cursor})

    async for item in async_iter_pages(first, fetch):
        yield item


async def iter_graduations_async(http: AsyncHttpClient, **opts: Any) -> AsyncIterator[GraduatedV1]:
    first = await async_graduations(http, **opts)
    base = {k: v for k, v in opts.items()}

    async def fetch(cursor: str) -> Page[GraduatedV1]:
        return await async_graduations(http, **{**base, "cursor": cursor})

    async for item in async_iter_pages(first, fetch):
        yield item


async def iter_trades_async(
    http: AsyncHttpClient, mint: str, **opts: Any
) -> AsyncIterator[SwapV1]:
    tokens = TokensAsyncApi(http)
    first = await tokens.trades(mint, **opts)
    base = {k: v for k, v in opts.items()}

    async def fetch(cursor: str) -> Page[SwapV1]:
        return await tokens.trades(mint, **{**base, "cursor": cursor})

    async for item in async_iter_pages(first, fetch):
        yield item


def iter_creations_sync(http: SyncHttpClient, **opts: Any) -> Iterator[CreatedV1]:
    first = sync_creations(http, **opts)
    base = dict(opts)

    def fetch(cursor: str) -> Page[CreatedV1]:
        return sync_creations(http, **{**base, "cursor": cursor})

    yield from sync_iter_pages(first, fetch)


def iter_graduations_sync(http: SyncHttpClient, **opts: Any) -> Iterator[GraduatedV1]:
    first = sync_graduations(http, **opts)
    base = dict(opts)

    def fetch(cursor: str) -> Page[GraduatedV1]:
        return sync_graduations(http, **{**base, "cursor": cursor})

    yield from sync_iter_pages(first, fetch)


def iter_trades_sync(http: SyncHttpClient, mint: str, **opts: Any) -> Iterator[SwapV1]:
    tokens = TokensApi(http)
    first = tokens.trades(mint, **opts)
    base = dict(opts)

    def fetch(cursor: str) -> Page[SwapV1]:
        return tokens.trades(mint, **{**base, "cursor": cursor})

    yield from sync_iter_pages(first, fetch)
