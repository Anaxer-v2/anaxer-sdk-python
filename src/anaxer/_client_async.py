"""AsyncClient: eager WS + REST namespaces."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any, Literal, overload

from anaxer._config import ClientConfig
from anaxer.errors import AnaxerError
from anaxer.models import (
    ConnectedInfo,
    CreatedV1,
    GraduatedV1,
    PriceUpdateV1,
    SubscriptionFiltersV1,
    SwapV1,
)
from anaxer.rest._endpoints import (
    LaunchpadsAsyncApi,
    TokensAsyncApi,
    async_creations,
    async_graduations,
    iter_creations_async,
    iter_graduations_async,
    iter_trades_async,
)
from anaxer.rest._http import AsyncHttpClient
from anaxer.rest._pagination import Page
from anaxer.ws._connection import ErrorHub, WsConnection
from anaxer.ws._subscription import Subscription


class _ErrorsAsyncIterator:
    def __init__(self, hub: ErrorHub) -> None:
        self._q = hub.subscribe()

    def __aiter__(self) -> _ErrorsAsyncIterator:
        return self

    async def __anext__(self) -> AnaxerError:
        item = await self._q.get()
        if item is None:
            raise StopAsyncIteration
        return item


class AsyncClient:
    def __init__(
        self,
        config: ClientConfig,
        *,
        http_transport: Any = None,
        open_transport_fn: Any = None,
    ) -> None:
        self._config = config
        self._error_hub = ErrorHub()
        self._conn = WsConnection(
            config, self._error_hub, open_transport_fn=open_transport_fn
        )
        self._http = AsyncHttpClient(
            base_url=config.base_url,
            api_key=config.api_key,
            max_retries=config.rest_max_retries,
            transport=http_transport,
        )
        self.tokens = TokensAsyncApi(self._http)
        self.launchpads = LaunchpadsAsyncApi(self._http)
        self._entered = False

    @property
    def connected(self) -> ConnectedInfo:
        info = self._conn.connected
        if info is None:
            raise AnaxerError("connection_closed", "Client is not connected")
        return info

    @property
    def errors(self) -> AsyncIterator[AnaxerError]:
        """Side-channel errors. Each access returns a new subscriber iterator."""
        return _ErrorsAsyncIterator(self._error_hub)

    def on_error(self, callback: Callable[[AnaxerError], None]) -> None:
        self._error_hub.on_error(callback)

    async def __aenter__(self) -> AsyncClient:
        await self._conn.open()
        self._entered = True
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._conn.close()
        await self._http.aclose()
        self._entered = False

    @overload
    def stream(
        self,
        channel: Literal["trades"],
        *,
        sources: list[str] | None = None,
        mints: list[str] | None = None,
        wallets: list[str] | None = None,
        min_volume_usd: float | None = None,
        max_volume_usd: float | None = None,
        sol_only: bool | None = None,
    ) -> Subscription[SwapV1]: ...

    @overload
    def stream(
        self,
        channel: Literal["creations"],
        *,
        sources: list[str] | None = None,
        enriched: bool | None = None,
        exclude_mayhem: bool | None = None,
    ) -> Subscription[CreatedV1]: ...

    @overload
    def stream(
        self,
        channel: Literal["graduations"],
        *,
        sources: list[str] | None = None,
        exclude_mayhem: bool | None = None,
        min_liquidity_sol: float | None = None,
    ) -> Subscription[GraduatedV1]: ...

    @overload
    def stream(
        self,
        channel: Literal["prices"],
        *,
        sources: list[str] | None = None,
        mints: list[str] | None = None,
    ) -> Subscription[PriceUpdateV1]: ...

    def stream(self, channel: str, **filters: Any) -> Subscription[Any]:
        if not self._entered and self._conn.state != "open":
            # Allow stream() only after successful __aenter__ (eager open).
            raise AnaxerError(
                "connection_closed",
                "Call stream() inside `async with anaxer.connect(...)`",
            )
        model = SubscriptionFiltersV1.model_validate(
            {
                "sources": filters.get("sources"),
                "mints": filters.get("mints"),
                "wallets": filters.get("wallets"),
                "minVolumeUsd": filters.get("min_volume_usd"),
                "maxVolumeUsd": filters.get("max_volume_usd"),
                "enriched": filters.get("enriched"),
                "excludeMayhem": filters.get("exclude_mayhem"),
                "minLiquiditySol": filters.get("min_liquidity_sol"),
                "solOnly": filters.get("sol_only"),
            }
        )
        return self._conn.stream(channel, model)  # type: ignore[arg-type]

    async def creations(self, **opts: Any) -> Page[CreatedV1]:
        return await async_creations(self._http, **opts)

    def iter_creations(self, **opts: Any) -> AsyncIterator[CreatedV1]:
        return iter_creations_async(self._http, **opts)

    async def graduations(self, **opts: Any) -> Page[GraduatedV1]:
        return await async_graduations(self._http, **opts)

    def iter_graduations(self, **opts: Any) -> AsyncIterator[GraduatedV1]:
        return iter_graduations_async(self._http, **opts)

    def iter_trades(self, mint: str, **opts: Any) -> AsyncIterator[SwapV1]:
        return iter_trades_async(self._http, mint, **opts)
