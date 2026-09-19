"""Sync Client — REST only."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from anaxer.models import CreatedV1, GraduatedV1, SwapV1
from anaxer.rest._endpoints import (
    LaunchpadsApi,
    TokensApi,
    iter_creations_sync,
    iter_graduations_sync,
    iter_trades_sync,
    sync_creations,
    sync_graduations,
)
from anaxer.rest._http import SyncHttpClient
from anaxer.rest._pagination import Page


class Client:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.anaxer.com",
        rest_max_retries: int = 2,
        http_transport: Any = None,
    ) -> None:
        self._http = SyncHttpClient(
            base_url=base_url,
            api_key=api_key,
            max_retries=rest_max_retries,
            transport=http_transport,
        )
        self.tokens = TokensApi(self._http)
        self.launchpads = LaunchpadsApi(self._http)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def creations(self, **opts: Any) -> Page[CreatedV1]:
        return sync_creations(self._http, **opts)

    def iter_creations(self, **opts: Any) -> Iterator[CreatedV1]:
        return iter_creations_sync(self._http, **opts)

    def graduations(self, **opts: Any) -> Page[GraduatedV1]:
        return sync_graduations(self._http, **opts)

    def iter_graduations(self, **opts: Any) -> Iterator[GraduatedV1]:
        return iter_graduations_sync(self._http, **opts)

    def iter_trades(self, mint: str, **opts: Any) -> Iterator[SwapV1]:
        return iter_trades_sync(self._http, mint, **opts)
