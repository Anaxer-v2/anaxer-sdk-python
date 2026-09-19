"""``anaxer.connect(...)`` factory."""

from __future__ import annotations

from anaxer._client_async import AsyncClient
from anaxer._config import ClientConfig, Reconnect, derive_ws_url, strip_trailing_slashes


def connect(
    api_key: str,
    *,
    base_url: str = "https://api.anaxer.com",
    ws_url: str | None = None,
    reconnect: bool | Reconnect = True,
    heartbeat_interval: float = 15.0,
    rest_max_retries: int = 2,
    _http_transport: object | None = None,
    _open_transport_fn: object | None = None,
) -> AsyncClient:
    base = strip_trailing_slashes(base_url)
    cfg = ClientConfig(
        api_key=api_key,
        base_url=base,
        ws_url=ws_url or derive_ws_url(base),
        reconnect=reconnect,
        heartbeat_interval=heartbeat_interval,
        rest_max_retries=rest_max_retries,
    )
    return AsyncClient(
        cfg,
        http_transport=_http_transport,
        open_transport_fn=_open_transport_fn,
    )
