"""httpx-backed GET wrapper with Bearer auth and 429/5xx retry."""

from __future__ import annotations

import asyncio
import time
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from anaxer.errors import AnaxerError, AnaxerErrorCode

QueryValue = str | int | float | bool | None


def parse_retry_after_seconds(header: str | None, fallback: float) -> float:
    if not header:
        return fallback
    try:
        as_int = float(header)
        if as_int >= 0:
            return as_int
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(header)
        return max(0.0, when.timestamp() - time.time())
    except (TypeError, ValueError, OverflowError):
        return fallback


def default_backoff_seconds(attempt: int) -> float:
    """0-based retry index (matches TS http.ts)."""
    return float(min(30.0, 0.5 * (2**attempt)))


def build_query(params: dict[str, QueryValue]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = str(value)
    return out


def map_http_error(status: int, body: Any) -> AnaxerError:
    err_obj = None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err_obj = body["error"]
    raw_code = err_obj.get("code") if err_obj else None
    code = _fallback_code(status)
    if isinstance(raw_code, str):
        code = raw_code  # type: ignore[assignment]
    raw_message = err_obj.get("message") if err_obj else None
    message = str(raw_message) if raw_message else f"HTTP {status}"
    return AnaxerError(code, message, status=status)


def _fallback_code(status: int) -> AnaxerErrorCode:
    if status == 400:
        return "invalid_request"
    if status == 401:
        return "unauthorized"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    if status == 503:
        return "upstream_unavailable"
    if status >= 500:
        return "internal"
    return "invalid_request"


class AsyncHttpClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        max_retries: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._max_retries = max_retries
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            transport=transport,
            timeout=30.0,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_json(
        self, path: str, query: dict[str, QueryValue] | None = None
    ) -> Any:
        params = build_query(query or {})
        attempt = 0
        while True:
            res = await self._client.get(path, params=params or None)
            if res.is_success:
                return res.json()

            status = res.status_code
            try:
                body: Any = res.json()
            except Exception:  # noqa: BLE001
                body = None

            retryable = status == 429 or status >= 500
            if retryable and attempt < self._max_retries:
                fallback = default_backoff_seconds(attempt)
                delay = parse_retry_after_seconds(res.headers.get("Retry-After"), fallback)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            raise map_http_error(status, body)


class SyncHttpClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        max_retries: int,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._max_retries = max_retries
        self._client = httpx.Client(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            transport=transport,
            timeout=30.0,
        )

    def close(self) -> None:
        self._client.close()

    def get_json(self, path: str, query: dict[str, QueryValue] | None = None) -> Any:
        params = build_query(query or {})
        attempt = 0
        while True:
            res = self._client.get(path, params=params or None)
            if res.is_success:
                return res.json()

            status = res.status_code
            try:
                body: Any = res.json()
            except Exception:  # noqa: BLE001
                body = None

            retryable = status == 429 or status >= 500
            if retryable and attempt < self._max_retries:
                fallback = default_backoff_seconds(attempt)
                delay = parse_retry_after_seconds(res.headers.get("Retry-After"), fallback)
                attempt += 1
                time.sleep(delay)
                continue
            raise map_http_error(status, body)
