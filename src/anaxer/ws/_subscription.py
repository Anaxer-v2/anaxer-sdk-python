"""Per-stream Subscription[T] handle (doc 22a §3 decision 4a)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any, Generic, TypeVar

from anaxer.errors import AnaxerError
from anaxer.models import SubscriptionFiltersV1

T = TypeVar("T")

_SENTINEL = object()


class Subscription(Generic[T]):
    """Async-iterable stream handle with explicit ``aclose()`` / context manager."""

    def __init__(
        self,
        sub_id: str,
        channel: str,
        filters: SubscriptionFiltersV1,
        on_close: Callable[[Subscription[Any]], Coroutine[Any, Any, None]],
    ) -> None:
        self.id = sub_id
        self.channel = channel
        self.filters = filters
        self._on_close = on_close
        self._queue: asyncio.Queue[T | AnaxerError | object] = asyncio.Queue()
        self._closed = False
        self._close_lock = asyncio.Lock()

    @property
    def closed(self) -> bool:
        return self._closed

    def push(self, item: T) -> None:
        if not self._closed:
            self._queue.put_nowait(item)

    def fail(self, err: AnaxerError) -> None:
        """Deliver a terminal error and mark the subscription closed.

        Does not send ``unsubscribe`` / invoke ``on_close`` — callers that need
        map cleanup (e.g. subscription-fatal server errors) pop the entry
        themselves. The server already rejected the sub; re-unsubscribing would
        only risk ``unknown_subscription``.
        """
        if self._closed:
            return
        self._closed = True
        self._queue.put_nowait(err)

    def __aiter__(self) -> AsyncIterator[T]:
        # Async generator so ``break`` delivers GeneratorExit → aclose().
        return self._iterate()

    async def __anext__(self) -> T:
        """Direct pull (tests / advanced use). Prefer ``async for`` / ``async with``."""
        item = await self._queue.get()
        if item is _SENTINEL:
            raise StopAsyncIteration
        if isinstance(item, AnaxerError):
            raise item
        return item  # type: ignore[return-value]

    async def _iterate(self) -> AsyncIterator[T]:
        try:
            while True:
                yield await self.__anext__()
        except StopAsyncIteration:
            return
        except (GeneratorExit, asyncio.CancelledError):
            await self.aclose()
            raise

    async def aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._queue.put_nowait(_SENTINEL)
            await self._on_close(self)

    async def __aenter__(self) -> Subscription[T]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
