"""Page[T] envelope + iter_* helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

from pydantic import BaseModel

from anaxer.models import Window

T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    data: list[T]
    next: str | None
    window: Window


async def async_iter_pages(
    first: Page[T],
    fetch_next: Callable[[str], Awaitable[Page[T]]],
) -> AsyncIterator[T]:
    page = first
    while True:
        for item in page.data:
            yield item
        if page.next is None:
            return
        page = await fetch_next(page.next)


def sync_iter_pages(
    first: Page[T],
    fetch_next: Callable[[str], Page[T]],
) -> Iterator[T]:
    page = first
    while True:
        yield from page.data
        if page.next is None:
            return
        page = fetch_next(page.next)


def page_from_envelope(envelope: dict[str, object], item_model: type[BaseModel]) -> Page[T]:
    data_raw = envelope.get("data")
    if not isinstance(data_raw, list):
        raise TypeError("page envelope missing data list")
    items: list[T] = [item_model.model_validate(x) for x in data_raw]  # type: ignore[misc]
    nxt = envelope.get("next")
    next_cursor: str | None
    if nxt is None or isinstance(nxt, str):
        next_cursor = nxt
    else:
        next_cursor = None
    window = Window.model_validate(envelope["window"])
    return Page(data=items, next=next_cursor, window=window)
