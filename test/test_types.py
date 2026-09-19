"""Usage snippets checked by ``mypy --strict`` (not pytest assertions).

Negative cases use ``# type: ignore[...]`` with ``warn_unused_ignores = true``
so CI fails if a forbidden call/attribute ever becomes accidentally legal (R3).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import anaxer
from anaxer import AsyncClient, Client, CreatedV1, Subscription, SwapV1

if TYPE_CHECKING:
    # typing_extensions: assert_type is 3.11+ in typing; we support 3.10.
    from typing_extensions import assert_type
else:

    def assert_type(val, typ):  # type: ignore[no-untyped-def]
        return val


async def _streaming_example() -> None:
    async with anaxer.connect(api_key="k") as client:
        sub = client.stream("creations")
        assert_type(sub, Subscription[CreatedV1])
        async for event in sub:
            assert_type(event, CreatedV1)
            break

        trades = client.stream("trades", sol_only=True)
        assert_type(trades, Subscription[SwapV1])

        # Expected type errors — ignore must stay necessary (warn_unused_ignores).
        client.stream("prices", min_volume_usd=1.0)  # type: ignore[call-overload]
        client.stream("creations", sol_only=True)  # type: ignore[call-overload]


def _sync_example() -> None:
    client = Client(api_key="k")
    _ = client.tokens.price("mint")
    # Expected attr-defined errors on REST-only Client.
    client.stream("creations")  # type: ignore[attr-defined]
    _ = client.errors  # type: ignore[attr-defined]
    client.on_error(lambda e: None)  # type: ignore[attr-defined]


async def _errors_side_channel(client: AsyncClient) -> None:
    errors: AsyncIterator[anaxer.AnaxerError] = client.errors
    async for err in errors:
        _ = err.code
        break
