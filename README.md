# anaxer

Official Python SDK for the [Anaxer](https://anaxer.com) Solana real-time data API.

```bash
pip install anaxer
```

Requires Python 3.10+.

## Quickstart — async streaming

```python
import asyncio
import anaxer

async def main():
    async with anaxer.connect(api_key="...") as client:
        # Plan limits from the server `connected` frame
        print(client.connected.connections_per_stream)

        async with client.stream("creations", exclude_mayhem=True) as sub:
            async for event in sub:
                print("new launch:", event.symbol, event.mint)
                break

asyncio.run(main())
```

`stream()` returns a `Subscription[T]` handle (async-iterable + async context manager).
Prefer `async with client.stream(...) as sub:` so leaving the block always sends
`unsubscribe`. Bare `async for` / `break` also unsubscribes (via `GeneratorExit`).

### Channels

| Channel | Yields | Filters (keyword-only, snake_case) |
|---|---|---|
| `trades` | `SwapV1` | `sources`, `mints`, `wallets`, `min_volume_usd`, `max_volume_usd`, `sol_only` |
| `creations` | `CreatedV1` | `sources`, `enriched`, `exclude_mayhem` |
| `graduations` | `GraduatedV1` | `sources`, `exclude_mayhem`, `min_liquidity_sol` |
| `prices` | `PriceUpdateV1` | `sources`, `mints` |

`transfers` is not wired in v1 (server ingestion deferred). `TransferV1` is still
exported as a type.

### Side-channel errors

Reconnect-triggering / non-fatal codes (e.g. `slow_consumer`) are delivered on
`client.on_error(callback)` and/or `async for err in client.errors` — they never
interrupt an open `Subscription`'s `async for`. Subscription-fatal codes
(`invalid_filters`, …) raise from that subscription only. `unauthorized` is
terminal.

## Quickstart — sync REST

```python
from anaxer import Client

client = Client(api_key="...")
price = client.tokens.price("3PFaeFMXiRVYueDCnHHSKndKDDpKZhbhFjQ13JXYpump")
print(price.price.usd)
client.close()
```

`Client` is REST-only — it has no `stream()` method. Use it when you do not want a
WebSocket. `AsyncClient` (from `anaxer.connect`) also exposes the same REST surface
via `await`.

### REST surface

- `tokens.get` / `batch` / `price` / `batch_price` / `trades`
- `creations` / `iter_creations`
- `graduations` / `iter_graduations`
- `iter_trades(mint, ...)`
- `launchpads.stats`

`programs()` is omitted in v1 (response type not in the public schemas).

List endpoints return a `Page` (`data`, `next`, `window`). Use `iter_*` to walk
`next` across pages. Wire field `from` is exposed in Python as `from_`
(reserved word) with a Pydantic alias so round-trips keep the wire key.

Auth is always `Authorization: Bearer <api_key>` (same as the TypeScript SDK).

## No gap recovery in v1

The WebSocket protocol has no sequence/cursor. After a disconnect the client
reconnects and re-subscribes, but events during the gap are not replayed.

## Config

```python
from anaxer import Reconnect, connect

async with connect(
    api_key="...",
    base_url="https://api.anaxer.com",  # local: http://localhost:3010
    reconnect=Reconnect(base_delay=0.5, max_delay=30.0, max_retries=None),
    heartbeat_interval=15.0,  # 0 disables
    rest_max_retries=2,
) as client:
    ...
```

## Errors

All failures surface as `anaxer.AnaxerError` with `.code`, `.message`, `.status`
(HTTP only), and `.subscription_id` (subscription-fatal WS errors only).

SDK-local codes: `connection_closed`, `timeout`, `invalid_payload` (client-side
parse failure — distinct from the wire's `invalid_message`).

## Links

- Wire contracts: [`docs/api/ws-v1.md`](../../docs/api/ws-v1.md), [`docs/api/rest-v1.md`](../../docs/api/rest-v1.md)
- System doc: [`docs/system/sdk-py.md`](../../docs/system/sdk-py.md)
- TypeScript sibling: [`@anaxer/sdk`](../../packages/sdk/README.md)

---

## About this repository

This is a **read-only mirror**. ``python/sdk`` in Anaxer's private monorepo is the
source of truth; this repo is regenerated from it on each release, so pull requests
here cannot be merged directly. Issues are very welcome — or reach us at
[anaxer.com/contact](https://anaxer.com/contact).
