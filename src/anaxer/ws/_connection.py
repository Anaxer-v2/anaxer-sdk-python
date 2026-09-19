"""WS lifecycle: connect, heartbeat, reconnect, resubscribe, error partition."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any, Literal

from anaxer._config import ClientConfig, Reconnect, reconnect_delay_seconds, resolve_reconnect
from anaxer.errors import SUBSCRIPTION_FATAL_CODES, AnaxerError, AnaxerErrorCode
from anaxer.models import (
    CHANNEL_PAYLOAD_MODELS,
    ConnectedInfo,
    ConnectedMessageV1,
    SubscriptionFiltersV1,
)
from anaxer.ws._subscription import Subscription
from anaxer.ws._transport import ConnectionClosed, Transport, open_transport

logger = logging.getLogger("anaxer.ws")

StreamChannel = Literal["trades", "creations", "graduations", "prices"]
ConnectionState = Literal["idle", "connecting", "open", "reconnecting", "closed"]


class ErrorHub:
    """Fan-out for side-channel errors: callbacks + per-consumer async iterators."""

    def __init__(self) -> None:
        self._callbacks: list[Callable[[AnaxerError], None]] = []
        self._queues: list[asyncio.Queue[AnaxerError | None]] = []
        self._closed = False

    def on_error(self, callback: Callable[[AnaxerError], None]) -> None:
        self._callbacks.append(callback)

    def subscribe(self) -> asyncio.Queue[AnaxerError | None]:
        q: asyncio.Queue[AnaxerError | None] = asyncio.Queue()
        self._queues.append(q)
        return q

    def publish(self, err: AnaxerError) -> None:
        if self._closed:
            return
        for cb in list(self._callbacks):
            try:
                cb(err)
            except Exception:
                logger.exception("on_error callback raised")
        for q in list(self._queues):
            q.put_nowait(err)

    def close(self, err: AnaxerError | None = None) -> None:
        self._closed = True
        if err is not None:
            for cb in list(self._callbacks):
                try:
                    cb(err)
                except Exception:
                    logger.exception("on_error callback raised on close")
            for q in list(self._queues):
                q.put_nowait(err)
        for q in list(self._queues):
            q.put_nowait(None)


OpenTransportFn = Callable[[str, dict[str, str]], Any]


class WsConnection:
    def __init__(
        self,
        config: ClientConfig,
        error_hub: ErrorHub,
        *,
        open_transport_fn: OpenTransportFn | None = None,
    ) -> None:
        self._config = config
        self._errors = error_hub
        self._open_transport = open_transport_fn or open_transport
        self._state: ConnectionState = "idle"
        self._transport: Transport | None = None
        self._subscriptions: dict[str, Subscription[Any]] = {}
        self._sub_seq = 0
        self._connected: ConnectedInfo | None = None
        self._terminal = False
        self._intentional_close = False
        self._reconnect_attempt = 0
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._last_inbound_at = 0.0
        self._open_event = asyncio.Event()
        self._enter_future: asyncio.Future[ConnectedInfo] | None = None
        self._send_lock = asyncio.Lock()

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def connected(self) -> ConnectedInfo | None:
        return self._connected

    def stream(
        self,
        channel: StreamChannel,
        filters: SubscriptionFiltersV1,
    ) -> Subscription[Any]:
        if self._state == "closed":
            code: AnaxerErrorCode = "unauthorized" if self._terminal else "connection_closed"
            msg = (
                "Client is closed after terminal auth failure"
                if self._terminal
                else "Client is closed"
            )
            raise AnaxerError(code, msg)

        sub_id = f"sub-{self._sub_seq + 1}"
        self._sub_seq += 1
        sub: Subscription[Any] = Subscription(sub_id, channel, filters, self._handle_sub_close)
        self._subscriptions[sub_id] = sub
        if self._state == "open":
            self._send_subscribe(sub)
        return sub

    async def open(self) -> ConnectedInfo:
        """Eager connect used by ``AsyncClient.__aenter__``."""
        if self._state == "open" and self._connected is not None:
            return self._connected
        if self._state == "closed":
            raise AnaxerError(
                "unauthorized" if self._terminal else "connection_closed",
                "Client is closed",
            )
        loop = asyncio.get_running_loop()
        self._enter_future = loop.create_future()
        await self._open_socket()
        return await self._enter_future

    async def close(self) -> None:
        self._intentional_close = True
        self._terminal = False
        self._cancel_reconnect()
        self._stop_heartbeat()
        for sub in list(self._subscriptions.values()):
            if not sub.closed:
                if self._state == "open":
                    self._send_raw({"type": "unsubscribe", "id": sub.id})
                self._subscriptions.pop(sub.id, None)
                sub.fail(AnaxerError("connection_closed", "Client closed"))
                await sub.aclose()
        transport = self._transport
        self._transport = None
        self._state = "closed"
        self._open_event.clear()
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if transport is not None:
            try:
                await transport.close(1000)
            except Exception:  # noqa: BLE001
                pass
        self._errors.close()

    async def _handle_sub_close(self, sub: Subscription[Any]) -> None:
        if sub.id in self._subscriptions:
            if self._state == "open":
                self._send_raw({"type": "unsubscribe", "id": sub.id})
            self._subscriptions.pop(sub.id, None)

    async def _open_socket(self) -> None:
        if self._intentional_close or self._terminal:
            return
        self._state = "reconnecting" if self._reconnect_attempt > 0 else "connecting"
        self._open_event.clear()
        try:
            transport = await self._open_transport(
                self._config.ws_url,
                {"Authorization": f"Bearer {self._config.api_key}"},
            )
        except Exception as exc:  # noqa: BLE001
            err = AnaxerError("internal_error", f"WebSocket open failed: {exc}")
            self._errors.publish(err)
            if self._enter_future and not self._enter_future.done():
                self._enter_future.set_exception(err)
            if not self._intentional_close and not self._terminal:
                self._schedule_reconnect()
            return

        self._transport = transport
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self._transport is not None
        try:
            while True:
                raw = await self._transport.recv()
                self._last_inbound_at = asyncio.get_running_loop().time()
                self._dispatch(raw)
        except ConnectionClosed:
            await self._on_socket_closed()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._errors.publish(AnaxerError("internal_error", str(exc)))
            await self._on_socket_closed()

    def _dispatch(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self._errors.publish(
                AnaxerError("invalid_payload", "Inbound frame is not valid JSON")
            )
            return
        if not isinstance(msg, dict):
            self._errors.publish(
                AnaxerError("invalid_payload", "Inbound frame is not a JSON object")
            )
            return

        msg_type = msg.get("type")
        if msg_type == "connected":
            self._on_connected(msg)
            return
        if msg_type == "subscribed":
            return
        if msg_type == "unsubscribed":
            return
        if msg_type == "pong":
            return
        if msg_type == "error":
            self._on_server_error(msg)
            return

        if msg.get("v") == 1 and isinstance(msg.get("channel"), str) and isinstance(
            msg.get("sub"), str
        ):
            self._on_data(msg)
            return

        # Unknown / future control — ignore (TS parity), do not crash.

    def _on_connected(self, msg: dict[str, Any]) -> None:
        try:
            parsed = ConnectedMessageV1.model_validate(msg)
        except Exception as exc:  # noqa: BLE001
            err = AnaxerError(
                "internal_error",
                f"Malformed connected frame: {exc}",
            )
            self._errors.publish(err)
            if self._enter_future and not self._enter_future.done():
                self._enter_future.set_exception(err)
            if self._transport is not None:
                asyncio.create_task(self._transport.close(4000))
            return

        self._connected = ConnectedInfo.from_limits(parsed.limits)
        self._state = "open"
        self._reconnect_attempt = 0
        self._open_event.set()
        self._start_heartbeat()
        self._flush_subscriptions()
        if self._enter_future and not self._enter_future.done():
            self._enter_future.set_result(self._connected)

    def _on_data(self, msg: dict[str, Any]) -> None:
        sub_id = str(msg["sub"])
        channel = str(msg["channel"])
        sub = self._subscriptions.get(sub_id)
        if sub is None:
            return
        model = CHANNEL_PAYLOAD_MODELS.get(channel)
        if model is None:
            self._errors.publish(
                AnaxerError("invalid_payload", f"Unknown channel on data frame: {channel}")
            )
            return
        try:
            payload = model.model_validate(msg.get("data"))
        except Exception as exc:  # noqa: BLE001
            self._errors.publish(
                AnaxerError("invalid_payload", f"Payload validation failed: {exc}")
            )
            return
        sub.push(payload)

    def _on_server_error(self, msg: dict[str, Any]) -> None:
        code = str(msg.get("code") or "internal_error")
        message = str(msg.get("message") or code)
        sub_id = msg.get("id") if isinstance(msg.get("id"), str) else None
        err = AnaxerError(
            code,  # type: ignore[arg-type]
            message,
            subscription_id=sub_id,
        )

        if code == "unauthorized":
            self._terminal = True
            for sid, sub in list(self._subscriptions.items()):
                if not sub.closed:
                    sub.fail(err)
                self._subscriptions.pop(sid, None)
            self._errors.publish(err)
            if self._enter_future and not self._enter_future.done():
                self._enter_future.set_exception(err)
            return

        if sub_id and code in SUBSCRIPTION_FATAL_CODES:
            fatal_sub = self._subscriptions.pop(sub_id, None)
            if fatal_sub is not None and not fatal_sub.closed:
                fatal_sub.fail(err)
            return

        # Side-channel (slow_consumer, connection-level internal_error, …)
        self._errors.publish(err)

    async def _on_socket_closed(self) -> None:
        self._stop_heartbeat()
        self._transport = None
        self._open_event.clear()

        if self._intentional_close or self._state == "closed":
            self._state = "closed"
            return

        if self._terminal:
            self._state = "closed"
            term = AnaxerError("unauthorized", "Connection closed (unauthorized)")
            for sub in list(self._subscriptions.values()):
                if not sub.closed:
                    sub.fail(term)
            self._errors.close(term)
            if self._enter_future and not self._enter_future.done():
                self._enter_future.set_exception(term)
            return

        reconnect = resolve_reconnect(self._config.reconnect)
        if reconnect is False:
            self._state = "closed"
            err = AnaxerError("connection_closed", "Connection closed")
            for sub in list(self._subscriptions.values()):
                if not sub.closed:
                    sub.fail(err)
            self._errors.close(err)
            if self._enter_future and not self._enter_future.done():
                self._enter_future.set_exception(err)
            return

        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        if self._intentional_close or self._terminal:
            return
        reconnect = resolve_reconnect(self._config.reconnect)
        if reconnect is False:
            return
        assert isinstance(reconnect, Reconnect)

        next_attempt = self._reconnect_attempt + 1
        if reconnect.max_retries is not None and next_attempt > reconnect.max_retries:
            self._state = "closed"
            err = AnaxerError("connection_closed", "Max reconnect retries exceeded")
            self._errors.publish(err)
            return

        self._reconnect_attempt = next_attempt
        self._state = "reconnecting"
        delay = reconnect_delay_seconds(next_attempt, reconnect)

        async def _go() -> None:
            await asyncio.sleep(delay)
            if not self._intentional_close and not self._terminal:
                await self._open_socket()

        self._cancel_reconnect()
        self._reconnect_task = asyncio.create_task(_go())

    def _flush_subscriptions(self) -> None:
        for sub in self._subscriptions.values():
            if not sub.closed:
                self._send_subscribe(sub)

    def _send_subscribe(self, sub: Subscription[Any]) -> None:
        filters = sub.filters.model_dump(by_alias=True, exclude_none=True)
        self._send_raw(
            {
                "type": "subscribe",
                "id": sub.id,
                "channel": sub.channel,
                "filters": filters,
            }
        )

    def _send_raw(self, payload: dict[str, Any]) -> None:
        if self._transport is None or self._state != "open":
            return
        data = json.dumps(payload, separators=(",", ":"))
        transport = self._transport

        async def _do() -> None:
            async with self._send_lock:
                if self._transport is transport and self._state == "open":
                    try:
                        await transport.send(data)
                    except Exception:  # noqa: BLE001
                        pass

        try:
            asyncio.get_running_loop().create_task(_do())
        except RuntimeError:
            pass

    def _start_heartbeat(self) -> None:
        self._stop_heartbeat()
        interval = self._config.heartbeat_interval
        if interval <= 0:
            return
        self._last_inbound_at = asyncio.get_running_loop().time()

        async def _beat() -> None:
            while self._state == "open" and self._transport is not None:
                await asyncio.sleep(interval)
                if self._state != "open" or self._transport is None:
                    return
                self._send_raw({"type": "ping"})
                stale = asyncio.get_running_loop().time() - self._last_inbound_at
                if stale > interval * 2:
                    try:
                        await self._transport.close(4000)
                    except Exception:  # noqa: BLE001
                        await self._on_socket_closed()
                    return

        self._heartbeat_task = asyncio.create_task(_beat())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = None

    def _cancel_reconnect(self) -> None:
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        self._reconnect_task = None
