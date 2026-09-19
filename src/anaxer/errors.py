"""AnaxerError and the closed error-code union (WS + REST + SDK-local)."""

from __future__ import annotations

from typing import Literal

AnaxerErrorCode = Literal[
    # WS (docs/api/ws-v1.md)
    "invalid_message",
    "invalid_filters",
    "unknown_channel",
    "subscription_limit",
    "duplicate_id",
    "unknown_subscription",
    "unauthorized",
    "slow_consumer",
    "plan_restricted",
    "internal_error",
    # REST (docs/api/rest-v1.md)
    "invalid_request",
    "not_found",
    "rate_limited",
    "quota_exceeded",
    "internal",
    "upstream_unavailable",
    # SDK-local
    "connection_closed",
    "timeout",
    "invalid_payload",
]

# Codes that kill a single subscription (raised from Subscription.__anext__).
SUBSCRIPTION_FATAL_CODES: frozenset[str] = frozenset(
    {
        "unknown_channel",
        "invalid_filters",
        "subscription_limit",
        "duplicate_id",
        "unknown_subscription",
        "plan_restricted",
    }
)


class AnaxerError(Exception):
    """Typed SDK / wire error. ``.message`` is the human-readable string."""

    code: AnaxerErrorCode
    message: str
    status: int | None
    subscription_id: str | None

    def __init__(
        self,
        code: AnaxerErrorCode,
        message: str,
        *,
        status: int | None = None,
        subscription_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.subscription_id = subscription_id
