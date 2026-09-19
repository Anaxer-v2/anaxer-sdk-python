"""Defaults and URL resolution (mirrors packages/sdk/src/config.ts)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Reconnect:
    base_delay: float = 0.5
    max_delay: float = 30.0
    max_retries: int | None = None  # None = unlimited


@dataclass(frozen=True)
class ClientConfig:
    api_key: str
    base_url: str
    ws_url: str
    reconnect: bool | Reconnect
    heartbeat_interval: float
    rest_max_retries: int


def strip_trailing_slashes(url: str) -> str:
    return url.rstrip("/")


def derive_ws_url(base_url: str) -> str:
    """``https://host`` → ``wss://host/v1/stream``; ``http://`` → ``ws://``."""
    base = strip_trailing_slashes(base_url)
    if base.startswith("https://"):
        return f"wss://{base[len('https://'):]}/v1/stream"
    if base.startswith("http://"):
        return f"ws://{base[len('http://'):]}/v1/stream"
    return f"{base}/v1/stream"


def reconnect_delay_seconds(attempt: int, cfg: Reconnect) -> float:
    """1-based attempt: first reconnect is ``1 × base_delay`` (±20% jitter)."""
    import random

    exp = min(cfg.max_delay, cfg.base_delay * (2 ** (attempt - 1)))
    jitter = 0.8 + random.random() * 0.4
    return float(max(0.0, exp * jitter))


def resolve_reconnect(reconnect: bool | Reconnect) -> bool | Reconnect:
    if reconnect is True:
        return Reconnect()
    return reconnect
