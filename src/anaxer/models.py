"""Hand-authored Pydantic v2 models mirroring packages/schemas public/v1 wire shapes."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

WIRE_VERSION = 1

# ``extra="ignore"`` (not ``forbid``): ws-v1 §Versioning promises additive-only
# changes within v1 (new optional fields). Forbidding extras would turn every
# server-side additive field into a channel-wide ``invalid_payload`` outage for
# older SDK installs. Unknown keys are dropped; known fields still validated.
_MODEL_CONFIG = ConfigDict(
    populate_by_name=True,
    extra="ignore",
)


class TokenLegV1(BaseModel):
    model_config = _MODEL_CONFIG

    mint: str
    amount: str
    ui_amount: float = Field(alias="uiAmount")
    decimals: int


class SwapLegsV1(BaseModel):
    model_config = _MODEL_CONFIG

    from_: TokenLegV1 = Field(alias="from")
    to: TokenLegV1


class SwapV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["swap"]
    source: str
    wallet: str
    volume_usd: float | None = Field(alias="volumeUsd")
    swap: SwapLegsV1
    signature: str
    slot: int
    timestamp: int | None


class SocialsV1(BaseModel):
    model_config = _MODEL_CONFIG

    website: str | None
    twitter: str | None
    telegram: str | None


class CreatedV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["created"]
    source: str
    mint: str | None
    name: str | None
    symbol: str | None
    creator: str | None
    uri: str | None
    mayhem_mode: bool = Field(alias="mayhemMode")
    socials: SocialsV1
    signature: str
    slot: int
    timestamp: int | None


class GraduatedV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["graduated"]
    source: str
    mint: str | None
    name: str | None
    symbol: str | None
    creator: str | None
    uri: str | None
    mayhem_mode: bool = Field(alias="mayhemMode")
    socials: SocialsV1
    liquidity_sol: float | None = Field(alias="liquiditySol")
    time_to_graduate_seconds: float | None = Field(alias="timeToGraduateSeconds")
    signature: str
    slot: int
    timestamp: int | None


class PriceQuoteV1(BaseModel):
    model_config = _MODEL_CONFIG

    sol: float
    usd: float | None


class PriceUpdateV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["price"]
    source: str
    mint: str
    price: PriceQuoteV1
    market_cap_usd: float | None = Field(alias="marketCapUsd")
    slot: int
    timestamp: int | None


class TransferLegsV1(BaseModel):
    model_config = _MODEL_CONFIG

    from_: str = Field(alias="from")
    to: str
    mint: str
    amount: str
    ui_amount: float = Field(alias="uiAmount")
    decimals: int


class TransferV1(BaseModel):
    """Type export only in v1 — not wired into ``stream()``."""

    model_config = _MODEL_CONFIG

    type: Literal["transfer"]
    source: str | None
    wallet: str
    transfer: TransferLegsV1
    signature: str
    slot: int
    timestamp: int | None


class TokenMetadataV1(BaseModel):
    model_config = _MODEL_CONFIG

    mint: str
    name: str | None
    symbol: str | None
    supply: str | None
    socials: SocialsV1


class Window(BaseModel):
    model_config = _MODEL_CONFIG

    from_: int = Field(alias="from")
    to: int


class LaunchpadStatsSourceRowV1(BaseModel):
    model_config = _MODEL_CONFIG

    source: str
    creations: int
    graduations: int
    graduation_rate: float = Field(alias="graduationRate")


class LaunchpadStatsV1(BaseModel):
    model_config = _MODEL_CONFIG

    window_hours: int = Field(alias="windowHours")
    window: Window
    sources: list[LaunchpadStatsSourceRowV1]


class SubscriptionFiltersV1(BaseModel):
    model_config = _MODEL_CONFIG

    sources: list[str] | None = None
    mints: list[str] | None = None
    wallets: list[str] | None = None
    min_volume_usd: float | None = Field(default=None, alias="minVolumeUsd")
    max_volume_usd: float | None = Field(default=None, alias="maxVolumeUsd")
    enriched: bool | None = None
    exclude_mayhem: bool | None = Field(default=None, alias="excludeMayhem")
    min_liquidity_sol: float | None = Field(default=None, alias="minLiquiditySol")
    sol_only: bool | None = Field(default=None, alias="solOnly")


class ConnectedLimitsV1(BaseModel):
    model_config = _MODEL_CONFIG

    connections_per_stream: int = Field(alias="connectionsPerStream")
    trades_requires_mints: bool = Field(alias="tradesRequiresMints")
    trades_mint_cap: int = Field(alias="tradesMintCap")


class ConnectedMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    v: Literal[1]
    type: Literal["connected"]
    ts: int
    limits: ConnectedLimitsV1


class ConnectedInfo(BaseModel):
    """Flattened plan limits from the wire ``connected`` frame."""

    model_config = _MODEL_CONFIG

    connections_per_stream: int
    trades_requires_mints: bool
    trades_mint_cap: int

    @classmethod
    def from_limits(cls, limits: ConnectedLimitsV1) -> ConnectedInfo:
        return cls(
            connections_per_stream=limits.connections_per_stream,
            trades_requires_mints=limits.trades_requires_mints,
            trades_mint_cap=limits.trades_mint_cap,
        )


class SubscribedMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    v: Literal[1]
    type: Literal["subscribed"]
    id: str
    channel: str
    filters: SubscriptionFiltersV1
    ts: int


class UnsubscribedMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    v: Literal[1]
    type: Literal["unsubscribed"]
    id: str
    ts: int


class PongMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    v: Literal[1]
    type: Literal["pong"]
    ts: int


class ErrorMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    v: Literal[1]
    type: Literal["error"]
    code: str
    message: str
    id: str | None = None
    ts: int


class SubscribeMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["subscribe"]
    id: str
    channel: str
    filters: SubscriptionFiltersV1


class UnsubscribeMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["unsubscribe"]
    id: str


class PingMessageV1(BaseModel):
    model_config = _MODEL_CONFIG

    type: Literal["ping"]


class DataMessageEnvelopeV1(BaseModel):
    """Loose envelope used by contract-example validation (channel+data)."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    v: Literal[1]
    channel: str
    type: str
    sub: str
    ts: int
    data: dict[str, Any]


CHANNEL_PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "trades": SwapV1,
    "creations": CreatedV1,
    "graduations": GraduatedV1,
    "prices": PriceUpdateV1,
    "transfers": TransferV1,
}

PAYLOAD_BY_TYPE: dict[str, type[BaseModel]] = {
    "swap": SwapV1,
    "created": CreatedV1,
    "graduated": GraduatedV1,
    "price": PriceUpdateV1,
    "transfer": TransferV1,
}
