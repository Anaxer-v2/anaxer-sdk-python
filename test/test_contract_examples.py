"""
Fixture-parity tests against docs/api contract markdown (doc 22a §3 decision 3).

## WS dispatch (port of packages/schemas/test/contract-examples.test.ts)
- Source: docs/api/ws-v1.md only
- Skip blocks containing the literal `"data": {}` placeholder
- Dispatch:
  - v==1 + channel + data → envelope (validate channel payload model)
  - v==1 + type → server control (connected/subscribed/unsubscribed/pong/error)
  - bare type swap/created/graduated/price/transfer → payload model
  - bare type subscribe/unsubscribe/ping → client control
  - filter-key bag (sources/mints/…/solOnly) → SubscriptionFiltersV1

## REST dispatch (this doc's own table — no TS precedent)
- Source: docs/api/rest-v1.md
- Skip the pagination illustration under "Pagination & windows"
  (empty ``data: []`` + next + window with no endpoint section context)
- Dispatch by shape:
  - ``windowHours`` + ``sources`` → LaunchpadStatsV1
  - ``mint`` + ``symbol`` + ``supply`` (no ``type``) → TokenMetadataV1
  - ``data`` + ``next`` + ``window`` → Page envelope; validate each item by its ``type``
    field (swap/created/graduated/price) when present; empty data arrays are accepted
    as long as window validates
  - ``data`` only (no ``next``) → batch of TokenMetadataV1 or PriceUpdateV1
    (item has ``type: price`` → PriceUpdateV1, else TokenMetadataV1)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from anaxer.models import (
    PAYLOAD_BY_TYPE,
    ConnectedMessageV1,
    ErrorMessageV1,
    LaunchpadStatsV1,
    PingMessageV1,
    PongMessageV1,
    PriceUpdateV1,
    SubscribedMessageV1,
    SubscribeMessageV1,
    SubscriptionFiltersV1,
    TokenMetadataV1,
    UnsubscribedMessageV1,
    UnsubscribeMessageV1,
    Window,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
WS_DOC = REPO_ROOT / "docs" / "api" / "ws-v1.md"
REST_DOC = REPO_ROOT / "docs" / "api" / "rest-v1.md"

FENCE_RE = re.compile(r"```jsonc?\r?\n([\s\S]*?)```")


def extract_json_examples(markdown: str, *, skip_empty_data_placeholder: bool) -> list[Any]:
    blocks: list[Any] = []
    for m in FENCE_RE.finditer(markdown):
        raw = m.group(1).strip()
        if skip_empty_data_placeholder and '"data": {}' in raw:
            continue
        blocks.append(json.loads(raw))
    return blocks


def _round_trip(model: BaseModel, original: dict[str, Any]) -> None:
    dumped = model.model_dump(by_alias=True, mode="json")
    assert set(dumped.keys()) == set(original.keys())


def schema_for_ws_example(value: Any) -> Any:
    if not isinstance(value, dict):
        raise TypeError("expected object example")
    obj = value
    if obj.get("v") == 1 and isinstance(obj.get("channel"), str) and isinstance(
        obj.get("data"), dict
    ):
        # Validate via payload type inside data
        data = obj["data"]
        t = data.get("type")
        if t not in PAYLOAD_BY_TYPE:
            raise ValueError(f"unknown data type {t}")
        return PAYLOAD_BY_TYPE[t]
    if obj.get("v") == 1 and isinstance(obj.get("type"), str):
        mapping: dict[str, Any] = {
            "connected": ConnectedMessageV1,
            "subscribed": SubscribedMessageV1,
            "unsubscribed": UnsubscribedMessageV1,
            "pong": PongMessageV1,
            "error": ErrorMessageV1,
        }
        if obj["type"] in mapping:
            return mapping[obj["type"]]
    if isinstance(obj.get("type"), str):
        t = obj["type"]
        if t in PAYLOAD_BY_TYPE:
            return PAYLOAD_BY_TYPE[t]
        if t == "subscribe":
            return SubscribeMessageV1
        if t == "unsubscribe":
            return UnsubscribeMessageV1
        if t == "ping":
            return PingMessageV1
    filter_keys = {
        "sources",
        "mints",
        "wallets",
        "minVolumeUsd",
        "maxVolumeUsd",
        "enriched",
        "excludeMayhem",
        "minLiquiditySol",
        "solOnly",
    }
    if filter_keys.intersection(obj.keys()):
        return SubscriptionFiltersV1
    raise ValueError(f"no schema mapping for example: {json.dumps(obj)[:120]}")


def validate_ws_example(example: Any) -> BaseModel:
    if (
        isinstance(example, dict)
        and example.get("v") == 1
        and isinstance(example.get("channel"), str)
        and isinstance(example.get("data"), dict)
    ):
        model_cls = schema_for_ws_example(example)
        parsed = model_cls.model_validate(example["data"])
        assert isinstance(parsed, BaseModel)
        _round_trip(parsed, example["data"])
        return parsed
    model_cls = schema_for_ws_example(example)
    parsed = model_cls.model_validate(example)
    assert isinstance(parsed, BaseModel)
    if isinstance(example, dict):
        _round_trip(parsed, example)
    return parsed


def schema_for_rest_example(value: Any) -> tuple[str, BaseModel | list[BaseModel] | None]:
    """Return (kind, validated model(s))."""
    if not isinstance(value, dict):
        raise TypeError("expected object")
    obj = value

    # Pagination illustration: empty data, no typed items — validate window only / skip items
    if (
        isinstance(obj.get("data"), list)
        and len(obj["data"]) == 0
        and "next" in obj
        and "window" in obj
        and "windowHours" not in obj
    ):
        Window.model_validate(obj["window"])
        return ("page_empty", None)

    if "windowHours" in obj and "sources" in obj:
        stats = LaunchpadStatsV1.model_validate(obj)
        _round_trip(stats, obj)
        return ("launchpad_stats", stats)

    if obj.get("type") == "price" and "price" in obj and "mint" in obj:
        price = PriceUpdateV1.model_validate(obj)
        _round_trip(price, obj)
        return ("price", price)

    if obj.get("type") in PAYLOAD_BY_TYPE and "data" not in obj:
        payload = PAYLOAD_BY_TYPE[obj["type"]].model_validate(obj)
        _round_trip(payload, obj)
        return ("payload", payload)

    if (
        "mint" in obj
        and "symbol" in obj
        and "supply" in obj
        and "type" not in obj
        and "data" not in obj
    ):
        meta = TokenMetadataV1.model_validate(obj)
        _round_trip(meta, obj)
        return ("token_metadata", meta)

    if "data" in obj and "next" in obj and "window" in obj:
        Window.model_validate(obj["window"])
        items: list[BaseModel] = []
        for item in obj["data"]:
            assert isinstance(item, dict)
            t = item.get("type")
            if t in PAYLOAD_BY_TYPE:
                m = PAYLOAD_BY_TYPE[t].model_validate(item)
                _round_trip(m, item)
                items.append(m)
            else:
                raise ValueError(f"untyped list item in page: {item}")
        return ("page", items)

    if "data" in obj and "next" not in obj:
        items = []
        for item in obj["data"]:
            assert isinstance(item, dict)
            if item.get("type") == "price":
                m = PriceUpdateV1.model_validate(item)
            else:
                m = TokenMetadataV1.model_validate(item)
            _round_trip(m, item)
            items.append(m)
        return ("batch", items)

    raise ValueError(f"no REST schema mapping: {json.dumps(obj)[:120]}")


def test_ws_contract_examples() -> None:
    markdown = WS_DOC.read_text(encoding="utf-8")
    examples = extract_json_examples(markdown, skip_empty_data_placeholder=True)
    assert len(examples) >= 8

    types: set[str] = set()
    for ex in examples:
        if isinstance(ex, dict) and isinstance(ex.get("type"), str):
            types.add(ex["type"])
        validate_ws_example(ex)

    for required in [
        "swap",
        "created",
        "graduated",
        "price",
        "transfer",
        "subscribe",
        "unsubscribe",
        "ping",
        "connected",
        "subscribed",
        "unsubscribed",
        "pong",
        "error",
    ]:
        assert required in types, f"missing example type {required}"


def test_rest_contract_examples() -> None:
    markdown = REST_DOC.read_text(encoding="utf-8")
    examples = extract_json_examples(markdown, skip_empty_data_placeholder=False)
    matched = 0
    for ex in examples:
        kind, _ = schema_for_rest_example(ex)
        matched += 1
        assert kind
    assert matched >= 6
