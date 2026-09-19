"""Forward-compat: additive wire fields must not break inbound validation (R1)."""

from __future__ import annotations

from anaxer.models import CreatedV1, SwapV1, TokenMetadataV1


def test_swap_ignores_unknown_additive_fields() -> None:
    payload = {
        "type": "swap",
        "source": "pump_fun",
        "wallet": "Wallet1111111111111111111111111111111111111",
        "volumeUsd": 12.5,
        "swap": {
            "from": {
                "mint": "So11111111111111111111111111111111111111112",
                "amount": "1000000000",
                "uiAmount": 1.0,
                "decimals": 9,
            },
            "to": {
                "mint": "3PFaeFMXiRVYueDCnHHSKndKDDpKZhbhFjQ13JXYpump",
                "amount": "1000",
                "uiAmount": 1000.0,
                "decimals": 6,
            },
        },
        "signature": "sig",
        "slot": 1,
        "timestamp": 1700000000,
        "newAdditiveField": "x",
    }
    parsed = SwapV1.model_validate(payload)
    assert parsed.type == "swap"
    assert not hasattr(parsed, "newAdditiveField")


def test_created_and_token_metadata_ignore_extras() -> None:
    created = CreatedV1.model_validate(
        {
            "type": "created",
            "source": "pump_fun",
            "mint": "Mint111111111111111111111111111111111111111",
            "name": "X",
            "symbol": "X",
            "creator": None,
            "uri": None,
            "mayhemMode": False,
            "socials": {"website": None, "twitter": None, "telegram": None},
            "signature": "s",
            "slot": 1,
            "timestamp": 1,
            "futureOptional": True,
        }
    )
    assert created.symbol == "X"

    meta = TokenMetadataV1.model_validate(
        {
            "mint": "Mint111111111111111111111111111111111111111",
            "name": "A",
            "symbol": "A",
            "supply": "1",
            "socials": {"website": None, "twitter": None, "telegram": None},
            "extraServerField": 1,
        }
    )
    assert meta.mint.startswith("Mint")
