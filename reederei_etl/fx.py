from __future__ import annotations

from decimal import Decimal

from .config import STATIC_FX_TO_USD


def rate(currency: str) -> Decimal:
    c = (currency or "").strip().upper()
    if c not in STATIC_FX_TO_USD:
        raise KeyError(f"Unsupported currency for static FX: {currency!r}")
    return Decimal(str(STATIC_FX_TO_USD[c]))


def to_usd(amount: Decimal, currency: str) -> Decimal:
    return amount * rate(currency)
