"""Flow normalization and validation for PANDA LIVE."""

import time
from typing import Any, Dict

from ..models.events import FlowEvent

# Reasonable timestamp bounds
_MIN_TIMESTAMP = 1_600_000_000  # ~Sep 2020
_MAX_FUTURE_DRIFT = 60  # Allow 60s clock drift


class FlowValidationError(ValueError):
    """Raised when a raw flow event fails validation."""


def normalize_flow(raw_data: Dict[str, Any]) -> FlowEvent:
    """Validate and normalize a raw flow dict into a FlowEvent.

    Args:
        raw_data: Dict with keys: wallet, timestamp, direction, amount_sol,
                  signature, token_ca.

    Returns:
        Validated FlowEvent.

    Raises:
        FlowValidationError: If any field is missing or invalid.
    """
    required_keys = {"wallet", "timestamp", "direction", "amount_sol", "signature", "token_ca"}
    missing = required_keys - set(raw_data.keys())
    if missing:
        raise FlowValidationError(f"Missing required fields: {missing}")

    wallet = str(raw_data["wallet"])
    if len(wallet) != 44:
        raise FlowValidationError(
            f"Invalid wallet address length: {len(wallet)} (expected 44)"
        )

    timestamp = int(raw_data["timestamp"])
    now = int(time.time())
    if timestamp < _MIN_TIMESTAMP:
        raise FlowValidationError(
            f"Timestamp too old: {timestamp} (min {_MIN_TIMESTAMP})"
        )
    if timestamp > now + _MAX_FUTURE_DRIFT:
        raise FlowValidationError(
            f"Timestamp too far in future: {timestamp} (now {now})"
        )

    direction = str(raw_data["direction"]).lower()
    if direction not in ("buy", "sell"):
        raise FlowValidationError(
            f"Invalid direction: {direction!r} (expected 'buy' or 'sell')"
        )

    amount_sol = float(raw_data["amount_sol"])
    if amount_sol <= 0:
        raise FlowValidationError(f"Amount must be > 0, got {amount_sol}")

    signature = str(raw_data["signature"])
    if not signature:
        raise FlowValidationError("Empty transaction signature")

    token_ca = str(raw_data["token_ca"])
    if not token_ca:
        raise FlowValidationError("Empty token contract address")

    return FlowEvent(
        wallet=wallet,
        timestamp=timestamp,
        direction=direction,
        amount_sol=amount_sol,
        signature=signature,
        token_ca=token_ca,
    )
