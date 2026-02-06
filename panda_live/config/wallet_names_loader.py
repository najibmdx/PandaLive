"""Load wallet address -> name mapping from JSON."""

import json
from pathlib import Path
from typing import Dict


def load_wallet_names(filepath: str = "config/wallet_names.json") -> Dict[str, str]:
    """Load wallet address -> name mapping from JSON file.

    Args:
        filepath: Path to JSON file with format {"FULL_WALLET_ADDRESS": "Name"}.

    Returns:
        Dict mapping 44-char Solana addresses to human-readable names.
        Returns empty dict if file not found or invalid.
    """
    try:
        data = json.loads(Path(filepath).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
