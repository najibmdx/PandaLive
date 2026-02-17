"""Session replay tool for PANDA LIVE."""

import json
from pathlib import Path
from typing import List, Dict, Any


def replay_session(filepath: str) -> List[Dict[str, Any]]:
    """Read a JSONL session log and return parsed events.

    Args:
        filepath: Path to a .jsonl session log file.

    Returns:
        List of parsed event dicts from the session log.

    Raises:
        FileNotFoundError: If the log file does not exist.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Session log not found: {filepath}")

    events: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # Skip malformed lines

    return events
