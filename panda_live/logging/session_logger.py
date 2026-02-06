"""JSONL session logger for PANDA LIVE."""

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, TextIO

from ..models.events import FlowEvent, WhaleEvent
from ..config.thresholds import LOG_LEVEL_DEFAULT, LOG_DIR


class SessionLogger:
    """Writes session events to a JSONL file.

    In INTELLIGENCE_ONLY mode (default), only session start/end events are logged.
    In FULL mode, all flow and whale events are also logged.
    """

    def __init__(
        self,
        token_ca: str,
        log_level: str = LOG_LEVEL_DEFAULT,
        output_dir: str = LOG_DIR,
    ) -> None:
        self.token_ca = token_ca
        self.log_level = log_level
        self._dir = Path(output_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

        ts = int(time.time())
        filename = f"panda_live_session_{token_ca}_{ts}.jsonl"
        self._filepath = self._dir / filename
        self._file: Optional[TextIO] = open(self._filepath, "a", encoding="utf-8")

    def _write_line(self, data: Dict[str, Any]) -> None:
        """Write a single JSON line to the log file."""
        if self._file and not self._file.closed:
            self._file.write(json.dumps(data, separators=(",", ":")) + "\n")
            self._file.flush()

    def log_session_start(self, config: Dict[str, Any]) -> None:
        """Log session start event (always logged regardless of level)."""
        self._write_line({
            "event_type": "SESSION_START",
            "timestamp": int(time.time()),
            "token_ca": self.token_ca,
            "config": config,
        })

    def log_flow(self, flow: FlowEvent) -> None:
        """Log a flow event (only in FULL mode)."""
        if self.log_level != "FULL":
            return
        self._write_line({
            "event_type": "FLOW",
            "timestamp": flow.timestamp,
            "wallet": flow.wallet,
            "direction": flow.direction,
            "amount_sol": flow.amount_sol,
            "signature": flow.signature,
            "token_ca": flow.token_ca,
        })

    def log_whale_event(self, whale: WhaleEvent) -> None:
        """Log a whale threshold crossing event (only in FULL mode)."""
        if self.log_level != "FULL":
            return
        self._write_line({
            "event_type": whale.event_type,
            "timestamp": whale.timestamp,
            "wallet": whale.wallet,
            "amount_sol": whale.amount_sol,
            "threshold": whale.threshold,
            "token_ca": whale.token_ca,
        })

    def log_session_end(self, reason: str) -> None:
        """Log session end event (always logged regardless of level)."""
        self._write_line({
            "event_type": "SESSION_END",
            "timestamp": int(time.time()),
            "reason": reason,
        })
        self.close()

    def close(self) -> None:
        """Close the log file."""
        if self._file and not self._file.closed:
            self._file.close()

    @property
    def filepath(self) -> Path:
        """Return the path to the log file."""
        return self._filepath
