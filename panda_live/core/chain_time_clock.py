"""Chain-aligned time clock for PANDA LIVE.

Ensures all timing math uses the same time domain as on-chain event
timestamps, preventing negative deltas when session wallclock differs
from chain timestamps.

Live mode: maps wallclock into chain-time via a stable offset.
Replay mode: uses only event timestamps, no wallclock.
"""

import time
from typing import Optional


class ChainTimeClock:
    """Maps wallclock time into chain-aligned time.

    Maintains the offset between system wallclock and on-chain timestamps
    so that chain_now() always returns a value in the same domain as
    event timestamps, and never below the latest observed chain timestamp.
    """

    def __init__(self, replay_mode: bool = False) -> None:
        self._replay_mode = replay_mode
        self._last_chain_ts: Optional[int] = None
        self._offset: Optional[int] = None  # wallclock - chain

    def observe(self, chain_ts: int) -> None:
        """Record an observed on-chain timestamp.

        Updates the offset on first observation and tracks the maximum
        chain timestamp seen.

        Args:
            chain_ts: On-chain event timestamp (unix epoch seconds).
        """
        if self._last_chain_ts is None or chain_ts > self._last_chain_ts:
            self._last_chain_ts = chain_ts

        if self._offset is None:
            self._offset = int(time.time()) - chain_ts

    def now(self) -> int:
        """Return chain-aligned current time.

        Replay mode: returns last observed chain timestamp.
        Live mode: maps wallclock via offset, clamped to >= last_chain_ts.
        Fallback (no events yet): returns wallclock.

        Returns:
            Chain-aligned current timestamp.
        """
        if self._last_chain_ts is None:
            return int(time.time())

        if self._replay_mode:
            return self._last_chain_ts

        # Live mode: map wallclock into chain domain
        chain_now = int(time.time()) - self._offset
        # Never go below the latest observed chain timestamp
        return max(chain_now, self._last_chain_ts)

    @property
    def last_chain_ts(self) -> Optional[int]:
        """The maximum on-chain timestamp observed so far."""
        return self._last_chain_ts

    @property
    def offset(self) -> Optional[int]:
        """The wallclock-to-chain offset (wallclock - chain). None if no events yet."""
        return self._offset
