"""
M4: Canonical Event Log Writer
Writes canonical events to append-only CSV log.
"""

import csv
import json
from pathlib import Path
from typing import Dict, Any, List, TextIO
from datetime import datetime


class CanonicalEvent:
    """Canonical event schema (minimal, strict)."""
    
    # Locked field ordering for CSV header
    FIELDS = [
        "session_id",
        "mint",
        "slot",
        "block_time",
        "signature",
        "event_type",
        "actors_json",
        "program_id",
        "dex",
        "token_mint",
        "amounts_json",
        "raw_ref"
    ]
    
    def __init__(
        self,
        session_id: str,
        mint: str,
        slot: int,
        block_time: int,
        signature: str,
        event_type: str,
        actors: List[str],
        program_id: str = "",
        dex: str = "",
        token_mint: str = "",
        amounts: Dict[str, Any] = None,
        raw_ref: str = ""
    ):
        self.session_id = session_id
        self.mint = mint
        self.slot = slot
        self.block_time = block_time
        self.signature = signature
        self.event_type = event_type
        self.actors = actors
        self.program_id = program_id
        self.dex = dex
        self.token_mint = token_mint
        self.amounts = amounts or {}
        self.raw_ref = raw_ref
    
    def to_row(self) -> Dict[str, str]:
        """Convert to CSV row dict with stable JSON encoding."""
        return {
            "session_id": self.session_id,
            "mint": self.mint,
            "slot": str(self.slot),
            "block_time": str(self.block_time),
            "signature": self.signature,
            "event_type": self.event_type,
            "actors_json": json.dumps(self.actors, sort_keys=True),
            "program_id": self.program_id,
            "dex": self.dex,
            "token_mint": self.token_mint,
            "amounts_json": json.dumps(self.amounts, sort_keys=True),
            "raw_ref": self.raw_ref
        }
    
    @classmethod
    def from_row(cls, row: Dict[str, str]) -> "CanonicalEvent":
        """Parse from CSV row."""
        return cls(
            session_id=row["session_id"],
            mint=row["mint"],
            slot=int(row["slot"]),
            block_time=int(row["block_time"]),
            signature=row["signature"],
            event_type=row["event_type"],
            actors=json.loads(row["actors_json"]),
            program_id=row.get("program_id", ""),
            dex=row.get("dex", ""),
            token_mint=row.get("token_mint", ""),
            amounts=json.loads(row.get("amounts_json", "{}")),
            raw_ref=row.get("raw_ref", "")
        )
    
    def __repr__(self) -> str:
        return f"CanonicalEvent(slot={self.slot}, sig={self.signature[:8]}..., type={self.event_type})"


class CanonicalEventLogWriter:
    """Append-only writer for canonical event logs."""
    
    def __init__(self, log_path: Path):
        self.log_path = Path(log_path)
        self.file_handle: TextIO | None = None
        self.writer: csv.DictWriter | None = None
        self._ensure_initialized()
    
    def _ensure_initialized(self):
        """Ensure log file exists with proper header."""
        is_new = not self.log_path.exists()
        
        if is_new:
            # Create new file with header
            with open(self.log_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CanonicalEvent.FIELDS)
                writer.writeheader()
    
    def open(self):
        """Open log for appending."""
        if self.file_handle is None:
            self.file_handle = open(self.log_path, 'a', newline='')
            self.writer = csv.DictWriter(self.file_handle, fieldnames=CanonicalEvent.FIELDS)
    
    def append(self, event: CanonicalEvent):
        """Append a single canonical event."""
        if self.writer is None:
            self.open()
        
        self.writer.writerow(event.to_row())
        self.file_handle.flush()  # Ensure immediate write
    
    def append_batch(self, events: List[CanonicalEvent]):
        """Append multiple canonical events."""
        if not events:
            return
        
        if self.writer is None:
            self.open()
        
        for event in events:
            self.writer.writerow(event.to_row())
        
        self.file_handle.flush()
    
    def close(self):
        """Close the log file."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            self.writer = None
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CanonicalEventLogReader:
    """Reader for canonical event logs (used in replay)."""
    
    def __init__(self, log_path: Path):
        self.log_path = Path(log_path)
    
    def read_all(self) -> List[CanonicalEvent]:
        """Read all events from log."""
        events = []
        
        with open(self.log_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                events.append(CanonicalEvent.from_row(row))
        
        return events
    
    def iter_events(self):
        """Iterate over events (memory efficient)."""
        with open(self.log_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield CanonicalEvent.from_row(row)


def selftest_event_log():
    """Self-test for event log writer/reader."""
    import tempfile
    import shutil
    
    tmpdir = Path(tempfile.mkdtemp())
    try:
        log_path = tmpdir / "test.events.csv"
        
        # Test writing
        writer = CanonicalEventLogWriter(log_path)
        
        event1 = CanonicalEvent(
            session_id="test_session",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            slot=12345,
            block_time=1640000000,
            signature="5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7",
            event_type="SWAP",
            actors=["wallet1", "wallet2"],
            program_id="prog1",
            dex="raydium",
            token_mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            amounts={"in": "1000", "out": "2000"}
        )
        
        event2 = CanonicalEvent(
            session_id="test_session",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            slot=12346,
            block_time=1640000001,
            signature="3j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia8",
            event_type="TOKEN_TRANSFER",
            actors=["wallet3"],
            token_mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            amounts={"amount": "500"}
        )
        
        with writer:
            writer.append(event1)
            writer.append(event2)
        
        # Test reading
        reader = CanonicalEventLogReader(log_path)
        events = reader.read_all()
        
        assert len(events) == 2
        assert events[0].slot == 12345
        assert events[0].event_type == "SWAP"
        assert events[0].actors == ["wallet1", "wallet2"]
        assert events[1].slot == 12346
        
        # Test stable JSON encoding
        row1 = event1.to_row()
        assert '"wallet1"' in row1["actors_json"]
        assert row1["amounts_json"] == '{"in": "1000", "out": "2000"}'
        
        print("✓ CanonicalEventLog selftest PASSED")
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    selftest_event_log()
