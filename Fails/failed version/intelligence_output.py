"""
M8: Intelligence Output Emitter
Writes intelligence transitions to CLI and alerts.tsv (transition-only, latched).
"""

import csv
from pathlib import Path
from typing import Dict, List, TextIO, Optional
from datetime import datetime


class IntelligenceTransition:
    """
    Intelligence transition (output contract).
    Latched state transitions only.
    """
    
    # Locked field ordering for TSV
    FIELDS = [
        "session_id",
        "mint",
        "token_name",
        "event_time",
        "entity_type",
        "entity_address",
        "entity_name",
        "transition_type",
        "transition_id",
        "supporting_refs"
    ]
    
    ENTITY_TYPE_WALLET = "WALLET"
    ENTITY_TYPE_TOKEN = "TOKEN"
    
    def __init__(
        self,
        session_id: str,
        mint: str,
        token_name: str,
        event_time: int,
        entity_type: str,
        entity_address: str,
        entity_name: str,
        transition_type: str,
        transition_id: str,
        supporting_refs: str = ""
    ):
        self.session_id = session_id
        self.mint = mint
        self.token_name = token_name
        self.event_time = event_time
        self.entity_type = entity_type
        self.entity_address = entity_address
        self.entity_name = entity_name
        self.transition_type = transition_type
        self.transition_id = transition_id
        self.supporting_refs = supporting_refs
    
    def to_row(self) -> Dict[str, str]:
        """Convert to TSV row dict."""
        return {
            "session_id": self.session_id,
            "mint": self.mint,
            "token_name": self.token_name,
            "event_time": str(self.event_time),
            "entity_type": self.entity_type,
            "entity_address": self.entity_address,
            "entity_name": self.entity_name,
            "transition_type": self.transition_type,
            "transition_id": self.transition_id,
            "supporting_refs": self.supporting_refs
        }
    
    @classmethod
    def from_row(cls, row: Dict[str, str]) -> "IntelligenceTransition":
        """Parse from TSV row."""
        return cls(
            session_id=row["session_id"],
            mint=row["mint"],
            token_name=row["token_name"],
            event_time=int(row["event_time"]),
            entity_type=row["entity_type"],
            entity_address=row["entity_address"],
            entity_name=row["entity_name"],
            transition_type=row["transition_type"],
            transition_id=row["transition_id"],
            supporting_refs=row.get("supporting_refs", "")
        )
    
    def __repr__(self) -> str:
        return (
            f"IntelligenceTransition({self.entity_type}:{self.entity_address[:8]}... "
            f"-> {self.transition_type})"
        )


class IntelligenceOutputWriter:
    """Append-only writer for intelligence transitions (alerts.tsv)."""
    
    def __init__(self, alerts_path: Path):
        self.alerts_path = Path(alerts_path)
        self.file_handle: TextIO | None = None
        self.writer: csv.DictWriter | None = None
        self._ensure_initialized()
    
    def _ensure_initialized(self):
        """Ensure alerts file exists with proper header."""
        is_new = not self.alerts_path.exists()
        
        if is_new:
            with open(self.alerts_path, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=IntelligenceTransition.FIELDS,
                    delimiter='\t'
                )
                writer.writeheader()
    
    def open(self):
        """Open alerts file for appending."""
        if self.file_handle is None:
            self.file_handle = open(self.alerts_path, 'a', newline='')
            self.writer = csv.DictWriter(
                self.file_handle,
                fieldnames=IntelligenceTransition.FIELDS,
                delimiter='\t'
            )
    
    def emit(self, transition: IntelligenceTransition):
        """Emit a single intelligence transition."""
        if self.writer is None:
            self.open()
        
        self.writer.writerow(transition.to_row())
        self.file_handle.flush()
    
    def emit_batch(self, transitions: List[IntelligenceTransition]):
        """Emit multiple intelligence transitions."""
        if not transitions:
            return
        
        if self.writer is None:
            self.open()
        
        for transition in transitions:
            self.writer.writerow(transition.to_row())
        
        self.file_handle.flush()
    
    def close(self):
        """Close the alerts file."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            self.writer = None
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class IntelligenceOutputReader:
    """Reader for intelligence transitions (used in replay validation)."""
    
    def __init__(self, alerts_path: Path):
        self.alerts_path = Path(alerts_path)
    
    def read_all(self) -> List[IntelligenceTransition]:
        """Read all transitions from alerts file."""
        transitions = []
        
        if not self.alerts_path.exists():
            return transitions
        
        with open(self.alerts_path, 'r', newline='') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                transitions.append(IntelligenceTransition.from_row(row))
        
        return transitions


class CLIDisplay:
    """CLI display for live intelligence monitoring."""
    
    def __init__(self, mint: str, token_name: str = ""):
        self.mint = mint
        self.token_name = token_name
        self.current_token_state = "TOKEN_QUIET"
        self.wallet_transitions: List[IntelligenceTransition] = []
        self.token_transitions: List[IntelligenceTransition] = []
    
    def display_header(self):
        """Display session header."""
        print("\n" + "="*80)
        print(f"PANDA LIVE 5.0 - Intelligence Console")
        print("="*80)
        print(f"Token CA: {self.mint}")
        if self.token_name:
            print(f"Name:     {self.token_name}")
        print("="*80 + "\n")
    
    def display_transition(self, transition: IntelligenceTransition, audit_status: str = "PASS"):
        """Display a single transition (live)."""
        timestamp = datetime.fromtimestamp(transition.event_time).strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{timestamp}] [{audit_status}]")
        print(f"  {transition.entity_type}: {transition.entity_address}", end="")
        
        if transition.entity_name:
            print(f" ({transition.entity_name})", end="")
        
        print(f"\n  -> {transition.transition_type}")
        
        if transition.supporting_refs:
            refs = transition.supporting_refs[:100]
            if len(transition.supporting_refs) > 100:
                refs += "..."
            print(f"  Refs: {refs}")
        
        print()
        
        # Update internal state
        if transition.entity_type == IntelligenceTransition.ENTITY_TYPE_WALLET:
            self.wallet_transitions.append(transition)
        else:
            self.token_transitions.append(transition)
            # Extract current state from transition type
            if "_ENTER" in transition.transition_type:
                state = transition.transition_type.replace("_ENTER", "")
                self.current_token_state = state
    
    def display_summary(self, audit_status: str = "PASS"):
        """Display current state summary."""
        print("\n" + "-"*80)
        print(f"CURRENT STATE [{audit_status}]")
        print(f"Token State: {self.current_token_state}")
        print(f"Wallet Transitions: {len(self.wallet_transitions)}")
        print(f"Token Transitions: {len(self.token_transitions)}")
        print("-"*80 + "\n")


def selftest_output():
    """Self-test for intelligence output."""
    import tempfile
    import shutil
    
    tmpdir = Path(tempfile.mkdtemp())
    try:
        alerts_path = tmpdir / "test.alerts.tsv"
        
        # Test writing
        writer = IntelligenceOutputWriter(alerts_path)
        
        transition1 = IntelligenceTransition(
            session_id="test_session",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            token_name="TestToken",
            event_time=1640000000,
            entity_type=IntelligenceTransition.ENTITY_TYPE_WALLET,
            entity_address="WalletAddress1234567890123456789012345678901234567890",
            entity_name="TestWallet",
            transition_type="WALLET_DEVIATION_ENTER",
            transition_id="test_transition_1",
            supporting_refs="sig1,sig2"
        )
        
        transition2 = IntelligenceTransition(
            session_id="test_session",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            token_name="TestToken",
            event_time=1640000100,
            entity_type=IntelligenceTransition.ENTITY_TYPE_TOKEN,
            entity_address="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            entity_name="TestToken",
            transition_type="TOKEN_IGNITION_ENTER",
            transition_id="test_transition_2",
            supporting_refs="sig3"
        )
        
        with writer:
            writer.emit(transition1)
            writer.emit(transition2)
        
        # Test reading
        reader = IntelligenceOutputReader(alerts_path)
        transitions = reader.read_all()
        
        assert len(transitions) == 2
        assert transitions[0].entity_type == IntelligenceTransition.ENTITY_TYPE_WALLET
        assert transitions[0].transition_type == "WALLET_DEVIATION_ENTER"
        assert transitions[0].entity_address == "WalletAddress1234567890123456789012345678901234567890"
        assert transitions[1].entity_type == IntelligenceTransition.ENTITY_TYPE_TOKEN
        
        print("✓ IntelligenceOutput selftest PASSED")
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    selftest_output()
