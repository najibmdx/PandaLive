"""
M1: CA Intake & Session Manager
Manages session lifecycle per CA (mint).
"""

import uuid
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any


class Session:
    """Represents a single CA monitoring session."""
    
    def __init__(
        self,
        mint: str,
        session_id: str,
        start_time: datetime,
        outdir: Path,
        cursor: Optional[Dict[str, Any]] = None
    ):
        self.mint = mint
        self.session_id = session_id
        self.start_time = start_time
        self.outdir = outdir
        self.cursor = cursor or {"slot": 0, "signature": None}
        self.status = "ACTIVE"
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "mint": self.mint,
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "status": self.status,
            "cursor": self.cursor
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], outdir: Path) -> "Session":
        return cls(
            mint=data["mint"],
            session_id=data["session_id"],
            start_time=datetime.fromisoformat(data["start_time"]),
            outdir=outdir,
            cursor=data.get("cursor")
        )


class SessionManager:
    """Manages CA session lifecycle and paths."""
    
    def __init__(self, outdir: Path):
        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.sessions: Dict[str, Session] = {}
        
    def create_session(self, mint: str, fresh: bool = False) -> Session:
        """
        Create or resume a session for a CA.
        
        Args:
            mint: Token mint address (full)
            fresh: If True, create new session. If False, try to resume.
        
        Returns:
            Session object
        """
        # Generate deterministic session_id based on timestamp
        session_id = f"{mint[:8]}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        if not fresh:
            # Try to load existing session state
            existing = self._load_session_state(mint)
            if existing:
                self.sessions[mint] = existing
                return existing
        
        # Create new session
        session = Session(
            mint=mint,
            session_id=session_id,
            start_time=datetime.now(timezone.utc),
            outdir=self.outdir
        )
        
        self.sessions[mint] = session
        self._save_session_state(session)
        
        return session
    
    def get_session(self, mint: str) -> Optional[Session]:
        """Get active session for a mint."""
        return self.sessions.get(mint)
    
    def stop_session(self, mint: str):
        """Stop a session."""
        if mint in self.sessions:
            self.sessions[mint].status = "STOPPED"
            self._save_session_state(self.sessions[mint])
    
    def get_events_path(self, mint: str) -> Path:
        """Get canonical event log path for a mint."""
        return self.outdir / f"{mint}.events.csv"
    
    def get_alerts_path(self, mint: str) -> Path:
        """Get intelligence transitions log path for a mint."""
        return self.outdir / f"{mint}.alerts.tsv"
    
    def get_minutes_path(self, mint: str) -> Path:
        """Get minute bars path for a mint (optional)."""
        return self.outdir / f"{mint}.minutes.tsv"
    
    def get_session_state_path(self, mint: str) -> Path:
        """Get session state file path."""
        return self.outdir / f"{mint}.session.json"
    
    def _save_session_state(self, session: Session):
        """Persist session state to disk."""
        state_path = self.get_session_state_path(session.mint)
        with open(state_path, 'w') as f:
            json.dump(session.to_dict(), f, indent=2)
    
    def _load_session_state(self, mint: str) -> Optional[Session]:
        """Load session state from disk."""
        state_path = self.get_session_state_path(mint)
        if not state_path.exists():
            return None
        
        try:
            with open(state_path, 'r') as f:
                data = json.load(f)
            return Session.from_dict(data, self.outdir)
        except (json.JSONDecodeError, KeyError):
            return None
    
    def update_cursor(self, mint: str, slot: int, signature: Optional[str] = None):
        """Update session cursor."""
        session = self.sessions.get(mint)
        if session:
            session.cursor = {"slot": slot, "signature": signature}
            self._save_session_state(session)


def selftest_session_manager():
    """Self-test for session manager."""
    import tempfile
    import shutil
    
    tmpdir = Path(tempfile.mkdtemp())
    try:
        # Test session creation
        sm = SessionManager(tmpdir)
        mint = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        
        session = sm.create_session(mint, fresh=True)
        assert session.mint == mint
        assert session.status == "ACTIVE"
        assert session.cursor["slot"] == 0
        
        # Test path generation
        events_path = sm.get_events_path(mint)
        assert events_path.name == f"{mint}.events.csv"
        
        alerts_path = sm.get_alerts_path(mint)
        assert alerts_path.name == f"{mint}.alerts.tsv"
        
        # Test cursor update
        sm.update_cursor(mint, 12345, "testsig")
        assert session.cursor["slot"] == 12345
        
        # Test session persistence
        sm2 = SessionManager(tmpdir)
        resumed = sm2.create_session(mint, fresh=False)
        assert resumed.cursor["slot"] == 12345
        
        print("✓ SessionManager selftest PASSED")
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    selftest_session_manager()
