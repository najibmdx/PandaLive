"""
PANDA LIVE Session Logger

Logs events to JSONL file for replay and audit.
"""

import json
import time
from pathlib import Path
from typing import Optional, Dict, Any
from panda_live.models.events import (
    FlowEvent, WhaleEvent, WalletSignalEvent,
    StateTransitionEvent, SessionStartEvent, SessionEndEvent
)
from panda_live.config.thresholds import LOG_LEVEL_DEFAULT


class SessionLogger:
    """
    Logs session events to JSONL file.
    
    Log levels:
    - FULL: All events (flows, whales, signals, states)
    - INTELLIGENCE_ONLY: Only signals and states (default)
    - MINIMAL: Only state transitions
    """
    
    def __init__(
        self,
        token_ca: str,
        log_level: str = LOG_LEVEL_DEFAULT,
        output_dir: str = "logs"
    ):
        self.token_ca = token_ca
        self.log_level = log_level
        self.output_dir = Path(output_dir)
        
        # Create logs directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate log filename
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        short_ca = token_ca[:8] if len(token_ca) > 8 else token_ca
        filename = f"panda_live_session_{short_ca}_{timestamp}.jsonl"
        self.log_path = self.output_dir / filename
        
        # Open file for append
        self.file_handle = open(self.log_path, 'a')
        
        print(f"Session log: {self.log_path}")
    
    def _write_event(self, event_dict: Dict[str, Any]):
        """Write event as JSON line"""
        json_line = json.dumps(event_dict, ensure_ascii=False)
        self.file_handle.write(json_line + '\n')
        self.file_handle.flush()
    
    def log_session_start(self, config: Dict[str, Any]):
        """Log session start event"""
        event = SessionStartEvent(
            timestamp=int(time.time()),
            token_ca=self.token_ca,
            config=config
        )
        self._write_event(event.to_dict())
    
    def log_flow(self, flow: FlowEvent):
        """
        Log flow event (only if log_level is FULL).
        
        Args:
            flow: FlowEvent to log
        """
        if self.log_level != "FULL":
            return
        
        event_dict = {
            "event_type": "FLOW",
            "timestamp": flow.timestamp,
            "wallet": flow.wallet,
            "direction": flow.direction,
            "amount_sol": flow.amount_sol,
            "signature": flow.signature,
            "token_ca": flow.token_ca
        }
        self._write_event(event_dict)
    
    def log_whale_detection(self, whale: WhaleEvent):
        """
        Log whale detection event (only if log_level is FULL).
        
        Args:
            whale: WhaleEvent to log
        """
        if self.log_level != "FULL":
            return
        
        self._write_event(whale.to_dict())
    
    def log_wallet_signal(self, signal: WalletSignalEvent):
        """
        Log wallet signal event (INTELLIGENCE_ONLY and FULL).
        
        Args:
            signal: WalletSignalEvent to log
        """
        if self.log_level == "MINIMAL":
            return
        
        self._write_event(signal.to_dict())
    
    def log_state_transition(self, transition: StateTransitionEvent):
        """
        Log state transition event (all log levels).
        
        Args:
            transition: StateTransitionEvent to log
        """
        self._write_event(transition.to_dict())
    
    def log_session_end(self, reason: str, final_state: str, episode_id: int):
        """
        Log session end event.
        
        Args:
            reason: Reason for session end
            final_state: Final token state
            episode_id: Final episode ID
        """
        event = SessionEndEvent(
            timestamp=int(time.time()),
            token_ca=self.token_ca,
            reason=reason,
            final_state=final_state,
            episode_id=episode_id
        )
        self._write_event(event.to_dict())
    
    def close(self):
        """Close log file"""
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
