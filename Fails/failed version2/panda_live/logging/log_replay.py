"""
PANDA LIVE Log Replay

Replays session from JSONL log file for analysis and debugging.
"""

import json
from pathlib import Path
from typing import Generator, Dict, Any, List


class LogReplay:
    """
    Replays session events from JSONL log file.
    """
    
    def __init__(self, log_path: str):
        self.log_path = Path(log_path)
        
        if not self.log_path.exists():
            raise FileNotFoundError(f"Log file not found: {log_path}")
    
    def replay_events(self) -> Generator[Dict[str, Any], None, None]:
        """
        Generator that yields events from log file in chronological order.
        
        Yields:
            Event dictionaries
        """
        with open(self.log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    event = json.loads(line)
                    yield event
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse log line: {e}")
                    continue
    
    def get_all_events(self) -> List[Dict[str, Any]]:
        """
        Load all events from log file into memory.
        
        Returns:
            List of event dictionaries
        """
        return list(self.replay_events())
    
    def filter_by_event_type(self, event_type: str) -> List[Dict[str, Any]]:
        """
        Get all events of a specific type.
        
        Args:
            event_type: Event type to filter (e.g., "WALLET_SIGNAL", "TOKEN_STATE_TRANSITION")
        
        Returns:
            List of matching events
        """
        return [
            event for event in self.replay_events()
            if event.get("event_type") == event_type
        ]
    
    def get_state_transitions(self) -> List[Dict[str, Any]]:
        """Get all state transition events"""
        return self.filter_by_event_type("TOKEN_STATE_TRANSITION")
    
    def get_wallet_signals(self) -> List[Dict[str, Any]]:
        """Get all wallet signal events"""
        return self.filter_by_event_type("WALLET_SIGNAL")
    
    def get_whale_events(self) -> List[Dict[str, Any]]:
        """Get all whale detection events"""
        whale_types = ["WHALE_TX", "WHALE_CUM_5M", "WHALE_CUM_15M"]
        return [
            event for event in self.replay_events()
            if event.get("event_type") in whale_types
        ]
    
    def get_session_metadata(self) -> Dict[str, Any]:
        """
        Get session start and end metadata.
        
        Returns:
            Dictionary with 'start' and 'end' events
        """
        metadata = {"start": None, "end": None}
        
        for event in self.replay_events():
            if event.get("event_type") == "SESSION_START":
                metadata["start"] = event
            elif event.get("event_type") == "SESSION_END":
                metadata["end"] = event
        
        return metadata
    
    def print_summary(self):
        """Print session summary"""
        events = self.get_all_events()
        metadata = self.get_session_metadata()
        
        print("=" * 80)
        print(f"SESSION REPLAY: {self.log_path.name}")
        print("=" * 80)
        
        if metadata["start"]:
            start = metadata["start"]
            print(f"\nToken CA: {start.get('token_ca')}")
            print(f"Start Time: {start.get('timestamp')}")
            if "config" in start:
                print(f"Config: {json.dumps(start['config'], indent=2)}")
        
        print(f"\nTotal Events: {len(events)}")
        
        # Count by type
        event_types = {}
        for event in events:
            event_type = event.get("event_type", "UNKNOWN")
            event_types[event_type] = event_types.get(event_type, 0) + 1
        
        print("\nEvent Breakdown:")
        for event_type, count in sorted(event_types.items()):
            print(f"  {event_type}: {count}")
        
        # State transitions
        transitions = self.get_state_transitions()
        if transitions:
            print(f"\nState Transitions ({len(transitions)}):")
            for t in transitions:
                print(f"  {t['from_state']} → {t['to_state']} ({t['trigger']})")
        
        if metadata["end"]:
            end = metadata["end"]
            print(f"\nFinal State: {end.get('final_state')}")
            print(f"End Reason: {end.get('reason')}")
            print(f"Episode ID: {end.get('episode_id')}")
        
        print("=" * 80)


def main():
    """CLI entry point for log replay"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m panda_live.logging.log_replay <log_file.jsonl>")
        sys.exit(1)
    
    log_path = sys.argv[1]
    
    try:
        replay = LogReplay(log_path)
        replay.print_summary()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error replaying log: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
