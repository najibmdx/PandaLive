#!/usr/bin/env python3
"""
PANDA LIVE 5.0
End-to-end, audit-ready, deterministic, real-time intelligence console.

OUTPUTS: Intelligence state transitions only (latched).
NEVER: price / TA / PnL / scores / rankings / telemetry as product output.
"""

import sys
import time
import argparse
from pathlib import Path
from typing import Optional

from session_manager import SessionManager
from event_log import CanonicalEventLogWriter
from ingestion import SolanaIngestion, CanonicalEventNormalizer
from intelligence_output import (
    IntelligenceOutputWriter,
    CLIDisplay
)
from intelligence_engine import (
    V4Primitives,
    IncrementalPrimitiveUpdater,
    WalletIntelligenceEngine,
    TokenIntelligenceCompressor
)
from audit_gate import AuditGate
from replay import ReplayRunner


class PandaLive:
    """Main orchestrator for PANDA LIVE 5.0."""
    
    def __init__(
        self,
        mint: str,
        outdir: Path,
        helius_api_key: Optional[str] = None,
        fresh: bool = False
    ):
        self.mint = mint
        self.outdir = Path(outdir)
        self.helius_api_key = helius_api_key
        self.fresh = fresh
        
        # Initialize components
        self.session_manager = SessionManager(self.outdir)
        self.audit_gate = AuditGate()
        
        # Session
        self.session = None
        
        # Token name (could be fetched from chain metadata)
        self.token_name = ""
    
    def run_live(self):
        """Run live monitoring mode."""
        print("\n" + "="*80)
        print("PANDA LIVE 5.0 - Starting Live Mode")
        print("="*80)
        
        if not self.helius_api_key:
            print("ERROR: Helius API key required for live mode")
            print("Use: --helius-api-key <KEY>")
            return
        
        # Create/resume session
        self.session = self.session_manager.create_session(self.mint, fresh=self.fresh)
        
        print(f"Session ID: {self.session.session_id}")
        print(f"Mint: {self.mint}")
        print(f"Output Dir: {self.outdir}")
        print(f"Fresh: {self.fresh}")
        print("="*80)
        
        # Get file paths
        events_path = self.session_manager.get_events_path(self.mint)
        alerts_path = self.session_manager.get_alerts_path(self.mint)
        
        # Initialize pipeline components
        ingestion = SolanaIngestion(self.helius_api_key, self.mint)
        normalizer = CanonicalEventNormalizer(self.session.session_id, self.mint)
        
        primitives = V4Primitives(
            mint=self.mint,
            session_id=self.session.session_id
        )
        updater = IncrementalPrimitiveUpdater(primitives)
        wallet_engine = WalletIntelligenceEngine(primitives)
        token_compressor = TokenIntelligenceCompressor(primitives)
        
        # Initialize output
        event_writer = CanonicalEventLogWriter(events_path)
        output_writer = IntelligenceOutputWriter(alerts_path)
        cli_display = CLIDisplay(self.mint, self.token_name)
        
        # Display header
        cli_display.display_header()
        
        print(f"Monitoring started... (Ctrl+C to stop)\n")
        
        try:
            with event_writer, output_writer:
                while True:
                    # Poll for new transactions
                    cursor_slot = self.session.cursor.get("slot", 0)
                    cursor_sig = self.session.cursor.get("signature")
                    
                    new_txs, new_slot, new_sig = ingestion.poll_new_transactions(
                        cursor_slot,
                        cursor_sig
                    )
                    
                    if new_txs:
                        print(f"Fetched {len(new_txs)} new transactions")
                        
                        for tx in new_txs:
                            # Normalize to canonical events
                            events = normalizer.normalize_transaction(tx)
                            
                            for event in events:
                                # Validate event
                                if not self.audit_gate.validate_canonical_event(event):
                                    print(f"AUDIT: Event validation failed")
                                    print(self.audit_gate.report())
                                    continue
                                
                                # Write to event log
                                event_writer.append(event)
                                
                                # Update primitives
                                updater.update(event)
                                
                                # Check wallet intelligence
                                wallet_transitions = wallet_engine.check_transitions(
                                    event,
                                    self.token_name
                                )
                                
                                # Emit wallet transitions
                                for transition in wallet_transitions:
                                    if self.audit_gate.validate_intelligence_transition(transition):
                                        if self.audit_gate.status == "PASS":
                                            output_writer.emit(transition)
                                            cli_display.display_transition(
                                                transition,
                                                self.audit_gate.status
                                            )
                                        else:
                                            print(self.audit_gate.report())
                                
                                # Check token state transition
                                if wallet_transitions:
                                    token_transition = token_compressor.compress(
                                        wallet_transitions,
                                        event.block_time,
                                        self.token_name
                                    )
                                    
                                    if token_transition:
                                        if self.audit_gate.validate_intelligence_transition(token_transition):
                                            if self.audit_gate.status == "PASS":
                                                output_writer.emit(token_transition)
                                                cli_display.display_transition(
                                                    token_transition,
                                                    self.audit_gate.status
                                                )
                                            else:
                                                print(self.audit_gate.report())
                        
                        # Update cursor
                        self.session_manager.update_cursor(self.mint, new_slot, new_sig)
                    
                    # Sleep before next poll
                    time.sleep(5)
        
        except KeyboardInterrupt:
            print("\n\nStopping...")
            cli_display.display_summary(self.audit_gate.status)
            self.session_manager.stop_session(self.mint)
            print("Session stopped.")
    
    def run_replay(self, events_log_path: Path):
        """Run deterministic replay mode."""
        print("\n" + "="*80)
        print("PANDA LIVE 5.0 - Replay Mode")
        print("="*80)
        
        events_path = Path(events_log_path)
        
        if not events_path.exists():
            print(f"ERROR: Events log not found: {events_path}")
            return
        
        print(f"Events log: {events_path}")
        
        # Determine output path for replay
        replay_alerts_path = self.outdir / f"{self.mint}_replay.alerts.tsv"
        
        print(f"Replay output: {replay_alerts_path}")
        print("="*80)
        
        # Run replay
        replay_runner = ReplayRunner(
            events_log_path=events_path,
            replay_alerts_path=replay_alerts_path,
            audit_gate=self.audit_gate
        )
        
        transitions = replay_runner.run(token_name=self.token_name)
        
        print(f"\nReplay complete: {len(transitions)} transitions")
        
        # Compare with original if exists
        original_alerts_path = self.session_manager.get_alerts_path(self.mint)
        
        if original_alerts_path.exists():
            is_deterministic = replay_runner.compare_with_original(original_alerts_path)
            
            if is_deterministic:
                print("\n✓ DETERMINISM VERIFIED")
            else:
                print("\n✗ DETERMINISM CHECK FAILED")
        else:
            print(f"\nNo original alerts file found at {original_alerts_path}")
            print("Cannot verify determinism (this may be expected for first run)")


def run_selftest():
    """Run all module selftests."""
    print("\n" + "="*80)
    print("PANDA LIVE 5.0 - Self-Test Suite")
    print("="*80 + "\n")
    
    try:
        from session_manager import selftest_session_manager
        from event_log import selftest_event_log
        from ingestion import selftest_normalizer
        from audit_gate import selftest_audit_gate
        from intelligence_output import selftest_output
        from intelligence_engine import selftest_intelligence_engine
        from replay import selftest_replay
        
        selftest_session_manager()
        selftest_event_log()
        selftest_normalizer()
        selftest_audit_gate()
        selftest_output()
        selftest_intelligence_engine()
        selftest_replay()
        
        print("\n" + "="*80)
        print("✓ ALL SELFTESTS PASSED")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n✗ SELFTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PANDA LIVE 5.0 - Real-time intelligence console",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (default)
  python panda_live.py

  # Live monitoring with args
  python panda_live.py --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \\
      --outdir ./data --helius-api-key YOUR_KEY

  # Fresh session
  python panda_live.py --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr --fresh

  # Replay from event log
  python panda_live.py --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \\
      --replay ./data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv

  # Run selftest
  python panda_live.py --selftest
        """
    )
    
    parser.add_argument(
        "--mint",
        type=str,
        help="Token mint address (CA) - if not provided, will prompt"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        help="Output directory for logs (default: ./panda_live_data)"
    )
    
    parser.add_argument(
        "--helius-api-key",
        type=str,
        help="Helius API key (default: reads from HELIUS_API_KEY env var)"
    )
    
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Create fresh session (don't resume)"
    )
    
    parser.add_argument(
        "--replay",
        type=str,
        help="Replay mode: path to <MINT>.events.csv"
    )
    
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="Run self-test suite"
    )
    
    args = parser.parse_args()
    
    # Selftest mode
    if args.selftest:
        run_selftest()
        return
    
    # Interactive mode: prompt for mint if not provided
    mint = args.mint
    if not mint and not args.replay:
        print("\n" + "="*80)
        print("PANDA LIVE 5.0 - Interactive Mode")
        print("="*80)
        mint = input("\nEnter token mint address: ").strip()
        
        if not mint:
            print("ERROR: Mint address is required")
            sys.exit(1)
    
    # Auto-create default output directory if not specified
    outdir = args.outdir
    if not outdir:
        outdir = "./panda_live_data"
        print(f"Using default output directory: {outdir}")
    
    # Get Helius API key from environment if not provided
    helius_api_key = args.helius_api_key
    if not helius_api_key and not args.replay:
        import os
        helius_api_key = os.environ.get("HELIUS_API_KEY")
        
        if not helius_api_key:
            print("\nERROR: Helius API key not found")
            print("Either:")
            print("  1. Set environment variable: export HELIUS_API_KEY=your_key")
            print("  2. Pass via argument: --helius-api-key YOUR_KEY")
            sys.exit(1)
        
        print("Using Helius API key from environment variable")
    
    # For replay mode, extract mint from replay path if not provided
    if args.replay and not mint:
        replay_path = Path(args.replay)
        # Try to extract mint from filename pattern: <MINT>.events.csv
        filename = replay_path.stem  # Gets filename without extension
        if filename.endswith('.events'):
            mint = filename.replace('.events', '')
        else:
            print("ERROR: Could not determine mint address from replay file")
            print("Please provide --mint argument")
            sys.exit(1)
    
    # Create PandaLive instance
    panda = PandaLive(
        mint=mint,
        outdir=Path(outdir),
        helius_api_key=helius_api_key,
        fresh=args.fresh
    )
    
    # Run mode
    if args.replay:
        panda.run_replay(Path(args.replay))
    else:
        panda.run_live()


if __name__ == "__main__":
    main()
