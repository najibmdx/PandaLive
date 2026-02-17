"""
PANDA LIVE - Main Entry Point

Real-time memecoin situational awareness.
"""

import argparse
import time
import sys
import os
import json
import requests
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from panda_live.models import TokenState, FlowEvent
from panda_live.core import (
    FlowIngestion,
    TimeWindowManager,
    WhaleDetector,
    WalletSignalsDetector,
    TokenStateMachine
)
from panda_live.logging import SessionLogger
from panda_live.config.wallet_names_loader import WalletNamesLoader
from panda_live.cli.panels import TerminalPanels


class PandaLive:
    """
    Main PANDA LIVE controller.
    
    Orchestrates flow ingestion, signal detection, state machine,
    and terminal UI rendering.
    """
    
    def __init__(
        self,
        token_ca: str,
        log_level: str = "INTELLIGENCE_ONLY",
        wallet_names_path: str = None,
        refresh_rate: float = 5.0,
        helius_api_key: str = None
    ):
        self.token_ca = token_ca
        self.refresh_rate = refresh_rate
        self.helius_api_key = helius_api_key
        
        # Initialize state
        self.token_state = TokenState(token_ca)
        
        # Initialize managers
        self.window_manager = TimeWindowManager(self.token_state)
        self.whale_detector = WhaleDetector(self.token_state)
        self.signals_detector = WalletSignalsDetector(self.token_state)
        self.state_machine = TokenStateMachine(self.token_state)
        
        # Ensure logs directory exists
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # Initialize logging
        self.logger = SessionLogger(
            token_ca=token_ca,
            log_level=log_level,
            output_dir="logs"
        )
        
        # Initialize wallet names
        self.wallet_names = WalletNamesLoader(wallet_names_path)
        
        # Initialize terminal UI
        self.panels = TerminalPanels(self.wallet_names)
        
        # Session metadata
        self.session_start = int(time.time())
        self.last_render = 0
        
        # Log session start
        config = {
            "token_ca": token_ca,
            "log_level": log_level,
            "refresh_rate": refresh_rate,
            "helius_enabled": helius_api_key is not None
        }
        self.logger.log_session_start(config)
        
        print(f"PANDA LIVE initialized for token: {token_ca}")
        print(f"Session log: {self.logger.log_path}")
        print(f"Wallet names loaded: {len(self.wallet_names.names)}")
        if helius_api_key:
            print(f"Helius API key: {'*' * 20}{helius_api_key[-4:]}")
        print()
    
    def process_flow(self, flow_event: FlowEvent):
        """
        Process a single flow event through the entire pipeline.
        
        Args:
            flow_event: FlowEvent to process
        """
        current_time = flow_event.timestamp
        
        # Set token birth if first flow
        if self.token_state.t0 == 0:
            self.token_state.set_t0(current_time)
        
        # Update time windows
        self.window_manager.process_flow(flow_event)
        
        # Detect whales
        whale_events = self.whale_detector.detect_whales(flow_event)
        
        for whale in whale_events:
            # Add to token history
            self.token_state.add_whale_event(
                whale.timestamp,
                whale.wallet,
                whale.event_type
            )
            
            # Add to event stream
            wallet_display = self.wallet_names.format_wallet_display(whale.wallet)
            self.panels.add_event(
                whale.event_type,
                f"{wallet_display} {whale.amount_sol:.1f} SOL"
            )
            
            # Check for re-ignition
            reignition = self.state_machine.handle_new_whale(whale.timestamp)
            if reignition:
                self.panels.set_state_transition(reignition)
                self.panels.add_event(
                    "STATE",
                    f"{reignition.from_state} → {reignition.to_state}"
                )
                self.logger.log_state_transition(reignition)
            
            # Detect signals
            signal_event = self.signals_detector.detect_signals(whale, current_time)
            if signal_event:
                self.panels.add_signal(signal_event)
                self.logger.log_wallet_signal(signal_event)
        
        # Evaluate state transitions
        transition = self.state_machine.evaluate_transition(current_time)
        if transition:
            self.panels.set_state_transition(transition)
            self.panels.add_event(
                "STATE",
                f"{transition.from_state} → {transition.to_state}"
            )
            self.logger.log_state_transition(transition)
        
        # Render UI if enough time has passed
        if current_time - self.last_render >= self.refresh_rate:
            self.render()
            self.last_render = current_time
    
    def render(self):
        """Render terminal UI"""
        display = self.panels.render_full_display(self.token_state)
        print(display, end='')
        sys.stdout.flush()
    
    def poll_helius(self):
        """
        Poll Helius Enhanced Transactions API for new transactions.
        """
        if not self.helius_api_key:
            print("Error: No Helius API key provided")
            return
        
        print(f"Polling Helius Enhanced Transactions API...")
        print(f"Monitoring token: {self.token_ca}")
        print(f"Poll interval: {self.refresh_rate}s")
        print()
        
        # Track last seen signature to avoid duplicates
        seen_signatures = set()
        
        # Helius Enhanced Transactions API endpoint
        url = f"https://api.helius.xyz/v0/addresses/{self.token_ca}/transactions"
        params = {
            "api-key": self.helius_api_key,
            "type": "SWAP"  # Only swap transactions
        }
        
        try:
            while True:
                try:
                    # Fetch transactions
                    response = requests.get(url, params=params, timeout=10)
                    
                    if response.status_code == 403:
                        print("Error: API key rejected (403 Forbidden)")
                        print("Please check your HELIUS_API_KEY")
                        break
                    
                    if response.status_code != 200:
                        print(f"API error: HTTP {response.status_code}")
                        time.sleep(self.refresh_rate)
                        continue
                    
                    data = response.json()
                    
                    # Process new transactions
                    new_count = 0
                    for tx in data:
                        sig = tx.get("signature")
                        
                        if sig in seen_signatures:
                            continue
                        
                        seen_signatures.add(sig)
                        
                        # Parse transaction into FlowEvent
                        flow = self.parse_helius_transaction(tx)
                        if flow:
                            self.process_flow(flow)
                            new_count += 1
                    
                    if new_count > 0:
                        print(f"[HELIUS] Processed {new_count} new transactions")
                    
                    # Render UI
                    self.render()
                    
                    # Wait before next poll
                    time.sleep(self.refresh_rate)
                
                except requests.exceptions.RequestException as e:
                    print(f"Network error: {e}")
                    time.sleep(self.refresh_rate)
                    continue
                
        except KeyboardInterrupt:
            print("\nStopping Helius polling...")
    
    def parse_helius_transaction(self, tx: dict) -> FlowEvent:
        """
        Parse Helius enhanced transaction into FlowEvent.
        
        Args:
            tx: Enhanced transaction from Helius API
        
        Returns:
            FlowEvent if valid swap, None otherwise
        """
        try:
            # Extract swap info from enhanced transaction
            signature = tx.get("signature")
            timestamp = tx.get("timestamp", int(time.time()))
            
            # Get swap details from tokenTransfers or swap field
            token_transfers = tx.get("tokenTransfers", [])
            
            # Find transfers involving our token
            for transfer in token_transfers:
                mint = transfer.get("mint")
                
                if mint != self.token_ca:
                    continue
                
                # Extract wallet (from or to)
                from_addr = transfer.get("fromUserAccount")
                to_addr = transfer.get("toUserAccount")
                amount = transfer.get("tokenAmount", 0)
                
                # Determine direction and wallet
                # If our token is being sent FROM a user = SELL
                # If our token is being sent TO a user = BUY
                if from_addr and from_addr != "":
                    wallet = from_addr
                    direction = "sell"
                elif to_addr and to_addr != "":
                    wallet = to_addr
                    direction = "buy"
                else:
                    continue
                
                # Get SOL amount from native transfers
                sol_amount = 0
                native_transfers = tx.get("nativeTransfers", [])
                for nt in native_transfers:
                    sol_amount += abs(nt.get("amount", 0)) / 1e9  # Convert lamports to SOL
                
                if sol_amount == 0:
                    sol_amount = amount  # Fallback to token amount if no SOL found
                
                # Create FlowEvent
                return FlowIngestion.normalize_flow(
                    wallet=wallet,
                    timestamp=timestamp,
                    direction=direction,
                    amount_sol=sol_amount,
                    signature=signature,
                    token_ca=self.token_ca
                )
            
        except Exception as e:
            print(f"Error parsing transaction: {e}")
            return None
        
        return None
    
    def shutdown(self):
        """Clean shutdown"""
        self.logger.log_session_end(
            reason="user_shutdown",
            final_state=self.token_state.current_state,
            episode_id=self.token_state.episode_id
        )
        self.logger.close()
        print("\nPANDA LIVE session ended.")
        print(f"Final state: {self.token_state.current_state}")
        print(f"Session log: {self.logger.log_path}")


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="PANDA LIVE - Real-time memecoin situational awareness"
    )
    
    parser.add_argument(
        "--token-ca",
        help="Token contract address (mint). If not provided, you will be prompted."
    )
    
    parser.add_argument(
        "--log-level",
        default="INTELLIGENCE_ONLY",
        choices=["FULL", "INTELLIGENCE_ONLY", "MINIMAL"],
        help="Session log level (default: INTELLIGENCE_ONLY)"
    )
    
    parser.add_argument(
        "--wallet-names",
        default="panda_live/config/wallet_names.json",
        help="Path to wallet names JSON file"
    )
    
    parser.add_argument(
        "--refresh-rate",
        type=float,
        default=5.0,
        help="Panel refresh rate in seconds (default: 5.0)"
    )
    
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run demo mode with simulated flows"
    )
    
    args = parser.parse_args()
    
    # Get token CA (prompt if not provided)
    token_ca = args.token_ca
    if not token_ca:
        token_ca = input("Enter token mint address: ").strip()
        if not token_ca:
            print("Error: Token mint address is required")
            sys.exit(1)
    
    # Get Helius API key from environment
    helius_api_key = os.environ.get("HELIUS_API_KEY")
    
    if not args.demo and not helius_api_key:
        print("Warning: HELIUS_API_KEY environment variable not set")
        print("Please set it with: export HELIUS_API_KEY='your-key-here'")
        print()
        use_demo = input("Run in demo mode instead? (y/n): ").strip().lower()
        if use_demo == 'y':
            args.demo = True
        else:
            print("Exiting. Please set HELIUS_API_KEY and try again.")
            sys.exit(1)
    
    # Initialize PANDA LIVE
    panda = PandaLive(
        token_ca=token_ca,
        log_level=args.log_level,
        wallet_names_path=args.wallet_names,
        refresh_rate=args.refresh_rate,
        helius_api_key=helius_api_key
    )
    
    try:
        if args.demo:
            # Demo mode: simulate flows
            run_demo(panda)
        else:
            # Production mode: poll Helius
            print("Starting Helius polling...")
            panda.poll_helius()
    
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        panda.shutdown()
    
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        panda.shutdown()
        sys.exit(1)


def run_demo(panda: PandaLive):
    """Run demo mode with simulated flows"""
    print("Running DEMO mode with simulated flows...")
    print("Press Ctrl+C to exit")
    print()
    
    base_time = int(time.time())
    
    # Simulated flow sequence
    demo_flows = [
        # Episode 1: Early coordination spike
        ("whale1", 0, "buy", 12.0),
        ("whale2", 20, "buy", 11.0),
        ("whale3", 40, "buy", 10.5),
        
        # Persistence
        ("whale1", 120, "buy", 8.0),
        ("whale2", 140, "buy", 9.0),
        
        # Expansion
        ("whale_new_1", 300, "buy", 11.0),
        ("whale_new_2", 320, "buy", 12.0),
        
        # Pressure burst
        ("whale_peak_1", 400, "buy", 10.0),
        ("whale_peak_2", 410, "buy", 10.5),
        ("whale_peak_3", 420, "buy", 11.0),
        ("whale_peak_4", 430, "buy", 12.0),
        ("whale_peak_5", 440, "buy", 13.0),
    ]
    
    for wallet, offset, direction, amount in demo_flows:
        timestamp = base_time + offset
        
        # Create flow event
        flow = FlowIngestion.normalize_flow(
            wallet=wallet,
            timestamp=timestamp,
            direction=direction,
            amount_sol=amount,
            signature=f"sig_{wallet}_{offset}",
            token_ca=panda.token_ca
        )
        
        if flow:
            panda.process_flow(flow)
        
        # Sleep to simulate real-time
        time.sleep(0.5)
    
    # Final render
    panda.render()
    
    print("\n\nDemo complete. Press Ctrl+C to exit, or wait for manual shutdown...")
    
    # Keep UI alive
    try:
        while True:
            time.sleep(panda.refresh_rate)
            panda.render()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
