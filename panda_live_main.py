#!/usr/bin/env python3
"""PANDA LIVE - Real-time memecoin situational awareness tool.

Entry point for the live monitoring system.

Usage:
    python panda_live_main.py                          # Interactive prompt
    python panda_live_main.py --token-ca MINT_ADDRESS  # Direct start
    python panda_live_main.py --demo                   # Demo mode (no Helius)

Environment:
    HELIUS_API_KEY  Required for live mode (not needed for --demo)
"""

import argparse
import os
import sys
import time

from panda_live.cli.renderer import CLIRenderer
from panda_live.config.thresholds import LOG_DIR, LOG_LEVEL_DEFAULT
from panda_live.config.wallet_names_loader import load_wallet_names
from panda_live.integrations.helius_client import HeliusClient
from panda_live.logging.session_logger import SessionLogger
from panda_live.models.events import FlowEvent
from panda_live.orchestration.live_processor import LiveProcessor


def build_demo_flows(token_ca: str) -> list:
    """Generate demo FlowEvent sequence demonstrating all state transitions."""
    t0 = int(time.time()) - 600  # Start 10 minutes ago

    wallets = [chr(ord("A") + i) * 44 for i in range(10)]

    flows = [
        # Episode 1: Early whales arrive
        FlowEvent(wallets[0], t0, "buy", 12.0, "sig_a1", token_ca),
        FlowEvent(wallets[1], t0 + 20, "buy", 15.0, "sig_b1", token_ca),
        FlowEvent(wallets[2], t0 + 40, "buy", 11.0, "sig_c1", token_ca),

        # Sustained activity (for coordination -> early phase)
        FlowEvent(wallets[0], t0 + 130, "buy", 5.0, "sig_a2", token_ca),
        FlowEvent(wallets[1], t0 + 140, "buy", 6.0, "sig_b2", token_ca),
        FlowEvent(wallets[2], t0 + 150, "buy", 4.0, "sig_c2", token_ca),

        # Persistence signals (wallet 0 and 1 re-appear in different minute buckets)
        FlowEvent(wallets[0], t0 + 200, "buy", 3.0, "sig_a3", token_ca),
        FlowEvent(wallets[1], t0 + 210, "buy", 3.0, "sig_b3", token_ca),

        # New non-early wallets arrive (participation expansion)
        FlowEvent(wallets[3], t0 + 300, "buy", 10.0, "sig_d1", token_ca),
        FlowEvent(wallets[4], t0 + 310, "buy", 12.0, "sig_e1", token_ca),
        FlowEvent(wallets[5], t0 + 320, "buy", 11.0, "sig_f1", token_ca),

        # Pressure peaking (many wallets in 2-min window)
        FlowEvent(wallets[6], t0 + 350, "buy", 10.0, "sig_g1", token_ca),
        FlowEvent(wallets[7], t0 + 360, "buy", 15.0, "sig_h1", token_ca),
        FlowEvent(wallets[0], t0 + 370, "buy", 8.0, "sig_a4", token_ca),
        FlowEvent(wallets[8], t0 + 380, "buy", 10.0, "sig_i1", token_ca),

        # Late sells (some early wallets start selling)
        FlowEvent(wallets[1], t0 + 450, "sell", 10.0, "sig_b4", token_ca),
        FlowEvent(wallets[2], t0 + 460, "sell", 8.0, "sig_c3", token_ca),
    ]

    return flows


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="PANDA LIVE - Real-time memecoin situational awareness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Environment: Set HELIUS_API_KEY for live mode.",
    )
    parser.add_argument(
        "--token-ca",
        type=str,
        default=None,
        help="Token mint address (skip interactive prompt)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=LOG_LEVEL_DEFAULT,
        choices=["FULL", "INTELLIGENCE_ONLY"],
        help=f"Log level (default: {LOG_LEVEL_DEFAULT})",
    )
    parser.add_argument(
        "--refresh-rate",
        type=float,
        default=5.0,
        help="Panel refresh rate in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--wallet-names",
        type=str,
        default="config/wallet_names.json",
        help="Path to wallet names JSON file",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run with simulated data (no Helius connection needed)",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Get token mint address
    token_ca = args.token_ca
    if not token_ca:
        try:
            token_ca = input("Enter token mint address: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            sys.exit(0)

    if not token_ca:
        print("Error: Token mint address is required.")
        sys.exit(1)

    # Auto-create logs directory
    os.makedirs(LOG_DIR, exist_ok=True)

    # Load wallet names
    wallet_names = load_wallet_names(args.wallet_names)
    if wallet_names:
        print(f"Loaded {len(wallet_names)} wallet names.")

    # Initialize Helius client (if not demo mode)
    helius_client = None
    if not args.demo:
        api_key = os.environ.get("HELIUS_API_KEY", "")
        if not api_key:
            print("Error: HELIUS_API_KEY environment variable is required for live mode.")
            print("Set it with: export HELIUS_API_KEY='your-api-key'")
            print("Or use --demo for simulated data.")
            sys.exit(1)
        helius_client = HeliusClient(api_key=api_key)
        print(f"Helius client initialized.")

    # Initialize session logger
    session_logger = SessionLogger(
        token_ca=token_ca,
        log_level=args.log_level,
        output_dir=LOG_DIR,
    )
    print(f"Session log: {session_logger.filepath}")

    # Initialize CLI renderer
    renderer = CLIRenderer(wallet_names=wallet_names)

    # Initialize live processor
    processor = LiveProcessor(
        token_ca=token_ca,
        helius_client=helius_client,
        session_logger=session_logger,
        cli_renderer=renderer,
        refresh_rate=args.refresh_rate,
    )

    # Run
    print(f"Starting PANDA LIVE for {token_ca[:8]}...{token_ca[-4:] if len(token_ca) > 8 else ''}")
    print(f"Mode: {'DEMO' if args.demo else 'LIVE'} | Log level: {args.log_level}")
    print("Press Ctrl+C to exit.\n")

    time.sleep(1)  # Brief pause before clearing screen

    if args.demo:
        demo_flows = build_demo_flows(token_ca)
        processor.run_demo(demo_flows)
    else:
        processor.run()


if __name__ == "__main__":
    main()
