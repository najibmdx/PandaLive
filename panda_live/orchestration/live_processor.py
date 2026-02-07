"""Main event processing loop for PANDA LIVE.

Orchestrates all phases in real-time:
1. Poll Helius -> get FlowEvents
2. Phase 1: Normalize flow, detect whale thresholds
3. Phase 2: Detect wallet signals
4. Phase 3: Evaluate state transitions
5. Phase 3.5: Compute severity (integrated in state machine)
6. Log events to JSONL
7. Update CLI renderer
"""

import time
from typing import List, Optional

from ..cli.renderer import CLIRenderer
from ..config.thresholds import EARLY_WINDOW
from ..core.signal_aggregator import SignalAggregator
from ..core.time_windows import TimeWindowManager
from ..core.token_state_machine import TokenStateMachine
from ..core.whale_detection import WhaleDetector
from ..integrations.helius_client import HeliusClient
from ..logging.session_logger import SessionLogger
from ..models.events import FlowEvent, WhaleEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState


class LiveProcessor:
    """Orchestrates all PANDA LIVE phases in a real-time event loop."""

    def __init__(
        self,
        token_ca: str,
        helius_client: Optional[HeliusClient],
        session_logger: SessionLogger,
        cli_renderer: CLIRenderer,
        refresh_rate: float = 5.0,
    ) -> None:
        self.token_ca = token_ca
        self.helius_client = helius_client
        self.session_logger = session_logger
        self.renderer = cli_renderer
        self.refresh_rate = refresh_rate

        # Phase 1 components
        self.time_window_mgr = TimeWindowManager()
        self.whale_detector = WhaleDetector()

        # Phase 2 components
        self.signal_aggregator = SignalAggregator()

        # Phase 3 components (includes Phase 3.5 severity)
        self.state_machine = TokenStateMachine()

        # Token state
        self.token_state = TokenState(ca=token_ca)

        # Tracking
        self._last_refresh = 0.0
        self._processed_signatures: set = set()
        self._running = False

    def run(self) -> None:
        """Main event loop: poll -> process -> display.

        Runs until interrupted (Ctrl+C) or shutdown() is called.
        """
        self._running = True
        self.session_logger.log_session_start({
            "token_ca": self.token_ca,
            "refresh_rate": self.refresh_rate,
            "mode": "live" if self.helius_client else "demo",
        })

        self.renderer.clear_screen()
        self.renderer.add_info(f"Session started for {self.token_ca[:8]}...")

        try:
            while self._running:
                # Poll for new events
                if self.helius_client:
                    flows = self.helius_client.poll_and_parse(self.token_ca)
                    for flow in flows:
                        if flow.signature not in self._processed_signatures:
                            self._processed_signatures.add(flow.signature)
                            self.process_flow(flow)

                # Refresh display
                if self._should_refresh():
                    self._refresh_display()

                # Sleep between polls
                time.sleep(self.refresh_rate)

        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def run_demo(self, demo_flows: List[FlowEvent]) -> None:
        """Run with pre-built demo data instead of live Helius polling.

        Processes all flows, refreshes display after each batch,
        then enters idle display loop.
        """
        self._running = True
        self.session_logger.log_session_start({
            "token_ca": self.token_ca,
            "mode": "demo",
        })

        self.renderer.clear_screen()
        self.renderer.add_info("DEMO MODE - Processing simulated events...")

        # Process all demo flows
        for flow in demo_flows:
            self.process_flow(flow)

        self._refresh_display()

        self.renderer.add_info("DEMO MODE - All events processed. Press Ctrl+C to exit.")
        self._refresh_display()

        # Idle display loop
        try:
            while self._running:
                time.sleep(1.0)
                self._refresh_display()
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def process_flow(self, flow: FlowEvent) -> None:
        """Process a single flow event through all phases.

        Phase 1: Normalize, update windows, detect whales
        Phase 2: Detect wallet signals
        Phase 3+3.5: Evaluate state transitions (with severity)
        """
        current_time = flow.timestamp

        # Set token birth time (first observed swap)
        if self.token_state.t0 is None:
            self.token_state.t0 = current_time

        # Get or create wallet state
        wallet = flow.wallet
        if wallet not in self.token_state.active_wallets:
            ws = WalletState(address=wallet)
            self.token_state.active_wallets[wallet] = ws
        else:
            ws = self.token_state.active_wallets[wallet]

        # Phase 1: Time windows + whale detection
        self.time_window_mgr.add_flow(ws, flow)
        whale_events = self.whale_detector.check_thresholds(ws, flow)

        # Log flow (FULL mode only)
        self.session_logger.log_flow(flow)

        # Process each whale event through Phase 2 + 3
        for whale_event in whale_events:
            self._process_whale_event(whale_event, ws, current_time)

        # Check exhaustion periodically (token-level)
        exhaust = self.signal_aggregator.check_exhaustion(
            self.token_state, current_time
        )
        if exhaust and exhaust.signals:
            self.session_logger.log_wallet_signal(exhaust)
            self.renderer.add_wallet_signal(exhaust)

        # Phase 3: Evaluate state transitions
        transition = self.state_machine.evaluate_transition(
            self.token_state, self.signal_aggregator, current_time
        )
        if transition:
            self.session_logger.log_state_transition(transition)
            self.renderer.add_transition(transition)

    def _process_whale_event(
        self, whale_event: WhaleEvent, ws: WalletState, current_time: int
    ) -> None:
        """Process a whale event through Phase 2 signals and density tracking."""
        # Log whale event (FULL mode only)
        self.session_logger.log_whale_event(whale_event)

        # Update density tracking
        self.state_machine.density_tracker.add_whale_event(
            self.token_state, whale_event.wallet, whale_event.timestamp
        )

        # Phase 2: Detect wallet signals
        signal_event = self.signal_aggregator.process_whale_event(
            whale_event, ws, self.token_state, current_time
        )

        if signal_event.signals:
            self.session_logger.log_wallet_signal(signal_event)
            self.renderer.add_wallet_signal(signal_event)

    def _should_refresh(self) -> bool:
        """Check if enough time has elapsed for a display refresh."""
        now = time.time()
        if now - self._last_refresh >= self.refresh_rate:
            self._last_refresh = now
            return True
        return False

    def _refresh_display(self) -> None:
        """Render and display the current frame."""
        current_time = int(time.time())
        frame = self.renderer.render_frame(self.token_state, current_time)
        self.renderer.display(frame)

    def shutdown(self) -> None:
        """Clean shutdown: close logger, clear state."""
        self._running = False
        self.session_logger.log_session_end("user_shutdown")
