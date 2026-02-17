"""
PANDA LIVE Terminal Panels

Renders terminal UI panels for token state, wallet signals, and events.
"""

import shutil
from typing import List, Optional
from collections import deque
from panda_live.models.token_state import TokenState
from panda_live.models.events import WalletSignalEvent, StateTransitionEvent
from panda_live.config.wallet_names_loader import WalletNamesLoader


class TerminalPanels:
    """
    Renders terminal UI with adaptive layout.
    
    Layout (minimum 80x24):
    - Token State Panel (top, 8-12 rows)
    - Wallet Signals Panel (middle, 6-8 rows)
    - Event Stream Panel (bottom, remaining rows)
    """
    
    def __init__(self, wallet_names_loader: Optional[WalletNamesLoader] = None):
        self.wallet_names = wallet_names_loader or WalletNamesLoader()
        self.event_buffer = deque(maxlen=100)
        self.signal_buffer = deque(maxlen=20)
        self.last_state_transition = None
        
        # Get terminal size
        self.update_terminal_size()
    
    def update_terminal_size(self):
        """Update terminal dimensions"""
        size = shutil.get_terminal_size(fallback=(80, 24))
        self.cols = max(size.columns, 80)
        self.rows = max(size.lines, 24)
        
        # Calculate panel heights
        self._calculate_panel_heights()
    
    def _calculate_panel_heights(self):
        """Calculate adaptive panel heights based on terminal size"""
        total_rows = self.rows
        
        # Reserve rows for borders and spacing
        usable_rows = total_rows - 6  # 3 panel borders + 3 spacing rows
        
        if usable_rows < 18:
            # Minimum layout (24 rows)
            self.token_panel_height = 8
            self.signal_panel_height = 6
            self.event_panel_height = 8
        elif usable_rows < 30:
            # Medium layout (30-40 rows)
            self.token_panel_height = 10
            self.signal_panel_height = 8
            self.event_panel_height = usable_rows - 18
        else:
            # Large layout (40+ rows)
            self.token_panel_height = 12
            self.signal_panel_height = 10
            self.event_panel_height = usable_rows - 22
    
    def format_wallet(self, address: str) -> str:
        """
        Format wallet address with optional name.
        
        Args:
            address: Wallet address
        
        Returns:
            Formatted string: "abc...xyz (Name)" or "abc...xyz"
        """
        return self.wallet_names.format_wallet_display(address)
    
    def add_event(self, event_type: str, message: str):
        """Add event to event stream buffer"""
        import time
        timestamp = time.strftime("%H:%M:%S")
        self.event_buffer.append(f"[{timestamp}] {event_type}: {message}")
    
    def add_signal(self, signal_event: WalletSignalEvent):
        """Add wallet signal to signal buffer"""
        self.signal_buffer.append(signal_event)
    
    def set_state_transition(self, transition: StateTransitionEvent):
        """Update last state transition"""
        self.last_state_transition = transition
    
    def render_token_panel(self, token_state: TokenState) -> str:
        """
        Render token state panel.
        
        Args:
            token_state: Current token state
        
        Returns:
            Formatted panel string
        """
        lines = []
        width = self.cols
        
        # Header
        lines.append("═" * width)
        lines.append(f"TOKEN STATE: {token_state.ca[:16]}...".ljust(width))
        lines.append("─" * width)
        
        # Current state (with color indicators)
        state_display = self._colorize_state(token_state.current_state)
        lines.append(f"State: {state_display}".ljust(width))
        lines.append(f"Episode: #{token_state.episode_id}".ljust(width))
        
        # Metrics
        lines.append("─" * width)
        lines.append(f"Active Wallets: {token_state.get_active_wallet_count()}".ljust(width))
        lines.append(f"Early Wallets: {token_state.get_early_wallet_count()}".ljust(width))
        lines.append(f"Persistent Wallets: {token_state.get_persistent_wallet_count()}".ljust(width))
        
        # Last transition
        if self.last_state_transition:
            lines.append("─" * width)
            lines.append(f"Last Transition: {self.last_state_transition.from_state} → {self.last_state_transition.to_state}".ljust(width))
            lines.append(f"Trigger: {self.last_state_transition.trigger}".ljust(width))
        
        lines.append("═" * width)
        
        # Pad to panel height
        while len(lines) < self.token_panel_height + 2:
            lines.append(" " * width)
        
        return "\n".join(lines[:self.token_panel_height + 2])
    
    def render_signal_panel(self) -> str:
        """
        Render wallet signals panel.
        
        Returns:
            Formatted panel string
        """
        lines = []
        width = self.cols
        
        # Header
        lines.append("═" * width)
        lines.append("WALLET SIGNALS".ljust(width))
        lines.append("─" * width)
        
        # Recent signals (newest first)
        if self.signal_buffer:
            for signal_event in list(self.signal_buffer)[-5:]:
                wallet_display = self.format_wallet(signal_event.wallet)
                signals_str = ", ".join(signal_event.signals)
                lines.append(f"{wallet_display}: {signals_str}".ljust(width)[:width])
        else:
            lines.append("(No signals detected yet)".ljust(width))
        
        lines.append("═" * width)
        
        # Pad to panel height
        while len(lines) < self.signal_panel_height + 2:
            lines.append(" " * width)
        
        return "\n".join(lines[:self.signal_panel_height + 2])
    
    def render_event_panel(self) -> str:
        """
        Render event stream panel.
        
        Returns:
            Formatted panel string
        """
        lines = []
        width = self.cols
        
        # Header
        lines.append("═" * width)
        lines.append("EVENT STREAM".ljust(width))
        lines.append("─" * width)
        
        # Recent events (newest at bottom)
        max_events = self.event_panel_height - 2
        if self.event_buffer:
            events_to_show = list(self.event_buffer)[-max_events:]
            for event in events_to_show:
                lines.append(event.ljust(width)[:width])
        else:
            lines.append("(Waiting for events...)".ljust(width))
        
        lines.append("═" * width)
        
        # Pad to panel height
        while len(lines) < self.event_panel_height + 2:
            lines.append(" " * width)
        
        return "\n".join(lines[:self.event_panel_height + 2])
    
    def render_full_display(self, token_state: TokenState) -> str:
        """
        Render complete terminal display.
        
        Args:
            token_state: Current token state
        
        Returns:
            Complete terminal output
        """
        # Update terminal size (in case window was resized)
        self.update_terminal_size()
        
        output = []
        
        # Clear screen (ANSI escape code)
        output.append("\033[2J\033[H")
        
        # Render panels
        output.append(self.render_token_panel(token_state))
        output.append(self.render_signal_panel())
        output.append(self.render_event_panel())
        
        return "".join(output)
    
    def _colorize_state(self, state: str) -> str:
        """
        Add simple visual indicator for state.
        
        Note: Using ASCII symbols instead of ANSI colors for compatibility.
        """
        state_symbols = {
            "TOKEN_QUIET": "○ QUIET",
            "TOKEN_IGNITION": "◉ IGNITION",
            "TOKEN_COORDINATION_SPIKE": "⚡ COORDINATION_SPIKE",
            "TOKEN_EARLY_PHASE": "▶ EARLY_PHASE",
            "TOKEN_PERSISTENCE_CONFIRMED": "✓ PERSISTENCE_CONFIRMED",
            "TOKEN_PARTICIPATION_EXPANSION": "↑ PARTICIPATION_EXPANSION",
            "TOKEN_PRESSURE_PEAKING": "▲ PRESSURE_PEAKING",
            "TOKEN_EXHAUSTION_DETECTED": "⚠ EXHAUSTION_DETECTED",
            "TOKEN_DISSIPATION": "↓ DISSIPATION"
        }
        
        return state_symbols.get(state, state)
