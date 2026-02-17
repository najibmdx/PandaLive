"""Rolling time window management for PANDA LIVE."""

from ..config.thresholds import WINDOW_5MIN, WINDOW_15MIN
from ..models.events import FlowEvent
from ..models.wallet_state import WalletState


class TimeWindowManager:
    """Manages rolling 5min and 15min windows for wallet flow tracking."""

    def add_flow(self, wallet_state: WalletState, flow: FlowEvent) -> None:
        """Add a flow event to the wallet's rolling windows.

        Updates 5min/15min deques, expires old entries, recalculates
        cumulative sums, and tracks 1-minute buckets.

        Args:
            wallet_state: The wallet state to update.
            flow: The incoming flow event.
        """
        ts = flow.timestamp
        amount = flow.amount_sol

        # Expire old flows first
        self.expire_old_flows(wallet_state, ts)

        # Add to rolling windows
        wallet_state.flows_5min.append((ts, amount))
        wallet_state.flows_15min.append((ts, amount))

        # Update cumulative sums
        wallet_state.cumulative_5min += amount
        wallet_state.cumulative_15min += amount

        # Track 1-minute bucket
        bucket = ts // 60
        wallet_state.minute_buckets.add(bucket)

        # Update seen timestamps and activity count
        if wallet_state.first_seen == 0:
            wallet_state.first_seen = ts
        wallet_state.last_seen = ts
        wallet_state.activity_count += 1

        # Direction tracking
        wallet_state.last_direction = flow.direction
        if flow.direction == "buy":
            wallet_state.total_buy_sol += amount
            wallet_state.buy_count += 1
        else:
            wallet_state.total_sell_sol += amount
            wallet_state.sell_count += 1
            wallet_state.has_sold = True

    def expire_old_flows(self, wallet_state: WalletState, current_time: int) -> None:
        """Remove flows older than window boundaries and adjust cumulative sums.

        Args:
            wallet_state: The wallet state to clean up.
            current_time: Current timestamp for window boundary calculation.
        """
        cutoff_5 = current_time - WINDOW_5MIN
        cutoff_15 = current_time - WINDOW_15MIN

        # Expire 5min window
        while wallet_state.flows_5min and wallet_state.flows_5min[0][0] < cutoff_5:
            _, expired_amount = wallet_state.flows_5min.popleft()
            wallet_state.cumulative_5min -= expired_amount

        # Expire 15min window
        while wallet_state.flows_15min and wallet_state.flows_15min[0][0] < cutoff_15:
            _, expired_amount = wallet_state.flows_15min.popleft()
            wallet_state.cumulative_15min -= expired_amount

        # Guard against floating point drift
        if wallet_state.cumulative_5min < 0:
            wallet_state.cumulative_5min = 0.0
        if wallet_state.cumulative_15min < 0:
            wallet_state.cumulative_15min = 0.0
