# PANDA LIVE - Dynamic Threshold Implementation Guide

## Problem Solved
Fixed thresholds (10/25/50 SOL) only work for medium-cap tokens.
Small tokens ($146K like dogshit) have max flows of 3.89 SOL → no whale events triggered.

## Solution
Dynamic thresholds based on token liquidity (like trading platforms - automatic, no user configuration).

---

## Files to Create/Modify

### 1. CREATE: `panda_live/config/dynamic_thresholds.py`
**Status:** New file (download from outputs)

**What it does:**
- Calculates whale thresholds as % of liquidity pool
- `estimate_liquidity_from_swaps()` - extract liquidity from Helius data
- `calculate_thresholds()` - compute dynamic thresholds with bounds

---

### 2. MODIFY: `panda_live/integrations/helius_client.py`

**Add to class HeliusClient:**

```python
def __init__(self, ...):
    # ... existing code ...
    self._estimated_liquidity_sol: Optional[float] = None

def get_estimated_liquidity(self) -> float:
    """Get estimated token liquidity (computed from first batch of swaps)."""
    if self._estimated_liquidity_sol is None:
        return 50.0  # Default fallback
    return self._estimated_liquidity_sol

def poll_and_parse(self, mint_address: str) -> List[FlowEvent]:
    """Fetch and parse transactions in one call."""
    
    transactions = self.fetch_transactions(mint_address)
    
    # ESTIMATE LIQUIDITY on first poll (cold start)
    if self._estimated_liquidity_sol is None and transactions:
        from ..config.dynamic_thresholds import estimate_liquidity_from_swaps
        self._estimated_liquidity_sol = estimate_liquidity_from_swaps(transactions)
        print(f"[PANDA] Estimated token liquidity: {self._estimated_liquidity_sol:.1f} SOL", flush=True)
    
    events: List[FlowEvent] = []
    for tx in transactions:
        flow = self.parse_transaction(tx, mint_address)
        if flow is not None:
            events.append(flow)
    
    return events
```

---

### 3. MODIFY: `panda_live/core/whale_detection.py`

**Change from static thresholds to dynamic:**

```python
from ..config.dynamic_thresholds import DynamicThresholds, DEFAULT_LIQUIDITY_SOL

class WhaleDetector:
    """Detects whale activity using DYNAMIC thresholds."""
    
    def __init__(self, thresholds: DynamicThresholds = None):
        """Initialize with dynamic thresholds.
        
        Args:
            thresholds: DynamicThresholds object. If None, uses default (50 SOL liquidity).
        """
        if thresholds is None:
            from ..config.dynamic_thresholds import calculate_thresholds
            thresholds = calculate_thresholds(DEFAULT_LIQUIDITY_SOL)
        
        self.thresholds = thresholds
        
        # Use dynamic thresholds instead of constants
        self.whale_single_tx_sol = thresholds.whale_single_tx_sol
        self.whale_cum_5min_sol = thresholds.whale_cum_5min_sol
        self.whale_cum_15min_sol = thresholds.whale_cum_15min_sol
        
        # Rest of __init__ stays the same
        self._fired_single_tx: set = set()
        self._fired_5min: set = set()
        self._fired_15min: set = set()
```

**The rest of whale_detection.py stays EXACTLY the same** - it already uses `self.whale_single_tx_sol` etc.

---

### 4. MODIFY: `panda_live/orchestration/live_processor.py`

**Update LiveProcessor.__init__:**

```python
def __init__(
    self,
    token_ca: str,
    helius_client: Optional[HeliusClient],
    session_logger: SessionLogger,
    cli_renderer: CLIRenderer,
    refresh_rate: float = 5.0,
    replay_mode: bool = False,
) -> None:
    self.token_ca = token_ca
    self.helius_client = helius_client
    self.session_logger = session_logger
    self.renderer = cli_renderer
    self.refresh_rate = refresh_rate

    # Chain-aligned time clock
    self.clock = ChainTimeClock(replay_mode=replay_mode)

    # Phase 1 components
    self.time_window_mgr = TimeWindowManager()
    
    # Whale detector will be initialized with dynamic thresholds after first poll
    self.whale_detector = None  # ← Changed from WhaleDetector()
    
    # ... rest stays the same ...
```

**Update process_flow method:**

```python
def process_flow(self, flow: FlowEvent) -> None:
    """Process a single flow event through all phases."""
    
    current_time = flow.timestamp
    self.clock.observe(current_time)
    
    if self.token_state.t0 is None:
        self.token_state.t0 = current_time
    
    # Get or create wallet state
    wallet = flow.wallet
    if wallet not in self.token_state.active_wallets:
        self._enforce_wallet_cap()
        ws = WalletState(address=wallet)
        self.token_state.active_wallets[wallet] = ws
    else:
        ws = self.token_state.active_wallets[wallet]
    
    # INITIALIZE WHALE DETECTOR with dynamic thresholds (on first flow)
    if self.whale_detector is None:
        from ..config.dynamic_thresholds import calculate_thresholds
        
        liquidity = 50.0  # Default
        if self.helius_client:
            liquidity = self.helius_client.get_estimated_liquidity()
        
        thresholds = calculate_thresholds(liquidity)
        
        from ..core.whale_detection import WhaleDetector
        self.whale_detector = WhaleDetector(thresholds)
        
        print(f"[PANDA] Dynamic thresholds: {thresholds}", flush=True)
    
    # Phase 1: Time windows + whale detection (rest stays the same)
    self.time_window_mgr.add_flow(ws, flow)
    whale_events = self.whale_detector.check_thresholds(ws, flow)
    
    # ... rest of method stays exactly the same ...
```

---

### 5. OPTIONAL: Display Liquidity in CLI

**In `panda_live/display/panels.py` - TokenPanel:**

Add to the display (optional, for transparency):

```
TOKEN: 5q8o...pump | Liquidity: ~28 SOL | Whale: ≥0.15 SOL
```

---

## Installation Steps

### Step 1: Add dynamic_thresholds.py
```cmd
# Download dynamic_thresholds.py from outputs
# Copy to: PandaLive5\panda_live\config\dynamic_thresholds.py
```

### Step 2: Modify helius_client.py
Add liquidity estimation as shown above.

### Step 3: Modify whale_detection.py
Change `__init__` to accept DynamicThresholds.

### Step 4: Modify live_processor.py
Initialize whale detector lazily with dynamic thresholds.

### Step 5: Test
```cmd
python panda_live_main.py --token-ca 5q8oqdPgc5EJNso4C7k9j8HdUDeN7x3KnE6QJiTupump
```

---

## Expected Output

```
[PANDA] Estimated token liquidity: 28.3 SOL
[PANDA] Dynamic thresholds: DynamicThresholds(liquidity=28.3 SOL, whale_tx=0.14, whale_5m=0.28, whale_15m=0.57)
[DEBUG] Flow 3.89 SOL → Triggered 1 whale event(s)
  - WHALE_SINGLE_TX: 3.89 SOL
[09:32:45] SIGNAL: 5ZLj...8iEa -> TIMING
[09:32:45] STATE: QUIET -> IGNITION [S1]
```

---

## Testing Different Token Sizes

### Tiny token (5 SOL liquidity):
- Calculated: 0.025 SOL
- With bounds: **0.1 SOL** (floor applied)

### Small token like dogshit (30 SOL):
- Calculated: 0.15 SOL
- With bounds: **0.15 SOL** ✓

### Medium token (500 SOL):
- Calculated: 2.5 SOL
- With bounds: **2.5 SOL** ✓

### Huge token (50,000 SOL):
- Calculated: 250 SOL
- With bounds: **100 SOL** (ceiling applied)

---

## Verification

After implementing, run the diagnostic:

```cmd
python diagnose_panda_pipeline.py
```

Should show:
```
✓ Flows meet whale threshold
✓ Whale events triggered
✓ State machine activating
```

---

## Fallback Behavior

If liquidity cannot be estimated:
- Uses DEFAULT_LIQUIDITY_SOL = 50 SOL
- Results in thresholds: 0.25 / 0.5 / 1.0 SOL
- Still better than fixed 10/25/50 SOL!
