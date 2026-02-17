# STATE-BASED REAL-TIME SILENT DETECTION - DESIGN
## Complete System Redesign for True Real-Time Behavior

---

## 🎯 **CORE PRINCIPLE**

**A wallet is "silent" when it STOPS PARTICIPATING in the current phase.**

**No time thresholds. State-based. Instant detection.**

---

## 📊 **THE STATE PROGRESSION**

### **Pump States (Active Phase):**
```
IGNITION → COORDINATION_SPIKE → EARLY_PHASE → 
PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION → PRESSURE_PEAKING
```

### **Dump States (Silent Phase):**
```
EXHAUSTION_DETECTED → DISSIPATION → QUIET
```

### **Key Transition Point:**

**PRESSURE_PEAKING = PEAK**

**Wallets that stop before/at PRESSURE_PEAKING = SILENT**
**Wallets that continue after PRESSURE_PEAKING = ACTIVE**

---

## 🔧 **IMPLEMENTATION DESIGN**

### **1. Add State Tracking to WalletState**

**File:** `models/wallet_state.py`

**Add new fields:**
```python
@dataclass
class WalletState:
    # ... existing fields ...
    
    # NEW: Track state when wallet last traded
    last_trade_state: Optional[str] = None
    
    # NEW: Track if wallet is currently silent
    is_silent: bool = False
```

**Why:**
- `last_trade_state`: Know what state wallet was in when it last traded
- `is_silent`: Cache silent status (recompute on state change)

---

### **2. Update Wallet on Every Trade**

**File:** `orchestration/live_processor.py`

**When processing flow:**
```python
def process_flow(self, flow):
    # ... existing code ...
    
    # Update wallet's last trade state
    ws.last_seen = current_time
    ws.last_trade_state = self.token_state.current_state  # ← NEW
    
    # Wallet is now active (trading)
    ws.is_silent = False  # ← NEW
```

**Why:**
- Every trade records WHAT STATE token was in
- Wallet marked as active (not silent) when trading

---

### **3. Recompute Silent Status on State Change**

**File:** `orchestration/live_processor.py`

**After state transition:**
```python
def process_flow(self, flow):
    # ... process whale event ...
    
    # Evaluate state transitions
    transition = self.state_machine.evaluate_transition(...)
    if transition:
        # State changed! Recompute all silent statuses
        self._recompute_silent_statuses(current_time)  # ← NEW
        
        self.session_logger.log_state_transition(transition)
        self.renderer.add_transition(transition)
```

**Why:**
- When token state changes, silent definition changes
- Need to recompute which wallets are now silent

---

### **4. Silent Detection Logic**

**File:** `orchestration/live_processor.py`

**New method:**
```python
def _recompute_silent_statuses(self, current_time: int) -> None:
    """Recompute is_silent flag for all wallets based on current state.
    
    Silent detection rules:
    1. In PRESSURE_PEAKING or later states:
       - Wallet is silent if last traded BEFORE pressure peaking
       - OR if hasn't traded in 60 seconds (inactive)
    
    2. In earlier states:
       - Wallet is silent if hasn't traded in 60 seconds
    
    No long time thresholds - real-time detection based on state.
    """
    current_state = self.token_state.current_state
    
    # Define "peak" states (after pressure builds)
    peak_states = [
        "TOKEN_PRESSURE_PEAKING",
        "TOKEN_EXHAUSTION_DETECTED", 
        "TOKEN_DISSIPATION"
    ]
    
    # Define "early" states (before peak)
    early_states = [
        "TOKEN_IGNITION",
        "TOKEN_COORDINATION_SPIKE",
        "TOKEN_EARLY_PHASE",
        "TOKEN_PERSISTENCE_CONFIRMED",
        "TOKEN_PARTICIPATION_EXPANSION"
    ]
    
    for wallet_addr, ws in self.token_state.active_wallets.items():
        if ws.last_trade_state is None:
            # Wallet never traded (shouldn't happen)
            ws.is_silent = False
            continue
        
        # Check if wallet is inactive (no trade in 60 seconds)
        silence_duration = current_time - ws.last_seen
        inactive = silence_duration >= 60  # Short threshold for "stopped trading"
        
        if current_state in peak_states:
            # Token at/past peak
            if ws.last_trade_state in early_states:
                # Wallet stopped BEFORE peak
                ws.is_silent = True
            elif ws.last_trade_state in peak_states:
                # Wallet traded AFTER peak
                if inactive:
                    # But hasn't traded recently
                    ws.is_silent = True
                else:
                    # Still active
                    ws.is_silent = False
            else:
                # Unknown state
                ws.is_silent = inactive
        else:
            # Token still pumping (before peak)
            # Only mark silent if inactive
            ws.is_silent = inactive
```

**Why:**
- **State-based:** Wallet silent if stopped before current phase
- **Real-time:** Detects instantly when state changes
- **Fallback:** 60-second inactivity check (not 3 minutes!)

---

### **5. Update Display to Use is_silent Flag**

**File:** `models/token_state.py`

**Replace compute_silent():**
```python
def compute_silent(self, current_time: int) -> Tuple[int, int, float]:
    """Count wallets marked as silent (state-based detection).
    
    Returns:
        (silent_x, silent_y, silent_pct)
    """
    # All wallets with activity
    eligible = [
        ws for ws in self.active_wallets.values()
        if ws.activity_count >= 1
    ]
    
    silent_y = len(eligible)
    if silent_y == 0:
        return 0, 0, 0.0
    
    # Count wallets marked as silent
    silent_x = sum(1 for ws in eligible if ws.is_silent)
    
    silent_pct = round(silent_x / silent_y, 2) if silent_y > 0 else 0.0
    return silent_x, silent_y, silent_pct
```

**Why:**
- Uses pre-computed `is_silent` flag
- No time threshold calculation here
- Instant, accurate count

---

### **6. Update Exhaustion Detection**

**File:** `core/wallet_signals.py`

**Replace detect_exhaustion():**
```python
def detect_exhaustion(
    self,
    token_state: TokenState,
    current_time: int,
) -> Tuple[bool, Dict]:
    """Detect if 60%+ early wallets are silent.
    
    Uses state-based silent detection (no time threshold).
    Replacement check REMOVED - only care about early wallets.
    """
    early_wallets = token_state.early_wallets
    
    if len(early_wallets) == 0:
        return False, {}
    
    # Count silent early wallets (using is_silent flag)
    silent_early = []
    for wallet_addr in early_wallets:
        wallet_state = token_state.active_wallets.get(wallet_addr)
        if wallet_state and wallet_state.is_silent:
            silent_early.append(wallet_addr)
    
    disengagement_pct = len(silent_early) / len(early_wallets)
    
    # CHANGED: Removed replacement check!
    # Exhaustion = early wallets stopped (period)
    if disengagement_pct >= EXHAUSTION_EARLY_WALLET_PERCENT:
        return True, {
            "disengagement_pct": round(disengagement_pct, 2),
            "silent_early_count": len(silent_early),
            "total_early_count": len(early_wallets),
        }
    
    return False, {}
```

**Why:**
- Uses `is_silent` flag (state-based)
- **Removed replacement check** (design decision)
- Exhaustion = early whales stopped (ignore late buyers)

---

## 📊 **EXPECTED BEHAVIOR ON YOUR TOKEN**

### **Your .gif Token Timeline:**

**Minute 0-1: Pump (98 signals)**
- State: IGNITION → COORDINATION_SPIKE → EARLY_PHASE
- Wallets: 224 early wallets buy
- Silent: 0 (all active)

**Minute 2: Peak (65 signals)**
- State: PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION → PRESSURE_PEAKING
- Wallets: Early wallets make last trades
- Silent: 0 (all still active at peak)

**Minute 3: Post-Peak (32 signals)**
- State: PRESSURE_PEAKING (stable)
- Wallets: Early wallets STOP trading
- **RECOMPUTE TRIGGERED**
- Early wallets last_trade_state: "EARLY_PHASE" / "PARTICIPATION_EXPANSION"
- Current state: "PRESSURE_PEAKING"
- **Early wallets marked is_silent = True**
- Display: Early Silent 200/224 (90%) ← INSTANT!
- **EXHAUSTION DETECTED** (60%+ early silent)
- **State: PRESSURE_PEAKING → EXHAUSTION_DETECTED**

**Minute 4-5: Dump (139 signals)**
- State: EXHAUSTION_DETECTED
- Wallets: Late buyers (447) still trading (exit liquidity)
- Silent: Early wallets remain silent
- Display: Early Silent 210/224 (94%)

**Minute 6-8: Death (72 signals → 1 signal)**
- Activity collapses
- Late buyers stop
- **State: EXHAUSTION_DETECTED → DISSIPATION**
- Display: Total Silent 650/671 (97%)

**Minute 10+: Silence**
- No activity for 10 minutes
- **State: DISSIPATION → QUIET**

---

## ✅ **WHAT THIS FIXES**

### **Issue #1: Time-Based Detection** ✅

**BEFORE:**
- Wait 3 minutes to detect silent
- Wait 9 minutes for display
- Delayed, batch-processing logic

**AFTER:**
- State-based instant detection
- Silent marked when wallet stops in current phase
- Real-time weapon behavior

---

### **Issue #2: Missing State Transitions** ✅

**BEFORE:**
- Exhaustion never triggered (time thresholds not met)
- Stuck in PRESSURE_PEAKING

**AFTER:**
- Exhaustion triggers minute 3 (early wallets stop)
- Dissipation triggers minute 8 (everyone stops)
- Full state progression works

---

### **Issue #3: Opaque Logic** ✅

**BEFORE:**
- Display: Silent 0/671 (0%)
- Can't see what's happening
- No diagnostics

**AFTER:**
- Display: Early Silent 200/224 (90%)
- Clear, instant, transparent
- Real-time feedback

---

### **Issue #4: Replacement Blocking** ✅

**BEFORE:**
- Late buyers blocked exhaustion
- 447 late buyers kept token "alive"
- Exhaustion never triggered

**AFTER:**
- Replacement check REMOVED
- Exhaustion = early wallets stopped (period)
- Late buyers irrelevant (exit liquidity)

---

## 📋 **IMPLEMENTATION CHECKLIST**

### **Files to Modify:**

1. **models/wallet_state.py**
   - Add `last_trade_state: Optional[str]`
   - Add `is_silent: bool`

2. **orchestration/live_processor.py**
   - Update `process_flow()` to set `last_trade_state`
   - Add `_recompute_silent_statuses()` method
   - Call recompute after state transitions

3. **models/token_state.py**
   - Replace `compute_silent()` to use `is_silent` flag
   - Remove time threshold logic

4. **core/wallet_signals.py**
   - Replace `detect_exhaustion()` to use `is_silent`
   - Remove replacement check
   - Remove time threshold

5. **config/thresholds.py**
   - Delete `SILENT_G_MIN_SECONDS` (no longer needed)
   - Delete `EXHAUSTION_SILENCE_THRESHOLD` (no longer needed)

---

## 🎯 **TESTING PLAN**

### **Test Case 1: Fast Pump/Dump (< 5 min)**

**Token:** Quick pump to 100K, dump to 10K in 3 minutes

**Expected:**
- Minute 1: PRESSURE_PEAKING reached
- Minute 2: Early wallets stop
- **Minute 2: EXHAUSTION triggered** (instant)
- Minute 3: DISSIPATION triggered
- **Success:** All states work on fast token

---

### **Test Case 2: Slow Grind (30+ min)**

**Token:** Gradual pump over 20 minutes, slow dump

**Expected:**
- Minute 15: PRESSURE_PEAKING reached
- Minute 20: Early wallets stop gradually
- **Minute 22: EXHAUSTION triggered** (when 60%+ early silent)
- Minute 30: DISSIPATION
- **Success:** Works on slow token too

---

### **Test Case 3: Your .gif Token (Retroactive)**

**If we had this system:**
- Minute 2: PRESSURE_PEAKING
- Minute 3: Early wallets stop
- **Minute 3: EXHAUSTION** (early silent 90%+)
- Minute 8: DISSIPATION (all activity stops)
- **Success:** Complete state progression

---

## ⏱️ **ESTIMATED TIME**

### **Implementation:**

**Phase 1: Core Changes (2 hours)**
- Add wallet state fields
- Update process_flow
- Add recompute_silent_statuses

**Phase 2: Detection Logic (1 hour)**
- Rewrite compute_silent
- Rewrite detect_exhaustion
- Remove thresholds

**Phase 3: Testing (1 hour)**
- Test on live tokens
- Verify state transitions
- Debug edge cases

**Total: 4 hours**

---

## 💬 **READY TO START?**

**This is the RIGHT way to build PANDA.**

**State-based, real-time, instant detection.**

**No more arbitrary time thresholds.**

**Want me to implement this now?**

