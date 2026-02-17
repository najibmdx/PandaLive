# EVENT-DRIVEN PATTERN DETECTION - IMPLEMENTATION ROADMAP
## Building Real-Time Weapon with Data-Driven Thresholds

---

## ✅ **COMPLETED SO FAR**

### **Phase 1A: Data Mining** ✅
- ✅ Extracted patterns from 7GB database
- ✅ Data-driven thresholds:
  - COHORT_WINDOW_SECONDS = 120 (2 min, P75)
  - ACTIVITY_DROP_THRESHOLD = 0.85 (85%, P75)
- ✅ Validated on .gif token retroactively

### **Phase 1B: Core Infrastructure** ✅
- ✅ Updated WalletState with pattern fields:
  - `is_silent`: Current silent status
  - `silent_pattern`: Which pattern triggered
  - `trade_history`: Recent trades (5min window)
  - `lifetime_trade_count`: Total trades
- ✅ Created EventDrivenPatternDetector class

---

## 🔧 **REMAINING WORK**

### **Phase 2: Integration (2 hours)**

**File: `live_processor.py`**

**2A. Initialize Pattern Detector** (15 min)
```python
# Add to __init__
from ..core.event_driven_patterns import EventDrivenPatternDetector

self.pattern_detector = EventDrivenPatternDetector()
```

**2B. Hook Event Trigger 1: Wallet Trade** (30 min)
```python
# In process_flow(), after processing whale events
def process_flow(self, flow):
    # ... existing code ...
    
    # EVENT TRIGGER 1: Wallet just traded
    self.pattern_detector.on_wallet_trade(ws, current_time, self.token_state)
    
    # EVENT TRIGGER 2: Token has activity (cohort comparison)
    self.pattern_detector.on_token_activity(self.token_state, current_time)
```

**2C. Hook Event Trigger 2: State Transition** (30 min)
```python
# In process_flow(), when state transition happens
transition = self.state_machine.evaluate_transition(...)
if transition:
    # EVENT TRIGGER 3: State changed
    self.pattern_detector.on_state_transition(
        self.token_state,
        transition.to_state,
        current_time
    )
```

**2D. Update Display to Use is_silent** (30 min)
```python
# File: token_state.py
def compute_silent(self, current_time):
    # Use pattern detector's compute_silent_metrics
    # (already done - just wire it up)
```

---

### **Phase 3: Exhaustion Detection Update** (1 hour)

**File: `wallet_signals.py`**

**3A. Update detect_exhaustion()** (45 min)
```python
def detect_exhaustion(self, token_state, current_time):
    early_wallets = token_state.early_wallets
    
    # Count silent early wallets (using is_silent flag)
    silent_early = []
    for wallet_addr in early_wallets:
        wallet_state = token_state.active_wallets.get(wallet_addr)
        if wallet_state and wallet_state.is_silent:  # EVENT-DRIVEN!
            silent_early.append(wallet_addr)
    
    disengagement_pct = len(silent_early) / len(early_wallets)
    
    # REMOVED: Replacement check (design decision)
    if disengagement_pct >= 0.60:
        return True, {...}
    
    return False, {}
```

**3B. Remove Old Time Thresholds** (15 min)
```python
# File: thresholds.py
# DELETE these lines:
# SILENT_G_MIN_SECONDS: int = 540
# EXHAUSTION_SILENCE_THRESHOLD: int = 180
```

---

### **Phase 4: Testing & Validation** (1-2 hours)

**4A. Unit Tests** (30 min)
- Test pattern detector in isolation
- Test cohort comparison logic
- Test state transition trigger

**4B. Live Token Test** (30-60 min)
- Run on new token
- Verify exhaustion triggers
- Check silent counts match expectations

**4C. Comparison Test** (30 min)
- Compare to old logic (if possible)
- Verify improvement

---

## 📋 **DETAILED INTEGRATION STEPS**

### **Step 1: Update live_processor.py**

**Location:** `/home/claude/panda_live_event_driven/orchestration/live_processor.py`

**Changes:**
1. Import EventDrivenPatternDetector
2. Initialize in __init__
3. Call on_wallet_trade in process_flow
4. Call on_token_activity in process_flow  
5. Call on_state_transition when state changes

---

### **Step 2: Update token_state.py**

**Location:** `/home/claude/panda_live_event_driven/models/token_state.py`

**Changes:**
1. Import pattern detector
2. Update compute_silent() to use is_silent flags
3. Remove old time-based logic

---

### **Step 3: Update wallet_signals.py**

**Location:** `/home/claude/panda_live_event_driven/core/wallet_signals.py`

**Changes:**
1. Update detect_exhaustion() to use is_silent
2. Remove time-based silence check
3. Remove replacement check

---

### **Step 4: Update thresholds.py**

**Location:** `/home/claude/panda_live_event_driven/config/thresholds.py`

**Changes:**
1. Delete SILENT_G_MIN_SECONDS
2. Delete EXHAUSTION_SILENCE_THRESHOLD
3. Add comment about event-driven patterns

---

## ⏱️ **TIME ESTIMATE**

**Phase 2: Integration** - 2 hours
**Phase 3: Exhaustion Update** - 1 hour
**Phase 4: Testing** - 1-2 hours

**Total Remaining: 4-5 hours**

---

## 💬 **READY TO CONTINUE?**

**We've completed:**
- ✅ WalletState updates
- ✅ EventDrivenPatternDetector class

**Next up:**
- Phase 2: Integration into live_processor
- Time: 2 hours

**Should I continue with Phase 2?**

