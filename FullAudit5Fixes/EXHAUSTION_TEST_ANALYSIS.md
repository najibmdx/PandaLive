# PANDA LIVE - EXHAUSTION TEST ANALYSIS
## Token: The Goose (FJCN...pump) | 16 Minutes Live Monitoring

---

## ⚠️ **CRITICAL FINDING: EARLY WALLET EVICTION BUG STILL EXISTS!**

**Despite Fix #4, early wallets are STILL being removed from tracking!**

---

## 📊 **SESSION OVERVIEW**

### Timeline
- **Start:** 15:54:52 (timestamp 1770710092)
- **End:** 16:11:37 (timestamp 1770711057)
- **Duration:** 16m 5s (965 seconds)
- **Total events logged:** 560

### Token Performance (From Axiom Chart)
- **Peak:** 53K mcap (marked "High")
- **Current:** 13.8K mcap
- **Dump magnitude:** -74% (53K → 13.8K)
- **Pattern:** Sharp pump followed by gradual dump
- **Current state:** In decline, low activity

---

## 📊 **STATE MACHINE PROGRESSION**

### All Transitions (6 total):

```
Time       From State              → To State                    Trigger
-------------------------------------------------------------------------------------
+0m00s     TOKEN_QUIET             → TOKEN_IGNITION              new_episode [S1]
+0m01s     TOKEN_IGNITION          → TOKEN_COORDINATION_SPIKE    3+ coordinated [S2]
+1m49s     TOKEN_COORDINATION_SPIKE → TOKEN_EARLY_PHASE          sustained 123s [S2]
+1m48s     TOKEN_EARLY_PHASE       → TOKEN_PERSISTENCE_CONFIRMED 38 persistent [S5]
+1m47s     TOKEN_PERSISTENCE_CONFIRMED → TOKEN_PARTICIPATION_EXP 119 new whales [S4]
+1m47s     TOKEN_PARTICIPATION_EXP → TOKEN_PRESSURE_PEAKING      69 whales, max density [S5]
```

### ⚠️ **MISSING TRANSITIONS:**

**Expected after dump:**
- ❌ PRESSURE_PEAKING → EXHAUSTION_DETECTED (60% early silent, no replacement)
- ❌ EXHAUSTION_DETECTED → DISSIPATION (whale activity fades)
- ❌ DISSIPATION → QUIET (10min silence)

**Actual:**
- ✅ Reached PRESSURE_PEAKING at +1m47s
- ⏸️ **STUCK in PRESSURE_PEAKING for 14+ minutes**
- ❌ **NO transition to EXHAUSTION despite token dumping 74%**

---

## 🚨 **THE SMOKING GUN: EARLY WALLET COUNT**

### From CLI Display:

**At 9m 32s (peak activity):**
```
Active: 200 | Early: 40 (20%) | Persist: 68
```

**At 15m 57s (later in session):**
```
Active: 200 | Early: 19 (9%) | Persist: 64
```

### ⚠️ **EARLY WALLET COUNT DROPPED FROM 40 → 19!**

**From log analysis:**
- Total unique wallets with TIMING signal: **212 wallets**
- Total TIMING signal events: 238
- At 9m: Display shows 40 early wallets (20%)
- At 16m: Display shows 19 early wallets (9%)

**What happened:**
1. 212 wallets were marked as early (TIMING signal fired)
2. As token activity grew, active_wallets hit 200 cap
3. LRU eviction kicked in
4. **Early wallets were evicted from active_wallets**
5. Display calculation:
   ```python
   early_active = len(active_wallets.intersection(early_wallets))
   ```
6. As early wallets got evicted from active_wallets, count dropped
7. Result: **Early: 40 → Early: 19 (more than 50% decrease!)**

---

## 🔍 **ROOT CAUSE: FIX #4 INCOMPLETE**

### What Fix #4 Did:

**File:** `live_processor.py` Line 253

**BEFORE:**
```python
for addr, _ in by_lru[:to_evict]:
    del active[addr]
    self.token_state.early_wallets.discard(addr)  # ← Removed early from set
```

**AFTER Fix #4:**
```python
for addr, _ in by_lru[:to_evict]:
    del active[addr]
    # DO NOT remove from early_wallets - preserve for metrics
```

**What we THOUGHT this would do:**
- Preserve early wallets in `early_wallets` set
- Display would show all early wallets, even evicted ones

**What ACTUALLY happened:**
- Early wallets ARE preserved in `early_wallets` set ✓
- But display calculates: `early_active = active_wallets ∩ early_wallets`
- This counts ONLY early wallets that are STILL in active_wallets!
- When early wallets get evicted, they disappear from count ✗

---

## 📊 **WHY EXHAUSTION DIDN'T TRIGGER**

### Exhaustion Detection Requirements:

From `wallet_signals.py` Lines 96-141:

```python
def detect_exhaustion(token_state, current_time):
    early_wallets = token_state.early_wallets  # The set (212 wallets)
    
    # Count silent early wallets
    silent_early = []
    for wallet_addr in early_wallets:
        wallet_state = token_state.active_wallets.get(wallet_addr)  # ← PROBLEM!
        if wallet_state:
            silence_duration = current_time - wallet_state.last_seen
            if silence_duration >= 180:  # 3 minutes
                silent_early.append(wallet_addr)
    
    # Check if 60%+ early wallets are silent
    disengagement_pct = len(silent_early) / len(early_wallets)
    if disengagement_pct < 0.60:
        return False
```

### The Bug:

**Line:** `wallet_state = token_state.active_wallets.get(wallet_addr)`

**Problem:**
1. Exhaustion checks ALL wallets in `early_wallets` set (212 wallets)
2. But tries to get their state from `active_wallets` (only 200 wallets)
3. For evicted early wallets: `get(wallet_addr)` returns **None**
4. These wallets are **SKIPPED** (not counted as silent!)
5. Result: Only early wallets STILL in active_wallets are checked

**Example calculation:**
- Total early wallets in set: 212
- Early wallets in active_wallets: 19 (at 16min mark)
- Early wallets evicted: 212 - 19 = 193
- Silent check: Only checks 19 wallets (skips 193 evicted!)
- Even if all 19 are silent: 19/212 = 9% (NOT 60%!)
- Exhaustion cannot trigger!

---

## 📊 **SIGNAL DISTRIBUTION (STILL CLEAN)**

### Fix #1 Still Working:

**Total signals:** 552 in 16 minutes
**Unique (wallet, timestamp):** 552
**Duplicates:** 0 ✓

**Signal breakdown:**
- TIMING: 238 (43%)
- COORDINATION: 550 (99.6%)
- PERSISTENCE: 78 (14%)

**Signal combinations:**
```
[COORDINATION]:                     250 (45.3%)
[TIMING, COORDINATION]:             222 (40.2%)
[COORDINATION, PERSISTENCE]:         64 (11.6%)
[TIMING, COORDINATION, PERSISTENCE]: 14 (2.5%)
[TIMING]:                             2 (0.4%)
```

**✅ Fix #1 verified still working** (no duplicates)

---

## 🎯 **THE REAL PROBLEM: TWO BUGS, NOT ONE**

### Bug #1: LRU Eviction Removes from early_wallets Set
**Status:** ✅ FIXED by Fix #4

### Bug #2: Display and Exhaustion Use Wrong Data Source
**Status:** ⚠️ **DISCOVERED - NOT FIXED**

**Display calculation (Line 101 in panels.py):**
```python
early_active = len(active_addrs.intersection(token_state.early_wallets))
```
**Problem:** Only counts early wallets STILL in active_wallets

**Exhaustion detection (Line 114 in wallet_signals.py):**
```python
wallet_state = token_state.active_wallets.get(wallet_addr)
if wallet_state:  # Skips evicted wallets!
    # check silence...
```
**Problem:** Only checks early wallets STILL in active_wallets

---

## 🔧 **THE ACTUAL FIXES NEEDED**

### Fix #4B: Display Should Show Total Early Count

**File:** `panda_live/cli/panels.py` Line 101

**CURRENT:**
```python
# Counts only early wallets still active
early_active = len(active_addrs.intersection(token_state.early_wallets))
```

**SHOULD BE:**
```python
# Show total early wallets ever marked
early_total = len(token_state.early_wallets)
# Also show how many are still active (optional)
early_active = len(active_addrs.intersection(token_state.early_wallets))
```

**Display could show:**
```
Early: 212 total (19 active) (9%)
```
or simpler:
```
Early: 212 (100%) | Active: 19 (9%)
```

### Fix #5: Exhaustion Must Track Evicted Wallets

**File:** `panda_live/core/wallet_signals.py` Line 114

**CURRENT:**
```python
for wallet_addr in early_wallets:
    wallet_state = token_state.active_wallets.get(wallet_addr)
    if wallet_state:  # ← SKIPS EVICTED WALLETS!
        silence_duration = current_time - wallet_state.last_seen
        if silence_duration >= EXHAUSTION_SILENCE_THRESHOLD:
            silent_early.append(wallet_addr)
```

**OPTION A: Track last_seen separately (RECOMMENDED)**

Add to TokenState:
```python
# Track last activity for ALL wallets (including evicted)
wallet_last_seen: Dict[str, int] = field(default_factory=dict)
```

Update in live_processor when processing flows:
```python
self.token_state.wallet_last_seen[wallet] = current_time
```

Update exhaustion detection:
```python
for wallet_addr in early_wallets:
    last_seen = token_state.wallet_last_seen.get(wallet_addr, 0)
    silence_duration = current_time - last_seen
    if silence_duration >= EXHAUSTION_SILENCE_THRESHOLD:
        silent_early.append(wallet_addr)
```

**OPTION B: Never evict early wallets (SIMPLE)**

Change LRU eviction to skip early wallets:
```python
def _enforce_wallet_cap(self):
    active = self.token_state.active_wallets
    if len(active) < MAX_ACTIVE_WALLETS:
        return
    
    # Don't evict early wallets
    non_early = {addr: ws for addr, ws in active.items() 
                 if addr not in self.token_state.early_wallets}
    
    if len(non_early) >= MAX_ACTIVE_WALLETS:
        # Evict from non-early wallets only
        by_lru = sorted(non_early.items(), key=lambda kv: kv[1].last_seen)
        to_evict = len(active) - MAX_ACTIVE_WALLETS + 1
        for addr, _ in by_lru[:to_evict]:
            del active[addr]
    else:
        # All wallets are early, use normal LRU
        by_lru = sorted(active.items(), key=lambda kv: kv[1].last_seen)
        to_evict = len(active) - MAX_ACTIVE_WALLETS + 1
        for addr, _ in by_lru[:to_evict]:
            del active[addr]
```

---

## 🎯 **IMPACT ON OPTION C**

### Can We Build Option C Now?

**Analysis:**

**Pattern analysis needs:**
1. ✅ Entry distribution (clean whale events) - Have this
2. ✅ Amount variance (unique amounts) - Have this
3. ⚠️ Return rate (persistence %) - Affected by eviction
4. ❌ Early/late mix (% early wallets) - **WRONG DATA**

**Specifically:**

**Early/Late Mix Pattern:**
```
Early Mix: ████████░░░░░░ 40% Early
```

**Current calculation would show:**
- At peak: 40 early / 200 active = 20%
- At 16min: 19 early / 200 active = 9%

**But actual should be:**
- Total early: 212 wallets
- Total active: 200 wallets
- Early %: 212/??? (what's the denominator?)

**The fundamental question:**
- Should "early %" be relative to active wallets (current 200)?
- Or relative to ALL wallets ever seen (could be 500+)?

**This is a DESIGN DECISION needed before Option C.**

---

## 💡 **RECOMMENDATIONS**

### Immediate (Before Option C):

**1. Fix Display to Show Total Early Count**
- Simple change in panels.py
- Shows true early wallet count
- 30 minutes work

**2. Fix Exhaustion Detection**
- Either track last_seen separately (Option A)
- Or never evict early wallets (Option B)
- 1-2 hours work

**3. Decide on Early % Semantics**
- What should denominator be?
- Active wallets (200)?
- Or total wallets seen in episode (500+)?
- This affects pattern analysis design

### Then:

**4. Test Exhaustion Again**
- Run on dumping token
- Verify EXHAUSTION triggers correctly
- Verify DISSIPATION works

**5. Build Option C**
- With correct early/late mix data
- Clean foundation verified

---

## 🎯 **VERDICT**

### Your Decision to Test Exhaustion First: **ABSOLUTELY CORRECT**

**You found TWO bugs we missed:**

**Bug #4B:** Display counts only active early wallets (not total)
**Bug #5:** Exhaustion detection skips evicted early wallets

**Without this test, we would have:**
1. ❌ Built Option C with wrong early/late mix data
2. ❌ Pattern analysis showing "0-20% early" when actually 100% early
3. ❌ Classified "organic FOMO" as "late chasing" incorrectly
4. ❌ Spent 10 hours building flawed analysis
5. ❌ Then discovered bugs and had to rebuild

**With this test, we:**
1. ✅ Discovered bugs BEFORE building Option C
2. ✅ Can fix them in 2 hours
3. ✅ Build Option C on solid foundation
4. ✅ Save 8+ hours of rework

**Your engineering judgment was correct.**

**Fix these bugs first, THEN Option C.**

---

## 📋 **NEXT STEPS**

**Priority Order:**

1. **Fix #4B:** Display total early count (30 min)
2. **Fix #5:** Exhaustion detection for evicted wallets (2 hours)
3. **Design decision:** Early % semantics (discussion)
4. **Re-test exhaustion:** Verify fixes work (1 hour)
5. **Build Option C:** Pattern analysis (10 hours)

**Total additional time: 3-4 hours before Option C**

**But prevents 8+ hours of rework.**

**Proceed with fixes?**

