# PATTERN MINING INSTRUCTIONS
## Extract Behavioral Patterns from Your PANDA v4 Database

---

## 🎯 **OBJECTIVE**

Extract data-driven thresholds from your 7GB `masterwalletsdb.db` to build pattern-based silent detection.

**What we'll mine:**
1. Activity drop thresholds (what % drop = "went silent")
2. Exit behavior patterns (sell → stop = exit?)
3. Silence duration thresholds (how long = "truly gone")
4. Early vs late wallet differences

---

## 📋 **STEP-BY-STEP INSTRUCTIONS**

### **Step 1: Download the Mining Script**

1. Download `mine_patterns.py` from this chat
2. Save to same directory as `masterwalletsdb.db`

**Your directory should have:**
```
C:\iSight\MiniCowScanner\Duck-GooseMerger\WalletScanner\
├── masterwalletsdb.db (7GB)
└── mine_patterns.py (new file)
```

---

### **Step 2: Run the Mining Script**

**Open Command Prompt:**
```cmd
cd C:\iSight\MiniCowScanner\Duck-GooseMerger\WalletScanner
```

**Run the script:**
```cmd
python mine_patterns.py --db masterwalletsdb.db --output patterns_report.json
```

**Expected output:**
```
================================================================================
PANDA v4 PATTERN MINING
================================================================================

📊 Database Info:
   Tables: files, swaps, spl_transfers, wallet_token_flow, whale_events
   wallet_token_flow: 1,234,567 rows
   ...

🔍 Mining Pattern 1: Activity Drop Thresholds...
   ✓ Activity Drop Analysis:
      Sample: 1,000 wallets
      Median drop: 72.3%
      P75: 85.1%
      P90: 94.2%

🔍 Mining Pattern 2: Exit Patterns...
   ✓ Exit Pattern Analysis:
      Sample: 843 wallets
      Exit after sell: 68.4%

🔍 Mining Pattern 3: Silence Duration Thresholds...
   ✓ Silence Duration Analysis:
      Median: 145s (2.4min)
      P75: 238s (4.0min)
      P90: 421s (7.0min)

✓ Report saved to: patterns_report.json

================================================================================
MINING COMPLETE
================================================================================
```

---

### **Step 3: Upload the Report**

**After the script finishes:**
1. Find `patterns_report.json` in same directory
2. Upload it to this chat
3. I'll analyze the patterns and build pattern-based detection

**File will be small (< 10KB) - easy to upload!**

---

## 🔧 **TROUBLESHOOTING**

### **Error: "No such table: wallet_token_flow"**

**Your database might use different table names.**

**Check what tables exist:**
```cmd
python -c "import sqlite3; conn = sqlite3.connect('masterwalletsdb.db'); cursor = conn.cursor(); cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\"'); print([row[0] for row in cursor.fetchall()])"
```

**Send me the table names**, I'll adjust the script.

---

### **Error: "No column named event_time"**

**Check column names:**
```cmd
python -c "import sqlite3; conn = sqlite3.connect('masterwalletsdb.db'); cursor = conn.cursor(); cursor.execute('PRAGMA table_info(wallet_token_flow)'); print([row[1] for row in cursor.fetchall()])"
```

**Send me the column names**, I'll adjust the script.

---

### **Script runs but finds no data**

**The database might be empty or have different structure.**

**Check row counts:**
```cmd
python -c "import sqlite3; conn = sqlite3.connect('masterwalletsdb.db'); cursor = conn.cursor(); cursor.execute('SELECT COUNT(*) FROM wallet_token_flow'); print(f'Rows: {cursor.fetchone()[0]:,}')"
```

**If 0 rows**, the table exists but is empty.

---

## ⏱️ **EXPECTED RUNTIME**

**Database size: 7GB**

**Estimated time:**
- Loading data: 2-5 minutes
- Pattern mining: 5-10 minutes
- **Total: 7-15 minutes**

**The script samples data (doesn't analyze all 7GB) to be fast.**

---

## 📊 **WHAT THE SCRIPT DOES**

### **Pattern 1: Activity Drop**
- Samples 1,000 wallets
- Compares first-half vs second-half trading frequency
- Calculates: "What % drop when wallets go silent?"
- **Output:** Threshold like "80% drop = silent"

### **Pattern 2: Exit Behavior**
- Analyzes last trades
- Counts: How many exit after selling vs buying
- **Output:** "68% exit after selling" → use in detection

### **Pattern 3: Silence Duration**
- Measures gaps between trades
- Calculates: "How long is too long?"
- **Output:** Threshold like "4 minutes = gone"

### **Pattern 4: Early vs Late**
- Compares early entrants vs late FOMO
- **Output:** Behavioral differences (if any)

---

## 🎯 **NEXT STEPS AFTER MINING**

**Once you upload `patterns_report.json`:**

1. I'll analyze the REAL data-driven thresholds
2. I'll build pattern-based detection using YOUR data
3. No more guessing - pure data-driven logic
4. Build PANDA with confidence

**Estimated time after you upload report: 4-5 hours implementation**

---

## 💬 **READY?**

**Run the script and upload the report!**

**Commands:**
```cmd
cd C:\iSight\MiniCowScanner\Duck-GooseMerger\WalletScanner
python mine_patterns.py --db masterwalletsdb.db --output patterns_report.json
```

**Then upload `patterns_report.json` to this chat.**

**Let me know when you're ready or if you hit any errors!**

