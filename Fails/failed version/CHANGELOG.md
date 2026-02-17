# PANDA LIVE 5.0 - Changelog

## Version 5.0.1 - Interactive Mode Update

**Date:** February 5, 2024

### Changes

#### New Default Behavior: Interactive Mode

The system now defaults to interactive mode for the best user experience:

**Before:**
```bash
python panda_live.py --mint <MINT> --outdir <DIR> --helius-api-key <KEY>
```

**After (New Default):**
```bash
# Set API key once
export HELIUS_API_KEY=your_key_here

# Run in interactive mode
python panda_live.py
# System prompts: Enter token mint address: [you paste mint here]
```

#### What Changed

1. **Interactive Prompts**
   - User is prompted for mint address if not provided via `--mint`
   - Clear, user-friendly prompt with instructions

2. **Auto-Created Output Directory**
   - Default output directory: `./panda_live_data/`
   - Auto-created if it doesn't exist
   - Can still override with `--outdir` flag

3. **Environment Variable Support**
   - Helius API key reads from `HELIUS_API_KEY` environment variable
   - Set once, use everywhere
   - Can still override with `--helius-api-key` flag

4. **Backward Compatibility**
   - All original command-line flags still work
   - Existing scripts are unaffected
   - No breaking changes

#### Updated Files

- `panda_live.py` - Added interactive prompt logic and environment variable support
- `README.md` - Updated usage examples with interactive mode first
- `QUICKSTART.md` - Reorganized with interactive mode as primary example
- `DELIVERY.md` - Updated commands and acceptance tests
- `CHANGELOG.md` - This file

#### Benefits

✓ **Easier to use** - Just run `python panda_live.py`
✓ **More secure** - API key in environment, not command line
✓ **Fewer arguments** - No need to remember all flags
✓ **Still flexible** - All original options available

#### Migration Guide

**If you have existing scripts:**

No changes needed! All original flags work exactly as before.

**If you want to use the new interactive mode:**

1. Set your API key once:
   ```bash
   export HELIUS_API_KEY=your_key_here
   # Add to ~/.bashrc or ~/.zshrc to make permanent
   ```

2. Run in interactive mode:
   ```bash
   python panda_live.py
   ```

3. Enter mint address when prompted

#### Examples

**Quick Start (Interactive):**
```bash
export HELIUS_API_KEY=abc123
python panda_live.py
# Prompt: Enter token mint address: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
```

**Traditional (Command Line):**
```bash
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --outdir ./data \
  --helius-api-key abc123
```

**Replay (Still the Same):**
```bash
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --replay ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
```

**Selftest (No Changes):**
```bash
python panda_live.py --selftest
```

---

## Version 5.0.0 - Initial Release

**Date:** February 5, 2024

### Features

- End-to-end intelligence console for Solana tokens
- Real-time monitoring via Helius API
- Canonical event logging (append-only)
- Intelligence state transitions (latched)
- Deterministic replay
- Runtime audit validation
- 8 core modules, fully tested
- Comprehensive documentation

See IMPLEMENTATION_SUMMARY.md for full details.
