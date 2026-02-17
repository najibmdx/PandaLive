#!/usr/bin/env python3
"""
mine_patterns.py

Extract behavioral patterns from PANDA v4 database for real-time silent detection.
Analyzes wallet behavior to derive data-driven thresholds for pattern-based detection.

Usage:
    python mine_patterns.py --db masterwalletsdb.db --output patterns_report.json
"""

import sqlite3
import json
import argparse
from collections import defaultdict, Counter
from datetime import datetime
import statistics


class PatternMiner:
    """Mine behavioral patterns from historical pump.fun token data."""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.patterns = {}
        
    def connect(self):
        """Connect to database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            print(f"✓ Connected to {self.db_path}")
            return True
        except Exception as e:
            print(f"✗ Error connecting: {e}")
            return False
    
    def get_table_info(self):
        """Get basic database info."""
        cursor = self.conn.cursor()
        
        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"\n📊 Database Info:")
        print(f"   Tables: {', '.join(tables)}")
        
        # Get row counts
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   {table}: {count:,} rows")
        
        return tables
    
    def mine_activity_patterns(self):
        """
        PATTERN 1: Activity Drop Thresholds
        
        Analyze how much wallet activity drops when they "go silent"
        Find: What % activity drop is significant?
        """
        print("\n🔍 Mining Pattern 1: Activity Drop Thresholds...")
        
        cursor = self.conn.cursor()
        
        # Check if we have wallet_token_flow table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wallet_token_flow'")
        if not cursor.fetchone():
            print("   ⚠️  wallet_token_flow table not found, skipping")
            return None
        
        # Get schema
        cursor.execute("PRAGMA table_info(wallet_token_flow)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"   Columns: {', '.join(columns[:10])}...")
        
        # Find time column
        time_col = None
        for candidate in ['event_time', 'block_time', 'flow_time', 'timestamp']:
            if candidate in columns:
                time_col = candidate
                break
        
        if not time_col:
            print("   ⚠️  No time column found")
            return None
        
        # Find wallet column
        wallet_col = None
        for candidate in ['wallet', 'wallet_address', 'scan_wallet']:
            if candidate in columns:
                wallet_col = candidate
                break
        
        if not wallet_col:
            print("   ⚠️  No wallet column found")
            return None
        
        print(f"   Using: {wallet_col}, {time_col}")
        
        # Sample wallets and analyze their activity patterns
        query = f"""
        SELECT {wallet_col}, {time_col}
        FROM wallet_token_flow
        WHERE {wallet_col} IS NOT NULL
        ORDER BY {time_col}
        LIMIT 100000
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"   Loaded {len(rows):,} flows")
        
        # Group by wallet
        wallet_times = defaultdict(list)
        for row in rows:
            wallet = row[0]
            timestamp = row[1]
            wallet_times[wallet].append(timestamp)
        
        print(f"   Found {len(wallet_times):,} unique wallets")
        
        # Analyze activity drops
        activity_drops = []
        
        for wallet, times in list(wallet_times.items())[:1000]:  # Sample 1000 wallets
            if len(times) < 5:
                continue
            
            times_sorted = sorted(times)
            
            # Calculate trades per minute in first half vs second half
            mid = len(times_sorted) // 2
            first_half = times_sorted[:mid]
            second_half = times_sorted[mid:]
            
            if len(first_half) < 2 or len(second_half) < 2:
                continue
            
            # Time span
            first_duration = (first_half[-1] - first_half[0]) / 60  # minutes
            second_duration = (second_half[-1] - second_half[0]) / 60  # minutes
            
            if first_duration < 1 or second_duration < 1:
                continue
            
            # Activity rate
            first_rate = len(first_half) / first_duration
            second_rate = len(second_half) / second_duration
            
            if first_rate < 0.1:  # Skip very inactive wallets
                continue
            
            # Activity drop
            drop_pct = (first_rate - second_rate) / first_rate if first_rate > 0 else 0
            
            if drop_pct > 0:  # Only count drops
                activity_drops.append(drop_pct)
        
        if not activity_drops:
            print("   ⚠️  No activity drop data")
            return None
        
        # Calculate statistics
        pattern = {
            'sample_size': len(activity_drops),
            'median_drop': statistics.median(activity_drops),
            'mean_drop': statistics.mean(activity_drops),
            'p25': sorted(activity_drops)[len(activity_drops)//4],
            'p50': sorted(activity_drops)[len(activity_drops)//2],
            'p75': sorted(activity_drops)[3*len(activity_drops)//4],
            'p90': sorted(activity_drops)[9*len(activity_drops)//10],
        }
        
        print(f"   ✓ Activity Drop Analysis:")
        print(f"      Sample: {pattern['sample_size']:,} wallets")
        print(f"      Median drop: {pattern['median_drop']*100:.1f}%")
        print(f"      Mean drop: {pattern['mean_drop']*100:.1f}%")
        print(f"      P75 (75% of wallets): {pattern['p75']*100:.1f}%")
        print(f"      P90 (90% of wallets): {pattern['p90']*100:.1f}%")
        
        self.patterns['activity_drop'] = pattern
        return pattern
    
    def mine_exit_patterns(self):
        """
        PATTERN 2: Exit Behavior
        
        Analyze wallets that sell then stop trading
        Find: What defines an "exit"?
        """
        print("\n🔍 Mining Pattern 2: Exit Patterns...")
        
        cursor = self.conn.cursor()
        
        # Check for direction column
        cursor.execute("PRAGMA table_info(wallet_token_flow)")
        columns = [row[1] for row in cursor.fetchall()]
        
        dir_col = None
        for candidate in ['sol_direction', 'direction', 'type']:
            if candidate in columns:
                dir_col = candidate
                break
        
        if not dir_col:
            print("   ⚠️  No direction column found")
            return None
        
        # Sample wallets with direction data
        query = f"""
        SELECT scan_wallet, block_time, {dir_col}
        FROM wallet_token_flow
        WHERE {dir_col} IS NOT NULL
        ORDER BY block_time
        LIMIT 100000
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"   Loaded {len(rows):,} flows with direction")
        
        # Analyze last-trade patterns
        wallet_trades = defaultdict(list)
        for row in rows:
            wallet = row[0]
            time = row[1]
            direction = str(row[2]).lower()
            wallet_trades[wallet].append((time, direction))
        
        # Count last trades
        last_trade_types = Counter()
        exit_patterns = []
        
        for wallet, trades in list(wallet_trades.items())[:1000]:
            if len(trades) < 3:
                continue
            
            # Sort by time
            trades_sorted = sorted(trades, key=lambda x: x[0])
            
            # Last trade
            last_time, last_dir = trades_sorted[-1]
            last_trade_types[last_dir] += 1
            
            # Check if stopped after sell
            if 'sell' in last_dir or 'out' in last_dir:
                # Check silence after last trade (if we have more data)
                # For now, just count
                exit_patterns.append({
                    'wallet': wallet,
                    'last_direction': last_dir,
                    'trade_count': len(trades)
                })
        
        pattern = {
            'sample_size': len(exit_patterns),
            'last_trade_distribution': dict(last_trade_types),
            'sell_exit_pct': sum(1 for p in exit_patterns if 'sell' in p['last_direction'].lower()) / len(exit_patterns) if exit_patterns else 0
        }
        
        print(f"   ✓ Exit Pattern Analysis:")
        print(f"      Sample: {pattern['sample_size']:,} wallets")
        print(f"      Last trade types: {pattern['last_trade_distribution']}")
        print(f"      Exit after sell: {pattern['sell_exit_pct']*100:.1f}%")
        
        self.patterns['exit_behavior'] = pattern
        return pattern
    
    def mine_silence_durations(self):
        """
        PATTERN 3: Silence Duration Thresholds
        
        Analyze how long wallets go silent before they're "truly gone"
        Find: What silence duration is significant?
        """
        print("\n🔍 Mining Pattern 3: Silence Duration Thresholds...")
        
        cursor = self.conn.cursor()
        
        query = """
        SELECT scan_wallet, block_time
        FROM wallet_token_flow
        WHERE scan_wallet IS NOT NULL
        ORDER BY scan_wallet, block_time
        LIMIT 100000
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Group by wallet
        wallet_times = defaultdict(list)
        for row in rows:
            wallet_times[row[0]].append(row[1])
        
        # Calculate gaps between trades
        gaps = []
        
        for wallet, times in list(wallet_times.items())[:1000]:
            if len(times) < 2:
                continue
            
            times_sorted = sorted(times)
            
            for i in range(1, len(times_sorted)):
                gap = times_sorted[i] - times_sorted[i-1]
                gaps.append(gap)
        
        if not gaps:
            print("   ⚠️  No gap data")
            return None
        
        # Convert to seconds and filter outliers
        gaps_seconds = [g for g in gaps if 0 < g < 86400]  # Filter: 0 - 24 hours
        
        if not gaps_seconds:
            print("   ⚠️  No valid gaps")
            return None
        
        gaps_sorted = sorted(gaps_seconds)
        
        pattern = {
            'sample_size': len(gaps_sorted),
            'median_gap_seconds': statistics.median(gaps_sorted),
            'mean_gap_seconds': statistics.mean(gaps_sorted),
            'p50': gaps_sorted[len(gaps_sorted)//2],
            'p75': gaps_sorted[3*len(gaps_sorted)//4],
            'p90': gaps_sorted[9*len(gaps_sorted)//10],
            'p95': gaps_sorted[19*len(gaps_sorted)//20],
        }
        
        print(f"   ✓ Silence Duration Analysis:")
        print(f"      Sample: {pattern['sample_size']:,} gaps")
        print(f"      Median: {pattern['median_gap_seconds']:.0f}s ({pattern['median_gap_seconds']/60:.1f}min)")
        print(f"      P75: {pattern['p75']:.0f}s ({pattern['p75']/60:.1f}min)")
        print(f"      P90: {pattern['p90']:.0f}s ({pattern['p90']/60:.1f}min)")
        print(f"      P95: {pattern['p95']:.0f}s ({pattern['p95']/60:.1f}min)")
        
        self.patterns['silence_duration'] = pattern
        return pattern
    
    def mine_early_vs_late_behavior(self):
        """
        PATTERN 4: Early vs Late Wallet Behavior
        
        Compare behavior of early entrants vs late entrants
        Find: What distinguishes early whales from late FOMO?
        """
        print("\n🔍 Mining Pattern 4: Early vs Late Behavior...")
        
        # This requires token-level analysis
        # Skip for now (requires more complex queries)
        print("   ⚠️  Requires token-level analysis, skipping in this version")
        
        return None
    
    def generate_report(self, output_file):
        """Generate JSON report with all patterns."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'database': self.db_path,
            'patterns': self.patterns,
            'recommendations': self._generate_recommendations()
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n✓ Report saved to: {output_file}")
        return report
    
    def _generate_recommendations(self):
        """Generate threshold recommendations from patterns."""
        recs = {}
        
        # Activity drop threshold
        if 'activity_drop' in self.patterns:
            p = self.patterns['activity_drop']
            # Use P75 (75% of wallets drop more than this)
            recs['activity_drop_threshold'] = {
                'value': p.get('p75', 0.80),
                'rationale': 'P75 threshold - 75% of wallets drop more than this when going silent',
                'conservative': p.get('p50', 0.60),
                'aggressive': p.get('p90', 0.90)
            }
        
        # Silence duration threshold
        if 'silence_duration' in self.patterns:
            p = self.patterns['silence_duration']
            # Use P75
            recs['silence_duration_threshold'] = {
                'value_seconds': int(p.get('p75', 180)),
                'value_minutes': int(p.get('p75', 180)) / 60,
                'rationale': 'P75 threshold - 75% of gaps are shorter than this',
                'conservative_seconds': int(p.get('p50', 120)),
                'aggressive_seconds': int(p.get('p90', 300))
            }
        
        # Exit pattern
        if 'exit_behavior' in self.patterns:
            p = self.patterns['exit_behavior']
            recs['exit_pattern_importance'] = {
                'sell_exit_rate': p.get('sell_exit_pct', 0.5),
                'rationale': f"{p.get('sell_exit_pct', 0.5)*100:.1f}% of wallets exit after selling",
                'use_in_detection': p.get('sell_exit_pct', 0) > 0.6
            }
        
        return recs
    
    def run_all(self, output_file):
        """Run all pattern mining analyses."""
        if not self.connect():
            return False
        
        print("=" * 80)
        print("PANDA v4 PATTERN MINING")
        print("=" * 80)
        
        self.get_table_info()
        
        self.mine_activity_patterns()
        self.mine_exit_patterns()
        self.mine_silence_durations()
        self.mine_early_vs_late_behavior()
        
        report = self.generate_report(output_file)
        
        print("\n" + "=" * 80)
        print("MINING COMPLETE")
        print("=" * 80)
        
        return report


def main():
    parser = argparse.ArgumentParser(
        description='Mine behavioral patterns from PANDA v4 database'
    )
    parser.add_argument(
        '--db',
        default='masterwalletsdb.db',
        help='Path to PANDA v4 database'
    )
    parser.add_argument(
        '--output',
        default='patterns_report.json',
        help='Output JSON file for pattern report'
    )
    
    args = parser.parse_args()
    
    miner = PatternMiner(args.db)
    miner.run_all(args.output)


if __name__ == '__main__':
    main()
