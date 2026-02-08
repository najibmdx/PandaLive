import argparse
import csv
import json
import time
from pathlib import Path

import requests


class RateLimiter:
    def __init__(self, rps):
        self.min_interval = 1.0 / float(rps) if rps > 0 else 0.0
        self.last_time = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        now = time.time()
        elapsed = now - self.last_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_time = time.time()


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Mine silent gap metrics from on-chain data using Helius."
    )
    parser.add_argument("--in", dest="infile", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--helius-key", required=True)
    parser.add_argument("--buffers", type=int, default=900)
    parser.add_argument("--max-wallets", type=int, default=50)
    parser.add_argument("--rate-limit-rps", type=float, default=8)
    parser.add_argument(
        "--candidates",
        default="1,2,3,4,5,6,7,8,9,10",
        help="Comma-separated list of candidate G values in minutes.",
    )
    return parser.parse_args()


def load_session_events(infile):
    events = []
    with open(infile, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def get_event_time(event):
    for key in ("event_time", "eventTime", "time", "timestamp"):
        value = safe_int(event.get(key))
        if value is not None:
            return value
    details = event.get("details") or {}
    for key in ("event_time", "eventTime", "time", "timestamp"):
        value = safe_int(details.get(key))
        if value is not None:
            return value
    return None


def extract_wallet_signal(event):
    event_type = (event.get("type") or event.get("event_type") or "").upper()
    if "WALLET_SIGNAL" not in event_type:
        return None
    details = event.get("details") or {}
    timing = details.get("timing") or {}
    is_early = timing.get("is_early")
    if is_early is not True:
        return None
    wallet = (
        details.get("wallet")
        or details.get("wallet_address")
        or event.get("wallet")
        or event.get("wallet_address")
    )
    if not wallet:
        return None
    return str(wallet)


def extract_transition(event):
    event_type = (event.get("type") or event.get("event_type") or "").upper()
    details = event.get("details") or {}
    from_state = event.get("from_state") or details.get("from_state")
    to_state = event.get("to_state") or details.get("to_state")
    if not (from_state and to_state):
        from_state = event.get("state_from") or details.get("state_from")
        to_state = event.get("state_to") or details.get("state_to")
    if not (from_state and to_state):
        if "TRANSITION" not in event_type:
            return None
    if not (from_state and to_state):
        return None
    return str(from_state), str(to_state)


def session_mint_from_filename(infile):
    stem = Path(infile).stem
    if "session_" in stem:
        return stem.split("session_", 1)[1]
    return stem


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def cached_read(path):
    if Path(path).exists():
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return None


def cached_write(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def helius_request(session, limiter, url, payload, retries=5):
    attempt = 0
    while True:
        limiter.wait()
        try:
            response = session.post(url, json=payload, timeout=30)
        except requests.RequestException as exc:
            print(f"Helius request exception: {exc}")
            response = None
        if response is not None:
            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    print("Helius request failed: invalid JSON response")
            else:
                error_text = response.text[:500]
                print(
                    "Helius request failed: "
                    f"status={response.status_code} body={error_text}"
                )
                if response.status_code in (401, 403):
                    raise RuntimeError(
                        f"Helius authorization failed: {response.status_code} {error_text}"
                    )
                if response.status_code == 429:
                    attempt += 1
                    if attempt > retries:
                        raise RuntimeError("Helius request failed after retries")
                    sleep_time = min(2 ** attempt, 30) + (attempt * 0.1)
                    time.sleep(sleep_time)
                    continue
        attempt += 1
        if attempt > retries:
            raise RuntimeError("Helius request failed after retries")
        sleep_time = min(2 ** attempt, 30) + (attempt * 0.1)
        time.sleep(sleep_time)


def get_signatures_for_address(
    session,
    limiter,
    api_url,
    address,
    start_ts,
    end_ts,
    cache_dir,
):
    cache_path = Path(cache_dir) / f"signatures_{address}_{start_ts}_{end_ts}.json"
    cached = cached_read(cache_path)
    if cached is not None:
        return cached
    signatures = []
    before = None
    while True:
        params = {"limit": 1000}
        if before:
            params["before"] = before
        payload = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "getSignaturesForAddress",
            "params": [address, params],
        }
        result = helius_request(session, limiter, api_url, payload)
        items = result.get("result") or []
        if not items:
            break
        signatures.extend(items)
        before = items[-1].get("signature")
        last_time = items[-1].get("blockTime")
        if last_time is not None and safe_int(last_time) < start_ts:
            break
        if len(items) < 1000:
            break
    cached_write(cache_path, signatures)
    return signatures


def get_transaction_detail(
    session, limiter, api_url, signature, cache_dir
):
    cache_path = Path(cache_dir) / f"tx_{signature}.json"
    cached = cached_read(cache_path)
    if cached is not None:
        return cached
    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getTransaction",
        "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}],
    }
    result = helius_request(session, limiter, api_url, payload)
    cached_write(cache_path, result)
    return result


def build_activity_timeline(
    session,
    limiter,
    api_url,
    cache_dir,
    wallet,
    start_ts,
    end_ts,
    buffer_seconds,
):
    buffered_start = start_ts - buffer_seconds
    buffered_end = end_ts + buffer_seconds
    signatures = get_signatures_for_address(
        session, limiter, api_url, wallet, buffered_start, buffered_end, cache_dir
    )
    in_range = []
    for item in signatures:
        block_time = safe_int(item.get("blockTime"))
        if block_time is not None and start_ts <= block_time <= end_ts:
            in_range.append(block_time)
    return sorted(in_range)


def compute_silent_curves(
    start_ts,
    end_ts,
    early_wallets,
    activities,
    candidates,
    exhaustion_ts,
):
    sample_times = list(range(start_ts, end_ts + 1, 30))
    if sample_times and sample_times[-1] != end_ts:
        sample_times.append(end_ts)
    results = {}
    for g_min in candidates:
        g_seconds = g_min * 60
        silent_curve = []
        for t in sample_times:
            silent_count = 0
            for wallet in early_wallets:
                times = activities.get(wallet, [])
                last_ts = None
                for ts in times:
                    if ts <= t:
                        last_ts = ts
                    else:
                        break
                if last_ts is None or (t - last_ts) >= g_seconds:
                    silent_count += 1
            silent_curve.append((t, silent_count))
        first_silent60_ts = None
        if early_wallets:
            for t, silent_count in silent_curve:
                if silent_count / float(len(early_wallets)) >= 0.6:
                    first_silent60_ts = t
                    break
        lead_time = None
        silent60_hit = 0
        if first_silent60_ts is not None:
            silent60_hit = 1
            if exhaustion_ts is not None:
                lead_time = exhaustion_ts - first_silent60_ts
        results[g_min] = {
            "curve": silent_curve,
            "first_silent60_ts": first_silent60_ts,
            "lead_time": lead_time,
            "silent60_hit": silent60_hit,
        }
    return results


def main():
    args = parse_args()
    outdir = Path(args.outdir)
    ensure_dir(outdir)
    cache_dir = outdir / "cache"
    ensure_dir(cache_dir)

    events = load_session_events(args.infile)
    times = [t for t in (get_event_time(e) for e in events) if t is not None]
    if not times:
        raise RuntimeError("No event times found in session log")
    episode_start = min(times)
    episode_end = max(times)

    early_wallets = {}
    transitions = []
    for event in events:
        event_time = get_event_time(event)
        wallet = extract_wallet_signal(event)
        if wallet and event_time is not None:
            if wallet not in early_wallets:
                early_wallets[wallet] = event_time
        transition = extract_transition(event)
        if transition and event_time is not None:
            from_state, to_state = transition
            transitions.append((from_state, to_state, event_time))

    sorted_wallets = sorted(early_wallets.items(), key=lambda item: item[1])
    if args.max_wallets and len(sorted_wallets) > args.max_wallets:
        sorted_wallets = sorted_wallets[: args.max_wallets]
    early_wallets = {wallet: ts for wallet, ts in sorted_wallets}

    exhaustion_ts = None
    for _from, to, ts in transitions:
        if to.upper() == "EXHAUSTION_DETECTED":
            exhaustion_ts = ts
            break
    if exhaustion_ts is None:
        for _from, to, ts in transitions:
            if to.upper() == "DISSIPATION":
                exhaustion_ts = ts
                break

    api_url = f"https://rpc.helius.xyz/?api-key={args.helius_key}"
    limiter = RateLimiter(args.rate_limit_rps)
    session = requests.Session()

    activities = {}
    for wallet in early_wallets:
        activities[wallet] = build_activity_timeline(
            session,
            limiter,
            api_url,
            cache_dir,
            wallet,
            episode_start,
            episode_end,
            args.buffers,
        )

    early_wallets_path = outdir / "early_wallets.csv"
    with open(early_wallets_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["wallet", "first_seen_ts_in_episode", "last_seen_ts_in_episode", "activity_count"]
        )
        for wallet, first_seen in early_wallets.items():
            times = activities.get(wallet, [])
            last_seen = times[-1] if times else ""
            writer.writerow([wallet, first_seen, last_seen, len(times)])

    gaps_path = outdir / "activity_gaps.csv"
    with open(gaps_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["wallet", "gap_seconds"])
        for wallet, times in activities.items():
            if len(times) < 2:
                continue
            for prev, current in zip(times, times[1:]):
                writer.writerow([wallet, current - prev])

    candidates = [
        int(item.strip())
        for item in args.candidates.split(",")
        if item.strip().isdigit()
    ]
    silent_results = compute_silent_curves(
        episode_start,
        episode_end,
        list(early_wallets.keys()),
        activities,
        candidates,
        exhaustion_ts,
    )

    for g_min, data in silent_results.items():
        curve_path = outdir / f"silent_curve_{g_min}min.csv"
        with open(curve_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["sample_ts", "silent_x", "early_y", "silent_pct"])
            for sample_ts, silent_count in data["curve"]:
                early_y = len(early_wallets)
                pct = (silent_count / float(early_y)) if early_y else 0.0
                writer.writerow([sample_ts, silent_count, early_y, f"{pct:.4f}"])

    summary_path = outdir / "silent_summary.tsv"
    with open(summary_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "G_min",
                "early_y",
                "exhaustion_ts",
                "first_silent60_ts",
                "lead_time_seconds",
                "silent60_hit",
                "post_exhaustion_activity_wallets",
            ]
        )
        for g_min in candidates:
            data = silent_results[g_min]
            post_exhaustion_wallets = ""
            if exhaustion_ts is not None:
                count = 0
                for times in activities.values():
                    if any(ts > exhaustion_ts for ts in times):
                        count += 1
                post_exhaustion_wallets = count
            writer.writerow(
                [
                    g_min,
                    len(early_wallets),
                    exhaustion_ts or "",
                    data["first_silent60_ts"] or "",
                    data["lead_time"] if data["lead_time"] is not None else "",
                    data["silent60_hit"],
                    post_exhaustion_wallets,
                ]
            )

    mint = session_mint_from_filename(args.infile)
    print("Session mint:", mint)
    print("Episode start:", episode_start)
    print("Episode end:", episode_end)
    print("Early wallets:", len(early_wallets))
    if exhaustion_ts is not None:
        print("Exhaustion moment:", exhaustion_ts)
    else:
        print("Exhaustion moment: NONE")
    print("G_min\tlead_time_seconds\tsilent60_hit")
    for g_min in candidates:
        data = silent_results[g_min]
        lead_time = data["lead_time"] if data["lead_time"] is not None else ""
        print(f"{g_min}\t{lead_time}\t{data['silent60_hit']}")


if __name__ == "__main__":
    main()
