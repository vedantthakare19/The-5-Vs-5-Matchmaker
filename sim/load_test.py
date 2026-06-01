#!/usr/bin/env python3
"""
Load test / simulation for the 5v5 matchmaker.

Injects thousands of concurrent `POST /queue` requests with a realistic,
bimodal skill distribution, polls `/metrics` live, and prints a latency
summary (p50/p95/p99) plus final engine metrics.

The 60/20/20 skill split (mid / high / low) deliberately stresses constraint
relaxation: the high- and low-skill tails are sparse, so those players must
wait long enough for the skill window to widen before they can be matched.

Usage:
    python load_test.py                      # defaults: 5000 players, 30s
    python load_test.py --players 20000 --duration 45
    python load_test.py --base http://localhost:3000 --rate 0   # burst mode

Requires: aiohttp   (pip install -r requirements.txt)
"""
import argparse
import asyncio
import random
import statistics
import time

import aiohttp

REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]


def random_skill() -> float:
    """Bimodal-ish distribution: 60% mid, 20% high, 20% low."""
    roll = random.random()
    if roll < 0.60:
        return max(0.0, min(100.0, random.gauss(50, 12)))
    if roll < 0.80:
        return max(0.0, min(100.0, random.gauss(85, 5)))
    return max(0.0, min(100.0, random.gauss(15, 5)))


async def queue_player(session, base, latencies, errors, sem):
    payload = {"skill": random_skill(), "region": random.choice(REGIONS)}
    async with sem:
        t0 = time.monotonic()
        try:
            async with session.post(f"{base}/queue", json=payload) as r:
                await r.read()
                latencies.append((time.monotonic() - t0) * 1000.0)
                if r.status != 202:
                    errors.append(f"HTTP {r.status}")
        except Exception as e:  # noqa: BLE001 - we want to count every failure
            errors.append(repr(e))


async def poll_metrics(base, stop_event, history):
    """Print /metrics every 2s and compute matches/sec between samples."""
    async with aiohttp.ClientSession() as s:
        prev_matches, prev_t = 0, time.monotonic()
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            try:
                async with s.get(f"{base}/metrics") as r:
                    m = await r.json()
            except Exception as e:  # noqa: BLE001
                print(f"  [metrics poll error: {e}]")
                continue
            now = time.monotonic()
            matches = m.get("matches_formed", 0)
            mps = (matches - prev_matches) / max(now - prev_t, 1e-9)
            prev_matches, prev_t = matches, now
            history.append(m)
            print(
                f"  [t+{now - START:5.1f}s] "
                f"queue_depth={m.get('queue_depth'):>6}  "
                f"pool={m.get('pool_size'):>6}  "
                f"matches={matches:>6}  "
                f"{mps:6.1f} match/s  "
                f"avg_wait={m.get('avg_wait_ms'):>5}ms  "
                f"max_wait={m.get('max_wait_ms'):>6}ms  "
                f"avg_q={m.get('avg_quality'):.3f}  "
                f"races={m.get('eviction_races')}  "
                f"xregion={m.get('cross_region_matches')}"
            )


async def feeder(session, base, latencies, errors, sem, total, rate):
    """Submit `total` players. If rate>0, pace at ~rate players/sec; else burst."""
    tasks = []
    interval = 1.0 / rate if rate > 0 else 0.0
    for i in range(total):
        tasks.append(
            asyncio.create_task(
                queue_player(session, base, latencies, errors, sem)
            )
        )
        if interval:
            await asyncio.sleep(interval)
        elif i % 1000 == 0:
            # Let the event loop breathe during a pure burst.
            await asyncio.sleep(0)
    await asyncio.gather(*tasks)


async def main(args):
    global START
    latencies, errors, history = [], [], []
    stop = asyncio.Event()
    sem = asyncio.Semaphore(args.concurrency)

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    timeout = aiohttp.ClientTimeout(total=30)
    START = time.monotonic()
    print(
        f"Injecting {args.players} players into {args.base} "
        f"(concurrency={args.concurrency}, rate={'burst' if args.rate == 0 else args.rate}/s)\n"
    )

    metrics_task = asyncio.create_task(poll_metrics(args.base, stop, history))

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        feed = asyncio.create_task(
            feeder(session, args.base, latencies, errors, sem, args.players, args.rate)
        )
        # Let the engine keep draining for the remainder of the duration.
        await feed
        sent_at = time.monotonic() - START
        remaining = max(0.0, args.duration - sent_at)
        print(f"\n  ...all {args.players} requests sent in {sent_at:.1f}s; "
              f"draining for {remaining:.1f}s more...\n")
        await asyncio.sleep(remaining)

    stop.set()
    await metrics_task

    # --- Latency summary ---
    latencies.sort()
    n = len(latencies)
    print("\n--- Load-test results ---")
    print(f"Requests sent : {args.players}")
    print(f"Succeeded     : {n}")
    print(f"Errors        : {len(errors)}")
    if errors[:5]:
        print(f"  sample errors: {errors[:5]}")
    if n:
        def pct(p):
            return latencies[min(n - 1, int(n * p))]
        print(f"Latency p50   : {pct(0.50):7.2f} ms")
        print(f"Latency p95   : {pct(0.95):7.2f} ms")
        print(f"Latency p99   : {pct(0.99):7.2f} ms")
        print(f"Latency max   : {latencies[-1]:7.2f} ms")
        print(f"Latency mean  : {statistics.mean(latencies):7.2f} ms")

    if history:
        final = history[-1]
        print("\n--- Final engine metrics ---")
        for k in (
            "matches_formed", "players_queued", "players_matched", "queue_depth",
            "pool_size", "avg_wait_ms", "max_wait_ms", "avg_quality",
            "eviction_races", "cross_region_matches",
        ):
            print(f"  {k:22}: {final.get(k)}")


def parse_args():
    ap = argparse.ArgumentParser(description="5v5 matchmaker load test")
    ap.add_argument("--base", default="http://localhost:3000", help="server base URL")
    ap.add_argument("--players", type=int, default=5000, help="total players to inject")
    ap.add_argument("--duration", type=float, default=30.0, help="total run seconds")
    ap.add_argument("--concurrency", type=int, default=500, help="max in-flight requests")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="players/sec (0 = burst as fast as possible)")
    return ap.parse_args()


if __name__ == "__main__":
    START = time.monotonic()
    asyncio.run(main(parse_args()))
