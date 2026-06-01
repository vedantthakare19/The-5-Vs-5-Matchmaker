# 5v5 Real-Time Competitive Matchmaker

A high-performance, thread-safe matchmaking engine in Rust. It holds waiting
players in memory, groups them into **balanced teams of 5-vs-5**, and optimises
for the central tension of matchmaking: **latency vs. match quality**.

```
HTTP API (/queue, /metrics, /health)
        │  enqueue
        ▼
┌──────────────────────────────────────────────┐
│                 PlayerPool                     │
│  DashMap<Uuid, Player>   ← sharded primary store
│  BTreeMap (skill)        ← O(log n + k) range queries
│  BTreeMap (wait/Instant) ← oldest() in O(log n): starvation guard
└──────────────────────────────────────────────┘
        │  oldest() → anchor
        ▼
┌──────────────────────────────────────────────┐
│            Matching Worker × N                 │
│  1. anchor = longest-waiting player            │
│  2. skill window relaxes with wait time        │
│  3. prefer same-region → cross-region fallback │
│  4. atomic eviction (remove ×10; losers retry) │
│  5. balance: C(10,5)=252 combos → optimal split│
└──────────────────────────────────────────────┘
        │  broadcast::channel<Match>
        ▼
   match-log consumer  +  lock-free atomic metrics
```

---

## Quick start

### Prerequisites
- **Rust** (stable, 2021 edition) — install from <https://rustup.rs>.
- **Python 3.8+** with `aiohttp` for the load test.

> On a Windows host without the MSVC C++ build tools, use the GNU toolchain
> (`rustup default stable-x86_64-pc-windows-gnu`) plus a mingw-w64 binutils/gcc
> set on `PATH`. On Linux/macOS or MSVC Windows, a plain `cargo build` works
> out of the box — the code itself is fully platform-agnostic.

### Run the service
```bash
cd matchmaker
cargo run --release          # listens on 0.0.0.0:3000
```

### Run the load test (in a second terminal)
```bash
pip install -r sim/requirements.txt
python sim/load_test.py --players 5000 --duration 30
```

### Talk to it directly
```bash
curl -X POST localhost:3000/queue -H 'content-type: application/json' \
     -d '{"skill": 62.5, "region": "us-east"}'
curl localhost:3000/metrics
curl localhost:3000/health
```

### Configuration (environment variables)
All optional; the engine runs with sensible defaults.

| Env var | Default | Meaning |
|---|---|---|
| `MM_BIND` | `0.0.0.0:3000` | Listen address |
| `MM_WORKERS` | #CPU cores | Concurrent matching workers |
| `MM_INITIAL_WINDOW` | `5.0` | Skill window width at t=0 (±2.5) |
| `MM_MAX_WINDOW` | `40.0` | Skill window width after `relax_secs` (±20) |
| `MM_RELAX_SECS` | `30.0` | Seconds to relax from initial → max |
| `MM_CROSS_REGION_AFTER_SECS` | `8.0` | Wait before cross-region fills are allowed |
| `MM_HARD_DEADLINE_SECS` | `45.0` | Wait after which the window is unbounded (anti-starvation) |
| `MM_CANDIDATE_CAP` | `256` | Max candidates scanned per attempt |
| `RUST_LOG` | `matchmaker=info` | Log verbosity (`debug` logs every match) |

---

## Engineering challenges

### 1. The core algorithm — latency vs. quality

The fundamental conflict: match a player **fast** (low latency) or match them
**well** (tight skill spread). A pure-FIFO matcher minimises latency but ignores
skill; a pure skill-first matcher maximises quality but lets outliers wait
forever.

We resolve it with a **sliding skill window that widens with wait time**. Each
match is anchored on the **longest-waiting** player; the window around that
anchor's skill starts tight and relaxes linearly:

```
frac    = min(wait_seconds / relax_secs, 1.0)
window  = initial_window + (max_window - initial_window) * frac
range   = [anchor.skill - window/2, anchor.skill + window/2]
```

With defaults: a fresh anchor accepts only **±2.5** skill points (high quality);
after 30 s of waiting it accepts **±20** (guaranteed progress). This beats both
extremes:

- **vs. pure FIFO:** FIFO pairs whoever arrives next regardless of skill, which
  produces lopsided games. Under a bimodal skill distribution it also starves
  mid-range players, who are constantly "skipped" by clustered extremes.
- **vs. pure skill-first:** a strict skill match never widens, so a lone
  high-skill player with no nearby peers waits unboundedly.

Anchoring on the oldest player (rather than a random one) is what makes the
relaxation *meaningful*: the player most at risk of a bad experience is always
the one whose constraints we relax first.

### 2. Thread-safe state & atomic eviction

`N` matching workers (default = CPU count) scan the pool concurrently. The
shared state is a [`PlayerPool`](src/pool.rs):

- **Primary store:** `DashMap<Uuid, Player>`. DashMap sharded the keyspace
  internally, so concurrent inserts/removes on different players never contend
  on one global lock.
- **Secondary indexes:** two `BTreeMap`s (skill-sorted, wait-sorted) each behind
  a short-lived `parking_lot::Mutex`.

**Atomic eviction via the `remove() → None` race protocol.** A worker that has
chosen 10 candidates calls `pool.remove(id)` on each. `DashMap::remove` is the
single atomic decision point: if two workers race for the same player, **exactly
one sees `Some` (wins ownership) and the other sees `None`**. A worker that ends
up with fewer than 10 players lost a race — it **re-queues** the players it did
grab and retries. No locks are held across the retry, so there is **no deadlock
and no double-booking**.

Two correctness invariants prevent stale state:

1. **Ordering invariant.** Insert writes the indexes first and the primary store
   *last*; remove deletes from the primary store *first*. The only possible
   transient inconsistency is "in an index but not yet in `players`" (a lookup
   returns `None` — harmless), never the reverse (a stale index entry pointing
   at an already-matched player).
2. **Fair re-queue.** Re-inserting a race-loser uses `reinsert()`, which
   **preserves the original `queued_at`**. A naive `insert()` would reset the
   wait clock, so a player who repeatedly loses races could be starved. (See the
   `reinsert_preserves_wait_clock` test.)

The two index mutexes are **never held simultaneously**, and a DashMap shard
guard is never held while locking an index — eliminating lock-ordering deadlocks.

### 3. Time-based constraint relaxation

The relaxation formula above is driven by each player's **`queued_at`**, which
is stamped **at pool insertion, not at HTTP arrival**. This deliberately keeps
network/queueing latency out of the measured wait time, so relaxation reflects
real time-in-pool. All elapsed-time math uses `std::time::Instant` (monotonic),
never `SystemTime` (which can jump backwards on clock adjustments).

Relaxation alone bounds *quality degradation* but not *worst-case wait*: if a
skill bucket has fewer than 10 players within even the maximum window, the
anchor still can't fill a match. We add a **hard anti-starvation deadline**
(`MM_HARD_DEADLINE_SECS`): once the anchor has waited that long, the window goes
**unbounded and region is ignored** — the oldest player is matched with the 9
nearest-skill players available, whoever they are. This guarantees that **as long
as ≥10 players are in the pool, no player waits forever**, which is exactly the
"high-skill / low-skill players might wait indefinitely" failure mode the brief
calls out. (See the `hard_deadline_matches_extreme_outlier_with_anyone` test and
the load-test results below, where it drains every last straggler.)

### 4. Team balance optimisation

Finding 10 compatible players is only half the job — they must be split fairly.
For **exactly 10 players**, the number of ways to choose Team A (Team B is the
complement) is a **compile-time constant**:

```
C(10,5) = 252
```

So [`balance_teams`](src/balancer.rs) **exhaustively evaluates all 252 splits**
and picks the one minimising `|sum(A) − sum(B)|` — a **globally optimal**
partition in `O(252) = O(1)` time. No approximation, no convergence limit, no
edge cases. Ties on skill gap are broken by the smaller combined intra-team
spread, so equally-balanced splits still yield the most internally-even teams.

**Why not snake-draft + 2-opt?** For *variable* group sizes, snake draft
(`O(n log n)`) followed by 2-opt local search is the correct general-case
algorithm and scales to any `n`. But here `n` is fixed at 10, so brute force is
strictly better: it's provably optimal *and* avoids the sort, the swap-evaluation
passes, and the convergence checks. Use the right tool for the fixed constraint.

**Quality score** (`0.0–1.0`, attached to every `Match`) penalises both the
inter-team skill gap *and* the intra-team spread:

```
quality = 1 − min(|sum(A)−sum(B)| / 5 / 100, 1) − min((stddev(A)+stddev(B)) / 200, 0.5)
```

This correctly rates `(50,50,50,50,50)` vs `(50,50,50,50,50)` above
`(90,90,10,10,50)` vs `(90,90,10,10,50)` — both have a zero inter-team gap, but
the latter has lopsided internal spread.

### 5. Low-latency health metrics

Monitoring must never slow the matching hot path. [`Metrics`](src/metrics.rs) is
**entirely lock-free**: every counter is a `std::sync::atomic` type updated with
`Ordering::Relaxed`. There are **no mutexes and no memory fences** on the match
path, so a `/metrics` scrape can never block match formation, and the counters
never cause cache-line contention from fence instructions.

`Relaxed` is correct here because these are independent monitoring aggregates —
we don't need happens-before ordering *between* counters; a dashboard seeing
`matches_formed` tick a few nanoseconds before `total_wait_ms` is fine. There is
no stable atomic float, so quality is accumulated as an integer (`quality × 1000`
in an `AtomicU64`) and divided back out at read time.

Exposed at `/metrics`: `matches_formed`, `players_queued`, `players_matched`,
`queue_depth`, `pool_size`, `avg_wait_ms`, `max_wait_ms`, `avg_quality`,
`eviction_races` (race-protocol activity), and `cross_region_matches`.

---

## Complexity analysis

Let `n` = pool size, `k` = candidates returned by a range query (`k ≤ cap`).

| Operation | Time | Space |
|---|---|---|
| Player insert / remove | `O(log n)` | `O(1)` |
| `oldest()` (anchor) | `O(log n)` | `O(1)` |
| Skill range query | `O(log n + k)` | `O(k)` |
| Candidate selection (sort) | `O(k log k)` | `O(k)` |
| Atomic eviction (10 removes) | `O(log n)` | `O(1)` |
| Team balance (fixed n=10) | `O(252) = O(1)` | `O(1)` |
| Team balance (general n) | `O(n log n)` | `O(n)` |
| Metrics read / write | `O(1)` | `O(1)` |

A single match-formation attempt is therefore `O(log n + k log k)` — dominated by
the candidate scan, with `k` bounded by `MM_CANDIDATE_CAP`. Memory is `O(n)` for
the pool plus `O(n)` across the two indexes.

---

## Measured performance

Hardware: 12 logical cores (12 workers), Windows host, server + load generator
running on the same machine (so HTTP latency includes contention from the Python
client). Workload: 5 000 players burst-injected, 60% mid-skill (μ=50, σ=12),
20% high (μ=85, σ=5), 20% low (μ=15, σ=5), across 4 regions.

| Metric | Value |
|---|---|
| Requests sent / errors | 5 000 / **0** |
| Enqueue latency p50 / p95 / p99 | **78 ms / 532 ms / 640 ms** |
| Matches formed | 500 (all 5 000 players matched) |
| Final `queue_depth` / `pool_size` | **0 / 0** (with 12 s hard deadline) |
| `avg_quality` | **0.993** |
| `avg_wait_ms` / `max_wait_ms` | 3 408 ms / 12 012 ms |
| Eviction races resolved | 91 |
| Cross-region matches | 55 |

Observations:
- **Quality stays ~0.99** even under a stress distribution, because most matches
  form inside the tight initial window.
- The **hard deadline drains the pool to zero**: `max_wait_ms ≈ 12 s` is exactly
  the configured deadline firing on the sparse extreme-skill stragglers — without
  it, ~30 outliers remain queued indefinitely once the firehose stops.
- **91 eviction races** were resolved with zero lost or double-booked players,
  exercising the `remove() → None` protocol under real contention.

---

## Project layout

```
matchmaker/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs       # entrypoint: wiring, workers, reporter, graceful shutdown
│   ├── model.rs      # Player, Match, QueueRequest/Response
│   ├── config.rs     # env-driven tunables + relaxation formula
│   ├── pool.rs       # DashMap + skill/wait BTreeMap indexes
│   ├── matcher.rs    # the core latency-vs-quality worker loop
│   ├── balancer.rs   # C(10,5) optimal team split + quality score
│   ├── metrics.rs    # lock-free atomic counters
│   └── api.rs        # axum routes: /queue, /metrics, /health
└── sim/
    ├── load_test.py  # asyncio + aiohttp concurrent load generator
    └── requirements.txt
```

## Tests

```bash
cargo test                          # 18 unit tests
cargo clippy --all-targets -- -D warnings   # lints clean
```

Coverage includes: pool insert/remove/double-remove, oldest-ordering,
range-query bounds, fair re-queue (wait-clock preservation), balancer optimality
(vs. an independent brute-force oracle) and quality bounds, the eviction path,
window tightness, cross-region fallback, and the hard-deadline starvation guard.

---

## Scaling challenges

The current engine is a **single-node, in-memory** service — ideal for a region
shard handling tens of thousands of concurrent players. To scale further:

**Horizontal scaling (shared state).** Replace the in-process indexes with a
shared store so instances become stateless:
- Use a **Redis sorted set** (`ZADD`/`ZRANGEBYSCORE`) as the `skill_index` — the
  same `O(log n + k)` range-query shape.
- Reproduce atomic eviction with a **Redis Lua script** that checks-and-removes a
  batch of 10 in one round trip (the server-side analogue of the
  `remove() → None` protocol — Lua scripts execute atomically).
- Each app instance then just runs workers against shared Redis; metrics
  aggregate via Redis counters or a stats sidecar.

**Sharding (partitioned state).** Partition the skill range (or region) across
instances via **consistent hashing**, so each node owns a slice of the keyspace
and cross-shard matches are handled by a thin coordinator. This avoids a single
hot Redis but complicates cross-region fallback.

**Other production hardening:**
- Bound the broadcast channel's slow-consumer drops with a durable `mpsc`
  per-subscriber path if guaranteed match delivery is needed.
- Add a `/matches` SSE/WebSocket stream so game servers learn of new matches
  without polling.
- Per-region worker pools (NUMA / locality) and back-pressure on `/queue` when
  the pool is saturated.
- Persist the queue to survive restarts (currently in-memory by design).

## Design notes & trade-offs

- **Tokio, not Rayon, for workers.** Workers are mostly sleeping or doing short
  bursts (a 252-combo balance is sub-microsecond), so async tasks fit better than
  Rayon's OS-thread parallelism. The balancer stays synchronous — `spawn_blocking`
  would cost more than the work it offloads.
- **Broadcast channel is best-effort.** A lagging match-log consumer drops
  messages rather than applying back-pressure to matching; match formation is the
  source of truth, the channel is an optional notifier.
- **`queue_depth` (counter) vs. `pool_size` (authoritative).** The cheap atomic
  counter can transiently drift under concurrency; `/metrics` also reports the
  exact `pool.len()` so dashboards have a ground-truth gauge.
```
