//! Matching worker: the core latency-vs-quality loop.
//!
//! N of these run concurrently as Tokio tasks. Each iteration:
//!
//! 1. **Anchor** on the longest-waiting player ([`PlayerPool::oldest`]). Anchoring
//!    on the oldest player — not a random one — makes starvation prevention an
//!    explicit, deterministic guarantee.
//! 2. **Relax** the skill window as a function of the anchor's wait time
//!    (tight early, wide after `relax_secs`). This is the knob that trades match
//!    quality for latency.
//! 3. **Gather** candidates in the skill window, preferring the anchor's region;
//!    once the anchor has waited past `cross_region_after_secs`, allow
//!    cross-region fills so extreme-skill / thin-region players never hang.
//! 4. **Atomically evict** the 10 chosen players. Losers of an eviction race
//!    come back `None`; the worker re-queues whatever it did grab (preserving
//!    wait clocks) and retries — no deadlock, no double-booking.
//! 5. **Balance & publish** the resulting match.

use crate::balancer;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::model::{Match, Player, MATCH_SIZE};
use crate::pool::PlayerPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

/// Run one matching worker forever.
pub async fn run_worker(
    pool: Arc<PlayerPool>,
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    match_tx: broadcast::Sender<Match>,
) {
    let idle = Duration::from_millis(cfg.idle_sleep_ms);
    loop {
        match try_form_match(&pool, &cfg, &metrics) {
            Some(players) => {
                let m = balancer::build_match(players, Instant::now());
                metrics.record_match(&m);
                // broadcast: lagging/absent receivers just drop — match formation
                // is the source of truth, the channel is an optional notifier.
                let _ = match_tx.send(m);
                // Yield so we don't monopolise the runtime under tight loops.
                tokio::task::yield_now().await;
            }
            None => {
                tokio::time::sleep(idle).await;
            }
        }
    }
}

/// One non-blocking attempt to assemble and atomically claim 10 players.
/// Returns the 10 evicted players on success, or `None` if the pool is too thin
/// or this worker lost an eviction race (caller should back off / retry).
fn try_form_match(
    pool: &Arc<PlayerPool>,
    cfg: &Config,
    metrics: &Metrics,
) -> Option<Vec<Player>> {
    let now = Instant::now();
    let anchor = pool.oldest()?;
    let wait_secs = anchor.wait_ms(now) as f32 / 1000.0;

    let chosen = select_candidates(pool, cfg, &anchor, wait_secs);
    if chosen.len() < MATCH_SIZE {
        return None;
    }

    // --- Atomic eviction phase ---
    let mut evicted: Vec<Player> = Vec::with_capacity(MATCH_SIZE);
    for id in &chosen {
        if let Some(p) = pool.remove(id) {
            evicted.push(p);
        }
    }

    if evicted.len() < MATCH_SIZE {
        // Lost a race for at least one player. Re-queue what we grabbed,
        // preserving wait clocks, and let the caller retry.
        metrics.record_eviction_race();
        for p in evicted {
            pool.reinsert(p);
        }
        return None;
    }

    Some(evicted)
}

/// Pick the 10 players nearest the anchor in skill, region-aware.
///
/// Strategy:
/// * Pull all IDs in the relaxed skill window (capped) from the index.
/// * Partition into same-region and cross-region, each sorted by skill distance
///   to the anchor (closest first → better balance).
/// * Take same-region first; only top up from cross-region once the anchor has
///   waited long enough (or if same-region simply can't fill a match).
fn select_candidates(
    pool: &Arc<PlayerPool>,
    cfg: &Config,
    anchor: &Player,
    wait_secs: f32,
) -> Vec<Uuid> {
    // Hard anti-starvation deadline: once the anchor has waited long enough, the
    // window becomes effectively unbounded and region is ignored — the oldest
    // player matches with whoever is nearest in skill. Guarantees liveness for
    // extreme-skill outliers stuck in a near-empty bucket.
    let starving = wait_secs >= cfg.hard_deadline_secs;

    let window = if starving { f32::INFINITY } else { cfg.window_for(wait_secs) };
    let (lo, hi) = if starving {
        (f32::NEG_INFINITY, f32::INFINITY)
    } else {
        (anchor.skill - window / 2.0, anchor.skill + window / 2.0)
    };

    let ids = pool.query_range(lo, hi, cfg.candidate_cap);

    let mut same: Vec<Player> = Vec::new();
    let mut cross: Vec<Player> = Vec::new();
    for id in ids {
        // The anchor itself is in range; it'll land in `same` and be selected.
        if let Some(p) = pool.get(&id) {
            if p.region == anchor.region {
                same.push(p);
            } else {
                cross.push(p);
            }
        }
    }

    let by_distance = |a: &Player, b: &Player| {
        let da = (a.skill - anchor.skill).abs();
        let db = (b.skill - anchor.skill).abs();
        da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
    };
    same.sort_by(by_distance);
    cross.sort_by(by_distance);

    let mut chosen: Vec<Uuid> = same.iter().take(MATCH_SIZE).map(|p| p.id).collect();

    // Region fallback: allowed once the anchor has waited past the threshold,
    // past the hard deadline, OR unconditionally if same-region can't possibly
    // fill a match (otherwise a player in a near-empty region would starve).
    let allow_cross =
        starving || wait_secs >= cfg.cross_region_after_secs || same.len() < MATCH_SIZE;
    if chosen.len() < MATCH_SIZE && allow_cross {
        for p in &cross {
            if chosen.len() >= MATCH_SIZE {
                break;
            }
            chosen.push(p.id);
        }
    }

    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(pool: &Arc<PlayerPool>, skill: f32, region: &str) {
        pool.insert(Player {
            id: Uuid::new_v4(),
            skill,
            region: region.to_string(),
            queued_at: None,
        });
    }

    #[test]
    fn forms_a_match_when_ten_compatible_players_exist() {
        let pool = PlayerPool::new();
        let cfg = Config::default();
        let metrics = Metrics::default();
        for _ in 0..10 {
            mk(&pool, 50.0, "us-east");
        }
        let got = try_form_match(&pool, &cfg, &metrics).expect("should form");
        assert_eq!(got.len(), 10);
        assert_eq!(pool.len(), 0, "all 10 must be evicted from the pool");
    }

    #[test]
    fn no_match_when_pool_too_thin() {
        let pool = PlayerPool::new();
        let cfg = Config::default();
        let metrics = Metrics::default();
        for _ in 0..9 {
            mk(&pool, 50.0, "us-east");
        }
        assert!(try_form_match(&pool, &cfg, &metrics).is_none());
        assert_eq!(pool.len(), 9, "nothing should be evicted on failure");
    }

    #[test]
    fn tight_window_excludes_far_skill_players() {
        let pool = PlayerPool::new();
        let cfg = Config::default(); // initial_window 5.0 => ±2.5 at t=0
        let metrics = Metrics::default();
        // Anchor + 9 others all far away in skill: no match while window is tight.
        mk(&pool, 50.0, "us-east");
        for _ in 0..9 {
            mk(&pool, 90.0, "us-east");
        }
        assert!(
            try_form_match(&pool, &cfg, &metrics).is_none(),
            "skill-90 players are outside the anchor's tight window"
        );
    }

    /// Insert a player with a back-dated wait clock (preserved by `reinsert`).
    fn mk_aged(pool: &Arc<PlayerPool>, skill: f32, region: &str, age_secs: u64) {
        pool.reinsert(Player {
            id: Uuid::new_v4(),
            skill,
            region: region.to_string(),
            queued_at: Some(Instant::now() - Duration::from_secs(age_secs)),
        });
    }

    #[test]
    fn hard_deadline_matches_extreme_outlier_with_anyone() {
        let pool = PlayerPool::new();
        let cfg = Config::default(); // hard_deadline_secs = 45.0
        let metrics = Metrics::default();

        // A lone high-skill anchor that has already waited past the deadline,
        // surrounded by 9 far-away low-skill players in another region. Normal
        // relaxation (max window 40 => +-20) would never reach them, but the
        // hard deadline forces a match.
        mk_aged(&pool, 99.0, "lonely", 60);
        for _ in 0..9 {
            mk_aged(&pool, 5.0, "us-east", 1);
        }
        let got = try_form_match(&pool, &cfg, &metrics)
            .expect("hard deadline must force a match for the starving anchor");
        assert_eq!(got.len(), 10);
    }

    #[test]
    fn cross_region_fallback_fills_when_same_region_is_thin() {
        let pool = PlayerPool::new();
        let cfg = Config::default();
        let metrics = Metrics::default();
        // Only 1 in the anchor's region; 9 elsewhere at the same skill.
        // same.len() < 10 => cross fallback allowed immediately.
        mk(&pool, 50.0, "solo-region");
        for _ in 0..9 {
            mk(&pool, 50.0, "us-east");
        }
        let got = try_form_match(&pool, &cfg, &metrics).expect("cross-region fill");
        assert_eq!(got.len(), 10);
    }
}
