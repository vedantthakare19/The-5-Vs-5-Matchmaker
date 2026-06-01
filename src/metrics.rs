//! Lock-free health metrics.
//!
//! Every counter is a `std::sync::atomic` type updated with `Relaxed` ordering.
//! There are **no mutexes and no memory fences** on the matching hot path, so a
//! `/metrics` scrape can never block or stall match formation.
//!
//! # Why `Relaxed` is correct here
//!
//! These counters are independent monitoring aggregates. We do not need
//! happens-before relationships *between* them — a dashboard observing
//! `matches_formed` tick a few nanoseconds before `total_wait_ms` is perfectly
//! acceptable. `Relaxed` avoids cache-line ping-pong from fence instructions.
//!
//! # Floats without float-atomics
//!
//! There is no stable `AtomicF32`. Quality (0.0–1.0) is accumulated as an
//! integer by storing `quality * 1000` in an `AtomicU64`, then dividing back out
//! at read time.

use crate::model::{Match, MATCH_SIZE};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub struct Metrics {
    /// Matches successfully formed.
    pub matches_formed: AtomicU64,
    /// Players accepted onto the queue (lifetime).
    pub players_queued: AtomicU64,
    /// Sum of per-match longest-wait, in ms. Divide by matches for the average.
    pub total_wait_ms: AtomicU64,
    /// Largest single-match wait observed, in ms.
    pub max_wait_ms: AtomicU64,
    /// Live queue size. Signed because concurrent enqueue/dequeue can transiently
    /// race; it self-corrects and is only a monitoring gauge.
    pub queue_depth: AtomicI64,
    /// Sum of `quality * 1000`. Divide by 1000 and by matches for avg quality.
    pub quality_sum: AtomicU64,
    /// Eviction races lost (a worker claimed <10 and had to re-queue + retry).
    pub eviction_races: AtomicU64,
    /// Matches that mixed regions (constraint-relaxation fallback fired).
    pub cross_region_matches: AtomicU64,
}

impl Metrics {
    pub fn record_enqueue(&self) {
        self.players_queued.fetch_add(1, Relaxed);
        self.queue_depth.fetch_add(1, Relaxed);
    }

    pub fn record_match(&self, m: &Match) {
        self.matches_formed.fetch_add(1, Relaxed);
        self.total_wait_ms.fetch_add(m.wait_ms, Relaxed);
        self.max_wait_ms.fetch_max(m.wait_ms, Relaxed);
        self.queue_depth.fetch_sub(MATCH_SIZE as i64, Relaxed);
        self.quality_sum
            .fetch_add((m.quality * 1000.0) as u64, Relaxed);
        if m.cross_region {
            self.cross_region_matches.fetch_add(1, Relaxed);
        }
    }

    pub fn record_eviction_race(&self) {
        self.eviction_races.fetch_add(1, Relaxed);
    }

    /// Point-in-time snapshot as JSON. O(1); never blocks the matching path.
    /// `live_pool` is read from the pool so the gauge is authoritative even if
    /// the cheap `queue_depth` counter has drifted.
    pub fn snapshot(&self, live_pool: usize) -> serde_json::Value {
        let matches = self.matches_formed.load(Relaxed);
        let denom = matches.max(1);
        serde_json::json!({
            "matches_formed":       matches,
            "players_queued":       self.players_queued.load(Relaxed),
            "queue_depth":          self.queue_depth.load(Relaxed),
            "pool_size":            live_pool,
            "players_matched":      matches * MATCH_SIZE as u64,
            "avg_wait_ms":          self.total_wait_ms.load(Relaxed) / denom,
            "max_wait_ms":          self.max_wait_ms.load(Relaxed),
            "avg_quality":          self.quality_sum.load(Relaxed) as f64 / denom as f64 / 1000.0,
            "eviction_races":       self.eviction_races.load(Relaxed),
            "cross_region_matches": self.cross_region_matches.load(Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Match, Player};
    use uuid::Uuid;

    fn dummy_match(quality: f32, wait_ms: u64, cross: bool) -> Match {
        let p = Player {
            id: Uuid::new_v4(),
            skill: 50.0,
            region: "r".into(),
            queued_at: None,
        };
        Match {
            id: Uuid::new_v4(),
            team_a: vec![p.clone(); 5],
            team_b: vec![p; 5],
            quality,
            wait_ms,
            skill_gap: 0.0,
            cross_region: cross,
        }
    }

    #[test]
    fn enqueue_and_match_update_counters() {
        let m = Metrics::default();
        for _ in 0..20 {
            m.record_enqueue();
        }
        m.record_match(&dummy_match(0.8, 1200, false));
        let snap = m.snapshot(10);
        assert_eq!(snap["players_queued"], 20);
        assert_eq!(snap["matches_formed"], 1);
        // 20 enqueued - 10 matched = 10 depth.
        assert_eq!(snap["queue_depth"], 10);
        assert_eq!(snap["avg_wait_ms"], 1200);
        assert_eq!(snap["max_wait_ms"], 1200);
        // 0.8 stored as 800 -> back to 0.8.
        assert!((snap["avg_quality"].as_f64().unwrap() - 0.8).abs() < 1e-6);
    }

    #[test]
    fn snapshot_on_empty_does_not_divide_by_zero() {
        let m = Metrics::default();
        let snap = m.snapshot(0);
        assert_eq!(snap["matches_formed"], 0);
        assert_eq!(snap["avg_wait_ms"], 0);
    }

    #[test]
    fn max_wait_tracks_the_peak() {
        let m = Metrics::default();
        m.record_match(&dummy_match(0.5, 500, false));
        m.record_match(&dummy_match(0.5, 3000, true));
        m.record_match(&dummy_match(0.5, 100, false));
        let snap = m.snapshot(0);
        assert_eq!(snap["max_wait_ms"], 3000);
        assert_eq!(snap["cross_region_matches"], 1);
    }
}
