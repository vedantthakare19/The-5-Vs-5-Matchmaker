//! Concurrent in-memory player pool.
//!
//! Shared mutable state for the whole engine. It is `Send + Sync` so any number
//! of Tokio matching workers can hammer it at once.
//!
//! # Design
//!
//! * `players` — a `DashMap<Uuid, Player>` is the **primary store**. DashMap
//!   shards internally, so concurrent inserts/removes on different keys never
//!   contend on a single lock.
//! * `skill_index` — a `BTreeMap` keyed by `(OrderedFloat<skill>, Uuid)` gives
//!   `O(log n + k)` skill-range queries to find players near an anchor.
//! * `wait_index` — a `BTreeMap` keyed by `(Instant, Uuid)` keeps players sorted
//!   by insertion time, so [`PlayerPool::oldest`] is `O(log n)`. This turns
//!   starvation-prevention into an explicit, testable guarantee rather than a
//!   probabilistic side effect of random selection.
//!
//! # Ordering invariant (avoids stale pointers)
//!
//! * **Insert** writes both indexes first, then `players` **last**.
//! * **Remove** deletes from `players` **first**, then both indexes.
//!
//! This guarantees the only transient inconsistency is "in an index but not yet
//! in `players`" (a `get` returns `None` — harmless), never the reverse (a stale
//! index entry pointing at a player that's already gone and could be re-matched).
//!
//! # Lock discipline (avoids deadlock)
//!
//! The two index mutexes are **never held simultaneously**. Each is locked,
//! mutated, and released independently. A DashMap shard guard is likewise never
//! held while locking an index mutex.

use crate::model::Player;
use dashmap::DashMap;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

pub struct PlayerPool {
    players: DashMap<Uuid, Player>,
    skill_index: Mutex<BTreeMap<(OrderedFloat<f32>, Uuid), ()>>,
    wait_index: Mutex<BTreeMap<(Instant, Uuid), ()>>,
}

impl PlayerPool {
    pub fn new() -> Arc<Self> {
        Arc::new(PlayerPool {
            players: DashMap::new(),
            skill_index: Mutex::new(BTreeMap::new()),
            wait_index: Mutex::new(BTreeMap::new()),
        })
    }

    /// Insert a fresh player, stamping `queued_at = now`. O(log n).
    pub fn insert(&self, mut player: Player) {
        let queued_at = Instant::now();
        player.queued_at = Some(queued_at);
        self.insert_with_time(player, queued_at);
    }

    /// Re-insert a player that lost an eviction race, **preserving** its original
    /// `queued_at`. Critical for fairness: a worker that claimed this player but
    /// couldn't complete the group must not reset the player's wait clock, or
    /// repeated races could starve them. Falls back to `now` if unset. O(log n).
    pub fn reinsert(&self, mut player: Player) {
        let queued_at = player.queued_at.unwrap_or_else(Instant::now);
        player.queued_at = Some(queued_at);
        self.insert_with_time(player, queued_at);
    }

    fn insert_with_time(&self, player: Player, queued_at: Instant) {
        let id = player.id;
        let skill = player.skill;
        // Indexes first...
        self.skill_index.lock().insert((OrderedFloat(skill), id), ());
        self.wait_index.lock().insert((queued_at, id), ());
        // ...primary store last.
        self.players.insert(id, player);
    }

    /// Remove a player atomically. Returns `None` if already evicted by a peer.
    ///
    /// `DashMap::remove` is the atomic decision point: in a race between two
    /// workers, exactly one observes `Some` and the other `None`.
    pub fn remove(&self, id: &Uuid) -> Option<Player> {
        // Primary store first — this is the ownership-deciding step.
        let (_, player) = self.players.remove(id)?;
        self.skill_index
            .lock()
            .remove(&(OrderedFloat(player.skill), *id));
        if let Some(qt) = player.queued_at {
            self.wait_index.lock().remove(&(qt, *id));
        }
        Some(player)
    }

    /// Clone of the longest-waiting player, or `None` if the pool is empty.
    /// O(log n). Used by workers as the match **anchor**.
    pub fn oldest(&self) -> Option<Player> {
        // Hold the wait_index lock only long enough to read the front key.
        let front = {
            let idx = self.wait_index.lock();
            idx.keys().next().copied()
        };
        front.and_then(|(_, id)| self.players.get(&id).map(|p| p.clone()))
    }

    /// IDs of players whose skill falls in `[lo, hi]`, capped at `cap`.
    /// Sorted ascending by skill. O(log n + k). Does **not** remove anything —
    /// the caller must `remove()` to take ownership.
    pub fn query_range(&self, lo: f32, hi: f32, cap: usize) -> Vec<Uuid> {
        let lo_key = (OrderedFloat(lo), Uuid::nil());
        let hi_key = (OrderedFloat(hi), Uuid::from_bytes([0xff; 16]));
        let idx = self.skill_index.lock();
        idx.range(lo_key..=hi_key)
            .take(cap)
            .map(|((_, id), _)| *id)
            .collect()
    }

    /// Clone of a single player by id, if still present.
    pub fn get(&self, id: &Uuid) -> Option<Player> {
        self.players.get(id).map(|p| p.clone())
    }

    /// Current number of queued players. O(1) amortised.
    pub fn len(&self) -> usize {
        self.players.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player(skill: f32, region: &str) -> Player {
        Player {
            id: Uuid::new_v4(),
            skill,
            region: region.to_string(),
            queued_at: None,
        }
    }

    #[test]
    fn insert_then_remove_roundtrip() {
        let pool = PlayerPool::new();
        let p = player(50.0, "us-east");
        let id = p.id;
        pool.insert(p);
        assert_eq!(pool.len(), 1);
        let got = pool.remove(&id).expect("present");
        assert_eq!(got.id, id);
        assert!(got.queued_at.is_some(), "insert must stamp queued_at");
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn double_remove_yields_none() {
        let pool = PlayerPool::new();
        let p = player(50.0, "us-east");
        let id = p.id;
        pool.insert(p);
        assert!(pool.remove(&id).is_some());
        assert!(pool.remove(&id).is_none(), "second remove must be None");
    }

    #[test]
    fn oldest_returns_first_inserted() {
        let pool = PlayerPool::new();
        let first = player(10.0, "eu-west");
        let first_id = first.id;
        pool.insert(first);
        // Force a distinct, later Instant for the second insert.
        std::thread::sleep(std::time::Duration::from_millis(2));
        pool.insert(player(90.0, "eu-west"));
        assert_eq!(pool.oldest().unwrap().id, first_id);
    }

    #[test]
    fn query_range_is_inclusive_and_skill_bounded() {
        let pool = PlayerPool::new();
        pool.insert(player(10.0, "r"));
        pool.insert(player(50.0, "r"));
        pool.insert(player(90.0, "r"));
        let ids = pool.query_range(40.0, 60.0, 100);
        assert_eq!(ids.len(), 1, "only the skill-50 player is in [40,60]");
    }

    #[test]
    fn reinsert_preserves_wait_clock() {
        let pool = PlayerPool::new();
        let mut p = player(50.0, "r");
        let id = p.id;
        let original = Instant::now() - std::time::Duration::from_secs(10);
        p.queued_at = Some(original);
        pool.reinsert(p);
        let got = pool.remove(&id).unwrap();
        assert_eq!(
            got.queued_at,
            Some(original),
            "reinsert must not reset the wait clock"
        );
    }
}
