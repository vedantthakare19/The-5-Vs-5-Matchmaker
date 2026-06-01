//! Core data model shared across the engine.
//!
//! Everything else (pool, matcher, balancer, api) depends on these types.

use serde::{Deserialize, Serialize};
use std::time::Instant;
use uuid::Uuid;

/// Number of players that form a single match (2 teams of 5).
pub const TEAM_SIZE: usize = 5;
pub const MATCH_SIZE: usize = TEAM_SIZE * 2; // 10

/// A player waiting in (or pulled from) the matchmaking pool.
///
/// `queued_at` is wrapped in `Option<Instant>` so the struct stays
/// `Serialize`/`Deserialize` (an `Instant` cannot be serialized). It is
/// `#[serde(skip)]`-ed and populated by [`crate::pool::PlayerPool::insert`]
/// at the moment of pool insertion — never at HTTP-request arrival. This keeps
/// network latency out of the measured wait time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Player {
    pub id: Uuid,
    /// Normalised skill rating, clamped to 0.0–100.0.
    pub skill: f32,
    /// Geographic region, e.g. "us-east".
    pub region: String,
    #[serde(skip)]
    pub queued_at: Option<Instant>,
}

impl Player {
    /// Milliseconds this player has waited since entering the pool.
    /// Returns 0 if `queued_at` was never set.
    pub fn wait_ms(&self, now: Instant) -> u64 {
        match self.queued_at {
            Some(t) => now.saturating_duration_since(t).as_millis() as u64,
            None => 0,
        }
    }
}

/// A completed match: two balanced teams of 5.
#[derive(Debug, Clone, Serialize)]
pub struct Match {
    pub id: Uuid,
    pub team_a: Vec<Player>,
    pub team_b: Vec<Player>,
    /// Match quality in 0.0–1.0; higher is better.
    pub quality: f32,
    /// Longest wait (ms) among the 10 players — the starvation-relevant figure.
    pub wait_ms: u64,
    /// Absolute skill difference between the two teams' totals.
    pub skill_gap: f32,
    /// Whether this match mixed players from more than one region.
    pub cross_region: bool,
}

/// Inbound queue request body for `POST /queue`.
#[derive(Debug, Deserialize)]
pub struct QueueRequest {
    /// Client may omit; the server assigns a fresh UUID.
    pub player_id: Option<Uuid>,
    pub skill: f32,
    pub region: String,
}

/// Response body for `POST /queue`.
#[derive(Debug, Serialize)]
pub struct QueueResponse {
    pub player_id: Uuid,
    pub queue_depth: i64,
}
