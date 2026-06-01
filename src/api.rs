//! HTTP API (Axum). Handlers are deliberately thin — all logic lives in the
//! pool / matcher / metrics modules.
//!
//! | Method | Path       | Description                                   |
//! |--------|------------|-----------------------------------------------|
//! | POST   | `/queue`   | Enqueue a player, return 202 + assigned id    |
//! | GET    | `/metrics` | Lock-free metrics snapshot as JSON            |
//! | GET    | `/health`  | Liveness + current queue depth                |

use crate::metrics::Metrics;
use crate::model::{Player, QueueRequest, QueueResponse};
use crate::pool::PlayerPool;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<PlayerPool>,
    pub metrics: Arc<Metrics>,
}

pub fn router(pool: Arc<PlayerPool>, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/queue", post(enqueue))
        .route("/metrics", get(show_metrics))
        .route("/health", get(health))
        .with_state(AppState { pool, metrics })
}

/// `POST /queue` — accept a player into the pool. Returns 202 Accepted.
async fn enqueue(
    State(s): State<AppState>,
    Json(req): Json<QueueRequest>,
) -> (StatusCode, Json<QueueResponse>) {
    let id = req.player_id.unwrap_or_else(Uuid::new_v4);
    let player = Player {
        id,
        skill: req.skill.clamp(0.0, 100.0),
        region: req.region,
        queued_at: None, // pool.insert() stamps this
    };
    s.metrics.record_enqueue();
    s.pool.insert(player);
    let depth = s.metrics.queue_depth.load(Relaxed);
    (
        StatusCode::ACCEPTED,
        Json(QueueResponse {
            player_id: id,
            queue_depth: depth,
        }),
    )
}

/// `GET /metrics` — JSON snapshot of lock-free counters.
async fn show_metrics(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(s.metrics.snapshot(s.pool.len()))
}

/// `GET /health` — liveness probe + live queue depth.
async fn health(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "queue_depth": s.pool.len(),
    }))
}
