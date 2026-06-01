//! 5v5 Real-Time Competitive Matchmaker — entrypoint.
//!
//! Wires together the pool, N matching workers, the metrics reporter, an
//! optional match-log consumer, and the HTTP API.

mod api;
mod balancer;
mod config;
mod matcher;
mod metrics;
mod model;
mod pool;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::model::Match;
use crate::pool::PlayerPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    // Structured logging; set RUST_LOG=debug for verbose output.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "matchmaker=info".into()),
        )
        .compact()
        .init();

    let cfg = Arc::new(Config::from_env());
    let pool = PlayerPool::new();
    let metrics = Arc::new(Metrics::default());
    let (match_tx, _rx) = broadcast::channel::<Match>(1024);

    tracing::info!(?cfg, "starting matchmaker");

    // --- Matching workers ---
    for i in 0..cfg.workers {
        let worker = matcher::run_worker(
            pool.clone(),
            cfg.clone(),
            metrics.clone(),
            match_tx.clone(),
        );
        tokio::spawn(async move {
            tracing::debug!(worker = i, "worker started");
            worker.await;
        });
    }
    tracing::info!(workers = cfg.workers, "matching workers spawned");

    // --- Match-log consumer (demonstrates the broadcast channel) ---
    {
        let mut rx = match_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(m) => tracing::debug!(
                        match_id = %m.id,
                        quality = m.quality,
                        skill_gap = m.skill_gap,
                        wait_ms = m.wait_ms,
                        cross_region = m.cross_region,
                        "match formed"
                    ),
                    // Lagged: we fell behind and dropped some — fine for a logger.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(dropped = n, "match-log consumer lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    // --- Periodic metrics reporter ---
    {
        let m = metrics.clone();
        let p = pool.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                let snap = m.snapshot(p.len());
                tracing::info!(target: "matchmaker::metrics", "{snap}");
            }
        });
    }

    // --- HTTP server ---
    let app = api::router(pool.clone(), metrics.clone());
    let listener = match tokio::net::TcpListener::bind(&cfg.bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %cfg.bind_addr, error = %e, "failed to bind");
            std::process::exit(1);
        }
    };
    tracing::info!(addr = %cfg.bind_addr, "matchmaker listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    tracing::info!("shutdown complete");
}

/// Resolve on Ctrl-C for a clean shutdown.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("ctrl-c received, shutting down");
}
