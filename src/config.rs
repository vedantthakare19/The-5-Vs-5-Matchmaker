//! Runtime configuration, read from environment variables with sane defaults.
//!
//! All knobs are optional — the engine runs out of the box. Overriding them
//! lets the simulation explore the latency/quality trade-off without a rebuild.

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    /// Socket address the HTTP API binds to.
    pub bind_addr: String,
    /// Number of concurrent matching workers.
    pub workers: usize,
    /// Skill window half-width at t=0 (tight). Window is `±initial_window/2`.
    pub initial_window: f32,
    /// Skill window width after `relax_secs` of waiting (loose).
    pub max_window: f32,
    /// Seconds over which the window relaxes from initial to max.
    pub relax_secs: f32,
    /// After waiting this many seconds, the anchor accepts cross-region fills.
    pub cross_region_after_secs: f32,
    /// Hard anti-starvation deadline. Once the anchor has waited this long, the
    /// skill window goes effectively unbounded and region is ignored: the oldest
    /// player is matched with the 9 nearest available players, whoever they are.
    /// This guarantees that as long as >=10 players are in the pool, no player
    /// waits forever — even extreme-skill outliers in a near-empty bucket.
    pub hard_deadline_secs: f32,
    /// Max candidate IDs pulled from the skill index per scan (caps work under load).
    pub candidate_cap: usize,
    /// Sleep between scans when fewer than 10 candidates are available.
    pub idle_sleep_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Config {
            bind_addr: "0.0.0.0:3000".to_string(),
            workers,
            initial_window: 5.0,
            max_window: 40.0,
            relax_secs: 30.0,
            cross_region_after_secs: 8.0,
            hard_deadline_secs: 45.0,
            candidate_cap: 256,
            idle_sleep_ms: 5,
        }
    }
}

impl Config {
    /// Build a config from defaults, overlaying any `MM_*` environment variables.
    pub fn from_env() -> Self {
        let mut c = Config::default();
        if let Ok(v) = env::var("MM_BIND") {
            c.bind_addr = v;
        }
        if let Some(v) = parse_env("MM_WORKERS") {
            c.workers = (v as usize).max(1);
        }
        if let Some(v) = parse_env("MM_INITIAL_WINDOW") {
            c.initial_window = v;
        }
        if let Some(v) = parse_env("MM_MAX_WINDOW") {
            c.max_window = v;
        }
        if let Some(v) = parse_env("MM_RELAX_SECS") {
            c.relax_secs = v.max(0.001);
        }
        if let Some(v) = parse_env("MM_CROSS_REGION_AFTER_SECS") {
            c.cross_region_after_secs = v;
        }
        if let Some(v) = parse_env("MM_HARD_DEADLINE_SECS") {
            c.hard_deadline_secs = v;
        }
        if let Some(v) = parse_env("MM_CANDIDATE_CAP") {
            c.candidate_cap = (v as usize).max(MATCH_SIZE_USIZE);
        }
        if let Some(v) = parse_env("MM_IDLE_SLEEP_MS") {
            c.idle_sleep_ms = v as u64;
        }
        c
    }

    /// Current skill-window width for a player who has waited `wait_secs`.
    ///
    /// Linear relaxation: tight at t=0, widening to `max_window` at `relax_secs`.
    pub fn window_for(&self, wait_secs: f32) -> f32 {
        let frac = (wait_secs / self.relax_secs).clamp(0.0, 1.0);
        self.initial_window + (self.max_window - self.initial_window) * frac
    }
}

const MATCH_SIZE_USIZE: usize = 10;

fn parse_env(key: &str) -> Option<f32> {
    env::var(key).ok().and_then(|v| v.trim().parse::<f32>().ok())
}
