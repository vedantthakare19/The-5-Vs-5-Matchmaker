//! Team balancer: split exactly 10 players into two fair teams of 5.
//!
//! # Algorithm — exhaustive C(10,5) enumeration
//!
//! For exactly 10 players there are `C(10,5) = 252` ways to choose team A
//! (team B is the complement). 252 is a compile-time constant, so exhaustively
//! evaluating every split is `O(252) = O(1)` and yields the **globally optimal**
//! partition that minimises `|sum(A) − sum(B)|`. No approximation, no
//! convergence limit, no edge cases.
//!
//! To break ties between equal skill gaps we prefer the split with the smaller
//! combined intra-team spread (stddev), which produces visibly fairer teams.
//!
//! See the README for why snake-draft + 2-opt is the right *general-case*
//! algorithm but strictly worse here, where n is fixed at 10.

use crate::model::{Match, Player, MATCH_SIZE, TEAM_SIZE};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::time::Instant;
use uuid::Uuid;

/// Split 10 players into the optimal (team_a, team_b) pair.
///
/// # Panics
/// If `players.len() != 10`.
pub fn balance_teams(players: &[Player]) -> (Vec<Player>, Vec<Player>) {
    assert_eq!(
        players.len(),
        MATCH_SIZE,
        "balancer requires exactly {MATCH_SIZE} players"
    );

    let best_indices = (0..MATCH_SIZE)
        .combinations(TEAM_SIZE)
        .min_by_key(|idx| {
            let sum_a: f32 = idx.iter().map(|&i| players[i].skill).sum();
            let total: f32 = players.iter().map(|p| p.skill).sum();
            let sum_b = total - sum_a;
            let gap = (sum_a - sum_b).abs();

            // Tie-break on combined spread so equally-balanced splits still pick
            // the one with the most internally-even teams.
            let team_a: Vec<f32> = idx.iter().map(|&i| players[i].skill).collect();
            let team_b: Vec<f32> = (0..MATCH_SIZE)
                .filter(|i| !idx.contains(i))
                .map(|i| players[i].skill)
                .collect();
            let spread = stddev(&team_a) + stddev(&team_b);

            (OrderedFloat(gap), OrderedFloat(spread))
        })
        .expect("C(10,5) is non-empty");

    let team_a: Vec<Player> = best_indices.iter().map(|&i| players[i].clone()).collect();
    let team_b: Vec<Player> = (0..MATCH_SIZE)
        .filter(|i| !best_indices.contains(i))
        .map(|i| players[i].clone())
        .collect();
    (team_a, team_b)
}

/// Build a full [`Match`] from 10 evicted players: balance, score, and stamp wait.
pub fn build_match(players: Vec<Player>, now: Instant) -> Match {
    let cross_region = players
        .iter()
        .any(|p| p.region != players[0].region);
    let wait_ms = players.iter().map(|p| p.wait_ms(now)).max().unwrap_or(0);

    let (team_a, team_b) = balance_teams(&players);
    let quality = quality_score(&team_a, &team_b);
    let skill_gap = (sum(&team_a) - sum(&team_b)).abs();

    Match {
        id: Uuid::new_v4(),
        team_a,
        team_b,
        quality,
        wait_ms,
        skill_gap,
        cross_region,
    }
}

/// Quality in 0.0–1.0. Penalises both the inter-team skill gap and the
/// intra-team spread, so a match of (50,50,50,50,50) vs (50,50,50,50,50)
/// scores higher than (90,90,10,10,50) vs (90,90,10,10,50) even though both
/// have a zero inter-team gap.
pub fn quality_score(a: &[Player], b: &[Player]) -> f32 {
    let skill_diff = (sum(a) - sum(b)).abs() / TEAM_SIZE as f32; // avg per-player gap
    let skill_a: Vec<f32> = a.iter().map(|p| p.skill).collect();
    let skill_b: Vec<f32> = b.iter().map(|p| p.skill).collect();
    let spread = stddev(&skill_a) + stddev(&skill_b);

    let gap_penalty = (skill_diff / 100.0).min(1.0);
    let spread_penalty = (spread / 200.0).min(0.5);
    (1.0 - gap_penalty - spread_penalty).clamp(0.0, 1.0)
}

fn sum(team: &[Player]) -> f32 {
    team.iter().map(|p| p.skill).sum()
}

fn stddev(xs: &[f32]) -> f32 {
    if xs.is_empty() {
        return 0.0;
    }
    let mean = xs.iter().sum::<f32>() / xs.len() as f32;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f32>() / xs.len() as f32;
    var.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn players(skills: &[f32]) -> Vec<Player> {
        skills
            .iter()
            .map(|&s| Player {
                id: Uuid::new_v4(),
                skill: s,
                region: "r".to_string(),
                queued_at: Some(Instant::now()),
            })
            .collect()
    }

    #[test]
    fn perfect_split_has_zero_gap() {
        // Pairable into two identical teams summing to 250 each.
        let p = players(&[10.0, 20.0, 30.0, 40.0, 50.0, 50.0, 40.0, 30.0, 20.0, 10.0]);
        let (a, b) = balance_teams(&p);
        assert_eq!(a.len(), 5);
        assert_eq!(b.len(), 5);
        assert!((sum(&a) - sum(&b)).abs() < 1e-3, "should find the zero-gap split");
    }

    #[test]
    fn all_equal_is_perfectly_balanced_and_top_quality() {
        let p = players(&[50.0; 10]);
        let (a, b) = balance_teams(&p);
        assert!((sum(&a) - sum(&b)).abs() < 1e-3);
        let q = quality_score(&a, &b);
        assert!(q > 0.99, "identical players => near-perfect quality, got {q}");
    }

    #[test]
    fn balancer_finds_global_optimum_not_greedy() {
        // Greedy "assign largest to lighter team" would split poorly here;
        // exhaustive search must find the exact best.
        let p = players(&[1.0, 2.0, 3.0, 4.0, 100.0, 99.0, 98.0, 97.0, 96.0, 0.0]);
        let (a, b) = balance_teams(&p);
        let gap = (sum(&a) - sum(&b)).abs();
        // Brute-force the optimum independently to compare.
        let skills: Vec<f32> = p.iter().map(|x| x.skill).collect();
        let total: f32 = skills.iter().sum();
        let best = (0..10)
            .combinations(5)
            .map(|idx| {
                let sa: f32 = idx.iter().map(|&i| skills[i]).sum();
                (sa - (total - sa)).abs()
            })
            .min_by(|x, y| x.partial_cmp(y).unwrap())
            .unwrap();
        assert!((gap - best).abs() < 1e-3, "gap {gap} must equal optimum {best}");
    }

    #[test]
    fn quality_is_bounded_unit_interval() {
        let p = players(&[100.0, 100.0, 100.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
        let (a, b) = balance_teams(&p);
        let q = quality_score(&a, &b);
        assert!((0.0..=1.0).contains(&q), "quality {q} out of range");
    }

    #[test]
    #[should_panic]
    fn wrong_count_panics() {
        let p = players(&[1.0, 2.0, 3.0]);
        let _ = balance_teams(&p);
    }
}
