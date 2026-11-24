//! Compare physical strategies on one immutable registered reader. One
//! unmeasured pass primes each strategy before timing the warm workload.
use super::{Explanation, PlanModel, Random, Sample, measure, percentile};
use crate::{Error, Object, Query, error, reader::Reader};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Serialize)]
pub struct Ranking {
    pub seed: u64,
    pub rounds: usize,
    pub scores: Vec<Score>,
}

#[derive(Debug, Serialize)]
pub struct Score {
    pub plan: Explanation,
    pub median_ns: f64,
    pub p95_ns: f64,
    pub samples: Vec<Sample>,
}

impl Ranking {
    /// Each execution must reproduce the caller's independent expected page,
    /// including order and multiplicity, before any ranking is returned.
    pub fn run(
        reader: &Reader,
        query: &Query,
        plans: &[PlanModel],
        expected: &[Object],
        rounds: usize,
        seed: u64,
    ) -> Result<Self, Error> {
        if rounds < 2 || plans.len() < 2 {
            return Err(error("ranking requires two or more plans and rounds"));
        }
        let mut names = HashSet::new();
        let mut scores = Vec::new();
        for plan in plans {
            if !names.insert(&plan.name) {
                return Err(error("ranked plans must have distinct names"));
            }
            scores.push(Score {
                plan: plan.explain(query)?,
                median_ns: 0.0,
                p95_ns: 0.0,
                samples: Vec::new(),
            });
        }
        let run = |plan: &PlanModel| {
            let (sample, rows) = measure(plan, || reader.query(query.clone()))?;
            if rows != expected {
                return Err(error(format!(
                    "{} differs from expected rows or order",
                    plan.name
                )));
            }
            Ok(sample)
        };
        for plan in plans {
            run(plan)?;
        }
        let mut order: Vec<_> = (0..plans.len()).collect();
        let mut rng = Random(seed);
        for round in 0..rounds {
            // Every plan occupies every timing position once per complete block.
            if round % plans.len() == 0 {
                for i in (1..order.len()).rev() {
                    order.swap(i, (rng.next() as usize) % (i + 1));
                }
            }
            for &i in &order {
                scores[i].samples.push(run(&plans[i])?);
            }
            order.rotate_left(1);
        }
        for score in &mut scores {
            let times: Vec<_> = score.samples.iter().map(|s| s.total_ns as f64).collect();
            score.median_ns = percentile(&times, 0.5);
            score.p95_ns = percentile(&times, 0.95);
        }
        scores.sort_by(|a, b| a.median_ns.total_cmp(&b.median_ns));
        Ok(Self {
            seed,
            rounds,
            scores,
        })
    }
}
