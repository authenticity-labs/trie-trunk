//! Development-only physical-plan experiments. Normal builds contain neither
//! the reference executor nor these timers/counters. No automatic promotion.
use crate::{Cursor, Error, Object, Query, Sources, error};
use serde::{Deserialize, Serialize};
#[cfg(feature = "registration")]
mod ranking;
#[cfg(feature = "registration")]
pub use ranking::{Ranking, Score};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Selection {
    Streaming,
    BoundedTopK,
    FullSortReference,
}

/// Allowed predicate pushdowns. The source's explicit time window and the
/// residual predicate still apply to every strategy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Access {
    #[default]
    Scan,
    Time,
    Leaves,
    TimeAndLeaves,
}

pub(crate) struct Execution {
    pub access: Access,
    pub selection: Selection,
    pub max_candidates: usize,
    pub peak_candidates: usize,
}
impl Default for Execution {
    fn default() -> Self {
        Self {
            access: Access::TimeAndLeaves,
            selection: Selection::BoundedTopK,
            max_candidates: usize::MAX,
            peak_candidates: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanModel {
    pub name: String,
    pub access: Access,
    pub selection: Selection,
    pub batch_rows: usize,
    pub max_candidates: usize,
}
impl PlanModel {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            access: Access::Scan,
            selection: Selection::Streaming,
            batch_rows: 256,
            max_candidates: 100_000,
        }
    }
    pub fn access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }
    pub fn selection(mut self, value: Selection) -> Self {
        self.selection = value;
        self
    }
    pub fn batch_rows(mut self, value: usize) -> Self {
        self.batch_rows = value;
        self
    }
    /// Cardinality guard, not a byte budget: a string key can be arbitrarily wide.
    pub fn max_candidates(mut self, value: usize) -> Self {
        self.max_candidates = value;
        self
    }
    pub fn explain(&self, query: &Query) -> Result<Explanation, Error> {
        if self.name.trim().is_empty() || self.batch_rows == 0 || self.max_candidates == 0 {
            return Err(error(
                "plan model requires a name and nonzero batch/candidate bounds",
            ));
        }
        let mut steps = vec![
            "Bind field ordinals once per registered Seed Definition",
            match self.access {
                Access::Scan => "Scan all rows in the selected source window",
                Access::Time => "Seek eligible predicate bounds in the published time index",
                Access::Leaves => "Test eligible predicates against version-bound leaf states",
                Access::TimeAndLeaves => "Seek eligible time bounds, then test eligible leaf predicates",
            },
            "Evaluate the residual predicate unless the source proves the entire predicate",
        ];
        match (query.lab_order(), self.selection) {
            (None, Selection::Streaming) => {
                steps.push("Coordinator applies one global offset/limit; early stop");
            }
            (Some((_, _, Some(limit), offset)), Selection::BoundedTopK) => {
                let count = offset.checked_add(limit).ok_or(Error::Overflow)?;
                if count > self.max_candidates {
                    return Err(error("plan model top-K exceeds candidate budget"));
                }
                steps
                    .push("Coordinator retains global top (offset + limit), then orders survivors");
            }
            (Some((_, _, Some(_), _)), Selection::FullSortReference) => {
                steps.push("Coordinator collects all matches within candidate budget, sorts, then applies global offset/limit");
            }
            _ => {
                return Err(error(
                    "plan selection does not match the query ordering/limit contract",
                ));
            }
        }
        steps.push("Project and materialize every returned field in Rust");
        Ok(Explanation {
            model: self.clone(),
            logical_query: serde_json::to_value(query).map_err(|e| error(e.to_string()))?,
            steps,
            required_properties: vec![
                "Immutable input snapshot and fixed source order",
                "Same comparison, null/absent and tie rules",
                "A source may skip the residual only with exact predicate coverage",
            ],
            ordering_ties: "Source ordinal, then member ordinal within this snapshot; not persistent event identity",
            status: "Executable development model; not an automatic optimizer",
        })
    }
}
#[derive(Debug, Serialize)]
pub struct Explanation {
    pub model: PlanModel,
    pub logical_query: serde_json::Value,
    pub steps: Vec<&'static str>,
    pub required_properties: Vec<&'static str>,
    pub ordering_ties: &'static str,
    pub status: &'static str,
}

pub struct Experiment {
    name: String,
    query: Query,
    baseline: PlanModel,
    candidate: PlanModel,
    expected: Option<Vec<Object>>,
    warmups: usize,
    pairs: usize,
    seed: u64,
    provenance: BTreeMap<String, String>,
}
impl Experiment {
    pub fn new(
        name: impl Into<String>,
        query: Query,
        baseline: PlanModel,
        candidate: PlanModel,
    ) -> Self {
        Self {
            name: name.into(),
            query,
            baseline,
            candidate,
            expected: None,
            warmups: 2,
            pairs: 12,
            seed: 1,
            provenance: BTreeMap::new(),
        }
    }
    /// Supply values computed independently from original ingress events.
    /// Without this, the report explicitly records differential agreement only.
    pub fn expected(mut self, rows: Vec<Object>) -> Self {
        self.expected = Some(rows);
        self
    }
    pub fn warmups(mut self, n: usize) -> Self {
        self.warmups = n;
        self
    }
    pub fn pairs(mut self, n: usize) -> Self {
        self.pairs = n;
        self
    }
    pub fn seed(mut self, n: u64) -> Self {
        self.seed = n;
        self
    }
    pub fn provenance(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.provenance.insert(key.into(), value.into());
        self
    }
    pub fn run(&self, sources: &Sources) -> Result<Report, Error> {
        if self.pairs < 2 {
            return Err(error(
                "planner experiment requires at least two measured pairs",
            ));
        }
        if self.baseline.name == self.candidate.name {
            return Err(error("plan models must have distinct names"));
        }
        let baseline = self.baseline.explain(&self.query)?;
        let candidate = self.candidate.explain(&self.query)?;
        let dataset = Dataset::inspect(sources);
        for _ in 0..self.warmups {
            let (_, a) = execute(sources, &self.query, &self.baseline)?;
            let (_, b) = execute(sources, &self.query, &self.candidate)?;
            self.verify(&a, &b)?;
        }
        let mut rng = Random(self.seed);
        let mut order: Vec<bool> = (0..self.pairs).map(|i| i % 2 == 0).collect();
        for i in (1..order.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            order.swap(i, j);
        }
        let mut samples = Vec::with_capacity(self.pairs);
        for (pair, &baseline_first) in order.iter().enumerate() {
            // Correctness checks and output destruction happen after each pair's
            // timed executions, never in one contender's execution only.
            let (a, b) = if baseline_first {
                (
                    execute(sources, &self.query, &self.baseline)?,
                    execute(sources, &self.query, &self.candidate)?,
                )
            } else {
                let b = execute(sources, &self.query, &self.candidate)?;
                let a = execute(sources, &self.query, &self.baseline)?;
                (a, b)
            };
            self.verify(&a.1, &b.1)?;
            samples.push(Pair {
                pair,
                baseline_first,
                baseline: a.0,
                candidate: b.0,
            });
        }
        let ratios: Vec<_> = samples
            .iter()
            .map(|p| (p.baseline.total_ns.max(1) as f64 / p.candidate.total_ns.max(1) as f64).ln())
            .collect();
        let median_speedup = percentile(&ratios, 0.5).exp();
        let mut boot = Vec::with_capacity(2000);
        let mut resample = Vec::with_capacity(ratios.len());
        for _ in 0..2000 {
            resample.clear();
            for _ in 0..ratios.len() {
                resample.push(ratios[(rng.next() as usize) % ratios.len()]);
            }
            boot.push(percentile(&resample, 0.5).exp());
        }
        Ok(Report {
            experiment: self.name.clone(),
            dataset,
            baseline,
            candidate,
            provenance: self.provenance.clone(),
            warmup_pairs: self.warmups,
            seed: self.seed,
            correctness: if self.expected.is_some() {
                "Both plans matched independent expected values and one another on every run"
            } else {
                "Differential agreement only: shared codec/comparison bugs are not excluded"
            },
            timing_boundary: "Prepared immutable Packs -> fresh query binding -> scan/filter/global selection -> complete projected native objects; temporary batch disposal included; verification, final output/cursor destruction, ingestion, upload, decompression, and rendering excluded",
            cache_state: "Prepared Pack indexes; warmups recorded; fresh cursor/output each execution; CPU/OS cache state uncontrolled",
            memory_measurement: "Candidate counts and unique input backing capacity only. Allocation peaks, RSS and output heap are not measured",
            inference_scope: "Paired bootstrap median speedup interval conditional on this fixture/process; not a workload-wide guarantee or automatic promotion",
            bootstrap_replicates: 2000,
            median_speedup,
            paired_bootstrap_95: [percentile(&boot, 0.025), percentile(&boot, 0.975)],
            samples,
        })
    }
    fn verify(&self, a: &[Object], b: &[Object]) -> Result<(), Error> {
        if a != b {
            return Err(error(
                "planner experiment rejected: result values/order/multiplicity differ",
            ));
        }
        if self.expected.as_ref().is_some_and(|expected| expected != a) {
            return Err(error(
                "planner experiment rejected: result differs from independent oracle",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub experiment: String,
    pub dataset: Dataset,
    pub baseline: Explanation,
    pub candidate: Explanation,
    pub provenance: BTreeMap<String, String>,
    pub warmup_pairs: usize,
    pub seed: u64,
    pub correctness: &'static str,
    pub timing_boundary: &'static str,
    pub cache_state: &'static str,
    pub memory_measurement: &'static str,
    pub inference_scope: &'static str,
    pub bootstrap_replicates: usize,
    pub median_speedup: f64,
    pub paired_bootstrap_95: [f64; 2],
    pub samples: Vec<Pair>,
}
#[derive(Debug, Serialize)]
pub struct Dataset {
    pub fingerprint: String,
    pub packs: usize,
    pub rows: usize,
    pub encoded_pack_bytes: usize,
    pub retained_backing_bytes: usize,
    pub registered_definitions: usize,
}
impl Dataset {
    fn inspect(sources: &Sources) -> Self {
        let mut hash = Sha256::new();
        let mut rows = 0;
        let mut bytes = 0;
        let mut retained = 0;
        let mut allocations = HashSet::new();
        let mut definitions = HashSet::new();
        hash.update(b"data-seeds-planner-fixture-v0");
        for p in &sources.packs {
            for data in [p.definition.prescription().bytes(), p.stored_bytes()] {
                hash.update((data.len() as u64).to_le_bytes());
                hash.update(data);
            }
            rows += p.len();
            bytes += p.byte_length();
            if allocations.insert(p.allocation) {
                retained += p.retained_bytes();
            }
            definitions.insert(std::sync::Arc::as_ptr(&p.definition) as usize);
        }
        Self {
            fingerprint: format!("{:x}", hash.finalize()),
            packs: sources.packs.len(),
            rows,
            encoded_pack_bytes: bytes,
            retained_backing_bytes: retained,
            registered_definitions: definitions.len(),
        }
    }
}
#[derive(Debug, Serialize)]
pub struct Pair {
    pub pair: usize,
    pub baseline_first: bool,
    pub baseline: Sample,
    pub candidate: Sample,
}
#[derive(Debug, Serialize)]
pub struct Sample {
    pub plan_ns: u64,
    pub fetch_ns: u64,
    pub materialize_ns: u64,
    pub other_ns: u64,
    pub total_ns: u64,
    pub first_output_ns: Option<u64>,
    pub scanned_rows: usize,
    pub matched_rows: usize,
    pub output_rows: usize,
    pub peak_sort_candidates: usize,
}
fn execute(
    sources: &Sources,
    query: &Query,
    model: &PlanModel,
) -> Result<(Sample, Vec<Object>), Error> {
    if model.access != Access::Scan {
        return Err(error("index strategies require a registered reader"));
    }
    measure(model, || Cursor::new(sources.packs.clone(), query.clone()))
}

fn measure(model: &PlanModel, open: impl FnOnce() -> Result<Cursor, Error>) -> Result<(Sample, Vec<Object>), Error> {
    let start = Instant::now();
    let mut cursor = open()?;
    cursor.lab = Execution {
        access: model.access,
        selection: model.selection,
        max_candidates: model.max_candidates,
        peak_candidates: 0,
    };
    let plan_ns = ns(start.elapsed());
    let mut fetch_ns = 0;
    let mut materialize_ns = 0;
    let mut output = Vec::new();
    let mut first_output_ns = None;
    loop {
        let at = Instant::now();
        let batch = cursor.fetch(model.batch_rows)?;
        fetch_ns += ns(at.elapsed());
        if batch.is_empty() {
            break;
        }
        let at = Instant::now();
        output.extend(batch.objects()?);
        materialize_ns += ns(at.elapsed());
        if first_output_ns.is_none() {
            first_output_ns = Some(ns(start.elapsed()));
        }
    }
    let total_ns = ns(start.elapsed());
    Ok((
        Sample {
            plan_ns,
            fetch_ns,
            materialize_ns,
            other_ns: total_ns.saturating_sub(plan_ns + fetch_ns + materialize_ns),
            total_ns,
            first_output_ns,
            scanned_rows: cursor.scanned,
            matched_rows: cursor.matched,
            output_rows: output.len(),
            peak_sort_candidates: cursor.lab.peak_candidates,
        },
        output,
    ))
}
fn ns(value: std::time::Duration) -> u64 {
    value.as_nanos().min(u64::MAX as u128) as u64
}
fn percentile(values: &[f64], p: f64) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let rank = p * (v.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    v[lo] + (v[hi] - v[lo]) * (rank - lo as f64)
}
// Reproducible experiment scheduling/resampling, never application entropy.
struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut v = self.0;
        v = (v ^ (v >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        v = (v ^ (v >> 27)).wrapping_mul(0x94d049bb133111eb);
        v ^ (v >> 31)
    }
}
