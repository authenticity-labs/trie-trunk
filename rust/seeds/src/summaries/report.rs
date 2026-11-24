//! Bounded display reductions over registered summary readers. Native clients,
//! command-line tools and WASM use the same counts and interval statistics.
use super::columns::{Block, Columns};
use super::{SUMMARY_PERIOD_MS, SummaryReader, SummaryWindow};
use crate::{Error, MemoryBudget, Predicate, error};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub(super) struct Metric {
    pub events: u64,
    pub lower: u64,
    pub samples: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub means: Vec<f64>,
}
impl Metric {
    fn add(&mut self, other: &Self) -> Result<(), Error> {
        self.events = self
            .events
            .checked_add(other.events)
            .ok_or(Error::Overflow)?;
        self.lower = self.lower.checked_add(other.lower).ok_or(Error::Overflow)?;
        self.samples = self
            .samples
            .checked_add(other.samples)
            .ok_or(Error::Overflow)?;
        self.sum += other.sum;
        if !self.sum.is_finite() {
            return Err(error("summary chart sum exceeds finite range"));
        }
        if let Some(v) = other.min {
            self.min = Some(self.min.map_or(v, |n| n.min(v)));
        }
        if let Some(v) = other.max {
            self.max = Some(self.max.map_or(v, |n| n.max(v)));
        }
        Ok(())
    }
    fn metrics(&self) -> Json {
        let mut means = self.means.clone();
        let p95 = if means.is_empty() {
            None
        } else {
            let index = (means.len() * 95).div_ceil(100) - 1;
            Some(*means.select_nth_unstable_by(index, f64::total_cmp).1)
        };
        json!({"events":self.events,"errors":null,"errorRate":null,"samples":self.samples,
            "average":(self.samples>0).then(||self.sum/self.samples as f64),"p95":p95})
    }
}

/// One report retains interval aggregates, never Packs or event objects. Each
/// source/time pair is consumed once. Unknown predicates preserve candidates.
pub struct SummaryReport {
    window: SummaryWindow,
    step: u64,
    metric: String,
    rows: Columns,
    memory: MemoryBudget,
    fields: BTreeSet<String>,
    pending: BTreeSet<String>,
    coverage: bool,
    complete: bool,
    packets: usize,
    context: crate::cast::Context,
}
impl SummaryReport {
    pub fn new(from: u64, to: u64, step: u64, metric: impl Into<String>) -> Result<Self, Error> {
        let window = SummaryWindow::new(from..to)?;
        if step == 0 || step % SUMMARY_PERIOD_MS != 0 || window.period_count() > 366 * 288 {
            return Err(error("summary report exceeds its interval budget"));
        }
        Ok(Self {
            window,
            step,
            metric: metric.into(),
            rows: Columns::default(),
            memory: MemoryBudget::new(64 * 1024 * 1024),
            fields: BTreeSet::new(),
            pending: BTreeSet::new(),
            coverage: true,
            complete: true,
            packets: 0,
            context: crate::cast::Context::default(),
        })
    }
    pub fn context(mut self, context:crate::cast::Context)->Self {self.context=context;self}
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Result<Self, Error> {
        if self.rows.len() != 0 {
            return Err(error("set summary budget before collection"));
        }
        self.memory = budget;
        Ok(self)
    }
    pub fn consume(
        &mut self,
        reader: &mut SummaryReader,
        source: &str,
        from: u64,
        to: u64,
        predicate: &Predicate,
    ) -> Result<(), Error> {
        let covered = self.window.covered();
        let extent = reader.extent()?;
        let begin = from.max(covered.start).max(extent.start);
        let end = to.min(covered.end).min(extent.end);
        if begin >= end {
            return Ok(());
        }
        let window = SummaryWindow::new(begin..end)?;
        // No predicate needs a second row traversal or a per-period bounds map.
        // Live snapshots still remain incomplete, including zero-count periods.
        let bounds = if matches!(predicate, Predicate::All) {
            self.coverage &= reader.sealed();
            None
        } else {
            let preview = reader.preview_with(&window, predicate,&self.context)?;
            self.coverage &= preview.coverage_complete;
            self.complete &= preview.predicate_complete;
            self.fields.extend(preview.fields);
            self.pending.extend(preview.pending);
            Some(
                preview
                    .periods
                    .into_iter()
                    .map(|p| (p.time, p.count))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        let count = usize::try_from(window.period_count()).map_err(|_| Error::Overflow)?;
        if self.rows.len().checked_add(count).ok_or(Error::Overflow)? > 1_000_000 {
            return Err(error("summary report exceeds its row budget"));
        }
        let mut staged = Block::new(source, count, &self.memory)?;
        reader.metric_rows(&window, &self.metric, |time, count, metric| {
            let bound = match &bounds {
                Some(bounds) => *bounds
                    .get(&time)
                    .ok_or_else(|| error("missing summary preview period"))?,
                None => super::CountBounds::exact(count),
            };
            let mut cell = Metric {
                events: bound.upper,
                lower: bound.lower,
                ..Metric::default()
            };
            // A partially selected population cannot supply exact field totals.
            if bound.lower == count && bound.upper == count {
                if let Some(value) = metric.as_ref() {
                    cell.samples = value.samples;
                    cell.sum = value.sum;
                    cell.min = Some(value.min);
                    cell.max = Some(value.max);
                }
            }
            staged.push(time, cell)
        })?;
        self.rows.insert(staged)?;
        self.packets += 1;
        Ok(())
    }
    pub fn finish(&self) -> Result<Json, Error> {
        let covered = self.window.covered();
        let bins = usize::try_from((covered.end - covered.start).div_ceil(self.step))
            .map_err(|_| Error::Overflow)?;
        if bins > 4096 {
            return Err(error("summary report exceeds chart point budget"));
        }
        let mut time = vec![Metric::default(); bins];
        let mut total = Metric::default();
        let mut halves = [Metric::default(), Metric::default()];
        let mut periods = BTreeMap::<u64, Metric>::new();
        // Resolve source labels once and reduce every dimension in the same
        // traversal. Period means are derived after merging all sources in a
        // group, preserving their statistical meaning.
        let mut labels = BTreeMap::<&str, [String; 2]>::new();
        let mut groups: BTreeMap<String, BTreeMap<String, (Metric, Vec<u64>)>> = BTreeMap::from([
            ("service".into(), BTreeMap::new()),
            ("resource".into(), BTreeMap::new()),
            ("level".into(), BTreeMap::new()),
        ]);
        let mut group_periods = [
            BTreeMap::<String, BTreeMap<u64, Metric>>::new(),
            BTreeMap::new(),
        ];
        for (source, at, cell) in self.rows.iter() {
            let bin = ((at - covered.start) / self.step) as usize;
            total.add(&cell)?;
            time[bin].add(&cell)?;
            halves[usize::from(at >= covered.start + (covered.end - covered.start) / 2)]
                .add(&cell)?;
            periods.entry(at).or_default().add(&cell)?;
            let keys = labels.entry(source).or_insert_with(|| {
                [
                    source.split('/').take(2).collect::<Vec<_>>().join("/"),
                    source.split('/').take(3).collect::<Vec<_>>().join("/"),
                ]
            });
            for (i, dimension) in ["service", "resource"].iter().enumerate() {
                let key = &keys[i];
                let dimension = groups.get_mut(*dimension).unwrap();
                if !dimension.contains_key(key) {
                    dimension.insert(key.clone(), (Metric::default(), vec![0; bins]));
                }
                let (value, trend) = dimension.get_mut(key).unwrap();
                value.add(&cell)?;
                trend[bin] = trend[bin].checked_add(cell.events).ok_or(Error::Overflow)?;
                if !group_periods[i].contains_key(key) {
                    group_periods[i].insert(key.clone(), BTreeMap::new());
                }
                group_periods[i]
                    .get_mut(key)
                    .unwrap()
                    .entry(at)
                    .or_default()
                    .add(&cell)?;
            }
        }
        for (&at, value) in &periods {
            if value.samples == 0 {
                continue;
            }
            let mean = value.sum / value.samples as f64;
            total.means.push(mean);
            time[((at - covered.start) / self.step) as usize]
                .means
                .push(mean);
            halves[usize::from(at >= covered.start + (covered.end - covered.start) / 2)]
                .means
                .push(mean);
        }
        for (dimension, periods) in ["service", "resource"].into_iter().zip(group_periods) {
            let groups = groups.get_mut(dimension).unwrap();
            for (key, periods) in periods {
                let means = &mut groups.get_mut(&key).unwrap().0.means;
                means.extend(
                    periods
                        .values()
                        .filter(|v| v.samples > 0)
                        .map(|v| v.sum / v.samples as f64),
                );
            }
        }
        let chart_groups = groups
            .iter()
            .map(|(dimension, groups)| {
                (
                    dimension.clone(),
                    Json::Array(
                        groups
                            .iter()
                            .map(|(key, (m, trend))| {
                                let mut v = m.metrics();
                                let o = v.as_object_mut().unwrap();
                                o.insert("key".into(), key.clone().into());
                                o.insert("label".into(), key.replace('/', " / ").into());
                                o.insert("trend".into(), json!(trend));
                                v
                            })
                            .collect(),
                    ),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let counts = time.iter().map(|v| v.events).collect::<Vec<_>>();
        let exact = total.events == total.lower;
        if total.events > 9_007_199_254_740_991 {
            return Err(error("summary report count exceeds lossless chart range"));
        }
        let summary = json!({"requested":{"from":self.window.requested().start,"to":self.window.requested().end},"from":covered.start,"to":covered.end,"step":self.step,"count":total.events,"counts":counts,"errors":vec![0u64;bins],"status":"committed-snapshot","coverage":{"complete":self.coverage},"filter":{"exact":exact,"complete":self.complete,"fields":self.fields,"pending":self.pending},"statistics":{"complete":false,"matched":total.events,"evaluated":0,"services":[],"levels":[],"duration":{"count":total.samples,"average":if total.samples>0{total.sum/total.samples as f64}else{0.0},"min":total.min.unwrap_or(0.0),"max":total.max.unwrap_or(0.0)},"operations":{"errors":[],"latency":[]}}});
        let fast = json!({"from":covered.start,"to":covered.end,"step":self.step,"packets":self.packets,"matched":total.events,"totals":total.metrics(),"time":time.iter().enumerate().map(|(i,v)|{let mut v=v.metrics();v.as_object_mut().unwrap().insert("time".into(),json!(covered.start+i as u64*self.step));v}).collect::<Vec<_>>(),"comparison":{"earlier":halves[0].metrics(),"later":halves[1].metrics()},"groups":chart_groups});
        let mut summary = summary;
        summary["errorsAvailable"] = false.into();
        summary["statistics"]["complete"] = (total.events == 0).into();
        summary["statistics"]["available"] =
            json!({"services":true,"levels":false,"errors":false,"duration":total.samples>0});
        summary["statistics"]["services"] = json!(
            groups["service"]
                .iter()
                .filter(|(_, (m, _))| m.events > 0)
                .map(|(name, (m, _))| json!({"name":name,"count":m.events}))
                .collect::<Vec<_>>()
        );
        Ok(json!({"summary":summary,"dashboard":fast}))
    }
}
