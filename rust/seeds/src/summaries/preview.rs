//! Summary-only predicate previews. Plans stop at the first unsupported
//! conjunct; partial OR expressions are never used to eliminate buckets.
use super::{
    SUMMARY_PERIOD_MS, SummaryPlan, SummarySelection,
    SummaryWindow,
};
use crate::{Compare, Error, Predicate, error};
use pil_codec::{
    Value,
    api::{NativeValue, PillView},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CountBounds {
    pub lower: u64,
    pub upper: u64,
}
impl CountBounds {
    pub fn exact(count: u64) -> Self {
        Self {
            lower: count,
            upper: count,
        }
    }
    pub fn is_exact(self) -> bool {
        self.lower == self.upper
    }
    fn and(self, other: Self, total: u64) -> Self {
        Self {
            lower: self.lower.saturating_sub(total.saturating_sub(other.lower)),
            upper: self.upper.min(other.upper),
        }
    }
    fn or(self, other: Self, total: u64) -> Self {
        Self {
            lower: self.lower.max(other.lower),
            upper: self.upper.saturating_add(other.upper).min(total),
        }
    }
}

enum Node<'a> {
    All,
    None,
    ClassCompare {
        plan: &'a SummaryPlan,
        index: usize,
        op: Compare,
        value: ciborium::Value,
        categories: Option<Vec<bool>>,
    },
    ClassPresence {
        plan: &'a SummaryPlan,
        index: usize,
        kind: u8,
    },
    And(Vec<Self>),
    Or(Vec<Self>),
}
impl Node<'_> {
    fn count(&self, row: &PillView<'_>, total: u64) -> Result<CountBounds, Error> {
        Ok(match self {
            Self::All => CountBounds::exact(total),
            Self::None => CountBounds::exact(0),
            Self::ClassCompare {
                plan,
                index,
                op,
                value,
                categories,
            } => {
                if plan.is_null(row, *index)? {
                    CountBounds {
                        lower: 0,
                        upper: total,
                    }
                } else {
                    plan.comparison_with(row, *index, *op, value, categories.as_deref())?
                }
            }
            Self::ClassPresence { plan, index, kind } => {
                if plan.is_null(row, *index)? {
                    return Ok(CountBounds {
                        lower: 0,
                        upper: total,
                    });
                }
                let (count, nulls) = plan.counts(row, *index)?;
                let present = count
                    .checked_add(nulls)
                    .filter(|n| *n <= total)
                    .ok_or_else(|| error("summary field counts exceed total"))?;
                CountBounds::exact(match kind {
                    0 => nulls,
                    1 => total - nulls,
                    2 => total - present,
                    _ => present,
                })
            }
            Self::And(nodes) => {
                let mut count = CountBounds::exact(total);
                for node in nodes {
                    count = count.and(node.count(row, total)?, total);
                    if count.upper == 0 {
                        break;
                    }
                }
                count
            }
            Self::Or(nodes) => {
                let mut count = CountBounds::exact(0);
                for node in nodes {
                    count = count.or(node.count(row, total)?, total);
                    if count.lower == total {
                        break;
                    }
                }
                count
            }
        })
    }
}

/// Compiled once per query/summary shape. Execution reads borrowed summary
/// fields through the same Rust accessor and comparison code as event queries.
pub struct SummaryPreview<'a> {
    node: Node<'a>,
    fields: Vec<String>,
    pending: Vec<String>,
    complete: bool,
}
impl SummaryPreview<'_> {
    pub fn window(
        &self,
        requested: std::ops::Range<u64>,
    ) -> Result<SummaryWindowPreview<'_, '_>, Error> {
        Ok(SummaryWindowPreview {
            preview: self,
            window: SummaryWindow::new(requested)?,
            periods: Vec::new(),
        })
    }
    pub fn fields(&self) -> &[String] {
        &self.fields
    }
    pub fn pending(&self) -> &[String] {
        &self.pending
    }
    pub fn complete(&self) -> bool {
        self.complete
    }
    pub fn count(&self, row: &PillView<'_>) -> Result<CountBounds, Error> {
        let total = uint(row, 0)?;
        if total == 0 {
            return Ok(CountBounds::exact(0));
        }
        self.node.count(row, total)
    }
}

/// One source/publication's preview. Callers bind Packs to their registered
/// Definition and calendar before observing rows. No event payloads are read.
pub struct SummaryWindowPreview<'a, 'p> {
    preview: &'a SummaryPreview<'p>,
    window: SummaryWindow,
    periods: Vec<SummaryPeriod>,
}
#[derive(Clone, Debug)]
pub struct SummaryPeriod {
    pub time: u64,
    pub count: CountBounds,
    pub complete: bool,
}
#[derive(Clone, Debug)]
pub struct SummaryWindowResult {
    pub window: SummaryWindow,
    pub periods: Vec<SummaryPeriod>,
    /// Counts describe whole overlapping buckets. None means coverage is
    /// incomplete; a missing bucket must never be interpreted as zero.
    pub count: Option<CountBounds>,
    pub coverage_complete: bool,
    pub predicate_complete: bool,
    /// Event access retains the exact requested timestamps, narrowed only by
    /// complete summary periods whose predicate upper bound is zero.
    pub eligible: Vec<std::ops::Range<u64>>,
    pub fields: Vec<String>,
    pub pending: Vec<String>,
}
impl SummaryWindowResult {
    /// Combine disjoint portions of one source's window across Definition
    /// revisions. Missing periods and unknown predicates remain conservative.
    pub fn combine(
        window: SummaryWindow,
        results: impl IntoIterator<Item = Self>,
    ) -> Result<Self, Error> {
        let mut periods = Vec::new();
        let mut fields = Vec::new();
        let mut pending = Vec::new();
        let mut predicate_complete = true;
        for result in results {
            if result.window.requested() != window.requested() {
                return Err(error("summary preview windows differ"));
            }
            if result.periods.is_empty() {
                continue;
            }
            predicate_complete &= result.predicate_complete;
            for name in result.fields {
                if !fields.contains(&name) {
                    fields.push(name);
                }
            }
            for name in result.pending {
                if !pending.contains(&name) {
                    pending.push(name);
                }
            }
            periods.extend(result.periods);
        }
        // Daily readers already emit ordered runs. Sorting these contiguous
        // rows is linear for that usual case and also accepts out-of-order
        // arrivals, without allocating a tree node for every period.
        periods.sort_unstable_by_key(|p| p.time);
        if periods.windows(2).any(|pair| pair[0].time == pair[1].time) {
            return Err(error("duplicate summary period for one source"));
        }
        let coverage_complete =
            periods.len() as u64 == window.period_count() && periods.iter().all(|p| p.complete);
        let mut selection = SummarySelection::new(window.requested())?;
        let mut count = CountBounds::exact(0);
        for period in &periods {
            selection.observe(
                period.time
                    ..period
                        .time
                        .checked_add(SUMMARY_PERIOD_MS)
                        .ok_or(Error::Overflow)?,
                period.count,
                period.complete,
            )?;
            count.lower = count
                .lower
                .checked_add(period.count.lower)
                .ok_or(Error::Overflow)?;
            count.upper = count
                .upper
                .checked_add(period.count.upper)
                .ok_or(Error::Overflow)?;
        }
        Ok(Self {
            window,
            periods,
            count: coverage_complete.then_some(count),
            coverage_complete,
            predicate_complete,
            eligible: selection.finish(),
            fields,
            pending,
        })
    }
}
impl SummaryWindowPreview<'_, '_> {
    pub fn observe(&mut self, time: u64, row: &PillView<'_>, complete: bool) -> Result<(), Error> {
        if time % SUMMARY_PERIOD_MS != 0 {
            return Err(error("summary period must start at a five-minute boundary"));
        }
        let covered = self.window.covered();
        if time < covered.start || time >= covered.end {
            return Ok(());
        }
        let position = if self.periods.last().is_none_or(|p| p.time < time) {
            self.periods.len()
        } else {
            match self.periods.binary_search_by_key(&time, |p| p.time) {
                Ok(_) => return Err(error("duplicate summary period for one source")),
                Err(position) => position,
            }
        };
        let count = self.preview.count(row)?;
        self.periods.insert(
            position,
            SummaryPeriod {
                time,
                count,
                complete,
            },
        );
        Ok(())
    }
    pub fn finish(self) -> Result<SummaryWindowResult, Error> {
        let coverage_complete = self.periods.len() as u64 == self.window.period_count()
            && self.periods.iter().all(|p| p.complete);
        let mut selection = SummarySelection::new(self.window.requested())?;
        let mut count = CountBounds::exact(0);
        for p in &self.periods {
            selection.observe(
                p.time
                    ..p.time
                        .checked_add(SUMMARY_PERIOD_MS)
                        .ok_or(Error::Overflow)?,
                p.count,
                p.complete,
            )?;
            count.lower = count
                .lower
                .checked_add(p.count.lower)
                .ok_or(Error::Overflow)?;
            count.upper = count
                .upper
                .checked_add(p.count.upper)
                .ok_or(Error::Overflow)?;
        }
        Ok(SummaryWindowResult {
            window: self.window,
            periods: self.periods,
            count: coverage_complete.then_some(count),
            coverage_complete,
            predicate_complete: self.preview.complete,
            eligible: selection.finish(),
            fields: self.preview.fields.clone(),
            pending: self.preview.pending.clone(),
        })
    }
}
impl SummaryPlan {
    pub fn preview(&self, predicate: &Predicate) -> SummaryPreview<'_> {
        self.preview_with(predicate, &crate::cast::Context::default()).unwrap_or_else(|e| SummaryPreview {
            node: Node::All, fields: Vec::new(), pending: vec![e.to_string()], complete: false,
        })
    }
    pub fn preview_with(&self, predicate: &Predicate, context: &crate::cast::Context) -> Result<SummaryPreview<'_>,Error> {
        Ok(preview(self, self.source().prepare(predicate,context)?.as_ref()))
    }
}
fn preview<'a>(plan: &'a SummaryPlan, predicate: &Predicate) -> SummaryPreview<'a> {
    let mut parts = Vec::new();
    conjuncts(predicate, &mut parts);
    let mut nodes = Vec::new();
    let mut fields = Vec::new();
    let mut pending = Vec::new();
    let mut complete = true;
    // A comparison against a value already counts only non-null, present
    // observations. Intersecting its SQL guards again can loosen an exact count.
    let compared: std::collections::BTreeSet<_> = parts.iter().filter_map(|part| match part {
        Predicate::Compare {field,value,..} if !matches!(value,ciborium::Value::Null) && plan.field_index(field).is_some() => Some(field.as_str()),
        _ => None,
    }).collect();
    for part in parts {
        let guarded = match part {
            Predicate::IsPresent(f) => compared.contains(f.as_str()),
            Predicate::Not(p) => matches!(p.as_ref(),Predicate::IsNull(f) if compared.contains(f.as_str())),
            _ => false,
        };
        if guarded {continue;}
        if complete {
            if let Some(node) = compile(plan, part, false) {
                nodes.push(node);
                names(part, &mut fields);
                continue;
            }
            complete = false;
        }
        names(part, &mut pending);
    }
    SummaryPreview {
        node: Node::And(nodes),
        fields,
        pending,
        complete,
    }
}
fn inverse(op: Compare) -> Compare {
    match op {
        Compare::Eq => Compare::Ne,
        Compare::Ne => Compare::Eq,
        Compare::Gt => Compare::Le,
        Compare::Ge => Compare::Lt,
        Compare::Lt => Compare::Ge,
        Compare::Le => Compare::Gt,
    }
}
fn compile<'a>(plan: &'a SummaryPlan, p: &Predicate, negative: bool) -> Option<Node<'a>> {
    Some(match p {
        Predicate::All => {
            if negative {
                Node::None
            } else {
                Node::All
            }
        }
        Predicate::Compare { field, op, value } => {
            let index = plan.field_index(field)?;
            let op = if negative { inverse(*op) } else { *op };
            Node::ClassCompare { plan, index, op,
                categories: plan.categories(index, op, value), value: value.clone() }
        }
        Predicate::IsNull(field) => Node::ClassPresence {
            plan, index: plan.field_index(field)?, kind: if negative { 1 } else { 0 },
        },
        Predicate::IsAbsent(field) => Node::ClassPresence {
            plan, index: plan.field_index(field)?, kind: if negative { 3 } else { 2 },
        },
        Predicate::IsPresent(field) => Node::ClassPresence {
            plan, index: plan.field_index(field)?, kind: if negative { 2 } else { 3 },
        },
        Predicate::And(parts) | Predicate::Or(parts) => {
            let nodes = parts
                .iter()
                .map(|p| compile(plan, p, negative))
                .collect::<Option<_>>()?;
            if matches!(p, Predicate::And(_)) != negative {
                Node::And(nodes)
            } else {
                Node::Or(nodes)
            }
        }
        // Push negation into comparisons/presence tests at planning time.
        // This preserves SQL unknown rather than complementing candidate counts.
        Predicate::Not(p) => compile(plan, p, !negative)?,
        Predicate::Cast{..}|Predicate::CastNull{..}|Predicate::Contains { .. } => return None,
    })
}
fn conjuncts<'a>(p: &'a Predicate, out: &mut Vec<&'a Predicate>) {
    if let Predicate::And(parts) = p {
        for part in parts {
            conjuncts(part, out);
        }
    } else {
        out.push(p);
    }
}
fn names(p: &Predicate, out: &mut Vec<String>) {
    match p {
        Predicate::Cast{field,..}|Predicate::CastNull{field,..}|Predicate::Compare { field, .. }
        | Predicate::Contains { field, .. }
        | Predicate::IsNull(field)
        | Predicate::IsAbsent(field)
        | Predicate::IsPresent(field) => {
            if !out.contains(field) {
                out.push(field.clone());
            }
        }
        Predicate::And(parts) | Predicate::Or(parts) => {
            for part in parts {
                names(part, out);
            }
        }
        Predicate::Not(p) => names(p, out),
        Predicate::All => {}
    }
}
fn uint(row: &PillView<'_>, at: usize) -> Result<u64, Error> {
    match row.native_value(at)? {
        NativeValue::Scalar(Value::U64(n)) => Ok(n),
        _ => Err(error("summary count must be uint(8)")),
    }
}
