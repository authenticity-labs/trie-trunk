//! Analytics over interval aggregates. Each interval contributes one sample;
//! event populations are used only to compute the interval's aggregate.
use super::{SUMMARY_PERIOD_MS, SummaryBoxReader, SummaryClass, SummaryPlan, SummaryWindow};
use crate::{Error, error};
use ciborium::Value as Data;
use std::{str::FromStr, sync::OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryAggregate {
    Count,
    NullCount,
    AbsentCount,
    Min,
    Max,
    Sum,
    Mean,
    TrueCount,
    FalseCount,
    TrueProportion,
    Distinct,
}
impl SummaryAggregate {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::NullCount => "null_count",
            Self::AbsentCount => "absent_count",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
            Self::Mean => "mean",
            Self::TrueCount => "true_count",
            Self::FalseCount => "false_count",
            Self::TrueProportion => "true_proportion",
            Self::Distinct => "distinct",
        }
    }
}
impl FromStr for SummaryAggregate {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Ok(match s {
            "count" => Self::Count,
            "null_count" => Self::NullCount,
            "absent_count" => Self::AbsentCount,
            "min" => Self::Min,
            "max" => Self::Max,
            "sum" => Self::Sum,
            "mean" => Self::Mean,
            "true_count" => Self::TrueCount,
            "false_count" => Self::FalseCount,
            "true_proportion" => Self::TrueProportion,
            "distinct" => Self::Distinct,
            _ => return Err(error("unsupported summary aggregate")),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryMeasure {
    pub field: Option<String>,
    pub aggregate: SummaryAggregate,
}
impl SummaryMeasure {
    pub fn new(field: impl Into<String>, aggregate: SummaryAggregate) -> Self {
        Self {
            field: Some(field.into()),
            aggregate,
        }
    }
    pub fn events() -> Self {
        Self {
            field: None,
            aggregate: SummaryAggregate::Count,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SummaryPoint {
    pub from: u64,
    pub to: u64,
    pub value: Option<Data>,
}
/// Exact nearest-rank percentiles of the calculated interval values. Empty
/// means/extrema and invalid sums are missing samples; counts include zeros.
/// Integer values never pass through f64 for ordering or selection.
pub struct SummarySeries {
    pub measure: SummaryMeasure,
    pub window: SummaryWindow,
    pub span: u64,
    pub count: u64,
    points: Vec<SummaryPoint>,
    samples: usize,
    ordered: OnceLock<Vec<Data>>,
}
impl SummarySeries {
    pub fn samples(&self) -> usize {
        self.samples
    }
    pub fn points(&self) -> &[SummaryPoint] {
        &self.points
    }
    pub fn perc(&self, n: f64) -> Result<Option<Data>, Error> {
        let rank = rank(n, self.samples() as u64)?;
        if rank == 0 {
            return Ok(None);
        }
        let values = self.ordered.get_or_init(|| {
            let mut values: Vec<_> = self.points.iter().filter_map(|p| p.value.clone()).collect();
            // Values originate from one checked numeric aggregate, so all
            // comparisons are defined, including signed/unsigned 128-bit data.
            values.sort_unstable_by(|a, b| {
                crate::value::compare_data(a, b)
                    .expect("summary aggregate is numeric")
                    .expect("summary aggregate is finite")
            });
            values
        });
        Ok(Some(values[(rank - 1) as usize].clone()))
    }
}

impl SummaryPlan {
    fn measure_index(&self, measure: &SummaryMeasure) -> Result<Option<usize>, Error> {
        use SummaryAggregate as A;
        let Some(name) = &measure.field else {
            return if measure.aggregate == A::Count {
                Ok(None)
            } else {
                Err(error("event series only supports count"))
            };
        };
        let (index, field) = self
            .fields()
            .enumerate()
            .find(|(_, f)| f.name() == name)
            .ok_or_else(|| error("field has no summary class"))?;
        let supported = match measure.aggregate {
            A::Count | A::NullCount | A::AbsentCount => true,
            A::Min | A::Max => matches!(
                field.class(),
                SummaryClass::Numeric | SummaryClass::Temporal
            ),
            A::Mean | A::Sum => field.class() == SummaryClass::Numeric,
            A::TrueCount | A::FalseCount | A::TrueProportion => {
                field.class() == SummaryClass::Boolean
            }
            A::Distinct => matches!(
                field.class(),
                SummaryClass::Categorical | SummaryClass::Ordinal
            ),
        };
        if !supported {
            return Err(error("aggregate is not supported by this summary class"));
        }
        Ok(Some(index))
    }
    /// Bind the requested aggregate series once, then feed selected summary
    /// rows from resident Packs, disk ranges or a Pack response stream.
    pub fn series<'a>(
        &'a self,
        window: &SummaryWindow,
        span: u64,
        measures: &[SummaryMeasure],
    ) -> Result<SummarySeriesBuilder<'a>, Error> {
        if span == 0 || span % SUMMARY_PERIOD_MS != 0 {
            return Err(error(
                "summary span must be a positive multiple of five minutes",
            ));
        }
        let indexes = measures
            .iter()
            .map(|m| self.measure_index(m))
            .collect::<Result<Vec<_>, _>>()?;
        let covered = window.covered();
        let result = measures
            .iter()
            .map(|measure| SummarySeries {
                measure: measure.clone(),
                window: window.clone(),
                span,
                count: 0,
                // Grow with observed intervals, not an untrusted requested range.
                points: Vec::new(),
                samples: 0,
                ordered: OnceLock::new(),
            })
            .collect();
        let counts_only = indexes.iter().all(Option::is_none);
        Ok(SummarySeriesBuilder {
            plan: self,
            window: window.clone(),
            span,
            indexes,
            result,
            bucket: self.bucket(),
            counts_only,
            interval_count: 0,
            begin: covered.start,
            end: covered.start.saturating_add(span).min(covered.end),
            next: covered.start,
            valid: true,
        })
    }
    pub fn series_pack(
        &self,
        pack: &pil_codec::api::Pack<'_>,
        first: u64,
        window: &SummaryWindow,
        span: u64,
        measures: &[SummaryMeasure],
    ) -> Result<Vec<SummarySeries>, Error> {
        let mut series = self.series(window, span, measures)?;
        series.pack(pack, first)?;
        series.finish()
    }
    pub fn series_box(
        &self,
        reader: &SummaryBoxReader<'_>,
        first: u64,
        window: &SummaryWindow,
        span: u64,
        measures: &[SummaryMeasure],
    ) -> Result<Vec<SummarySeries>, Error> {
        self.check_box(reader)?;
        let rows = self.layout().rows();
        let selected = window.rows(
            first,
            reader.len().checked_mul(rows).ok_or(Error::Overflow)?,
        )?;
        if selected.len() as u64 != window.period_count() {
            return Err(error("incomplete summary coverage for series"));
        }
        let mut series = self.series(window, span, measures)?;
        for p in selected.start / rows..selected.end.div_ceil(rows) {
            let at = first
                .checked_add(
                    (p as u64)
                        .checked_mul(rows as u64)
                        .and_then(|n| n.checked_mul(SUMMARY_PERIOD_MS))
                        .ok_or(Error::Overflow)?,
                )
                .ok_or(Error::Overflow)?;
            series.pack(&reader.pack(p)?, at)?;
        }
        series.finish()
    }
}

/// One scan across Packs, with one mutable interval state and one value per
/// completed interval/measure. No event payloads or whole-Box copies are held.
/// Intervals begin at the covered window start; the final interval is clipped
/// to its covered end and retains its actual timestamps.
pub struct SummarySeriesBuilder<'a> {
    plan: &'a SummaryPlan,
    window: SummaryWindow,
    span: u64,
    indexes: Vec<Option<usize>>,
    result: Vec<SummarySeries>,
    bucket: super::SummaryBucket,
    counts_only: bool,
    interval_count: u64,
    begin: u64,
    end: u64,
    next: u64,
    valid: bool,
}
impl SummarySeriesBuilder<'_> {
    pub fn pack(
        &mut self,
        pack: &pil_codec::api::Pack<'_>,
        first: u64,
    ) -> Result<&mut Self, Error> {
        if !pack.is_sealed() {
            return Err(error("summary series requires completed periods from sealed Packs"));
        }
        let rows = self.window.rows(first, pack.len())?;
        let mut cursor = pack.reader();
        for ordinal in rows {
            self.observe(
                first + ordinal as u64 * SUMMARY_PERIOD_MS,
                &cursor.get(ordinal)?,
            )?;
        }
        Ok(self)
    }
    pub fn observe(
        &mut self,
        time: u64,
        row: &pil_codec::api::PillView<'_>,
    ) -> Result<&mut Self, Error> {
        if !self.valid {
            return Err(error("summary series contains a failed observation"));
        }
        let covered = self.window.covered();
        if time < covered.start || time >= covered.end {
            return Ok(self);
        }
        if time != self.next {
            return Err(error("missing, duplicate or unordered summary period"));
        }
        self.valid = false;
        let count = if self.counts_only {
            let pil_codec::api::NativeValue::Scalar(pil_codec::Value::U64(count)) =
                row.native_value(0)?
            else {
                return Err(error("invalid summary event count"));
            };
            count
        } else {
            let state = self.plan.restore(row)?;
            self.plan.merge(&mut self.bucket, &state)?;
            state.count()
        };
        self.interval_count = self
            .interval_count
            .checked_add(count)
            .ok_or(Error::Overflow)?;
        self.next = time + SUMMARY_PERIOD_MS;
        if self.next == self.end {
            for (series, index) in self.result.iter_mut().zip(&self.indexes) {
                series.count = series
                    .count
                    .checked_add(self.interval_count)
                    .ok_or(Error::Overflow)?;
                let value = if index.is_none() {
                    Some(self.interval_count.into())
                } else {
                    self.plan
                        .aggregate_value(&self.bucket, *index, series.measure.aggregate)?
                };
                series.samples += usize::from(value.is_some());
                series.points.push(SummaryPoint {
                    from: self.begin,
                    to: self.end,
                    value,
                });
            }
            if !self.counts_only {
                self.bucket = self.plan.bucket();
            }
            self.interval_count = 0;
            self.begin = self.end;
            self.end = self.begin.saturating_add(self.span).min(covered.end);
        }
        self.valid = true;
        Ok(self)
    }
    pub fn finish(self) -> Result<Vec<SummarySeries>, Error> {
        if !self.valid || self.next != self.window.covered().end {
            return Err(error("incomplete summary coverage for series"));
        }
        Ok(self.result)
    }
}

// Compute nearest rank without rounding a large population through f64.
fn rank(percentile: f64, count: u64) -> Result<u64, Error> {
    if !percentile.is_finite() || !(0.0..=100.0).contains(&percentile) {
        return Err(error("percentile must be finite and from 0 to 100"));
    }
    if count == 0 {
        return Ok(0);
    }
    if percentile == 0.0 {
        return Ok(1);
    }
    if percentile == 100.0 {
        return Ok(count);
    }
    let bits = percentile.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = (bits & ((1u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1u64 << 52 };
    let shift = 1075 - exponent.max(1);
    if shift > 121 {
        return Ok(1);
    }
    let numerator = u128::from(mantissa) * u128::from(count);
    let denominator = 100u128 << shift;
    u64::try_from(numerator.div_ceil(denominator)).map_err(|_| Error::Overflow)
}
