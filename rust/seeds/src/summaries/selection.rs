use super::CountBounds;
use crate::{Error, error};
use std::ops::Range;

/// A source-specific query window. Missing or partial summaries leave periods
/// eligible; only complete zero proofs remove them. Publication/Definition
/// binding must be checked by the summary reader before supplying evidence.
pub struct SummarySelection {
    window: Range<u64>,
    excluded: Vec<Range<u64>>,
}
impl SummarySelection {
    pub fn new(window: Range<u64>) -> Result<Self, Error> {
        if window.start >= window.end {
            return Err(error("summary window must increase"));
        }
        Ok(Self {
            window,
            excluded: Vec::new(),
        })
    }
    pub fn observe(
        &mut self,
        period: Range<u64>,
        count: CountBounds,
        complete: bool,
    ) -> Result<(), Error> {
        if period.start >= period.end || count.lower > count.upper {
            return Err(error("invalid summary period evidence"));
        }
        if !complete || count.upper != 0 {
            return Ok(());
        }
        let period = period.start.max(self.window.start)..period.end.min(self.window.end);
        if period.start < period.end {
            self.excluded.push(period);
        }
        Ok(())
    }
    /// Coalesce once after all source summaries have been evaluated. The
    /// original period resolution is retained; histogram display bins do not
    /// determine which event rows may be read.
    pub fn finish(mut self) -> Vec<Range<u64>> {
        self.excluded.sort_unstable_by_key(|r| r.start);
        let mut from = self.window.start;
        let mut eligible = Vec::new();
        for period in self.excluded {
            if period.start > from {
                eligible.push(from..period.start);
            }
            from = from.max(period.end);
        }
        if from < self.window.end {
            eligible.push(from..self.window.end);
        }
        eligible
    }
    /// Hand the remaining periods to optional query-pruning stages. This
    /// shares the same selection consumed by the indexed reader, not a
    /// separate Bloom result set or a UI-owned candidate list.
    pub fn candidates<B: Eq>(self, binding: B) -> Result<crate::packs::CandidatePeriods<B>, Error> {
        crate::packs::CandidatePeriods::new(binding, self.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_partial_and_candidate_periods_remain_eligible() {
        let mut plan = SummarySelection::new(0..1_800_000).unwrap();
        plan.observe(0..300_000, CountBounds::exact(0), true)
            .unwrap();
        plan.observe(300_000..600_000, CountBounds::exact(0), false)
            .unwrap();
        plan.observe(
            900_000..1_200_000,
            CountBounds {
                lower: 0,
                upper: 12,
            },
            true,
        )
        .unwrap();
        plan.observe(1_200_000..1_500_000, CountBounds::exact(0), true)
            .unwrap();
        assert_eq!(
            plan.finish(),
            vec![300_000..1_200_000, 1_500_000..1_800_000]
        );
    }
    #[test]
    fn only_full_zero_coverage_eliminates_search() {
        let mut plan = SummarySelection::new(60_000..840_000).unwrap();
        for period in [
            600_000..900_000,
            0..300_000,
            300_000..600_000,
            300_000..600_000,
        ] {
            plan.observe(period, CountBounds::exact(0), true).unwrap();
        }
        assert!(plan.finish().is_empty());
        assert_eq!(
            SummarySelection::new(0..300_000).unwrap().finish(),
            vec![0..300_000]
        );
    }
    #[test]
    fn later_stages_only_probe_summary_survivors() {
        use crate::packs::{PruningDecision, PruningEvidence};
        let binding = ("source", "publication", "query");
        let mut summary = SummarySelection::new(0..900_000).unwrap();
        summary
            .observe(0..300_000, CountBounds::exact(0), true)
            .unwrap();
        let mut remaining = summary.candidates(binding).unwrap();
        assert_eq!(remaining.periods(), &[300_000..900_000]);
        remaining
            .refine([PruningEvidence {
                binding: &binding,
                period: 600_000..900_000,
                complete: true,
                decision: PruningDecision::NoMatch,
            }])
            .unwrap();
        assert_eq!(remaining.into_ranges(), vec![300_000..600_000]);
    }
}
