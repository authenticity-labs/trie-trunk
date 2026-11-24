//! Query-local exclusion stages. Providers supply proofs, not result rows.
//! This module has no Bloom encoding, hashing, tokenization or storage policy.
use crate::{Error, error};
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruningDecision {
    /// No record in the covered period can satisfy the planned predicate.
    NoMatch,
    /// Includes probabilistic positives; exact evaluation is still required.
    Possible,
    /// Missing filter, unsupported predicate, or otherwise unusable evidence.
    Unavailable,
}

/// The binding is the provider's query/publication identity, not a new wire
/// identifier. It must include source, publication, Definition/field meaning,
/// and predicate semantics (including text normalization when applicable).
/// An incomplete index cannot supply a negative proof for the whole period.
pub struct PruningEvidence<'a, B> {
    pub binding: &'a B,
    pub period: Range<u64>,
    pub complete: bool,
    pub decision: PruningDecision,
}

/// Monotonically shrinking periods shared by summary and future filter stages.
/// Each stage can remove candidates, never add matches or change statistics.
pub struct CandidatePeriods<B> {
    binding: B,
    periods: Vec<Range<u64>>,
}
impl<B: Eq> CandidatePeriods<B> {
    pub fn new(binding: B, mut periods: Vec<Range<u64>>) -> Result<Self, Error> {
        if periods.iter().any(|r| r.start >= r.end) {
            return Err(error("candidate periods require increasing bounds"));
        }
        coalesce(&mut periods);
        Ok(Self { binding, periods })
    }
    pub fn periods(&self) -> &[Range<u64>] {
        &self.periods
    }
    pub fn is_empty(&self) -> bool {
        self.periods.is_empty()
    }
    pub fn into_ranges(self) -> Vec<Range<u64>> {
        self.periods
    }

    /// Consume only current, complete negative evidence. An absent stage or a
    /// stage with no usable negatives allocates no replacement period vector.
    /// The caller probes only these remaining periods, after summary pruning.
    pub fn refine<'a>(
        &mut self,
        evidence: impl IntoIterator<Item = PruningEvidence<'a, B>>,
    ) -> Result<(), Error>
    where
        B: 'a,
    {
        if self.periods.is_empty() {
            return Ok(());
        }
        let mut excluded = Vec::new();
        for proof in evidence {
            if proof.binding != &self.binding
                || !proof.complete
                || proof.decision != PruningDecision::NoMatch
            {
                continue;
            }
            if proof.period.start >= proof.period.end {
                return Err(error("invalid pruning period"));
            }
            excluded.push(proof.period);
        }
        if excluded.is_empty() {
            return Ok(());
        }
        coalesce(&mut excluded);
        let mut kept = Vec::new();
        let mut first = 0;
        for period in &self.periods {
            let mut from = period.start;
            while first < excluded.len() && excluded[first].end <= from {
                first += 1;
            }
            for removed in &excluded[first..] {
                if removed.start >= period.end {
                    break;
                }
                if removed.start > from {
                    kept.push(from..removed.start);
                }
                from = from.max(removed.end).min(period.end);
                if from == period.end {
                    break;
                }
            }
            if from < period.end {
                kept.push(from..period.end);
            }
        }
        self.periods = kept;
        Ok(())
    }
}

fn coalesce(periods: &mut Vec<Range<u64>>) {
    periods.sort_unstable_by_key(|r| r.start);
    let mut length = 0;
    for read in 0..periods.len() {
        if length > 0 && periods[read].start <= periods[length - 1].end {
            periods[length - 1].end = periods[length - 1].end.max(periods[read].end);
        } else {
            if read != length {
                periods[length] = periods[read].clone();
            }
            length += 1;
        }
    }
    periods.truncate(length);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn optional_stage_retains_possible_missing_stale_and_partial_evidence() {
        let binding = ("source", "publication", "definition", "predicate");
        let mut selected = CandidatePeriods::new(binding, vec![0..300, 600..900]).unwrap();
        let original = selected.periods().as_ptr();
        for (bound, complete, decision) in [
            (binding, true, PruningDecision::Possible),
            (binding, true, PruningDecision::Unavailable),
            (
                ("other", binding.1, binding.2, binding.3),
                true,
                PruningDecision::NoMatch,
            ),
            (
                (binding.0, "old", binding.2, binding.3),
                true,
                PruningDecision::NoMatch,
            ),
            (
                (binding.0, binding.1, "other", binding.3),
                true,
                PruningDecision::NoMatch,
            ),
            (
                (binding.0, binding.1, binding.2, "other"),
                true,
                PruningDecision::NoMatch,
            ),
            (binding, false, PruningDecision::NoMatch),
        ] {
            selected
                .refine([PruningEvidence {
                    binding: &bound,
                    period: 0..900,
                    complete,
                    decision,
                }])
                .unwrap();
        }
        assert_eq!(selected.periods(), &[0..300, 600..900]);
        assert_eq!(
            selected.periods().as_ptr(),
            original,
            "no replacement allocation without usable negatives"
        );
    }
    #[test]
    fn stages_only_remove_candidates_and_do_not_probe_an_empty_plan() {
        let binding = 7;
        let mut selected = CandidatePeriods::new(binding, vec![0..300, 600..900]).unwrap();
        selected
            .refine(
                [200..800, 100..250]
                    .into_iter()
                    .map(|period| PruningEvidence {
                        binding: &binding,
                        period,
                        complete: true,
                        decision: PruningDecision::NoMatch,
                    }),
            )
            .unwrap();
        assert_eq!(selected.periods(), &[0..100, 800..900]);
        selected
            .refine([PruningEvidence {
                binding: &binding,
                period: 0..1000,
                complete: true,
                decision: PruningDecision::NoMatch,
            }])
            .unwrap();
        assert!(selected.is_empty());
        selected
            .refine(std::iter::from_fn(
                || -> Option<PruningEvidence<'_, i32>> {
                    panic!("empty selections must not probe filters")
                },
            ))
            .unwrap();
    }
    #[test]
    fn invalid_proofs_leave_candidates_unchanged() {
        let mut selected = CandidatePeriods::new(1, vec![0..100]).unwrap();
        let binding = 1;
        assert!(
            selected
                .refine([0..10, 20..20].into_iter().map(|period| PruningEvidence {
                    binding: &binding,
                    period,
                    complete: true,
                    decision: PruningDecision::NoMatch,
                }))
                .is_err()
        );
        assert_eq!(selected.periods(), &[0..100]);
    }
}
