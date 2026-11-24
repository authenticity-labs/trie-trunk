//! Per-origin frequency evidence. Published ordinals live in the BOX; the shortlist lives in RAM.
use crate::TIME_HOUR_MS;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

const PERIOD: u64 = 30 * 24 * TIME_HOUR_MS;
const MAX_TEXT_BYTES: usize = 4096;

struct Count {
    hits: u64,
    first_hour: u64,
    recurring: bool,
}

pub(super) struct Strings {
    limit: usize,
    start: Option<u64>,
    counts: HashMap<Box<str>, Count>,
    selected: HashSet<Box<str>>,
}

impl Strings {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            start: None,
            counts: HashMap::new(),
            selected: HashSet::new(),
        }
    }

    // Preparing a seal is repeatable. Only acknowledgment advances the observation window.
    pub(super) fn candidates(&self, hour: u64) -> Cow<'_, HashSet<Box<str>>> {
        let periods = hour.saturating_sub(self.start.unwrap_or(hour)) / PERIOD;
        match periods {
            0 => Cow::Borrowed(&self.selected),
            1 => {
                let mut ranked: Vec<_> = self.counts.iter().collect();
                ranked
                    .sort_unstable_by(|(a, x), (b, y)| y.hits.cmp(&x.hits).then_with(|| a.cmp(b)));
                let half = ranked.len().div_ceil(2);
                Cow::Owned(
                    ranked
                        .into_iter()
                        .take(half)
                        .filter(|(_, c)| c.recurring && c.hits >= 3)
                        .map(|(s, _)| s.clone())
                        .collect(),
                )
            }
            _ => Cow::Owned(HashSet::new()),
        }
    }

    pub(super) fn advance(&mut self, hour: u64) {
        let start = *self.start.get_or_insert(hour);
        let periods = hour.saturating_sub(start) / PERIOD;
        if periods > 0 {
            self.selected = self.candidates(hour).into_owned();
            self.counts.clear();
            self.start = Some(start + periods * PERIOD);
        }
    }

    // Misra–Gries. A full-table miss cancels one vote per resident string.
    // Counts are lower bounds and one O(limit) sweep costs limit prior votes.
    pub(super) fn observe(&mut self, hour: u64, text: &str) {
        if self.limit == 0 || !(3..=MAX_TEXT_BYTES).contains(&text.len()) {
            return;
        }
        if let Some(count) = self.counts.get_mut(text) {
            count.hits = count.hits.saturating_add(1);
            count.recurring |= count.first_hour != hour;
        } else if self.counts.len() < self.limit {
            self.counts.insert(
                text.into(),
                Count {
                    hits: 1,
                    first_hour: hour,
                    recurring: false,
                },
            );
        } else {
            self.counts.retain(|_, c| {
                c.hits -= 1;
                c.hits != 0
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_the_previous_periods_top_half_can_enter_review() {
        let mut s = Strings::new(4);
        for hour in [0, TIME_HOUR_MS] {
            assert!(s.candidates(hour).is_empty());
            s.advance(hour);
            for (text, n) in [("first", 8), ("second", 6), ("third", 4), ("fourth", 2)] {
                for _ in 0..n {
                    s.observe(hour, text);
                }
            }
        }
        assert!(s.candidates(PERIOD - TIME_HOUR_MS).is_empty());
        assert_eq!(
            s.candidates(PERIOD).as_ref(),
            &HashSet::from(["first".into(), "second".into()])
        );
        assert!(s.candidates(PERIOD - TIME_HOUR_MS).is_empty());
        s.advance(PERIOD);
        assert_eq!(s.candidates(PERIOD).len(), 2);
        assert!(s.candidates(3 * PERIOD).is_empty());
    }
    #[test]
    fn churn_and_a_single_hour_spike_do_not_create_candidates() {
        let mut s = Strings::new(4);
        s.advance(0);
        for n in 0..1000 {
            s.observe(0, &format!("rare-{n}"));
            assert!(s.counts.len() <= 4);
        }
        for _ in 0..100 {
            s.observe(0, "spike");
        }
        assert!(s.candidates(PERIOD).is_empty());
    }
}
