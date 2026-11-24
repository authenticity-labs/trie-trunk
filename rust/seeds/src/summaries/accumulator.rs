//! Bounded live summary state. The commit scheduler supplies the completion
//! watermark; wall-clock time alone never makes a period complete.
use super::{SUMMARY_PERIOD_MS, SummaryBucket, SummaryPack, SummaryPlan, SummaryWindow};
use crate::{Error, Seed, error};
use pil_codec::api::PillView;

/// One pending fixed-size summary Pack. Completed periods reject additions,
/// while later periods continue to accept events, including stragglers.
/// Event retention, commit scheduling and file I/O remain caller-owned.
#[derive(Clone)]
pub struct SummaryAccumulator {
    plan: SummaryPlan,
    first: u64,
    end: u64,
    complete: usize,
    buckets: Vec<SummaryBucket>,
    sealed: bool,
}

impl SummaryPlan {
    /// Create a daily accumulator for this Definition using the node's
    /// published calendar. The calendar never delays Definition acceptance;
    /// concurrently active Definitions need separate accumulators.
    pub fn accumulator_day(&self, time: u64, calendar: super::SummaryCalendar) -> Result<SummaryAccumulator, Error> {
        if self.layout().rows() != super::SUMMARY_DAY_ROWS {
            return Err(error("daily summaries require 288 five-minute periods"));
        }
        self.accumulator(calendar.day(time)?.start)
    }

    pub fn accumulator(&self, first: u64) -> Result<SummaryAccumulator, Error> {
        if first % SUMMARY_PERIOD_MS != 0 {
            return Err(error("summary start must align to a five-minute period"));
        }
        let length = u64::try_from(self.layout().rows())
            .map_err(|_| Error::Overflow)?
            .checked_mul(SUMMARY_PERIOD_MS)
            .ok_or(Error::Overflow)?;
        let end = first.checked_add(length).ok_or(Error::Overflow)?;
        Ok(SummaryAccumulator {
            plan: self.clone(),
            first,
            end,
            complete: 0,
            sealed: false,
            buckets: (0..self.layout().rows()).map(|_| self.bucket()).collect(),
        })
    }
}

impl SummaryAccumulator {
    /// Exceptional commit for concurrently active source Definitions. Each
    /// source retains the ordinary accumulator; only this daily commit builds
    /// a union shape and combines matching period ordinals. A failed commit
    /// leaves every source available for inspection or retry.
    pub fn seal_mixed(
        sources: &mut [Self],
    ) -> Result<(super::MixedSummaryPlan, SummaryPack), Error> {
        Self::seal_mixed_with_definitions(sources, &mut super::SummaryDefinitions::new())
    }
    pub fn seal_mixed_with_definitions(
        sources: &mut [Self], definitions: &mut super::SummaryDefinitions,
    ) -> Result<(super::MixedSummaryPlan, SummaryPack), Error> {
        Self::seal_mixed_using(sources, definitions, |_, _| Ok(()))
    }
    pub(crate) fn seal_mixed_using(
        sources: &mut [Self], definitions: &mut super::SummaryDefinitions,
        prepare: impl FnOnce(&super::SummaryDefinitions, &mut super::SummaryPackBuilder<'_>) -> Result<(), Error>,
    ) -> Result<(super::MixedSummaryPlan, SummaryPack), Error> {
        let first = sources
            .first()
            .ok_or_else(|| error("missing summary sources"))?;
        if sources.iter().any(|source| {
            source.sealed
                || source.first != first.first
                || source.end != first.end
                || source.complete != source.buckets.len()
        }) {
            return Err(error(
                "mixed summary sources must be complete and cover the same periods",
            ));
        }
        let plans = sources
            .iter()
            .map(|source| source.plan.clone())
            .collect::<Vec<_>>();
        let plan = super::MixedSummaryPlan::with_definitions(&plans, definitions)?;
        let mut pack = plan.layout().fill()?;
        prepare(definitions, &mut pack)?;
        for ordinal in 0..plan.layout().rows() {
            let mut period = plan.bucket();
            for source in sources.iter() {
                plan.merge(&mut period, &source.plan, &source.buckets[ordinal])?;
            }
            plan.load(&mut pack, &period)?;
        }
        let sealed = pack.seal()?;
        drop(pack);
        for source in sources {
            source.sealed = true;
            source.buckets.clear();
            source.buckets.shrink_to_fit();
        }
        Ok((plan, sealed))
    }

    pub fn plan(&self) -> &SummaryPlan {
        &self.plan
    }
    pub fn first(&self) -> u64 {
        self.first
    }
    pub fn end(&self) -> u64 {
        self.end
    }
    pub fn complete_through(&self) -> u64 {
        self.first + self.complete as u64 * SUMMARY_PERIOD_MS
    }
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
    pub fn bucket(&self, ordinal: usize) -> Result<&SummaryBucket, Error> {
        self.buckets
            .get(ordinal)
            .ok_or_else(|| error("summary period outside accumulator"))
    }
    fn writable(&self, time: u64) -> Result<usize, Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        if time < self.first || time >= self.end {
            return Err(error("event outside summary calendar range"));
        }
        let ordinal = ((time - self.first) / SUMMARY_PERIOD_MS) as usize;
        if ordinal < self.complete {
            return Err(error("summary period is complete"));
        }
        Ok(ordinal)
    }
    /// Observe the already-decoded event, without retaining its bytes or view.
    pub fn observe(&mut self, time: u64, row: &PillView<'_>) -> Result<(), Error> {
        let ordinal = self.writable(time)?;
        self.plan.observe(&mut self.buckets[ordinal], row)
    }
    #[cfg(feature="registration")]
    pub(crate) fn observe_bound(&mut self, time:u64, row:&crate::storage::BoundRow<'_>)->Result<(),Error>{
        let ordinal=self.writable(time)?;
        self.plan.observe_values(&mut self.buckets[ordinal], |i|row.native_value(i))
    }
    /// The capped Seed is returned for the existing event Pack append path.
    /// No event builder or event payload is retained by the accumulator.
    pub fn ingest<'a, T>(
        &mut self,
        time: u64,
        event: T,
        transform: impl FnOnce(T, &mut Seed<'a>) -> Result<(), Error>,
    ) -> Result<Seed<'a>, Error> {
        let ordinal = self.writable(time)?;
        self.plan
            .ingest_owned(&mut self.buckets[ordinal], event, transform)
    }
    /// Called after the scheduler closes the straggler window and commits the
    /// corresponding events. A silent period becomes known-empty here.
    pub fn complete(&mut self, through: u64) -> Result<&mut Self, Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        if through < self.complete_through()
            || through > self.end
            || through % SUMMARY_PERIOD_MS != 0
        {
            return Err(error("invalid summary completion watermark"));
        }
        self.complete = ((through - self.first) / SUMMARY_PERIOD_MS) as usize;
        Ok(self)
    }
    /// Merge complete overlapping periods directly from accumulator state.
    /// Unknown/incomplete periods cannot be silently reported as zeros.
    pub fn rollup(&self, window: &SummaryWindow) -> Result<SummaryBucket, Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        let rows = window.rows(self.first, self.buckets.len())?;
        if rows.len() as u64 != window.period_count() || rows.end > self.complete {
            return Err(error("incomplete summary coverage for rollup"));
        }
        let mut result = self.plan.bucket();
        for row in rows {
            self.plan.merge(&mut result, &self.buckets[row])?;
        }
        Ok(result)
    }
    /// Recovery snapshot only; still unsealed even after all periods complete.
    /// The caller stores its event checkpoint and completion watermark with
    /// its existing recovery metadata, separate from the summary row schema.
    pub fn snapshot(&self) -> Result<Vec<u8>, Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        let mut pack = self.plan.layout().fill()?;
        for bucket in &self.buckets {
            pack.load(self.plan.seed(bucket)?)?;
        }
        pack.snapshot()
    }
    /// Restore a bounded recovery snapshot with the caller's checkpoint.
    /// Sealed daily Packs must be read through SummaryBoxReader instead.
    pub fn restore(
        plan: &SummaryPlan,
        first: u64,
        through: u64,
        bytes: &[u8],
    ) -> Result<Self, Error> {
        let mut result = plan.accumulator(first)?;
        let pack = plan.layout().open(bytes)?;
        if pack.is_sealed() { return Err(error("invalid open summary snapshot")); }
        let mut reader = pack.reader();
        for ordinal in 0..result.buckets.len() {
            let row = reader.get(ordinal)?;
            if row.mode() != crate::EncodingMode::FixedFull {
                return Err(error("summary rows must use Fixed Full encoding"));
            }
            result.buckets[ordinal] = plan.restore(&row)?;
        }
        result.complete(through)?;
        Ok(result)
    }
    /// Only a complete daily Pack can become a sealed Box child. Failure
    /// leaves the accumulator available for inspection or retry.
    pub fn seal(&mut self) -> Result<SummaryPack, Error> {
        self.seal_using(|_| Ok(()))
    }
    #[cfg(feature = "registration")]
    pub fn seal_registered(&mut self, trie: &trie_trunk_taxonomy::Trunk, branch: u16) -> Result<SummaryPack, Error> {
        // Selection comes from the owning branch; a daily PAC does not carry
        // a second reference to the shared summary class library.
        let branches = trie.storage_versions(branch).map_err(Error::Definition)?;
        let registry = trie.definitions().map_err(Error::Definition)?;
        let [definition, _] = self.plan.source_context()?;
        if !registry.resources().iter().any(|r| branches.contains(&r.branch) && r.definition == definition) {
            return Err(error("summary source is outside the selected branch"));
        }
        self.seal()
    }
    pub(crate) fn seal_using(&mut self, prepare: impl FnOnce(&mut super::SummaryPackBuilder<'_>) -> Result<(), Error>) -> Result<SummaryPack, Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        if self.complete != self.buckets.len() {
            return Err(error("incomplete summary Pack"));
        }
        let mut pack = self.plan.layout().fill()?;
        prepare(&mut pack)?;
        for bucket in &self.buckets {
            pack.load(self.plan.seed(bucket)?)?;
        }
        let sealed = pack.seal()?;
        self.sealed = true;
        // Persisted Packs own their bytes; no editable per-period state needs
        // to survive sealing. Existing readers hold their own snapshots.
        self.buckets.clear();
        self.buckets.shrink_to_fit();
        Ok(sealed)
    }
}
