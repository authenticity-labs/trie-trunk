//! Registered, already-encoded PIL ingress. Transport and file I/O remain host
//! adapters; the queue retains encoded Pack bytes, never record builders.
use super::{DEFINITION_HEADER, Packs};
use crate::{Error, HourIndexBuilder, IndexFields, SeedPack, TIME_HOUR_MS, error};
use pil_codec::api::Fill;
use std::collections::{BTreeMap, HashMap};
#[cfg(any(unix, windows))]
pub mod files;
pub mod publication;
pub mod recovery;
mod strings;
mod factoring;
pub use factoring::StatePaths;
mod summaries;
pub use summaries::{SUMMARY_FILE, SummaryCommit, SummaryStart, SummaryWrite};

/// One scheduler decision, used by every binding. Hosts execute the returned
/// publications; they do not calculate hour boundaries or straggler deadlines.
#[derive(Debug, serde::Serialize)]
pub struct CommitPlan {
    pub head: u64,
    pub through: u64,
    pub hours: Vec<u64>,
}

/// Owned form of the existing ingress lane for native process adapters. The
/// stable scope and borrowing writer have one lifetime and one drop order.
/// No adapter needs to extend a codec lifetime itself.
pub struct OwnedIngress {
    live: Ingress<'static>,
    scope: Box<Packs>,
    branch: u16,
    summaries: Option<super::SummaryIngress>,
    strings: strings::Strings,
    text_fields: HashMap<u16, Vec<usize>>,
    failure: Option<String>,
}
impl OwnedIngress {
    pub fn new(
        scope: Packs,
        branch: u16,
        limits: IngressLimits,
        through: u64,
    ) -> Result<Self, Error> {
        let scope = Box::new(scope);
        // SAFETY: the private Box never moves its referent or exposes a mutable
        // reference. `live` drops first. No borrowing writer can escape this
        // owner; all public operations return owned values or scoped borrows.
        let stable = unsafe { &*(scope.as_ref() as *const Packs) };
        let live = stable.ingress(branch, limits, through)?;
        let text_fields = stable.definitions.iter().filter_map(|(&id, binding)| {
            let rx = binding.definition.prescription();
            let fields: Vec<_> = (0..rx.definition().fields().len())
                .filter(|&i| rx.native_scalar_kind(i) == Some(pil_codec::Scalar::String)).collect();
            (!fields.is_empty()).then_some((id, fields))
        }).collect();
        Ok(Self {
            strings: strings::Strings::new(limits.string_candidates),
            text_fields,
            live,
            scope,
            branch,
            summaries: None,
            failure: None,
        })
    }
    #[cfg(feature="compression")]
    pub fn publication(&mut self, hour:u64, now:u64, time:&[u8], footer:&[u8],
        length:u64, compression:Option<crate::Compression>) -> Result<publication::Publication,Error> {
        self.publish_using(hour,now,time,footer,length,compression,None)
    }
    #[cfg(feature="compression")]
    fn publish_using(&mut self, hour:u64, now:u64, time:&[u8], footer:&[u8],
        length:u64, compression:Option<crate::Compression>, state:Option<&mut factoring::Forest>) -> Result<publication::Publication,Error> {
        self.healthy()?;
        let mut writer = crate::packs::Writer::resume(footer.to_vec(),length)?;
        if let Some(c) = compression { writer.compression(c)?; }
        let beds = writer.next_beds_address();
        let candidates = self.strings.candidates(hour);
        let ((),index,count) = self.live.seal_using(hour,now,state,|pack| writer.selected(pack,&candidates))?;
        publication::Publication::finish(writer,hour,count,index,time,beds)
    }
    pub fn scope(&self) -> &Packs {
        &self.scope
    }
    pub fn retained_input_bytes(&self) -> usize {
        self.live.retained_input_bytes()
    }
    pub fn hours(&self) -> impl Iterator<Item = u64> + '_ {
        self.live.hours()
    }
    pub fn count(&self, hour: u64) -> usize {
        self.live.count(hour)
    }
    pub fn through(&self) -> u64 {
        self.live.through()
    }
    pub fn inspect(&self, branch: u16, bytes: &[u8], now: u64) -> Result<u64, Error> {
        self.healthy()?;
        self.live.inspect(branch, bytes, now)
    }
    /// Validate before writing, make recovery durable, then expose the event to
    /// queries and accumulators. Every ingress adapter uses this boundary.
    pub fn persist(
        &mut self,
        branch: u16,
        bytes: &[u8],
        now: u64,
        durable: impl FnOnce(&[u8], &[u8]) -> Result<(), Error>,
    ) -> Result<u64, Error> {
        self.healthy()?;
        let coordinates = self.live.coordinates(branch, bytes, Some(now))?;
        let prefix = recovery::prefix(branch, bytes)?;
        let result = durable(&prefix, bytes).and_then(|()| {
            let time = self.live.append(branch, bytes, coordinates)?;
            self.observe(branch, bytes, time)?;
            Ok(time)
        });
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }
        result
    }
    pub fn load(&mut self, branch: u16, bytes: &[u8], now: u64) -> Result<u64, Error> {
        self.healthy()?;
        let time = self.live.load(branch, bytes, now)?;
        self.observe(branch, bytes, time)?;
        Ok(time)
    }
    pub fn recover(&mut self, branch: u16, bytes: &[u8]) -> Result<Option<u64>, Error> {
        self.healthy()?;
        let time = self.live.recover(branch, bytes)?;
        if let Some(time) = time {
            self.observe(branch, bytes, time)?;
        }
        Ok(time)
    }
    pub fn healthy(&self) -> Result<(), Error> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |message| Err(error(message.clone())))
    }
    pub fn branch(&self) -> u16 {
        self.branch
    }
    pub fn commits(&self, now: u64) -> Result<CommitPlan, Error> {
        self.healthy()?;
        let through =
            now.saturating_sub(self.live.limits.straggler_ms) / TIME_HOUR_MS * TIME_HOUR_MS;
        Ok(CommitPlan {
            head: now / TIME_HOUR_MS * TIME_HOUR_MS,
            through,
            hours: self
                .live
                .hours()
                .take_while(|hour| *hour < through)
                .collect(),
        })
    }
    /// Empty-hour publication cannot move beyond a retained, unpublished hour.
    pub fn completion(
        &self,
        time: &crate::TimeTrie,
        now: u64,
    ) -> Result<Option<crate::TimeTrie>, Error> {
        let mut through = self.commits(now)?.through;
        if let Some(first) = self.hours().next() {
            through = through.min(first);
        }
        if through <= time.header().sealed {
            return Ok(None);
        }
        Ok(Some(time.publish(through, None)?))
    }
    fn observe(&mut self, branch: u16, bytes: &[u8], time: u64) -> Result<(), Error> {
        let Some(summaries) = &mut self.summaries else {
            return Ok(());
        };
        let result = (|| {
            let definition = self.scope.definition(branch)?;
            let mut decoder = definition.prescription().decoder();
            summaries.observe(branch, time, &decoder.read(bytes)?)
        })();
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }
        result
    }
    pub fn snapshot(&self, hour: u64) -> Result<Vec<u8>, Error> {
        self.healthy()?;
        self.live.snapshot(hour)
    }
    pub fn rows(
        &self,
        hour: u64,
        from: u64,
        to: u64,
    ) -> Result<Vec<std::ops::Range<usize>>, Error> {
        self.healthy()?;
        self.live.rows(hour, from, to)
    }
    pub fn seal(&mut self, hour: u64, now: u64) -> Result<SealedHour, Error> {
        self.healthy()?;
        self.live.seal(hour, now)
    }
    pub fn published(&mut self, hour: u64) -> Result<(), Error> {
        self.healthy()?;
        let state = self.live.take_published(hour)?;
        if self.live.limits.string_candidates != 0 && !self.text_fields.is_empty() {
            self.strings.advance(hour);
            let mut reader = state.pack.reader();
            for (i, &(_, branch, _)) in state.rows.iter().enumerate() {
                let Some(fields) = self.text_fields.get(&branch) else { continue };
                // Statistics cannot invalidate a durably committed hour.
                if let Ok(row) = reader.get(i) {
                    for &field in fields {
                        if let Ok(pil_codec::api::NativeValue::Scalar(pil_codec::Value::String(s))) = row.native_value(field) {
                            self.strings.observe(hour, s);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub fn advance(&mut self, through: u64) -> Result<(), Error> {
        self.healthy()?;
        self.live.advance(through)
    }
}

#[derive(Clone, Copy)]
pub struct IngressLimits {
    pub bytes: usize,
    pub pill_bytes: usize,
    pub open_hours: usize,
    pub string_candidates: usize,
    pub straggler_ms: u64,
}
impl Default for IngressLimits {
    fn default() -> Self {
        Self {
            bytes: 64 << 20,
            pill_bytes: 1 << 20,
            open_hours: 2,
            string_candidates: 1024,
            straggler_ms: 900_000,
        }
    }
}

struct Hour<'a> {
    pack: SeedPack<'a>,
    rows: Vec<(u64, u16, u16)>,
    bytes: usize,
    prepared: bool,
}

/// One resource/type lane. The registered scope permits its retained versions;
/// the PAC version directory handles deployment and rollback transitions.
pub struct Ingress<'a> {
    scope: &'a Packs,
    limits: IngressLimits,
    hours: BTreeMap<u64, Hour<'a>>,
    bytes: usize,
    family: u16,
    through: u64,
    fields: HashMap<u16, IndexFields>,
}

pub struct SealedHour {
    pub hour: u64,
    pub count: usize,
    pub pack: Vec<u8>,
    pub index: Vec<u8>,
}

impl Packs {
    pub fn ingress(
        &self,
        branch: u16,
        limits: IngressLimits,
        through: u64,
    ) -> Result<Ingress<'_>, Error> {
        let binding = self
            .definitions
            .get(&branch)
            .ok_or_else(|| error("unregistered ingress branch"))?;
        if limits.bytes == 0
            || limits.pill_bytes == 0
            || limits.pill_bytes > limits.bytes
            || limits.open_hours == 0
            || through % TIME_HOUR_MS != 0
        {
            return Err(error("invalid ingress limits or watermark"));
        }
        let fields = self
            .definitions
            .iter()
            .filter(|(_, b)| b.family == binding.family)
            .map(|(&id, b)| Ok((id, IndexFields::from_definition(&b.definition)?)))
            .collect::<Result<_, Error>>()?;
        Ok(Ingress {
            scope: self,
            limits,
            hours: BTreeMap::new(),
            bytes: 0,
            family: binding.family,
            through,
            fields,
        })
    }
}

impl<'a> Ingress<'a> {
    pub fn retained_input_bytes(&self) -> usize {
        self.bytes
    }
    pub fn hours(&self) -> impl Iterator<Item = u64> + '_ {
        self.hours.keys().copied()
    }
    pub fn count(&self, hour: u64) -> usize {
        self.hours.get(&hour).map_or(0, |h| h.rows.len())
    }

    /// Inspect storage coordinates before admission. Other annotated
    /// validators are not run implicitly. A POST body is one bounded full PIL.
    pub fn inspect(&self, branch: u16, bytes: &[u8], now: u64) -> Result<u64, Error> {
        self.coordinates(branch, bytes, Some(now))
            .map(|(time, _)| time)
    }

    fn coordinates(
        &self,
        branch: u16,
        bytes: &[u8],
        now: Option<u64>,
    ) -> Result<(u64, u16), Error> {
        if bytes.is_empty() || bytes.len() > self.limits.pill_bytes {
            return Err(error("ingress Pill size limit exceeded"));
        }
        let binding = self
            .scope
            .definitions
            .get(&branch)
            .ok_or_else(|| error("unregistered ingress branch"))?;
        if binding.family != self.family {
            return Err(error("ingress branch belongs to another resource type"));
        }
        let definition = &binding.definition;
        let fields = self.fields[&branch];
        let mut decoder = definition.prescription().decoder();
        let row = decoder.read(bytes)?;
        let time = fields.read_time(&row)?;
        let leaf = fields.read_leaf(&row)?;
        let hour = time / TIME_HOUR_MS * TIME_HOUR_MS;
        if hour < self.through
            || now.is_some_and(|now| {
                time > now
                    || now
                        >= hour
                            .saturating_add(TIME_HOUR_MS)
                            .saturating_add(self.limits.straggler_ms)
            })
        {
            return Err(error("event outside the open ingress window"));
        }
        if !self.hours.contains_key(&hour) && self.hours.len() >= self.limits.open_hours {
            return Err(error("ingress open-hour limit exceeded"));
        }
        // This is an input admission limit, not a measurement of allocator
        // capacity. The node must also budget Pack capacity and snapshots.
        if bytes
            .len()
            .checked_add(16)
            .and_then(|n| self.bytes.checked_add(n))
            .is_none_or(|n| n > self.limits.bytes)
        {
            return Err(error("ingress input budget exhausted"));
        }
        if self.hours.get(&hour).is_some_and(|h| h.prepared) {
            return Err(error("hour is awaiting publication"));
        }
        Ok((time, leaf))
    }

    pub fn load(&mut self, branch: u16, bytes: &[u8], now: u64) -> Result<u64, Error> {
        let coordinates = self.coordinates(branch, bytes, Some(now))?;
        self.append(branch, bytes, coordinates)
    }

    /// Replay durable ingress without applying today's straggler deadline to
    /// yesterday's acknowledged input. Published hours are not replayed twice.
    pub fn recover(&mut self, branch: u16, bytes: &[u8]) -> Result<Option<u64>, Error> {
        let definition = self.scope.definition(branch)?;
        let fields = self
            .fields
            .get(&branch)
            .ok_or_else(|| error("recovery branch belongs to another resource type"))?;
        let mut decoder = definition.prescription().decoder();
        let time = fields.read_time(&decoder.read(bytes)?)?;
        if time / TIME_HOUR_MS * TIME_HOUR_MS < self.through {
            return Ok(None);
        }
        let coordinates = self.coordinates(branch, bytes, None)?;
        self.append(branch, bytes, coordinates).map(Some)
    }

    pub fn through(&self) -> u64 {
        self.through
    }

    /// Advance complete-empty hours only after their Time publication is durable.
    pub fn advance(&mut self, through: u64) -> Result<(), Error> {
        if through < self.through
            || through % TIME_HOUR_MS != 0
            || self.hours.keys().next().is_some_and(|&h| h < through)
        {
            return Err(error("cannot advance past unpublished ingress"));
        }
        self.through = through;
        Ok(())
    }

    fn append(
        &mut self,
        branch: u16,
        bytes: &[u8],
        (time, leaf): (u64, u16),
    ) -> Result<u64, Error> {
        let hour = time / TIME_HOUR_MS * TIME_HOUR_MS;
        let definition = self.scope.definition(branch)?;
        let pill = definition.prescription().bind(Fill::Encoded(bytes))?;
        if !self.hours.contains_key(&hour) {
            let mut pack = self.scope.fill(branch, 0)?;
            pack.hour(hour)?;
            pack.header(crate::integrity::HEADER, vec![0; 4])?;
            let mut rows = Vec::new();
            rows.try_reserve(1).map_err(|_| Error::Overflow)?;
            pack.load(pill)?;
            rows.push((time, branch, leaf));
            let charged = bytes.len() + std::mem::size_of::<(u64, u16, u16)>();
            self.hours.insert(
                hour,
                Hour {
                    pack,
                    rows,
                    bytes: charged,
                    prepared: false,
                },
            );
            self.bytes += charged;
            return Ok(time);
        }
        let state = self.hours.get_mut(&hour).unwrap();
        state.rows.try_reserve(1).map_err(|_| Error::Overflow)?;
        state.pack.load(pill)?;
        state.rows.push((time, branch, leaf));
        state.bytes += bytes.len() + 16;
        self.bytes += bytes.len() + 16;
        Ok(time)
    }

    /// Unsealed snapshots are for resident queries/recovery, never Box insertion.
    pub fn snapshot(&self, hour: u64) -> Result<Vec<u8>, Error> {
        self.hours
            .get(&hour)
            .ok_or_else(|| error("unknown open hour"))?
            .pack
            .snapshot()
    }

    /// Exact live-window selection from the ingress coordinates already held
    /// alongside the open Pack. No extra Pill decode or payload scan is needed.
    pub fn rows(
        &self,
        hour: u64,
        from: u64,
        to: u64,
    ) -> Result<Vec<std::ops::Range<usize>>, Error> {
        let state = self
            .hours
            .get(&hour)
            .ok_or_else(|| error("unknown open hour"))?;
        let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
        for (ordinal, (time, _, _)) in state.rows.iter().enumerate() {
            if *time >= from && *time < to {
                if let Some(last) = ranges.last_mut().filter(|r| r.end == ordinal) {
                    last.end += 1;
                } else {
                    ranges.push(ordinal..ordinal + 1);
                }
            }
        }
        Ok(ranges)
    }

    /// Prepare without discarding the live hour; publication must be acknowledged
    /// separately. Stable time sorting is Seed work, outside the PIL/PAC codec.
    pub fn seal(&mut self, hour: u64, now: u64) -> Result<SealedHour, Error> {
        let (pack,index,count) = self.seal_using(hour, now, None, |mut pack| {
            pack.seal()?;
            let mut bytes = pack.into_bytes()?;
            crate::integrity::stamp(&mut bytes)?;
            Ok(bytes)
        })?;
        Ok(SealedHour { hour, count, index, pack })
    }
    fn seal_using<T>(&mut self, hour:u64, now:u64, mut forest:Option<&mut factoring::Forest>,
        finish: impl FnOnce(SeedPack<'_>) -> Result<T,Error>) -> Result<(T,Vec<u8>,usize),Error> {
        if now
            < hour
                .saturating_add(TIME_HOUR_MS)
                .saturating_add(self.limits.straggler_ms)
        {
            return Err(error("hour still accepts stragglers"));
        }
        let state = self
            .hours
            .get(&hour)
            .ok_or_else(|| error("unknown open hour"))?;
        if state.rows.is_empty() {
            return Err(error("cannot seal an empty ingress hour"));
        }
        let mut order: Vec<usize> = (0..state.rows.len()).collect();
        order.sort_by_key(|&i| state.rows[i].0);
        let branch = state.rows[order[0]].1;
        let mut pack = self.scope.fill(branch, state.rows.len())?;
        pack.hour(hour)?;
        pack.header(DEFINITION_HEADER, branch.to_le_bytes().to_vec())?;
        pack.header(crate::integrity::HEADER, vec![0; 4])?;
        let mut index = HourIndexBuilder::new(
            hour,
            state.rows.len(),
            IndexFields::from_definition(self.scope.definition(branch)?)?,
        )?;
        for i in order {
            let (time, branch, supplied) = state.rows[i];
            let pill = state.pack.get(i)?;
            let leaf = if let Some(forest) = forest.as_deref_mut() {
                forest.store(branch, pill, &mut pack, supplied)?
            } else {
                pack.load(pill)?;
                supplied
            };
            index.append(time, leaf)?;
        }
        let output = finish(pack)?;
        let index = index.finish().bytes().to_vec();
        let count = state.rows.len();
        self.hours.get_mut(&hour).unwrap().prepared = true;
        Ok((output,index,count))
    }

    pub fn published(&mut self, hour: u64) -> Result<(), Error> {
        self.take_published(hour).map(drop)
    }
    fn take_published(&mut self, hour: u64) -> Result<Hour<'a>, Error> {
        if self.hours.keys().next().copied() != Some(hour) {
            return Err(error("publish ingress hours in order"));
        }
        if !self.hours[&hour].prepared {
            return Err(error("hour has not been prepared for publication"));
        }
        let state = self
            .hours
            .remove(&hour)
            .ok_or_else(|| error("unknown open hour"))?;
        self.bytes -= state.bytes;
        self.through = hour + TIME_HOUR_MS;
        Ok(state)
    }
}
