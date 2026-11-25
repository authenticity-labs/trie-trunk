//! Read the existing YY/DDD/HH directory through the same native/WASM API.
//! Addresses stay in the time trie, independent of Definition layout or version.
use crate::{Error, error};
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::{ops::Range, sync::Arc};

pub const TIME_HEADER_BYTES: usize = 64;
pub const TIME_HOUR_MS: u64 = 3_600_000;
const YEAR_HOURS: usize = 366 * 24;
const REPLICA_FILES: u8 = 31;
const REPLICA_INDEX: u8 = 2;
const REPLICA_LEAVES: u8 = 4;
const REPLICA_BEDS: u8 = 16;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "registration", derive(serde::Serialize))]
pub struct TimeHeader {
    pub first: u64,
    pub origin: u64,
    pub tail: u64,
    pub sealed: u64,
    pub generation: [u8; 16],
    pub summary_offset: i8,
    pub widths: [u8; 6],
    #[cfg_attr(feature = "registration", serde(skip))]
    base_year: i32,
    #[cfg_attr(feature = "registration", serde(skip))]
    stride: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "registration", derive(serde::Serialize))]
pub struct TimeLeaf {
    pub hour: u64,
    pub start: u64,
    pub end: u64,
    pub packs: u64,
    pub beds: u64,
    pub count: u64,
}

/// Retains a published snapshot. Header planning happens once per snapshot;
/// selecting an hour reads exactly its leaf, including complete-empty hours.
pub struct TimeTrie {
    bytes: Arc<Vec<u8>>,
    header: TimeHeader,
    received: Option<Vec<Range<u64>>>,
}

/// A bounded traversal of a published timeline. It borrows the existing
/// snapshot and allocates no second calendar or directory.
pub struct TimeRange<'a> {
    trie: &'a TimeTrie,
    next: u64,
    end: u64,
}

impl Iterator for TimeRange<'_> {
    type Item = Result<TimeLeaf, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.end { return None; }
        let hour = self.next;
        self.next = self.next.saturating_add(TIME_HOUR_MS);
        Some(self.trie.leaf(hour).and_then(|leaf|
            leaf.ok_or_else(|| error("missing published hour"))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end.saturating_sub(self.next).div_ceil(TIME_HOUR_MS);
        match usize::try_from(remaining) {
            Ok(n) => (n, Some(n)),
            Err(_) => (usize::MAX, None),
        }
    }
}

fn date(ms: u64) -> Result<DateTime<Utc>, Error> {
    let ms = i64::try_from(ms).map_err(|_| error("time outside calendar range"))?;
    DateTime::from_timestamp_millis(ms).ok_or_else(|| error("time outside calendar range"))
}

fn uint(bytes: &[u8]) -> u64 {
    let mut value = [0; 8];
    value[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(value)
}

impl TimeTrie {
    /// Start an origin timeline using the existing YY/DDD/HH layout. Nothing
    /// beyond `first` is advertised as complete until publication advances it.
    pub fn create(first: u64, generation: [u8; 16], summary_offset: i8) -> Result<Self, Error> {
        crate::summaries::SummaryCalendar::new(summary_offset)?;
        if first % TIME_HOUR_MS != 0 { return Err(error("invalid initial time boundary")); }
        let mut raw = vec![0; TIME_HEADER_BYTES + YEAR_HOURS * 16];
        raw[..8].copy_from_slice(b"TTTIME00");
        raw[8..16].copy_from_slice(&first.to_le_bytes());
        raw[16..24].copy_from_slice(&first.to_le_bytes());
        raw[32..40].copy_from_slice(&first.to_le_bytes());
        raw[40..46].copy_from_slice(&[4,5,5,2,2,2]);
        raw[47]=summary_offset as u8;
        raw[48..64].copy_from_slice(&generation);
        Self::open(Arc::new(raw))
    }

    /// Prepare the next immutable publication; I/O and the commit barrier
    /// remain with the storage owner. Existing readers keep their snapshot.
    pub fn publish(&self, sealed: u64, member: Option<TimeLeaf>) -> Result<Self, Error> {
        if self.received.is_some() || sealed < self.header.sealed || sealed % TIME_HOUR_MS != 0 {
            return Err(error("invalid origin time publication"));
        }
        let mut tail=self.header.tail;
        let mut widths=self.header.widths;
        if let Some(leaf)=member {
            if leaf.hour < self.header.sealed || leaf.hour >= sealed || leaf.hour % TIME_HOUR_MS != 0
                || leaf.start != tail || leaf.count == 0
                || leaf.start.checked_add(1).and_then(|n|n.checked_add(leaf.count)) != Some(leaf.end) {
                return Err(error("invalid hourly publication"));
            }
            tail=leaf.end;
            for (i,n) in [(1,leaf.packs),(2,leaf.beds),(3,leaf.count),(5,leaf.count)] {
                widths[i]=widths[i].max((64-n.leading_zeros()).div_ceil(8) as u8);
            }
        }
        widths[0]=widths[0].max((64-tail.leading_zeros()).div_ceil(8) as u8);
        let span=sealed.saturating_sub(1).saturating_sub(self.header.origin)/TIME_HOUR_MS;
        widths[4]=widths[4].max((64-span.leading_zeros()).div_ceil(8) as u8);
        let last=date(self.header.first.max(sealed.saturating_sub(1)))?.year();
        if last>9999 { return Err(error("unsupported time trie calendar year")); }
        let stride=widths[..4].iter().map(|&n|n as usize).sum::<usize>();
        let length=TIME_HEADER_BYTES+(last-self.header.base_year+1) as usize*YEAR_HOURS*stride;
        let mut raw=Vec::new();raw.try_reserve_exact(length).map_err(|_|Error::Overflow)?;
        if widths[..4]==self.header.widths[..4] {raw.extend_from_slice(self.bytes());raw.resize(length,0);}
        else {raw.resize(length,0);raw[..TIME_HEADER_BYTES].copy_from_slice(&self.bytes[..TIME_HEADER_BYTES]);}
        raw[24..32].copy_from_slice(&tail.to_le_bytes());raw[32..40].copy_from_slice(&sealed.to_le_bytes());
        raw[40..46].copy_from_slice(&widths);
        let mut write=|leaf:TimeLeaf|->Result<(),Error>{
            let day=date(leaf.hour)?;
            let ordinal=(day.year()-self.header.base_year) as usize*YEAR_HOURS+day.ordinal0() as usize*24+day.hour() as usize;
            let mut at=TIME_HEADER_BYTES+ordinal*stride;
            for (n,width) in [leaf.start,leaf.packs,leaf.beds,leaf.count].into_iter().zip(widths) {
                let end=at+width as usize;raw[at..end].copy_from_slice(&n.to_le_bytes()[..width as usize]);at=end;
            }
            Ok(())
        };
        if widths[..4]!=self.header.widths[..4] && self.header.first<self.header.sealed {
            for leaf in self.range(self.header.first,self.header.sealed)? {write(leaf?)?;}
        }
        let mut next=self.header.tail;
        for hour in (self.header.sealed..sealed).step_by(TIME_HOUR_MS as usize) {
            let leaf=match member.filter(|l|l.hour==hour) {
                Some(leaf)=>{next=leaf.end;leaf},
                None=>TimeLeaf{hour,start:next,end:next,packs:0,beds:0,count:0},
            };
            write(leaf)?;
        }
        Self::open(Arc::new(raw))
    }

    pub fn open(bytes: Arc<Vec<u8>>) -> Result<Self, Error> {
        Self::open_window(bytes, None)
    }

    /// Read the existing sparse client Time representation. Its one completion
    /// byte per leaf is local state; received ranges distinguish holes from
    /// complete-empty hours. Neither addresses nor file format are changed.
    pub fn open_replica(bytes: Arc<Vec<u8>>, received: Vec<Range<u64>>) -> Result<Self, Error> {
        Self::open_window(bytes, Some(received))
    }

    fn open_window(bytes: Arc<Vec<u8>>, received: Option<Vec<Range<u64>>>) -> Result<Self, Error> {
        if bytes.len() < TIME_HEADER_BYTES || &bytes[..8] != b"TTTIME00" {
            return Err(error("invalid time trie header"));
        }
        let widths: [u8; 6] = bytes[40..46].try_into().unwrap();
        for (width, minimum) in widths.iter().zip([4, 5, 5, 2, 2, 2]) {
            if *width < minimum || *width > 8 {
                return Err(error("invalid time trie width"));
            }
        }
        let first = uint(&bytes[8..16]);
        let origin = uint(&bytes[16..24]);
        let sealed = uint(&bytes[32..40]);
        let summary_offset = bytes[47] as i8;
        if bytes[46] != u8::from(received.is_some()) || !(-12..=14).contains(&summary_offset)
            || origin > first || first > sealed
            || [first, origin, sealed].iter().any(|n| n % TIME_HOUR_MS != 0)
        {
            return Err(error("invalid time trie bounds"));
        }
        let base_year = date(first)?.year();
        let last_year = date(first.max(sealed.saturating_sub(1)))?.year();
        if base_year < 1 || last_year > 9999 {
            return Err(error("unsupported time trie calendar year"));
        }
        if let Some(ranges) = &received {
            let mut end = first;
            for range in ranges {
                if range.start < end || range.start >= range.end || range.end > sealed
                    || range.start % TIME_HOUR_MS != 0 || range.end % TIME_HOUR_MS != 0 {
                    return Err(error("invalid sparse time range"));
                }
                end = range.end;
            }
        }
        let stride = widths[..4].iter().map(|n| usize::from(*n)).sum::<usize>() + usize::from(received.is_some());
        let expected = TIME_HEADER_BYTES + (last_year - base_year + 1) as usize * YEAR_HOURS * stride;
        if bytes.len() != expected {
            return Err(error("invalid time trie extent"));
        }
        let header = TimeHeader {
            first, origin, sealed, tail: uint(&bytes[24..32]),
            generation: bytes[48..64].try_into().unwrap(), summary_offset,
            widths, base_year, stride,
        };
        Ok(Self { bytes, header, received })
    }

    pub fn header(&self) -> &TimeHeader { &self.header }
    pub fn summary_calendar(&self) -> Result<crate::summaries::SummaryCalendar, Error> {
        crate::summaries::SummaryCalendar::new(self.header.summary_offset)
    }
    pub fn bytes(&self) -> &[u8] { &self.bytes }

    /// Prepare one replica completion byte. Callers stage these patches with
    /// their file transaction; constructing a patch never publishes coverage.
    pub(crate) fn collected(&self, expected: TimeLeaf) -> Result<(usize, u8), Error> {
        let (start, bytes) = self.collected_span(expected)?;
        Ok((start + bytes.len() - 1, Self::collected_flags(bytes[bytes.len() - 1])?))
    }
    pub(crate) fn collected_span(&self, expected: TimeLeaf) -> Result<(usize, &[u8]), Error> {
        if self.received.is_none() || expected.hour % TIME_HOUR_MS != 0
            || expected.count == 0 || self.leaf(expected.hour)? != Some(expected) {
            return Err(error("Seed Pack and committed Time/index disagree"));
        }
        let start = self.position(expected.hour)?;
        Ok((start, &self.bytes[start..start + self.header.stride]))
    }
    pub(crate) fn collected_flags(flags: u8) -> Result<u8, Error> {
        // Existing sparse Time bits: summaries, indexes, leaves, inverted, beds.
        let required = REPLICA_INDEX | REPLICA_LEAVES;
        if flags & !REPLICA_FILES != 0 || flags & required != required {
            return Err(error("Seed Pack requires committed indexes and leaves"));
        }
        Ok(flags | REPLICA_BEDS)
    }

    /// Select hours intersecting [from, to), including published empty hours.
    /// Unknown hours outside the published extent are not reported as empty.
    /// Both native and browser hosts use this traversal; I/O stays host-owned.
    pub fn range(&self, from: u64, to: u64) -> Result<TimeRange<'_>, Error> {
        if from >= to { return Err(error("invalid timeline selection")); }
        Ok(TimeRange {
            trie: self,
            next: (from / TIME_HOUR_MS * TIME_HOUR_MS).max(self.header.first),
            end: to.min(self.header.sealed),
        })
    }

    /// Occupied UTC hours in the retained origin. Row ordinals are cumulative,
    /// so leading and trailing empty hours can be skipped with two lower bounds.
    pub fn occupied(&self) -> Result<Option<Range<u64>>, Error> {
        if self.received.is_some() { return Err(error("occupied bounds require an origin timeline")); }
        let h = &self.header;
        let Some(first) = self.leaf(h.first)? else { return Ok(None) };
        if first.start == h.tail { return Ok(None); }
        let containing = |ordinal: u64| -> Result<u64, Error> {
            let mut lo = h.first / TIME_HOUR_MS;
            let mut hi = h.sealed / TIME_HOUR_MS;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let leaf = self.leaf(mid * TIME_HOUR_MS)?.ok_or_else(|| error("missing origin hour"))?;
                if leaf.end <= ordinal { lo = mid + 1; } else { hi = mid; }
            }
            Ok(lo * TIME_HOUR_MS)
        };
        Ok(Some(containing(first.start)?..containing(h.tail - 1)? + TIME_HOUR_MS))
    }

    fn position(&self, ms: u64) -> Result<usize, Error> {
        let at = date(ms)?;
        let year = usize::try_from(at.year() - self.header.base_year)
            .map_err(|_| error("hour precedes time trie"))?;
        let ordinal = year * YEAR_HOURS + at.ordinal0() as usize * 24 + at.hour() as usize;
        let start = TIME_HEADER_BYTES + ordinal * self.header.stride;
        if start.checked_add(self.header.stride).is_none_or(|end| end > self.bytes.len()) {
            return Err(error("hour exceeds time trie"));
        }
        Ok(start)
    }

    pub fn leaf(&self, ms: u64) -> Result<Option<TimeLeaf>, Error> {
        let hour = ms / TIME_HOUR_MS * TIME_HOUR_MS;
        if hour < self.header.first || hour >= self.header.sealed { return Ok(None); }
        if let Some(ranges) = &self.received {
            let index = ranges.partition_point(|range| range.end <= hour);
            if ranges.get(index).is_none_or(|range| range.start > hour) { return Ok(None); }
        }
        let mut at = self.position(hour)?;
        let mut values = [0; 4];
        for (value, width) in values.iter_mut().zip(self.header.widths) {
            let end = at + usize::from(width);
            *value = uint(&self.bytes[at..end]);
            at = end;
        }
        let [start, packs, beds, count] = values;
        let end = if count == 0 { start } else {
            start.checked_add(1).and_then(|n| n.checked_add(count))
                .ok_or_else(|| error("time trie row extent overflow"))?
        };
        if end > self.header.tail || count > 0 && hour < self.header.origin {
            return Err(error("invalid time trie row extent"));
        }
        Ok(Some(TimeLeaf { hour, start, end, packs, beds, count }))
    }

    /// Existing timeline response ranges. Leap-year padding is omitted from
    /// each selected window. Returned ranges borrow the published file bytes.
    pub fn spans(&self, from: u64, to: u64) -> Result<Vec<Range<usize>>, Error> {
        if self.received.is_some() { return Err(error("native response spans require an origin Time trie")); }
        if from >= to || from % TIME_HOUR_MS != 0 || to % TIME_HOUR_MS != 0 {
            return Err(error("timeline range must select complete UTC hours"));
        }
        let mut from = from.max(self.header.first);
        let to = to.min(self.header.sealed);
        let mut spans = Vec::new();
        while from < to {
            let at = date(from)?;
            let hours = u64::from((if at.date_naive().leap_year() { 366 } else { 365 }) - at.ordinal0()) * 24
                - u64::from(at.hour());
            let end = to.min(from + hours * TIME_HOUR_MS);
            let start = self.position(from)?;
            let length = usize::try_from((end - from) / TIME_HOUR_MS)
                .map_err(|_| error("timeline span overflow"))? * self.header.stride;
            spans.push(start..start + length);
            from = end;
        }
        Ok(spans)
    }
}
