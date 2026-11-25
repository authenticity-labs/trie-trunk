//! INFO.WRITE slotted UUID/revision pages. Only virtual pointers move on insertion.
use super::{Binding, MAX_REV, Posting, Uuid, invalid};
use std::io;

pub(super) const SIZE: usize = 4096;
pub(super) const UNITS: usize = 123;
const POINTERS: usize = 37;
// Slot zero reserves an ordinary .pods locator (hour and one-based record) for
// the UUID's current complete object. All zero means it has not been built.
const VIEW_SLOT: usize = 22;
const PARENT_REFS: usize = 32 - VIEW_SLOT;
#[derive(Clone)]
pub(super) struct Page(pub [u8; SIZE]);

#[derive(Clone, Copy)]
pub(super) struct Link {
    pub page: u32,
    pub slot: u8,
}

#[inline(always)]
pub(super) fn number(bytes: &[u8]) -> u32 {
    let mut raw = [0; 4];
    raw[..bytes.len()].copy_from_slice(bytes);
    u32::from_le_bytes(raw)
}

fn store(bytes: &mut [u8], value: u32) {
    bytes.copy_from_slice(&value.to_le_bytes()[..bytes.len()]);
}

pub(super) fn checksum(raw: &[u8], at: usize) -> u32 {
    let crc = crc32c::crc32c(&raw[..at]);
    let crc = crc32c::crc32c_append(crc, &[0; 4]);
    crc32c::crc32c_append(crc, &raw[at + 4..])
}

impl Page {
    pub fn new(source: Binding) -> Self {
        let mut p = Self([0; SIZE]);
        p.0[..4].copy_from_slice(b"OBJ0");
        p.0[4..20].copy_from_slice(&source.generation);
        store(&mut p.0[20..24], source.minor);
        p.0[29..32].copy_from_slice(&[1, 2, 3]);
        p.0[POINTERS..160].fill(128);
        p
    }
    pub fn used(&self) -> usize {
        self.0[28] as usize
    }
    fn start(&self, n: usize) -> bool {
        self.0[POINTERS + n] < 128
    }
    pub fn slot(&self, n: usize) -> u8 {
        self.0[POINTERS + n] & 127
    }
    pub(super) fn at(&self, n: usize) -> usize {
        self.slot(n) as usize * 32
    }
    pub fn key(&self, n: usize) -> Uuid {
        self.0[self.at(n)..self.at(n) + 16].try_into().unwrap()
    }
    pub fn end(&self, n: usize) -> usize {
        (n + 1..self.used())
            .find(|&i| self.start(i))
            .unwrap_or(self.used())
    }
    pub fn groups(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.used()).filter(|&n| self.start(n))
    }
    pub fn base(&self, n: usize) -> u32 {
        let a = self.at(n);
        number(&self.0[a + 16..a + 19])
    }
    fn head(&self, n: usize) -> usize {
        let a = self.at(n);
        number(&self.0[a + 19..a + 21]) as usize
    }
    fn width(&self) -> usize {
        usize::from(self.0[30] + self.0[31])
    }
    fn parents(&self) -> usize {
        PARENT_REFS / self.width() - 1
    }
    fn per_unit(&self) -> usize {
        32 / self.width()
    }
    fn raw_capacity(&self, n: usize) -> usize {
        self.parents() + (self.end(n) - n - 1) * self.per_unit()
    }
    fn linked(&self, n: usize) -> bool {
        self.0[self.at(n) + 21] & 1 != 0
    }
    fn capacity(&self, n: usize) -> usize {
        self.raw_capacity(n) - usize::from(self.linked(n))
    }
    pub fn max_span(&self) -> usize {
        self.parents() + (UNITS - 1) * self.per_unit()
    }
    fn units(&self, span: usize) -> usize {
        1 + span
            .saturating_sub(self.parents())
            .div_ceil(self.per_unit())
    }
    pub(super) fn ref_at(&self, n: usize, pos: usize) -> usize {
        if pos < self.parents() {
            return self.at(n) + VIEW_SLOT + (1 + pos) * self.width();
        }
        let offset = pos - self.parents();
        self.at(n + 1 + offset / self.per_unit()) + (offset % self.per_unit()) * self.width()
    }
    fn value(&self, n: usize, pos: usize) -> u64 {
        let at = self.ref_at(n, pos);
        let mid = at + self.0[30] as usize;
        (u64::from(number(&self.0[at..mid])) << 32)
            | u64::from(number(&self.0[mid..at + self.width()]))
    }
    fn put_ref(&mut self, n: usize, pos: usize, addr: u64) {
        let at = self.ref_at(n, pos);
        let mid = at + self.0[30] as usize;
        let end = at + self.width();
        store(&mut self.0[at..mid], (addr >> 32) as u32);
        store(&mut self.0[mid..end], addr as u32);
    }
    pub fn link(&self, n: usize) -> Option<Link> {
        self.linked(n).then(|| {
            let at = self.ref_at(n, self.raw_capacity(n) - 1);
            Link {
                page: number(&self.0[at..at + 4]),
                slot: self.0[at + 4],
            }
        })
    }
    pub fn set_link(&mut self, n: usize, link: Link) {
        let at = self.ref_at(n, self.raw_capacity(n) - 1);
        let end = at + self.width();
        self.0[at..end].fill(0);
        store(&mut self.0[at..at + 4], link.page);
        self.0[at + 4] = link.slot;
        let flag = self.at(n) + 21;
        self.0[flag] = 1;
    }
    pub fn find(&self, id: &Uuid) -> Option<usize> {
        let (mut lo, mut hi) = (0, self.used());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let n = (lo..=mid).rev().find(|&i| self.start(i))?;
            match self.key(n).cmp(id) {
                std::cmp::Ordering::Equal => return Some(n),
                std::cmp::Ordering::Less => lo = self.end(n),
                std::cmp::Ordering::Greater => hi = n,
            }
        }
        None
    }
    pub fn reference(&self, n: usize, rev: u32) -> Option<u64> {
        let delta = rev.checked_sub(self.base(n))? as usize;
        let cap = self.capacity(n);
        if delta >= cap {
            return None;
        }
        let addr = self.value(n, (self.head(n) + delta) % cap);
        (addr != 0).then_some(addr)
    }
    pub fn entries(&self, n: usize) -> impl DoubleEndedIterator<Item = (u32, u64)> + '_ {
        let cap = self.capacity(n);
        let base = self.base(n);
        let head = self.head(n);
        (0..cap.min((MAX_REV - base + 1) as usize)).filter_map(move |d| {
            let value = self.value(n, (head + d) % cap);
            (value != 0).then_some((base + d as u32, value))
        })
    }
    // Decode consecutive locators from each physical unit together. The two runs
    // below restore logical revision order when a group's head has rotated.
    pub fn visit_entries(&self, n: usize, mut visit: impl FnMut(u32, u64)) {
        #[inline(always)]
        fn component<const N: usize>(bytes: &[u8]) -> u32 {
            match N {
                2 => u32::from(u16::from_le_bytes([bytes[0], bytes[1]])),
                3 => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]),
                4 => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                _ => unreachable!(),
            }
        }
        fn scan<const H: usize, const R: usize>(
            p: &Page,
            n: usize,
            mut visit: impl FnMut(u32, u64),
        ) {
            let width = H + R;
            let parents = PARENT_REFS / width - 1;
            let per = 32 / width;
            let cap = parents + (p.end(n) - n - 1) * per - usize::from(p.linked(n));
            let head = p.head(n);
            let mut rev = p.base(n);
            for (mut pos, end) in [(head, cap), (0, head)] {
                while pos < end {
                    let (at, available) = if pos < parents {
                        (p.at(n) + VIEW_SLOT + (1 + pos) * width, parents - pos)
                    } else {
                        let delta = pos - parents;
                        let within = delta % per;
                        (p.at(n + 1 + delta / per) + within * width, per - within)
                    };
                    let count = available.min(end - pos);
                    for bytes in p.0[at..at + count * width].chunks_exact(width) {
                        let addr = (u64::from(component::<H>(bytes)) << 32)
                            | u64::from(component::<R>(&bytes[H..]));
                        if addr != 0 {
                            visit(rev, addr);
                        }
                        rev += 1;
                    }
                    pos += count;
                }
            }
        }
        match (self.0[30], self.0[31]) {
            (2, 3) => scan::<2, 3>(self, n, &mut visit),
            (2, 4) => scan::<2, 4>(self, n, &mut visit),
            (3, 3) => scan::<3, 3>(self, n, &mut visit),
            (3, 4) => scan::<3, 4>(self, n, &mut visit),
            (4, 3) => scan::<4, 3>(self, n, &mut visit),
            (4, 4) => scan::<4, 4>(self, n, &mut visit),
            _ => unreachable!("validated collection locator widths"),
        }
    }
    fn add(&mut self, pos: usize, extension: bool) {
        let used = self.used();
        let mut occupied = 31u128;
        for i in 0..used {
            occupied |= 1u128 << self.slot(i)
        }
        let slot = occupied.trailing_ones() as usize;
        self.0
            .copy_within(POINTERS + pos..POINTERS + used, POINTERS + pos + 1);
        self.0[POINTERS + pos] = slot as u8 | if extension { 128 } else { 0 };
        self.0[slot * 32..(slot + 1) * 32].fill(0);
        self.0[28] += 1;
    }
    pub fn widths(&self, post: &Posting) -> [u8; 2] {
        let width = |v: u32| (32 - v.leading_zeros()).div_ceil(8) as u8;
        [
            self.0[30].max(width(post.hour)),
            self.0[31].max(width(post.record)),
        ]
    }
    // False requests a split or repack. The page is untouched in that case.
    pub fn put(&mut self, post: &Posting) -> io::Result<bool> {
        if self.0[30..32] != self.widths(post) {
            return Ok(false);
        }
        let addr = post.address();
        let rev = post.revision;
        let Some(n) = self.find(&post.object) else {
            let units = self.units(1);
            if self.used() + units > UNITS {
                return Ok(false);
            }
            let n = self
                .groups()
                .find(|&n| self.key(n) > post.object)
                .unwrap_or(self.used());
            self.add(n, false);
            if units == 2 {
                self.add(n + 1, true);
            }
            let a = self.at(n);
            self.0[a..a + 16].copy_from_slice(&post.object);
            store(&mut self.0[a + 16..a + 19], rev);
            self.put_ref(n, 0, addr);
            return Ok(true);
        };
        let base = self.base(n);
        let cap = self.capacity(n);
        if rev >= base && ((rev - base) as usize) < cap {
            let pos = (self.head(n) + (rev - base) as usize) % cap;
            let old = self.value(n, pos);
            if old != 0 && old != addr {
                return Err(invalid("conflicting object revision"));
            }
            self.put_ref(n, pos, addr);
            return Ok(true);
        }
        if self.linked(n) {
            return Ok(false);
        }
        let low = base.min(rev);
        let high = if rev >= base {
            rev
        } else {
            self.entries(n).next_back().map_or(base, |v| v.0)
        };
        let needed = self.units((high - low + 1) as usize);
        let end = self.end(n);
        let extra = needed.saturating_sub(end - n);
        if self.used() + extra > UNITS {
            return Ok(false);
        }
        let rotate = low != base || self.head(n) != 0;
        let retained = rotate.then(|| self.entries(n).collect::<Vec<_>>());
        for j in 0..extra {
            self.add(end + j, true)
        }
        if let Some(entries) = retained {
            let a = self.at(n);
            store(&mut self.0[a + 16..a + 19], low);
            self.0[a + 19..a + 21].fill(0);
            for pos in 0..self.capacity(n) {
                self.put_ref(n, pos, 0)
            }
            for (r, v) in entries {
                self.put_ref(n, (r - low) as usize, v)
            }
        }
        self.put_ref(n, (rev - low) as usize, addr);
        Ok(true)
    }
    pub fn reserve_link(&mut self, high: u32, link: Link) -> io::Result<()> {
        let units = self.units((high - self.base(0) + 2) as usize);
        if units > UNITS {
            return Err(invalid("continuation exceeds page"));
        }
        while self.used() < units {
            self.add(self.used(), true)
        }
        self.set_link(0, link);
        Ok(())
    }
    pub fn append_group(&mut self, other: &Page) {
        for i in 0..other.used() {
            self.add(self.used(), i != 0);
            let at = self.at(self.used() - 1);
            self.0[at..at + 32].copy_from_slice(&other.0[other.at(i)..other.at(i) + 32]);
        }
    }
    pub fn seal(&mut self) {
        let crc = checksum(&self.0, 24);
        store(&mut self.0[24..28], crc)
    }
    pub fn validate(&self, k: Binding) -> io::Result<()> {
        let b = &self.0;
        if &b[..4] != b"OBJ0"
            || b[4..20] != k.generation
            || number(&b[20..24]) != k.minor
            || number(&b[24..28]) != checksum(b, 24)
            || !(1..=2).contains(&b[29])
            || !(2..=4).contains(&b[30])
            || !(3..=4).contains(&b[31])
            || self.used() > UNITS
            || b[32..37] != [0; 5]
            || b[POINTERS + self.used()..160].iter().any(|&v| v != 128)
        {
            return Err(invalid("invalid collection page header or checksum"));
        }
        let mut occupied = 31u128;
        let mut previous = None;
        for n in 0..self.used() {
            let bit = 1u128 << self.slot(n);
            if occupied & bit != 0 || n == 0 && !self.start(n) {
                return Err(invalid("invalid collection page slot"));
            }
            occupied |= bit;
            if self.start(n) {
                let key = self.key(n);
                if previous >= Some(key) {
                    return Err(invalid("unsorted collection page"));
                }
                previous = Some(key);
            }
        }
        for n in self.groups() {
            let a = self.at(n);
            if b[a + VIEW_SLOT..a + VIEW_SLOT + self.width()]
                .iter()
                .any(|&byte| byte != 0)
            {
                return Err(invalid("unpublished materialized view pointer"));
            }
            if self.raw_capacity(n) <= usize::from(self.linked(n)) {
                return Err(invalid("empty collection group capacity"));
            }
            let cap = self.capacity(n);
            if self.head(n) >= cap
                || b[self.at(n) + 21] > 1
                || self
                    .link(n)
                    .is_some_and(|l| l.page == 0 || !(5..128).contains(&l.slot))
            {
                return Err(invalid("invalid collection group"));
            }
            for delta in (MAX_REV - self.base(n) + 1) as usize..cap {
                if self.value(n, (self.head(n) + delta) % cap) != 0 {
                    return Err(invalid("revision exceeds U24"));
                }
            }
        }
        Ok(())
    }
}
