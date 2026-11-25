//! Source-bound UUID and revision index for uncompressed object collections.
//! The committed source PACs provide redo from each published watermark.
mod page;
mod publication;
mod transaction;

use crate::Uuid;
use page::{Link, Page, SIZE};

/// Highest revision addressable by a collection page.
pub const MAX_REV: u32 = 0xff_ffff;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io,
    os::unix::fs::FileExt,
    sync::{Arc, Mutex, RwLock, Weak},
};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
#[repr(C)]
pub struct Binding {
    pub minor: u32,
    pub generation: Uuid,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Posting {
    pub object: Uuid,
    pub revision: u32,
    pub hour: u32,
    pub record: u32,
}
impl Posting {
    pub fn address(&self) -> u64 {
        u64::from(self.hour) << 32 | u64::from(self.record)
    }
}

#[derive(Clone, Default)]
struct Source {
    width: u8,
    through: u64,
    fences: Vec<(Uuid, u32)>,
    overflow: BTreeSet<u32>,
}
#[derive(Default)]
struct View {
    sequence: u64,
    sources: BTreeMap<Binding, Arc<Source>>,
    metadata: Vec<u32>,
}
impl View {
    fn protect(&self, set: &mut BTreeSet<u32>) {
        set.extend(&self.metadata);
        for s in self.sources.values() {
            set.extend(s.fences.iter().map(|f| f.1));
            set.extend(&s.overflow)
        }
    }
}
fn read_page(file: &File, id: u32, binding: Binding) -> io::Result<Page> {
    let mut p = Page([0; SIZE]);
    file.read_exact_at(&mut p.0, u64::from(id) * SIZE as u64)?;
    p.validate(binding)?;
    Ok(p)
}
fn check_child(p: &Page, link: Link, key: Uuid, high: u32) -> io::Result<()> {
    if p.0[29] != 2
        || p.used() == 0
        || p.end(0) != p.used()
        || p.key(0) != key
        || p.slot(0) != link.slot
        || p.base(0) <= high
    {
        return Err(invalid(
            "invalid collection continuation owner, slot or revision",
        ));
    }
    Ok(())
}

/// Opaque continuation position for a pinned history traversal.
pub struct HistoryLink {
    link: Link,
    high: u32,
}
fn history_link(p: &Page, at: usize) -> io::Result<Option<HistoryLink>> {
    let Some(link) = p.link(at) else {
        return Ok(None);
    };
    let (high, _) = p
        .entries(at)
        .next_back()
        .ok_or_else(|| invalid("empty collection group"))?;
    Ok(Some(HistoryLink { link, high }))
}

// Copy-on-write pages are immutable for the lifetime of a publication. Keep a
// bounded set per open index, shared by pinned readers. The sequence prevents
// a reused file address from ever serving a page from another publication.
struct PageCache {
    buckets: Box<[Mutex<CachedPage>]>,
}
struct CachedPage {
    tag: Option<(u64, u32, Binding)>,
    page: Arc<Page>,
}
impl Default for PageCache {
    fn default() -> Self {
        let buckets = (0..32)
            .map(|_| {
                let bucket = Mutex::new(CachedPage {
                    tag: None,
                    page: Arc::new(Page([0; SIZE])),
                });
                // Darwin allocates its native mutex on first lock.
                drop(bucket.lock().unwrap());
                bucket
            })
            .collect();
        Self { buckets }
    }
}
impl PageCache {
    fn read(&self, file: &File, sequence: u64, id: u32, binding: Binding) -> io::Result<Arc<Page>> {
        let mut bucket = self.buckets[id as usize % self.buckets.len()]
            .lock()
            .unwrap();
        let tag = Some((sequence, id, binding));
        if bucket.tag != tag {
            let page = read_page(file, id, binding)?;
            // Reuse the buffer unless another reader still holds the evicted page.
            if let Some(slot) = Arc::get_mut(&mut bucket.page) {
                *slot = page;
            } else {
                bucket.page = Arc::new(page);
            }
            bucket.tag = tag;
        }
        Ok(bucket.page.clone())
    }
}

#[derive(Clone)]
pub struct Snapshot {
    cache: Arc<PageCache>,
    file: Arc<File>,
    view: Arc<View>,
}
impl Snapshot {
    pub fn sequence(&self) -> u64 {
        self.view.sequence
    }

    /// Number of routed leaves, overflow pages and shared directory pages in this snapshot.
    pub fn page_counts(&self, k: Binding) -> (usize, usize, usize) {
        let source = self.view.sources.get(&k);
        (
            source.map_or(0, |s| s.fences.len()),
            source.map_or(0, |s| s.overflow.len()),
            self.view.metadata.len(),
        )
    }
    pub fn sources(&self) -> impl Iterator<Item = (Binding, u8, u64)> + '_ {
        self.view
            .sources
            .iter()
            .map(|(&k, s)| (k, s.width, s.through))
    }
    fn first_group(&self, k: Binding, key: Uuid) -> io::Result<Option<(Arc<Page>, usize)>> {
        let Some(s) = self.view.sources.get(&k) else {
            return Ok(None);
        };
        let n = s.fences.partition_point(|f| f.0 <= key);
        if n == 0 {
            return Ok(None);
        }
        let page = self
            .cache
            .read(&self.file, self.view.sequence, s.fences[n - 1].1, k)?;
        Ok(page.find(&key).map(|at| (page, at)))
    }
    fn next_group(&self, k: Binding, key: Uuid, next: HistoryLink) -> io::Result<Arc<Page>> {
        if !self
            .view
            .sources
            .get(&k)
            .is_some_and(|s| s.overflow.contains(&next.link.page))
        {
            return Err(invalid("unpublished collection continuation"));
        }
        let page = self
            .cache
            .read(&self.file, self.view.sequence, next.link.page, k)?;
        check_child(&page, next.link, key, next.high)?;
        Ok(page)
    }
    fn walk(
        &self,
        k: Binding,
        key: Uuid,
        mut visit: impl FnMut(&Page, usize) -> bool,
    ) -> io::Result<()> {
        let Some((mut page, mut at)) = self.first_group(k, key)? else {
            return Ok(());
        };
        while visit(&page, at) {
            let Some(next) = history_link(&page, at)? else {
                break;
            };
            page = self.next_group(k, key, next)?;
            at = 0;
        }
        Ok(())
    }
    /// Stream one physical group per call across a language boundary. Resume only
    /// on this pinned snapshot, with the same binding and key.
    pub fn history_page(
        &self,
        k: Binding,
        key: Uuid,
        next: Option<HistoryLink>,
        visit: impl FnMut(u32, u64),
    ) -> io::Result<Option<HistoryLink>> {
        let (page, at) = match next {
            Some(next) => (self.next_group(k, key, next)?, 0),
            None => match self.first_group(k, key)? {
                Some(group) => group,
                None => return Ok(None),
            },
        };
        page.visit_entries(at, visit);
        history_link(&page, at)
    }
    pub fn get(&self, k: Binding, key: Uuid, rev: u32) -> io::Result<Option<u64>> {
        let mut found = None;
        self.walk(k, key, |p, n| {
            found = p.reference(n, rev);
            found.is_none() && p.base(n) <= rev
        })?;
        Ok(found)
    }
    pub fn latest(&self, k: Binding, key: Uuid) -> io::Result<Option<(u32, u64)>> {
        let mut latest = None;
        self.walk(k, key, |p, n| {
            latest = p.entries(n).next_back();
            true
        })?;
        Ok(latest)
    }
    pub fn history(
        &self,
        k: Binding,
        key: Uuid,
        mut visit: impl FnMut(u32, u64),
    ) -> io::Result<()> {
        self.walk(k, key, |p, n| {
            p.visit_entries(n, &mut visit);
            true
        })
    }
    pub fn each(
        &self,
        k: Binding,
        mut visit: impl FnMut(Uuid, u32, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        if let Some(s) = self.view.sources.get(&k) {
            for &(_, id) in &s.fences {
                let root = read_page(&self.file, id, k)?;
                for n in root.groups() {
                    let key = root.key(n);
                    for (r, v) in root.entries(n) {
                        visit(key, r, v)?
                    }
                    let mut link = root.link(n);
                    let mut high = root
                        .entries(n)
                        .next_back()
                        .ok_or_else(|| invalid("empty collection group"))?
                        .0;
                    while let Some(next) = link {
                        if !s.overflow.contains(&next.page) {
                            return Err(invalid("unpublished collection continuation"));
                        }
                        let child = read_page(&self.file, next.page, k)?;
                        check_child(&child, next, key, high)?;
                        for (r, v) in child.entries(0) {
                            visit(key, r, v)?;
                            high = r
                        }
                        link = child.link(0);
                    }
                }
            }
        }
        Ok(())
    }
}
struct Writer {
    roots: [Option<Arc<View>>; 2],
    retired: Vec<Weak<View>>,
    next: u32,
    failed: bool,
}
/// One writer owns each index file. Snapshots pin the publication they opened.
pub struct Index {
    cache: Arc<PageCache>,
    file: Arc<File>,
    current: RwLock<Arc<View>>,
    writer: Mutex<Writer>,
}
impl Index {
    pub fn open(file: File) -> io::Result<Self> {
        let bytes = file.metadata()?.len();
        let next = u32::try_from(bytes.div_ceil(SIZE as u64))
            .map_err(|_| invalid("collection index too large"))?
            .max(1);
        let mut roots = [None, None];
        let mut current = Arc::new(View::default());
        if bytes != 0 {
            let mut raw = [0; 128];
            file.read_exact_at(&mut raw, 0)?;
            for (slot, root) in raw.chunks_exact(64).enumerate() {
                match publication::read(&file, root, next) {
                    Ok(v) if v.sequence != 0 && v.sequence % 2 == slot as u64 => {
                        let v = Arc::new(v);
                        if v.sequence > current.sequence {
                            current = v.clone()
                        }
                        roots[slot] = Some(v);
                    }
                    Err(e)
                        if !matches!(
                            e.kind(),
                            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                        ) =>
                    {
                        return Err(e);
                    }
                    _ => {}
                }
            }
            if current.sequence == 0 {
                return Err(invalid("no valid collection root"));
            }
        }
        Ok(Self {
            cache: Arc::default(),
            file: Arc::new(file),
            current: RwLock::new(current),
            writer: Mutex::new(Writer {
                roots,
                retired: Vec::new(),
                next,
                failed: false,
            }),
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            cache: self.cache.clone(),
            file: self.file.clone(),
            view: self.current.read().unwrap().clone(),
        }
    }
    pub fn append(
        &self,
        k: Binding,
        width: u8,
        from: u64,
        through: u64,
        posts: &[Posting],
    ) -> io::Result<()> {
        self.update(k, width, Some(from), through, posts, |_| Ok(()))
    }
    pub fn merge_retained(
        &self,
        k: Binding,
        width: u8,
        through: u64,
        posts: &[Posting],
    ) -> io::Result<()> {
        self.update(k, width, None, through, posts, |_| Ok(()))
    }
    /// Publish one source batch. The optional starting watermark distinguishes replay from
    /// a private retention build. The callback brackets durable root publication for fault tests.
    pub fn update(
        &self,
        k: Binding,
        width: u8,
        from: Option<u64>,
        through: u64,
        posts: &[Posting],
        mut fault: impl FnMut(bool) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        if writer.failed {
            return Err(invalid("reopen collection index after failed publication"));
        }
        let old = self.snapshot().view;
        let prior = old.sources.get(&k);
        if !matches!(width, 1 | 2 | 4 | 8)
            || through == 0
            || through > 1 << 48
            || from.is_some_and(|v| v != prior.map_or(0, |s| s.through) || through <= v)
            || prior.is_some_and(|s| s.width != width || through < s.through)
        {
            return Err(invalid("invalid collection watermark or revision width"));
        }
        let mut source = prior.map_or_else(Source::default, |s| Source::clone(s));
        source.width = width;
        source.through = through;
        let mut protected = BTreeSet::new();
        for view in writer.roots.iter().flatten() {
            view.protect(&mut protected)
        }
        writer.retired.retain(|weak| {
            if let Some(v) = weak.upgrade() {
                v.protect(&mut protected);
                true
            } else {
                false
            }
        });
        let free = (1..writer.next)
            .rev()
            .filter(|n| !protected.contains(n))
            .collect();
        let mut tx = transaction::Transaction::new(&self.file, k, source, free, writer.next);
        let mut sorted = posts.to_vec();
        sorted.sort_unstable_by_key(|p| (p.object, p.revision));
        for p in sorted {
            if p.record == 0
                || p.revision > MAX_REV
                || width == 1 && p.revision > 255
                || width == 2 && p.revision > 65535
            {
                return Err(invalid("invalid object posting"));
            }
            tx.put(p)?;
        }
        let mut sources = old.sources.clone();
        sources.insert(k, Arc::new(std::mem::take(&mut tx.source)));
        let mut view = View {
            sources,
            sequence: old
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("collection sequence exhausted"))?,
            metadata: Vec::new(),
        };
        let raw = publication::encode(&view)?;
        for _ in 0..raw.len().div_ceil(4080) {
            view.metadata.push(tx.alloc()?)
        }
        let result = (|| {
            if old.sequence == 0 {
                self.file.set_len(SIZE as u64)?
            }
            for (id, mut page) in tx.dirty {
                page.seal();
                self.file
                    .write_all_at(&page.0, u64::from(id) * SIZE as u64)?
            }
            publication::write_directory(&self.file, &view, &raw)?;
            self.file.sync_all()?;
            fault(false)?;
            publication::write_root(&self.file, &view, raw.len())?;
            self.file.sync_all()?;
            fault(true)
        })();
        if let Err(e) = result {
            writer.failed = true;
            return Err(e);
        }
        let slot = (view.sequence % 2) as usize;
        let view = Arc::new(view);
        if let Some(prior) = writer.roots[slot].replace(view.clone()) {
            writer.retired.push(Arc::downgrade(&prior))
        }
        writer.next = tx.next;
        *self.current.write().unwrap() = view;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
