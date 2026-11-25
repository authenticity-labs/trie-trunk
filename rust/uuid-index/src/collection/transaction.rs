use super::*;
use page::UNITS;

pub(super) struct Transaction<'a> {
    file: &'a File,
    binding: Binding,
    pub source: Source,
    free: Vec<u32>,
    pub next: u32,
    pub dirty: BTreeMap<u32, Page>,
}
impl<'a> Transaction<'a> {
    pub fn new(
        file: &'a File,
        binding: Binding,
        source: Source,
        free: Vec<u32>,
        next: u32,
    ) -> Self {
        Self {
            file,
            binding,
            source,
            free,
            next,
            dirty: BTreeMap::new(),
        }
    }
    pub fn alloc(&mut self) -> io::Result<u32> {
        if let Some(id) = self.free.pop() {
            return Ok(id);
        }
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or_else(|| invalid("collection page address exhausted"))?;
        Ok(id)
    }
    fn read(&self, id: u32) -> io::Result<Page> {
        match self.dirty.get(&id) {
            Some(p) => Ok(p.clone()),
            None => read_page(self.file, id, self.binding),
        }
    }
    fn save(&mut self, old: u32, p: Page) -> io::Result<u32> {
        let id = if self.dirty.contains_key(&old) {
            old
        } else {
            self.alloc()?
        };
        self.source.overflow.remove(&old);
        if p.0[29] == 2 {
            self.source.overflow.insert(id);
        }
        self.dirty.insert(id, p);
        Ok(id)
    }
    pub fn put(&mut self, post: Posting) -> io::Result<()> {
        let key = post.object;
        if self.source.fences.is_empty() {
            let mut p = Page::new(self.binding);
            let widths = p.widths(&post);
            p.0[30..32].copy_from_slice(&widths);
            p.put(&post)?;
            let id = self.save(0, p)?;
            self.source.fences.push((key, id));
            return Ok(());
        }
        let fence = self
            .source
            .fences
            .partition_point(|f| f.0 <= key)
            .saturating_sub(1);
        let mut id = self.source.fences[fence].1;
        let mut p = self.read(id)?;
        let mut path = Vec::new();
        while let Some(at) = p.find(&key) {
            let Some(link) = p.link(at) else { break };
            let child = self.read(link.page)?;
            let high = p
                .entries(at)
                .next_back()
                .ok_or_else(|| invalid("empty collection group"))?
                .0;
            check_child(&child, link, key, high)?;
            if post.revision < child.base(0) {
                break;
            }
            path.push((id, at));
            id = link.page;
            p = child;
        }
        let mut fences = Vec::new();
        if p.put(&post)? {
            let first = p.key(0);
            let saved = self.save(id, p)?;
            if path.is_empty() {
                self.source.fences[fence] = (first, saved);
                return Ok(());
            }
            id = saved;
        } else {
            for (n, q) in self.repack(&p, post)?.into_iter().enumerate() {
                fences.push((q.key(0), self.save(if n == 0 { id } else { 0 }, q)?));
            }
            if path.is_empty() {
                self.source.fences.splice(fence..=fence, fences);
                return Ok(());
            }
            if fences.len() != 1 {
                return Err(invalid("overflow page split across objects"));
            }
            id = fences[0].1;
        }
        for (parent, at) in path.into_iter().rev() {
            let mut p = self.read(parent)?;
            p.set_link(
                at,
                Link {
                    page: id,
                    slot: self.dirty[&id].slot(0),
                },
            );
            id = self.save(parent, p)?;
        }
        self.source.fences[fence].1 = id;
        Ok(())
    }
    // Repack only the affected page. Other objects' overflow chains remain shared.
    fn repack(&mut self, old: &Page, post: Posting) -> io::Result<Vec<Page>> {
        let mut template = Page::new(self.binding);
        template.0[29] = old.0[29];
        template.0[30..32].copy_from_slice(&old.widths(&post));
        let mut groups = BTreeMap::new();
        for n in old.groups() {
            groups.insert(
                old.key(n),
                (old.entries(n).collect::<Vec<_>>(), old.link(n)),
            );
        }
        let (entries, _) = groups.entry(post.object).or_default();
        match entries.binary_search_by_key(&post.revision, |v| v.0) {
            Ok(n) if entries[n].1 != post.address() => {
                return Err(invalid("conflicting object revision"));
            }
            Ok(_) => {}
            Err(n) => entries.insert(n, (post.revision, post.address())),
        }
        let mut heads = Vec::with_capacity(groups.len());
        let mut total = 0;
        for (key, (entries, mut link)) in groups {
            let mut parts = Vec::new();
            let mut rest = &entries[..];
            while !rest.is_empty() {
                let base = rest[0].0;
                let max = template.max_span() as u32;
                let needs_link = link.is_some() || rest.last().unwrap().0 - base >= max;
                let end = rest.partition_point(|v| v.0 - base < max - u32::from(needs_link));
                parts.push(&rest[..end]);
                rest = &rest[end..];
            }
            for (part, values) in parts.into_iter().enumerate().rev() {
                let mut q = template.clone();
                for &(revision, addr) in values {
                    if !q.put(&Posting {
                        object: key,
                        revision,
                        hour: (addr >> 32) as u32,
                        record: addr as u32,
                    })? {
                        return Err(invalid("collection split failed to fit"));
                    }
                }
                if let Some(l) = link {
                    q.reserve_link(values.last().unwrap().0, l)?
                }
                if part == 0 {
                    total += q.used();
                    heads.push(q)
                } else {
                    q.0[29] = 2;
                    let slot = q.slot(0);
                    link = Some(Link {
                        page: self.save(0, q)?,
                        slot,
                    });
                }
            }
        }
        let target = total.div_ceil(total.div_ceil(UNITS));
        let mut result = Vec::new();
        let mut page = template.clone();
        for head in heads {
            if page.used() != 0 && page.used() + head.used() > target {
                result.push(page);
                page = template.clone()
            }
            page.append_group(&head);
        }
        result.push(page);
        Ok(result)
    }
}
