use super::*;
use page::{checksum, number};

fn count(n: usize) -> io::Result<u32> {
    u32::try_from(n).map_err(|_| invalid("collection directory too large"))
}
pub(super) fn encode(v: &View) -> io::Result<Vec<u8>> {
    let mut raw = Vec::new();
    raw.extend(count(v.sources.len())?.to_le_bytes());
    for (k, s) in &v.sources {
        raw.extend(k.generation);
        raw.extend(k.minor.to_le_bytes());
        raw.extend([s.width, 0, 0, 0]);
        raw.extend(s.through.to_le_bytes());
        raw.extend(count(s.fences.len())?.to_le_bytes());
        raw.extend([0; 4]);
        for (key, id) in &s.fences {
            raw.extend(key);
            raw.extend(id.to_le_bytes())
        }
    }
    Ok(raw)
}
pub(super) fn write_directory(file: &File, v: &View, raw: &[u8]) -> io::Result<()> {
    for (n, chunk) in raw.chunks(4080).enumerate() {
        let mut p = [0; SIZE];
        p[..4].copy_from_slice(b"CDIR");
        p[4..8].copy_from_slice(&v.metadata.get(n + 1).copied().unwrap_or(0).to_le_bytes());
        p[16..16 + chunk.len()].copy_from_slice(chunk);
        let crc = crc32c::crc32c(&p);
        p[8..12].copy_from_slice(&crc.to_le_bytes());
        file.write_all_at(&p, u64::from(v.metadata[n]) * SIZE as u64)?;
    }
    Ok(())
}
pub(super) fn write_root(file: &File, v: &View, len: usize) -> io::Result<()> {
    let mut root = [0; 64];
    root[..8].copy_from_slice(b"COLLIDX0");
    root[8..16].copy_from_slice(&v.sequence.to_le_bytes());
    root[16..20].copy_from_slice(&v.metadata[0].to_le_bytes());
    root[20..24].copy_from_slice(&count(v.metadata.len())?.to_le_bytes());
    root[24..32].copy_from_slice(&(len as u64).to_le_bytes());
    let crc = crc32c::crc32c(&root);
    root[32..36].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&root, (v.sequence % 2) * 64)
}
fn claim(seen: &mut BTreeSet<u32>, id: u32, limit: u32) -> io::Result<()> {
    if id == 0 || id >= limit || !seen.insert(id) {
        return Err(invalid("invalid or repeated collection page address"));
    }
    Ok(())
}
pub(super) fn read(file: &File, root: &[u8], limit: u32) -> io::Result<View> {
    if &root[..8] != b"COLLIDX0" || number(&root[32..36]) != checksum(root, 32) {
        return Err(invalid("invalid collection root"));
    }
    let mut v = View {
        sequence: u64::from_le_bytes(root[8..16].try_into().unwrap()),
        ..Default::default()
    };
    let mut next = number(&root[16..20]);
    let pages = number(&root[20..24]);
    let len = u64::from_le_bytes(root[24..32].try_into().unwrap());
    if pages >= limit || len < 4 || len.div_ceil(4080) != u64::from(pages) {
        return Err(invalid("invalid collection directory extent"));
    }
    let mut seen = BTreeSet::new();
    let mut raw = Vec::new();
    for _ in 0..pages {
        claim(&mut seen, next, limit)?;
        v.metadata.push(next);
        let mut p = [0; SIZE];
        file.read_exact_at(&mut p, u64::from(next) * SIZE as u64)?;
        if &p[..4] != b"CDIR" || number(&p[8..12]) != checksum(&p, 8) {
            return Err(invalid("invalid collection directory page"));
        }
        next = number(&p[4..8]);
        let bytes = (len - raw.len() as u64).min(4080) as usize;
        raw.extend_from_slice(&p[16..16 + bytes]);
    }
    if next != 0 {
        return Err(invalid("unterminated collection directory"));
    }
    let sources = number(&raw[..4]);
    let mut rest = &raw[4..];
    for _ in 0..sources {
        let (h, after) = rest
            .split_at_checked(40)
            .ok_or_else(|| invalid("short collection directory"))?;
        rest = after;
        let k = Binding {
            minor: number(&h[16..20]),
            generation: h[..16].try_into().unwrap(),
        };
        let mut s = Source {
            width: h[20],
            through: u64::from_le_bytes(h[24..32].try_into().unwrap()),
            ..Default::default()
        };
        if v.sources.contains_key(&k)
            || !matches!(s.width, 1 | 2 | 4 | 8)
            || s.through > 1 << 48
            || h[21..24] != [0; 3]
            || h[36..] != [0; 4]
        {
            return Err(invalid("invalid collection source"));
        }
        for _ in 0..number(&h[32..36]) {
            let (f, after) = rest
                .split_at_checked(20)
                .ok_or_else(|| invalid("short collection fence"))?;
            rest = after;
            let key: Uuid = f[..16].try_into().unwrap();
            if s.fences.last().is_some_and(|v| v.0 >= key) {
                return Err(invalid("unordered collection fences"));
            }
            let id = number(&f[16..20]);
            claim(&mut seen, id, limit)?;
            s.fences.push((key, id));
        }
        for (i, &(first, id)) in s.fences.iter().enumerate() {
            let p = read_page(file, id, k)?;
            if p.0[29] != 1 || p.used() == 0 || p.key(0) != first {
                return Err(invalid("invalid collection fence owner"));
            }
            for n in p.groups() {
                let key = p.key(n);
                if s.fences.get(i + 1).is_some_and(|f| key >= f.0) {
                    return Err(invalid("overlapping collection fences"));
                }
                let mut link = p.link(n);
                let mut high = p
                    .entries(n)
                    .next_back()
                    .ok_or_else(|| invalid("empty collection group"))?
                    .0;
                while let Some(l) = link {
                    claim(&mut seen, l.page, limit)?;
                    s.overflow.insert(l.page);
                    let q = read_page(file, l.page, k)?;
                    check_child(&q, l, key, high)?;
                    high = q
                        .entries(0)
                        .next_back()
                        .ok_or_else(|| invalid("empty collection continuation"))?
                        .0;
                    link = q.link(0);
                }
            }
        }
        v.sources.insert(k, Arc::new(s));
    }
    if !rest.is_empty() {
        return Err(invalid("trailing collection directory bytes"));
    }
    Ok(v)
}
