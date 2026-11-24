//! Transport snapshot captured by the node's existing commit/read lock.
//! File descriptors remain pinned while transports use their pooled streaming
//! buffers; neither published Packs nor complete datastores are copied here.
use crate::{Node, Result, read::bounded};
use std::{fs::File, path::Path, sync::atomic::Ordering};

pub struct Published {
    pub time: Vec<u8>,
    pub packs: Option<File>,
    pub index: Option<File>,
    pub summaries: Option<File>,
    pub footer: Vec<u8>,
    pub summary_footer: Vec<u8>,
    pub length: u64,
    pub summary_length: u64,
    pub index_length: u64,
}
pub struct Snapshot {
    pub generation: u64,
    pub published: Option<Published>,
    pub live: Vec<Vec<u8>>,
    pub summaries: Vec<(u64, Vec<u8>)>,
    pub registry: data_seeds::registration::Snapshot,
    pub(crate) memory: Vec<data_seeds::MemoryLease>,
}
fn optional(path: &Path) -> std::io::Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
pub(crate) fn footer(
    file: &File,
    memory: &data_seeds::MemoryBudget,
) -> Result<(Vec<u8>, u64, data_seeds::MemoryLease)> {
    use std::os::unix::fs::FileExt;
    let length = file.metadata()?.len();
    let n = length.min(64) as usize;
    let mut tail = [0; 64];
    file.read_exact_at(&mut tail[..n], length - n as u64)?;
    let range = data_seeds::packs::footer_range(&tail[..n], length)?;
    let n = usize::try_from(range.end - range.start)?;
    let lease = memory.reserve(n)?;
    let mut bytes = vec![0; n];
    file.read_exact_at(&mut bytes, range.start)?;
    Ok((bytes, length, lease))
}
impl Node {
    pub fn publication(
        &self,
        branch: u16,
        windows: &[(u64, u64)],
        summaries: bool,
        known: Option<u64>,
        clock: u64,
    ) -> Result<Snapshot> {
        if windows.len() > 128 || windows.iter().any(|(a, b)| a >= b) {
            return Err("Invalid publication windows".into());
        }
        let mut runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        let route = self.trie().route(branch)?;
        let paths = route.event_storage()?;
        let base = format!("{}/{}", paths.directory, paths.stem);
        let prefix = self.root.join(&base);
        let path = |suffix: &str| {
            let mut name = prefix.as_os_str().to_owned();
            name.push(suffix);
            std::path::PathBuf::from(name)
        };
        let mut snapshot = Snapshot {
            generation: 0,
            published: None,
            live: vec![],
            summaries: vec![],
            registry: self.trie(),
            memory: vec![],
        };
        if summaries {
            let day_ms = data_seeds::summaries::SUMMARY_DAY_MS;
            for &(from, to) in windows {
                let mut day = from;
                while day < to && day <= clock {
                    if let Some(bytes) = runtime.summary_bytes(self, &base, day)? {
                        snapshot.memory.push(self.memory.reserve(bytes.capacity())?);
                        snapshot.summaries.push((day, bytes));
                    }
                    day = day.checked_add(day_ms).ok_or("Summary window overflow")?;
                }
            }
        } else {
            snapshot.live =
                runtime.snapshots(&base, windows, &self.memory, &mut snapshot.memory)?;
        }
        snapshot.generation = self
            .registry_generation
            .load(Ordering::Acquire)
            .max(runtime.generation(&base));
        snapshot.registry = self.trie();
        if known != Some(snapshot.generation) {
            let time = bounded(&path(".time"), 16 << 20)?;
            snapshot.memory.push(self.memory.reserve(time.capacity())?);
            let packs = optional(&path(self.configuration.storage.suffix()))?;
            let (footer, length) = if let Some(file) = &packs {
                let (bytes, length, lease) = footer(file, &self.memory)?;
                snapshot.memory.push(lease);
                (bytes, length)
            } else {
                (vec![], 0)
            };
            let index = optional(&path(".indx"))?;
            let index_length = index
                .as_ref()
                .map(|f| f.metadata().map(|m| m.len()))
                .transpose()?
                .unwrap_or(0);
            let summaries = optional(&path(".summ"))?;
            let (summary_footer, summary_length) = if let Some(file) = &summaries {
                let (bytes, length, lease) = self::footer(file, &self.memory)?;
                snapshot.memory.push(lease);
                (bytes, length)
            } else {
                (vec![], 0)
            };
            snapshot.published = Some(Published {
                time,
                packs,
                index,
                summaries,
                footer,
                summary_footer,
                length,
                summary_length,
                index_length,
            });
        }
        Ok(snapshot)
    }
}
