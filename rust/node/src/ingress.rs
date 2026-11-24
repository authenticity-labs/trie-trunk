//! Local I/O ownership around the existing Seed ingress and commit protocols.
//! PILs are acknowledged after the recovery spool is durable. Open Packs stay
//! resident; only sealed Packs enter the configured Packs or Beds datastore.
use crate::{
    Node, Result,
    read::{bounded, within},
    sync_directory,
};
use data_seeds::registration::IngressLimits;
use data_seeds::registration::ingress::files::{publish as apply, summary as apply_summary};
use data_seeds::registration::ingress::{
    OwnedIngress, SummaryCommit, publication::Publication, recovery,
};
use data_seeds::writer::{CommitScheduler, QueueLimits};
use data_seeds::{MemoryBudget, MemoryLease, TIME_HOUR_MS, TimeTrie};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) struct Runtime {
    lanes: HashMap<String, Lane>,
    scheduler: CommitScheduler,
}
struct Lane {
    generation: u64,
    branch: u16,
    prefix: PathBuf,
    live: OwnedIngress,
    state: data_seeds::registration::ingress::StatePaths,
    head: u64,
    files: BTreeMap<u64, File>,
    charges: BTreeMap<u64, Vec<MemoryLease>>,
    charged: BTreeMap<u64, usize>,
    failed: Option<String>,
    summary_day: u64,
}
impl Runtime {
    pub(crate) fn generation(&self, base: &str) -> u64 {
        self.lanes.get(base).map_or(0, |l| l.generation)
    }
    pub(crate) fn snapshots(
        &self,
        base: &str,
        windows: &[(u64, u64)],
        budget: &MemoryBudget,
        leases: &mut Vec<MemoryLease>,
    ) -> Result<Vec<Vec<u8>>> {
        let Some(lane) = self.lanes.get(base) else {
            return Ok(vec![]);
        };
        if let Some(error) = &lane.failed {
            return Err(error.clone().into());
        }
        let mut packs = Vec::new();
        for hour in lane.live.hours() {
            if !windows
                .iter()
                .any(|(a, b)| hour < *b && hour.saturating_add(TIME_HOUR_MS) > *a)
            {
                continue;
            }
            let lease = budget.reserve(
                lane.live
                    .retained_input_bytes()
                    .saturating_mul(2)
                    .saturating_add(4096),
            )?;
            packs.push(lane.live.snapshot(hour)?);
            leases.push(lease);
        }
        Ok(packs)
    }
    pub(crate) fn import(
        &mut self,
        node: &Node,
        branch: u16,
        hours: BTreeMap<u64, PathBuf>,
        clock: u64,
    ) -> Result<()> {
        let first = *hours.keys().next().ok_or("Empty import")?;
        let storage = node.trie().route(branch)?.event_storage()?;
        let base = format!("{}/{}", storage.directory, storage.stem);
        if self.lanes.contains_key(&base) || node.root.join(format!("{base}.time")).exists() {
            return Err("Import target already contains events; choose a new branch".into());
        }
        let mut lane = Lane::open(
            node,
            branch,
            &base,
            first + TIME_HOUR_MS - 1,
            &mut self.scheduler,
        )?;
        let result = (|| -> Result<()> {
            for (hour, path) in hours {
                lane.rotate(hour)?;
                lane.reserve(
                    hour,
                    usize::try_from(std::fs::metadata(&path)?.len())?,
                    &node.memory,
                )?;
                let staged = suffix(&lane.prefix, &format!(".{hour}.pack"));
                let mut output = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&staged)?;
                records(&path, |_, pill, _| {
                    output.write_all(&recovery::prefix(branch, pill)?)?;
                    output.write_all(pill)?;
                    Ok(())
                })?;
                output.sync_all()?;
                sync_directory(staged.parent().unwrap())?;
                records(&staged, |_, pill, _| {
                    lane.live.recover(branch, pill)?;
                    Ok(())
                })?;
                output.seek(SeekFrom::End(0))?;
                lane.files.insert(hour, output);
                if clock >= hour + TIME_HOUR_MS + IngressLimits::default().straggler_ms {
                    lane.publish(node, hour, clock, &mut self.scheduler)?;
                    lane.publish_summaries(node, &mut self.scheduler)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = &result {
            lane.failed = Some(error.to_string());
        }
        self.lanes.insert(base, lane);
        result
    }
    pub(crate) fn refresh(&mut self, node: &Node, branch: u16) -> Result<()> {
        let route = node.trie().route(branch)?;
        if route.pods()?.is_some() {
            return Ok(());
        }
        let storage = route.storage()?;
        let base = format!("{}/{}", storage.directory, storage.stem);
        if self.lanes.remove(&base).is_some() {
            let lane = Lane::open(node, branch, &base, now(), &mut self.scheduler)?;
            self.lanes.insert(base, lane);
        }
        Ok(())
    }
    pub(crate) fn new(memory: MemoryBudget) -> Result<Self> {
        Ok(Self {
            lanes: HashMap::new(),
            scheduler: CommitScheduler::new(
                memory,
                QueueLimits {
                    batches: 8,
                    operations: 128,
                },
            )?,
        })
    }
    pub(crate) fn source(
        &self,
        base: &str,
        from: u64,
        to: u64,
        budget: &MemoryBudget,
    ) -> Result<Option<LiveSource>> {
        self.source_ranges(base, &[(from, to)], budget)
    }

    /// Pin a mutable hour once even when several disjoint periods require a scan.
    pub(crate) fn source_ranges(
        &self,
        base: &str,
        ranges: &[(u64, u64)],
        budget: &MemoryBudget,
    ) -> Result<Option<LiveSource>> {
        let Some(lane) = self.lanes.get(base) else {
            return Ok(None);
        };
        let mut source = LiveSource {
            packs: Vec::new(),
            rows: Vec::new(),
            memory: Vec::new(),
        };
        for hour in lane.live.hours() {
            let mut rows = Vec::new();
            for &(from, to) in ranges
                .iter()
                .skip_while(|(_, to)| *to <= hour)
                .take_while(|(from, _)| *from < hour.saturating_add(TIME_HOUR_MS))
            {
                rows.extend(lane.live.rows(hour, from, to)?);
            }
            rows.sort_unstable_by_key(|r| r.start);
            if rows.is_empty() {
                continue;
            }
            // Charge before copying the mutable writer's snapshot. Immutable
            // published Packs continue to use the existing positional reader.
            let lease = budget.reserve(
                lane.live
                    .retained_input_bytes()
                    .saturating_mul(2)
                    .saturating_add(4096),
            )?;
            let bytes = lane.live.snapshot(hour)?;
            let length = bytes.len();
            source.packs.push(
                lane.live
                    .scope()
                    .open_pack(Arc::new(bytes), 0..length, 64 << 20)?,
            );
            source.rows.push(rows);
            source.memory.push(lease);
        }
        Ok((!source.packs.is_empty()).then_some(source))
    }
    pub(crate) fn counts(&self, base: &str) -> Vec<(u64, u64)> {
        self.lanes
            .get(base)
            .map(|lane| {
                lane.live
                    .hours()
                    .map(|h| (h, lane.live.count(h) as u64))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub(crate) fn summary(
        &mut self,
        node: &Node,
        base: &str,
        day: u64,
    ) -> Result<Option<data_seeds::summaries::SummaryReader>> {
        let branch = match self.lanes.get(base) {
            Some(lane) => lane.branch,
            None => return Ok(None),
        };
        let Some(bytes) = self.summary_bytes(node, base, day)? else {
            return Ok(None);
        };
        Ok(Some(data_seeds::summaries::SummaryReader::registered(
            node.trie().registered(),
            branch,
            bytes,
            day,
        )?))
    }
    pub(crate) fn summary_bytes(
        &mut self,
        node: &Node,
        base: &str,
        day: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(lane) = self.lanes.get_mut(base) else {
            return Ok(None);
        };
        if day < lane.summary_day {
            return Ok(None);
        }
        if !lane.live.has_summaries() {
            return Ok(None);
        }
        let trie = node.trie();
        let bytes = if let Some(bytes) = lane.live.summary_snapshot(trie.bytes(), day)? {
            bytes
        } else {
            let write =
                lane.live
                    .summary_prepare(&trie, day, lane.live.through(), &[], 0, false)?;
            node.publish_registry(
                &write.trie,
                Some((
                    data_seeds::registration::ingress::SUMMARY_FILE,
                    &write.document,
                )),
            )?;
            write.bytes
        };
        Ok(Some(bytes))
    }
}
pub(crate) struct LiveSource {
    packs: Vec<Arc<data_seeds::SeedPackSnapshot>>,
    rows: Vec<Vec<std::ops::Range<usize>>>,
    memory: Vec<MemoryLease>,
}
impl data_seeds::PackSource for LiveSource {
    fn len(&self) -> usize {
        self.packs.len()
    }
    fn definitions(&self) -> Vec<Arc<data_seeds::SeedDefinition>> {
        let mut seen = std::collections::HashSet::new();
        self.packs
            .iter()
            .flat_map(|p| p.definitions())
            .filter(|d| seen.insert(Arc::as_ptr(d)))
            .cloned()
            .collect()
    }
    fn load(
        &mut self,
        ordinal: usize,
    ) -> std::result::Result<Arc<data_seeds::SeedPackSnapshot>, data_seeds::Error> {
        self.packs
            .get(ordinal)
            .cloned()
            .ok_or(data_seeds::Error::Incomplete)
    }
    fn row_selection(
        &mut self,
        ordinal: usize,
    ) -> std::result::Result<data_seeds::RowSelection, data_seeds::Error> {
        Ok(data_seeds::RowSelection::Ranges(
            self.rows
                .get(ordinal)
                .ok_or(data_seeds::Error::Incomplete)?
                .clone(),
        ))
    }
}
fn file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?)
}
fn suffix(prefix: &Path, extension: &str) -> PathBuf {
    PathBuf::from(format!("{}{extension}", prefix.display()))
}
pub(crate) fn atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = suffix(path, ".pending");
    let mut output = file(&temporary)?;
    output.set_len(0)?;
    output.write_all(bytes)?;
    output.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(path.parent().ok_or("Missing parent")?)?;
    Ok(())
}
fn footer(path: &Path, budget: &MemoryBudget) -> Result<(Vec<u8>, u64, MemoryLease)> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    let n = length.min(64) as usize;
    let mut tail = vec![0; n];
    file.read_exact_at(&mut tail, length - n as u64)?;
    let range = data_seeds::packs::footer_range(&tail, length)?;
    let n = usize::try_from(range.end - range.start)?;
    let lease = budget.reserve(n)?;
    let mut bytes = vec![0; n];
    file.read_exact_at(&mut bytes, range.start)?;
    Ok((bytes, length, lease))
}
fn spool_paths(prefix: &Path) -> Result<Vec<PathBuf>> {
    let parent = prefix.parent().ok_or("Missing resource directory")?;
    if !parent.exists() {
        return Ok(Vec::new());
    }
    let stem = prefix
        .file_name()
        .ok_or("Missing resource name")?
        .to_string_lossy();
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let staged = name
            .strip_prefix(&format!("{stem}."))
            .and_then(|s| s.strip_suffix(".pack"))
            .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
        if staged || name == format!("{stem}.seeds") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}
fn records(path: &Path, mut visit: impl FnMut(u16, &[u8], &[u8]) -> Result<()>) -> Result<()> {
    let mut input = file(path)?;
    let length = input.metadata()?.len();
    let mut at = 0;
    while at < length {
        if length - at < recovery::PREFIX as u64 {
            input.set_len(at)?;
            input.sync_all()?;
            break;
        }
        let mut prefix = [0; recovery::PREFIX];
        input.read_exact(&mut prefix)?;
        let size = recovery::length(&prefix, 256 << 20)?;
        if size as u64 > length - at {
            input.set_len(at)?;
            input.sync_all()?;
            break;
        }
        let mut bytes = vec![0; size];
        bytes[..recovery::PREFIX].copy_from_slice(&prefix);
        input.read_exact(&mut bytes[recovery::PREFIX..])?;
        let (branch, pill) = recovery::read(&bytes, 256 << 20)?;
        visit(branch, pill, &bytes)?;
        at += size as u64;
    }
    Ok(())
}

impl Node {
    pub fn prepare(&self, branch: u16, clock: u64) -> Result<()> {
        let storage = self.trie().route(branch)?.event_storage()?;
        let base = format!("{}/{}", storage.directory, storage.stem);
        let mut runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        if !runtime.lanes.contains_key(&base) {
            let lane = Lane::open(self, branch, &base, clock, &mut runtime.scheduler)?;
            runtime.lanes.insert(base, lane);
        }
        Ok(())
    }
    pub(crate) fn recover(&self, clock: u64) -> Result<()> {
        let branches = self
            .trie()
            .definitions()?
            .resources()
            .iter()
            .map(|r| r.branch)
            .collect::<Vec<_>>();
        let mut runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        for (branch, base) in self.sources(&branches)? {
            let trie = self.trie();
            let definition = trie
                .registered()
                .definition(trie.route(branch)?.definition)?;
            let ingress = (0..definition.prescription().definition().fields().len()).any(|i| {
                definition
                    .annotations(i)
                    .unwrap_or_default()
                    .iter()
                    .any(|a| a == "@time" || a.starts_with("@time("))
            });
            if !ingress && spool_paths(&self.root.join(&base))?.is_empty() {
                continue;
            }
            if self.root.join(format!("{base}.time")).exists()
                || !spool_paths(&self.root.join(&base))?.is_empty()
            {
                let lane = Lane::open(self, branch, &base, clock, &mut runtime.scheduler)?;
                runtime.lanes.insert(base, lane);
            }
        }
        Ok(())
    }
    pub fn ingest(&self, branch: u16, pill: &[u8], now: u64) -> Result<u64> {
        let storage = self.trie().route(branch)?.event_storage()?;
        let base = format!("{}/{}", storage.directory, storage.stem);
        let mut runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        if !runtime.lanes.contains_key(&base) {
            let lane = Lane::open(self, branch, &base, now, &mut runtime.scheduler)?;
            runtime.lanes.insert(base.clone(), lane);
        }
        let lane = runtime.lanes.get_mut(&base).unwrap();
        if let Some(error) = &lane.failed {
            return Err(error.clone().into());
        }
        let time = lane
            .live
            .inspect(branch, pill, now)
            .map_err(crate::InputError)?;
        let hour = time / TIME_HOUR_MS * TIME_HOUR_MS;
        lane.reserve(hour, pill.len() + 16, &self.memory)?;
        let result = (|| -> Result<u64> {
            lane.rotate(now / TIME_HOUR_MS * TIME_HOUR_MS)?;
            lane.spool(hour)?;
            let Lane { live, files, .. } = lane;
            Ok(data_seeds::registration::ingress::files::ingest(
                live,
                files.get_mut(&hour).unwrap(),
                branch,
                pill,
                now,
            )?)
        })();
        if let Err(e) = &result {
            lane.failed = Some(e.to_string());
        }
        result
    }
    pub fn commit(&self, now: u64) -> Result<usize> {
        let mut runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        let Runtime { lanes, scheduler } = &mut *runtime;
        let mut count = 0;
        for lane in lanes.values_mut() {
            if let Some(e) = &lane.failed {
                return Err(e.clone().into());
            }
            let plan = lane.live.commits(now)?;
            for hour in plan.hours {
                if let Err(e) = lane.publish(self, hour, now, scheduler) {
                    lane.failed = Some(e.to_string());
                    return Err(e);
                }
                count += 1;
            }
            if plan.through > lane.live.through() {
                let path = suffix(&lane.prefix, ".time");
                let time = TimeTrie::open(Arc::new(bounded(&path, 16 << 20)?))?;
                if let Some(time) = lane.live.completion(&time, now)? {
                    atomic(&path, time.bytes())?;
                    lane.generation = self.changed();
                    lane.live.advance(time.header().sealed)?;
                }
            }
            if let Err(e) = lane.publish_summaries(self, scheduler) {
                lane.failed = Some(e.to_string());
                return Err(e);
            }
        }
        Ok(count)
    }
}
impl Lane {
    fn publish_summaries(&mut self, node: &Node, scheduler: &mut CommitScheduler) -> Result<()> {
        if !self.live.has_summaries() {
            return Ok(());
        }
        let end = self.live.through();
        let day_ms = data_seeds::summaries::SUMMARY_DAY_MS;
        while self
            .summary_day
            .checked_add(day_ms)
            .is_some_and(|next| next <= end)
        {
            let first = self.summary_day;
            let path = suffix(&self.prefix, ".summ");
            let (bytes, length, _lease) = if path.exists() {
                let (bytes, length, lease) = footer(&path, &node.memory)?;
                (bytes, length, Some(lease))
            } else {
                (Vec::new(), 0, None)
            };
            let write =
                self.live
                    .summary_prepare(&node.trie(), first, end, &bytes, length, true)?;
            node.publish_registry(
                &write.trie,
                Some((
                    data_seeds::registration::ingress::SUMMARY_FILE,
                    &write.document,
                )),
            )?;
            let publication = write.commit();
            atomic(
                &suffix(&self.prefix, ".summ.pack"),
                &publication.checkpoint()?,
            )?;
            apply_summary(&self.prefix, &publication, scheduler)?;
            self.generation = node.changed();
            std::fs::remove_file(suffix(&self.prefix, ".summ.pack"))?;
            sync_directory(self.prefix.parent().unwrap())?;
            self.live.summary_published(first);
            self.summary_day += day_ms;
        }
        Ok(())
    }
    fn open(
        node: &Node,
        branch: u16,
        base: &str,
        now: u64,
        scheduler: &mut CommitScheduler,
    ) -> Result<Self> {
        let prefix = node.root.join(base);
        let storage = node.configuration.storage;
        if !suffix(&prefix, storage.suffix()).try_exists()?
            && suffix(&prefix, storage.alternative().suffix()).try_exists()?
        {
            return Err(format!("Storage configuration selects {} but this datastore contains {}; changing configuration does not convert stored data", storage.suffix(), storage.alternative().suffix()).into());
        }
        let parent = prefix.parent().ok_or("Missing resource directory")?;
        fs::create_dir_all(parent)?;
        // Canonical containment applies to directories created by registrations too.
        within(
            &node.root,
            &node.trie().route(branch)?.event_storage()?.directory,
        )?;
        restore_summary(&prefix, scheduler)?;
        for path in spool_paths(&prefix)? {
            let mut committed = false;
            records(&path, |reference, _, raw| {
                if committed {
                    return Err("Recovery data follows a commit checkpoint".into());
                }
                if reference == 0 {
                    apply(
                        &prefix,
                        node.configuration.storage,
                        &Publication::read(raw)?,
                        scheduler,
                    )?;
                    committed = true;
                }
                Ok(())
            })?;
            if committed {
                fs::remove_file(&path)?;
                sync_directory(parent)?;
            }
        }
        let time_path = suffix(&prefix, ".time");
        if !time_path.exists() {
            let first = now.saturating_sub(IngressLimits::default().straggler_ms) / TIME_HOUR_MS
                * TIME_HOUR_MS;
            let mut generation = [0; 16];
            File::open("/dev/urandom")?.read_exact(&mut generation)?;
            let mut writer = data_seeds::packs::Writer::new(&[])?;
            atomic(
                &suffix(&prefix, node.configuration.storage.suffix()),
                writer.commit()?.bytes,
            )?;
            atomic(&suffix(&prefix, ".indx"), &[])?;
            atomic(&time_path, TimeTrie::create(first, generation, 0)?.bytes())?;
            node.changed();
        }
        let time = TimeTrie::open(Arc::new(bounded(&time_path, 16 << 20)?))?;
        let leaf_path = node.root.join(node.trie().route(branch)?.leaf_path()?);
        if !leaf_path.try_exists()? {
            let trie = node.trie();
            let mut leaves = trie.branches().select([branch]).leaves()?.create()?;
            leaves.register(branch)?;
            atomic(&leaf_path, &leaves.publish()?)?;
        }
        let calendar = time.summary_calendar()?;
        let scope = node.trie().branches().select([branch]).packs()?;
        let state = data_seeds::registration::ingress::StatePaths::registered(
            &node.trie(),
            branch,
            &scope,
        )?;
        let mut live = OwnedIngress::new(
            scope,
            branch,
            IngressLimits::default(),
            time.header().sealed,
        )?;
        let summary_path = suffix(&prefix, ".summ");
        let (summary_footer, summary_length, _lease) = if summary_path.exists() {
            let (b, l, c) = footer(&summary_path, &node.memory)?;
            (b, l, Some(c))
        } else {
            (Vec::new(), 0, None)
        };
        let summary_start = live.summaries_start(
            node.trie().registered(),
            &time,
            &[],
            &summary_footer,
            summary_length,
        )?;
        let mut lane = Self {
            generation: node.changed(),
            branch,
            prefix,
            live,
            state,
            head: now / TIME_HOUR_MS * TIME_HOUR_MS,
            summary_day: calendar.day(time.header().first)?.start,
            files: BTreeMap::new(),
            charges: BTreeMap::new(),
            charged: BTreeMap::new(),
            failed: None,
        };
        if let Some(summary_start) = summary_start {
            let start = summary_start.first;
            lane.summary_day = start;
            if start < time.header().sealed {
                if let Some(reader) = node.reader(branch, start, time.header().sealed)? {
                    for i in 0..data_seeds::PackSource::len(&reader) {
                        lane.live
                            .summary_restore_snapshot(reader.pack(i)?.as_ref())?;
                    }
                }
            }
        }
        for path in spool_paths(&lane.prefix)? {
            let mut hour = None;
            records(&path, |branch, pill, _| {
                if branch == 0 {
                    return Err("Unrecovered commit checkpoint".into());
                }
                if let Some(time) = lane.live.recover(branch, pill)? {
                    let at = time / TIME_HOUR_MS * TIME_HOUR_MS;
                    if hour.is_some_and(|h| h != at) {
                        return Err("Recovery spool mixes hours".into());
                    }
                    hour = Some(at);
                    lane.reserve(at, pill.len() + 16, &node.memory)?;
                }
                Ok(())
            })?;
            if let Some(hour) = hour {
                let staged = suffix(&lane.prefix, &format!(".{hour}.pack"));
                if path != staged {
                    fs::rename(&path, &staged)?;
                    sync_directory(staged.parent().unwrap())?;
                }
                let mut input = file(&staged)?;
                input.seek(SeekFrom::End(0))?;
                lane.files.insert(hour, input);
            } else {
                fs::remove_file(&path)?;
            }
        }
        Ok(lane)
    }
    fn reserve(&mut self, hour: u64, bytes: usize, budget: &MemoryBudget) -> Result<()> {
        let used = self.charged.get(&hour).copied().unwrap_or(0);
        let next = used
            .checked_add(bytes.checked_mul(2).ok_or("Ingress budget overflow")?)
            .ok_or("Ingress budget overflow")?;
        let chunk = 64 << 10;
        let extra = next.div_ceil(chunk) - used.div_ceil(chunk);
        if extra > 0 {
            self.charges
                .entry(hour)
                .or_default()
                .push(budget.reserve(extra * chunk)?);
        }
        self.charged.insert(hour, next);
        Ok(())
    }
    fn rotate(&mut self, hour: u64) -> Result<()> {
        if hour > self.head {
            let path = suffix(&self.prefix, ".seeds");
            if path.exists() {
                fs::rename(path, suffix(&self.prefix, &format!(".{}.pack", self.head)))?;
                sync_directory(self.prefix.parent().unwrap())?;
            }
            self.head = hour;
        }
        Ok(())
    }
    fn spool(&mut self, hour: u64) -> Result<&mut File> {
        if !self.files.contains_key(&hour) {
            let path = if hour == self.head {
                suffix(&self.prefix, ".seeds")
            } else {
                suffix(&self.prefix, &format!(".{hour}.pack"))
            };
            let mut output = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            output.seek(SeekFrom::End(0))?;
            sync_directory(self.prefix.parent().unwrap())?;
            self.files.insert(hour, output);
        }
        Ok(self.files.get_mut(&hour).unwrap())
    }
    fn publish(
        &mut self,
        node: &Node,
        hour: u64,
        now: u64,
        scheduler: &mut CommitScheduler,
    ) -> Result<()> {
        self.rotate(now / TIME_HOUR_MS * TIME_HOUR_MS)?;

        let time = bounded(&suffix(&self.prefix, ".time"), 16 << 20)?;
        let storage = node.configuration.storage;
        let (footer, length, _lease) =
            footer(&suffix(&self.prefix, storage.suffix()), &node.memory)?;
        let publication = if self.state.is_empty() {
            self.live
                .publication(hour, now, &time, &footer, length, storage.compression())?
        } else {
            let trie = node.trie();
            let leaves = trie.branches().select([self.branch]).leaves()?;
            let path = node.root.join(trie.route(self.branch)?.leaf_path()?);
            // Shared with metadata writers for this resource. Readers retain immutable snapshots.
            let directory = File::open(path.parent().ok_or("Missing leaf directory")?)?;
            directory.lock()?;
            let bytes = bounded(&path, 16 << 20)?;
            let (publication, update) = self.live.publish_factored(
                hour,
                now,
                &time,
                &footer,
                length,
                storage.compression(),
                &self.state,
                &leaves,
                &bytes,
                16 << 20,
            )?;
            if let Some(update) = update {
                if update.len() > 16 << 20 {
                    return Err("Leaf file exceeds node read limit".into());
                }
                // Persist immutable identities before their referencing checkpoint.
                // A retry can safely reuse a committed prefix from these roots.
                atomic(&path, &update)?;
            }
            publication
        };
        let checkpoint = publication.checkpoint()?;
        let spool = self.spool(hour)?;
        spool.write_all(&checkpoint)?;
        spool.sync_all()?;
        apply(&self.prefix, storage, &publication, scheduler)?;
        self.generation = node.changed();
        self.live.published(hour)?;
        self.files.remove(&hour);
        self.charges.remove(&hour);
        self.charged.remove(&hour);
        fs::remove_file(suffix(&self.prefix, &format!(".{hour}.pack")))?;
        sync_directory(self.prefix.parent().unwrap())?;
        Ok(())
    }
}
fn restore_summary(prefix: &Path, scheduler: &mut CommitScheduler) -> Result<()> {
    let path = suffix(prefix, ".summ.pack");
    if path.exists() {
        let plan = SummaryCommit::restore(&bounded(&path, 64 << 20)?, 64 << 20)?;
        apply_summary(prefix, &plan, scheduler)?;
        std::fs::remove_file(path)?;
        sync_directory(prefix.parent().unwrap())?;
    }
    Ok(())
}
pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
