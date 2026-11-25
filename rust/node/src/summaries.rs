//! Positioned summary collection over the shared registered reader/reducer.
//! Only a footer and selected daily PACs are read, never the whole datastore.
use crate::{
    Node, Result,
    read::{bounded, within},
};
use data_seeds::summaries::{SUMMARY_DAY_MS, SummaryBoxReader, SummaryReader, SummaryReport};
use data_seeds::{Predicate, TIME_HOUR_MS, TimeTrie};
use serde_json::{Value, json};
use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

impl Node {
    pub(crate) fn sources(&self, branches: &[u16]) -> Result<Vec<(u16, String)>> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for &branch in branches {
            for version in self.trie().branch_versions(branch)? {
                let route = self.trie().route(version)?;
                if route.pods()?.is_some() {
                    continue;
                }
                let storage = route.storage()?;
                let base = format!("{}/{}", storage.directory, storage.stem);
                if seen.insert(base.clone()) {
                    out.push((version, base));
                }
            }
        }
        Ok(out)
    }

    /// Read occupied hours across the selected physical stores and their open heads.
    pub fn time_bounds(&self, branches: &[u16]) -> Result<Option<[u64; 2]>> {
        let runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        let mut bounds: Option<[u64; 2]> = None;
        let mut include = |from: u64, to: u64| {
            let extent = bounds.get_or_insert([from, to]);
            extent[0] = extent[0].min(from);
            extent[1] = extent[1].max(to);
        };
        for (_, base) in self.sources(branches)? {
            let path = self.root.join(format!("{base}.time"));
            if path.try_exists()? {
                let trie = TimeTrie::open(Arc::new(bounded(&path, 16 << 20)?))?;
                if let Some(range) = trie.occupied()? { include(range.start, range.end); }
            }
            for (hour, count) in runtime.counts(&base) {
                if count > 0 { include(hour, hour.checked_add(TIME_HOUR_MS).ok_or("hour overflow")?); }
            }
        }
        Ok(bounds)
    }

    pub fn timeline(&self, branches: &[u16], from: u64, to: u64) -> Result<Value> {
        let from = from / TIME_HOUR_MS * TIME_HOUR_MS;
        let to = to
            .checked_add(TIME_HOUR_MS - 1)
            .ok_or("Time window overflow")?
            / TIME_HOUR_MS
            * TIME_HOUR_MS;
        if from >= to || (to - from) / TIME_HOUR_MS > 8 * 366 * 24 {
            return Err("Invalid time window".into());
        }
        let mut counts = vec![0u64; ((to - from) / TIME_HOUR_MS) as usize];
        let mut errors = counts.clone();
        let mut received = 0usize;
        let mut first: Option<u64> = None;
        let runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        for (branch, base) in self.sources(branches)? {
            if !self.root.join(format!("{base}.time")).exists() {
                continue;
            }
            let time = TimeTrie::open(Arc::new(bounded(
                &within(&self.root, &format!("{base}.time"))?,
                16 << 20,
            )?))?;
            first = Some(first.map_or(time.header().first, |value| value.min(time.header().first)));
            for (hour, count) in runtime.counts(&base) {
                if hour >= from && hour < to {
                    counts[((hour - from) / TIME_HOUR_MS) as usize] += count;
                    received += 1;
                }
            }
            let is_error = self.trie().route(branch)?.source()?.ends_with("/error");
            for leaf in time.range(from, to)? {
                let leaf = leaf?;
                let slot = ((leaf.hour - from) / TIME_HOUR_MS) as usize;
                counts[slot] = counts[slot]
                    .checked_add(leaf.count)
                    .ok_or("Count overflow")?;
                if is_error {
                    errors[slot] = errors[slot]
                        .checked_add(leaf.count)
                        .ok_or("Count overflow")?;
                }
                received += 1;
            }
        }
        let total = counts
            .iter()
            .try_fold(0u64, |a, b| a.checked_add(*b))
            .ok_or("Count overflow")?;
        let mut window = json!({"from":from,"to":to,"counts":counts,"errors":errors,
            "count":total,"receivedHours":received});
        if let Some(first) = first {
            window["first"] = first.into();
        }
        Ok(window)
    }

    pub fn summaries(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        step: u64,
        metric: &str,
        predicate: &Predicate,
    ) -> Result<Value> {
        let mut report =
            SummaryReport::new(from, to, step, metric)?.memory_budget(self.memory.clone())?;
        let missing = self.summary_read(branches, from, to, |_, source, reader| {
            report.consume(reader, source, from, to, predicate)?;
            Ok(())
        })?;
        let mut result = report.finish()?;
        if !missing.is_empty() {
            result["summary"]["coverage"]["complete"] = false.into();
        }
        result["missing"] = json!(missing);
        Ok(result)
    }

    fn summary_read(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        visit: impl FnMut(u16, &str, &mut SummaryReader) -> Result<()>,
    ) -> Result<Vec<Value>> {
        let mut runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        self.summary_read_locked(branches, from, to, &mut runtime, visit)
    }

    fn summary_read_locked(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        runtime: &mut crate::ingress::Runtime,
        mut visit: impl FnMut(u16, &str, &mut SummaryReader) -> Result<()>,
    ) -> Result<Vec<Value>> {
        let mut missing = Vec::new();
        for (branch, base) in self.sources(branches)? {
            let source = self.trie().route(branch)?.source()?;
            let time_path = format!("{base}.time");
            let summary_path = format!("{base}.summ");
            if !self.root.join(&time_path).exists() {
                continue;
            }
            let time = TimeTrie::open(Arc::new(bounded(
                &within(&self.root, &time_path)?,
                16 << 20,
            )?))?;
            let calendar = time.summary_calendar()?;
            let first = calendar.day(time.header().first)?.start;
            let begin = calendar.day(from.max(time.header().first))?.start;
            let end = to.min(
                runtime
                    .counts(&base)
                    .iter()
                    .map(|(h, _)| h + TIME_HOUR_MS)
                    .max()
                    .unwrap_or(time.header().sealed)
                    .max(time.header().sealed),
            );
            if begin >= end {
                continue;
            }
            if !self.root.join(&summary_path).exists() {
                let mut at = begin;
                while at < end {
                    if let Some(mut reader) = runtime.summary(self, &base, at)? {
                        visit(branch, &source, &mut reader)?;
                    } else {
                        missing.push(
                            json!({"source":source,"from":at,"to":(at+SUMMARY_DAY_MS).min(end)}),
                        );
                    }
                    at += SUMMARY_DAY_MS;
                }
                continue;
            }
            let file = File::open(within(&self.root, &summary_path)?)?;
            let length = file.metadata()?.len();
            let mut tail = vec![0; length.min(64) as usize];
            let tail_start = length - tail.len() as u64;
            file.read_exact_at(&mut tail, tail_start)?;
            let range = data_seeds::packs::footer_range(&tail, length)?;
            let size = usize::try_from(range.end - range.start)?;
            let _footer = self.memory.reserve(size)?;
            let mut footer = vec![0; size];
            file.read_exact_at(&mut footer, range.start)?;
            let index = SummaryBoxReader::open_footer_owned(footer, length)?;
            let mut at = begin;
            while at < end {
                let ordinal = usize::try_from((at - first) / SUMMARY_DAY_MS)?;
                if ordinal >= index.len() {
                    if let Some(mut reader) = runtime.summary(self, &base, at)? {
                        visit(branch, &source, &mut reader)?;
                    } else {
                        missing.push(
                            json!({"source":source,"from":at,"to":(at+SUMMARY_DAY_MS).min(end)}),
                        );
                    }
                    at += SUMMARY_DAY_MS;
                    continue;
                }
                let range = index.storage_range(ordinal)?;
                let size = usize::try_from(range.end - range.start)?;
                let _pack = self.memory.reserve(size)?;
                let mut bytes = vec![0; size];
                file.read_exact_at(&mut bytes, range.start)?;
                let mut reader =
                    SummaryReader::registered(self.trie().registered(), branch, bytes, at)?;
                visit(branch, &source, &mut reader)?;
                at = at
                    .checked_add(SUMMARY_DAY_MS)
                    .ok_or("Summary window overflow")?;
            }
        }
        Ok(missing)
    }

    pub fn summary_table(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        plan: &Value,
        context: &Value,
    ) -> Result<Value> {
        self.aggregate(branches, from, to, plan, context, || Ok(()))
    }

    pub(crate) fn aggregate(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        plan: &Value,
        context: &Value,
        check: impl Fn() -> Result<()>,
    ) -> Result<Value> {
        use data_seeds::summaries::{Coverage, SummaryTable};
        check()?;
        let mut runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        let totals = plan["source"] != "summaries" && plan["relational"]["groupBy"].is_array();
        let mut coverage = Vec::new();
        let mut stores = std::collections::BTreeMap::new();
        let mut digests = std::collections::HashMap::new();
        for (branch, base) in self.sources(branches)? {
            check()?;
            let path = format!("{base}.time");

            let trie = self.trie();
            let route = trie.route(branch)?;
            let digest = crate::presentation::hex(
                &trie.definitions()?.definitions()[route.definition as usize].digest,
            );
            if self.root.join(&path).try_exists()? {
                let time =
                    TimeTrie::open(Arc::new(bounded(&within(&self.root, &path)?, 16 << 20)?))?;
                for leaf in time.range(from, to)? {
                    let leaf = leaf?;
                    coverage.push(Coverage {
                        source: route.source()?,
                        definition: digest.clone(),
                        time: leaf.hour,
                        count: leaf.count,
                    });
                }
            }
            for (hour, count) in runtime.counts(&base) {
                if hour < to && hour + TIME_HOUR_MS > from {
                    coverage.push(Coverage {
                        source: route.source()?,
                        definition: digest.clone(),
                        time: hour,
                        count,
                    });
                }
            }
            stores.insert((route.source()?, digest.clone()), (branch, base));
            digests.insert(branch, digest);
        }
        // An unbounded SQL session requests the full published calendar.
        let to = if to == u64::MAX {
            coverage
                .iter()
                .map(|r| r.time + TIME_HOUR_MS)
                .max()
                .unwrap_or(from.saturating_add(1))
        } else {
            to
        };
        let mut table = SummaryTable::new(plan, coverage, from, to)?
            .context(data_seeds::request::context(&json!({"context":context}))?)
            .memory_budget(self.memory.clone());
        let missing = self.summary_read_locked(
            branches,
            from,
            to,
            &mut runtime,
            |branch, source, reader| {
                check()?;
                table.read(reader, source, &digests[&branch])?;
                Ok(())
            },
        )?;
        if totals {
            let mut cursors = Vec::new();
            for (source, digest, ranges) in table.remaining()? {
                check()?;
                let (branch, base) = &stores[&(source, digest)];
                let (start, end) = (ranges[0].0, ranges.last().unwrap().1);
                let mut inputs: Vec<Box<dyn data_seeds::PackSource>> = Vec::new();
                if let Some(reader) = self.reader(*branch, start, end)? {
                    inputs.push(Box::new(
                        reader.ranges(ranges.iter().map(|&(a, b)| a..b).collect())?,
                    ));
                }
                if let Some(live) = runtime.source_ranges(base, &ranges, &self.memory)? {
                    inputs.push(Box::new(live));
                }
                if !inputs.is_empty() {
                    cursors.push(self.cursor(inputs, table.event_query())?);
                }
            }
            drop(runtime);
            for mut cursor in cursors {
                loop {
                    check()?;
                    let slice = cursor.poll(256, 4096)?;
                    if !slice.batch.is_empty() {
                        table.events(&slice.batch)?;
                    }
                    if slice.done {
                        break;
                    }
                }
            }
        } else if !missing.is_empty() {
            return Err("Summary coverage is incomplete for the selected window".into());
        }
        Ok(table.finish()?)
    }
}
