//! Native I/O selection. Time, indexes, codecs and evaluation remain Seed APIs.
use crate::{Node, Result};
use data_seeds::reader::{IndexSelection, Selection};
use data_seeds::writer::NativeFiles;
use data_seeds::{Cursor, PackSource, Query, TimeTrie};
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub(crate) fn within(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
        || relative.as_os_str().is_empty()
    {
        return Err("Invalid resource path".into());
    }
    let path = root.join(relative).canonicalize()?;
    if !path.starts_with(root.canonicalize()?) {
        return Err("Resource escapes Trunk directory".into());
    }
    Ok(path)
}
pub(crate) fn bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > maximum as u64 {
        return Err("Metadata exceeds node limit".into());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err("Metadata exceeds node limit".into());
    }
    Ok(bytes)
}

impl Node {
    pub fn catalog(&self) -> Result<serde_json::Value> {
        let runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        let trie = self.trie();
        let resources = trie.definitions()?.resources();
        let mut out = std::collections::BTreeMap::<String, serde_json::Value>::new();
        for r in resources {
            let route = trie.route(r.branch)?;
            let storage = route.storage()?;
            let relative = format!("{}/{}.time", storage.directory, storage.stem);
            let window = if route.pods()?.is_none() && self.root.join(&relative).exists() {
                let time = TimeTrie::open(Arc::new(bounded(
                    &within(&self.root, &relative)?,
                    16 << 20,
                )?))?;
                Some(
                    serde_json::json!({"from":time.header().first,"to":runtime.counts(&format!("{}/{}",storage.directory,storage.stem)).iter().map(|(h,_)|h+data_seeds::TIME_HOUR_MS).max().unwrap_or(time.header().sealed).max(time.header().sealed)}),
                )
            } else {
                None
            };
            let source = route.source()?;
            let mut item = serde_json::json!({"route":route,"source":source,
                "definition":trie.registered().describe(r.definition)?,"window":window});
            if let Some(old) = out.get(&source) {
                let merged = match (old["window"].as_object(), item["window"].as_object()) {
                    (Some(a), Some(b)) => {
                        serde_json::json!({"from":a["from"].as_u64().unwrap().min(b["from"].as_u64().unwrap()),"to":a["to"].as_u64().unwrap().max(b["to"].as_u64().unwrap())})
                    }
                    (Some(_), None) => old["window"].clone(),
                    _ => item["window"].clone(),
                };
                if (
                    old["route"]["major"].as_u64(),
                    old["route"]["minor"].as_u64(),
                ) > (Some(route.major.into()), Some(route.minor.into()))
                {
                    item = old.clone();
                }
                item["window"] = merged;
            }
            out.insert(source, item);
        }
        Ok(serde_json::Value::Array(out.into_values().collect()))
    }

    /// Execute through the existing registered, positional native reader. No
    /// entire .beds file upload, client replica or host-side event filtering.
    pub fn query(&self, branch: u16, from: u64, to: u64, query: Query) -> Result<Cursor> {
        self.query_many(&[branch], from, to, query)
            .map(|(cursor, _)| cursor)
    }

    pub fn query_many(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        query: Query,
    ) -> Result<(Cursor, Vec<u16>)> {
        if from >= to {
            return Err("Invalid query time window".into());
        }
        let runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        let (readers, origins) = self.inputs(&runtime, branches, from, to)?;
        drop(runtime);
        Ok((self.cursor(readers, query)?, origins))
    }

    fn inputs(
        &self,
        runtime: &crate::ingress::Runtime,
        branches: &[u16],
        from: u64,
        to: u64,
    ) -> Result<(Vec<Box<dyn PackSource>>, Vec<u16>)> {
        let mut readers: Vec<Box<dyn PackSource>> = Vec::new();
        let mut origins = Vec::new();
        for (version, base) in self.sources(branches)? {
            if let Some(reader) = self.reader(version, from, to)? {
                origins.extend(std::iter::repeat_n(version, reader.len()));
                readers.push(Box::new(reader));
            }
            if let Some(reader) = runtime.source(&base, from, to, &self.memory)? {
                origins.extend(std::iter::repeat_n(version, reader.len()));
                readers.push(Box::new(reader));
            }
        }
        Ok((readers, origins))
    }
    pub(crate) fn cursor(
        &self,
        readers: Vec<Box<dyn PackSource>>,
        query: Query,
    ) -> std::result::Result<Cursor, data_seeds::Error> {
        data_seeds::reader::Reader::query_sources(readers, query)?
            .memory_budget(self.memory.clone())
    }

    /// Bind and pin both inputs under the same publication lock.
    pub fn join(
        &self,
        branches: &[u16],
        from: u64,
        to: u64,
        plan: &data_seeds::relational::join::Plan,
        limit: Option<usize>,
        context: &data_seeds::cast::Context,
    ) -> Result<data_seeds::relational::join::Join> {
        if from >= to || branches.is_empty() {
            return Err("Join requires a branch scope and time window".into());
        }
        let runtime = self.runtime.lock().map_err(|_| "Native writer poisoned")?;
        let trie = self.trie();
        let mut path = trie.path(trie.branch(branches[0])?.id)?;
        for &id in &branches[1..] {
            let other = trie.path(trie.branch(id)?.id)?;
            let common = path.iter().zip(&other).take_while(|(a, b)| a == b).count();
            path.truncate(common);
        }
        let scope = *path.last().ok_or("Missing join scope")?;
        let resolve = |input: &data_seeds::relational::join::Input| -> Result<_> {
            let ids = if input.path == ["events"] {
                branches.to_vec()
            } else {
                trie.resolve(scope, &input.path)?
            };
            let mut defs = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for &id in &ids {
                for version in trie.branch_versions(id)? {
                    let route = trie.route(version)?;
                    if route.pods()?.is_some() {
                        return Err("Join inputs must be event sources".into());
                    }
                    if seen.insert(route.definition) {
                        defs.push(trie.registered().definition(route.definition)?);
                    }
                }
            }
            Ok((ids, defs))
        };
        let (left, ld) = resolve(&plan.inputs[0])?;
        let (right, rd) = resolve(&plan.inputs[1])?;
        let (left, _) = self.inputs(&runtime, &left, from, to)?;
        let (right, _) = self.inputs(&runtime, &right, from, to)?;
        drop(runtime);
        Ok(plan.bind([&ld, &rd])?.open(
            self.memory.clone(),
            limit,
            |query| self.cursor(left, query.context(context.clone())),
            |query| self.cursor(right, query.context(context.clone())),
        )?)
    }

    pub(crate) fn reader(
        &self,
        branch: u16,
        from: u64,
        to: u64,
    ) -> Result<Option<data_seeds::reader::Reader>> {
        let trie = self.trie();
        let route = trie.route(branch)?;
        let paths = route.event_storage()?;
        let base = format!("{}/{}", paths.directory, paths.stem);
        if !self.root.join(format!("{base}.time")).exists() {
            return Ok(None);
        }
        let time = Arc::new(TimeTrie::open(Arc::new(bounded(
            &within(&self.root, &format!("{base}.time"))?,
            16 << 20,
        )?))?);
        let mut scope = trie.branches().select([branch]).packs()?;
        let storage = self.configuration.storage;
        let mut addresses = Vec::new();
        let mut hours = Vec::new();
        let mut needs_index = false;
        for leaf in time.range(from, to)? {
            let leaf = leaf?;
            if leaf.count == 0 {
                continue;
            }
            addresses.push(storage.address(&leaf));
            hours.push(leaf.hour);
            needs_index |= leaf.end > leaf.start;
        }
        let budget = self.memory.clone();
        if addresses.is_empty() {
            return Ok(None);
        }
        let data = File::open(within(&self.root, &format!("{base}{}", storage.suffix()))?)?;
        let (footer,length,_charge) = crate::publication::footer(&data,&self.memory)?;
        scope.box_footer(footer,length)?;
        let mut files = HashMap::from([(1, data)]);
        let index_length = if needs_index {
            let file = File::open(within(&self.root, &format!("{base}.indx"))?)?;
            let length = file.metadata()?.len();
            files.insert(2, file);
            Some(length)
        } else {
            None
        };
        let leaves = if index_length.is_some() && self.root.join(route.leaf_path()?).try_exists()? {
            let bytes=bounded(&within(&self.root,&route.leaf_path()?)?,16<<20)?;
            trie.branches().select([branch]).leaves()?.read_roots(&bytes,&scope)?
        } else { Vec::new() };
        let mut reader = scope.reader(
            Arc::new(NativeFiles(files)),
            Selection {
                file: 1,
                length,
                addresses,
            },
            32 << 20,
            budget.clone(),
        )?;
        if let Some(length) = index_length {
            reader.index(IndexSelection {
                file: 2,
                length,
                time,
                hours,
                space: storage.space(),
                leaves,
            })?;
        }
        // Time selects intersecting hours. The existing reader applies the
        // precise event-time bounds using registered annotation plans.
        if index_length.is_none() {
            return Err("Event queries require the registered hour index".into());
        }
        Ok(Some(reader.range(from, to)?))
    }
}
