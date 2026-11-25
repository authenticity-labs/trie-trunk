//! Ephemeral resource discovery projected from trunk.trie. File existence and
//! lengths come from host I/O; registrations and storage names come from Rust.
use crate::{Error, Trunk};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct File {
    pub path: String,
    #[serde(rename = "branchId", skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<u16>,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub definitions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    pub size: u64,
    pub bootstrap: bool,
}
#[derive(Debug, Serialize)]
pub struct Binding {
    pub tap: String,
    pub definition: String,
    #[serde(rename = "branchId")]
    pub branch_id: u16,
}
#[derive(Debug, Serialize)]
pub struct Catalog {
    pub scope: String,
    pub directories: Vec<String>,
    pub files: Vec<File>,
    pub sources: Vec<String>,
    pub collections: Vec<String>,
    pub bindings: Vec<Binding>,
}
impl Trunk {
    pub fn catalog(&self, cache: &str) -> Result<Catalog, Error> {
        let registry = self.definitions()?;
        let mut directories = BTreeSet::new();
        let mut sources = BTreeSet::new();
        let mut collections = BTreeSet::new();
        let mut files = BTreeMap::new();
        let mut bindings = Vec::new();
        let mut routes = registry.resources().iter().map(|r| self.route(r.branch))
            .collect::<Result<Vec<_>, _>>()?;
        routes.sort_by_key(|r| (r.major, r.minor));
        for route in routes.into_iter().filter(|r| cache.is_empty() || r.cache == cache) {
            let storage = route.storage()?;
            let tap = route.source()?;
            sources.insert(tap.clone());
            bindings.push(Binding { tap, definition: route.digest.clone(), branch_id: route.branch });
            directories.insert(route.cache.clone());
            directories.insert(format!("{}/{}", route.cache, route.service));
            directories.insert(storage.directory.clone());
            if let Some(path)=route.pods()? {
                collections.insert(route.source()?);
                files.insert(path.clone(),File {path,branch_id:Some(route.branch),definitions:vec![],kind:"pods".into(),definition:Some(route.digest.clone()),digest:None,size:0,bootstrap:false});
            } else {
            for (suffix, kind) in [("time", "time"), ("summ", "sum"), ("indx", "idx"), ("packs", "packs"), ("beds", "beds")] {
                let path = format!("{}/{}.{}", storage.directory, storage.stem, suffix);
                files.insert(path.clone(), File { path, branch_id:Some(route.branch), definitions:vec![], kind:kind.into(), definition:Some(route.digest.clone()), digest:None, size:0, bootstrap:false });
            }
            let path = route.leaf_path()?;
            let state = files.entry(path.clone()).or_insert_with(|| File { path, branch_id:None, definitions:vec![], kind:"state".into(), definition:None, digest:None, size:0, bootstrap:true });
            state.branch_id = Some(route.branch);
            if !state.definitions.contains(&route.digest) { state.definitions.push(route.digest.clone()); }
            }
            let definition = self.definition(route.definition)?;
            let artifact = registry.artifact(&definition.file)?;
            let path = format!("definitions/{}", definition.file);
            files.insert(path.clone(), File { path, branch_id:None, definitions:vec![], kind:"definition".into(), definition:None, digest:Some(artifact.etag.clone()), size:artifact.size, bootstrap:true });
            for id in &registry.resources().iter().find(|r| r.branch == route.branch).ok_or("Unregistered branch")?.summaries {
                let definition = self.definition(*id)?;
                let artifact = registry.artifact(&definition.file)?;
                let path = format!("definitions/{}", definition.file);
                files.insert(path.clone(), File { path, branch_id:None, definitions:vec![], kind:"definition".into(), definition:None, digest:Some(artifact.etag.clone()), size:artifact.size, bootstrap:true });
            }
        }
        let path = "trunk.trie".to_owned();
        files.insert(path.clone(), File { path, branch_id:None, definitions:vec![], kind:"taxonomy".into(), definition:None, digest:None, size:self.bytes().len() as u64, bootstrap:true });
        bindings.sort_by(|a,b| (&a.tap, a.branch_id).cmp(&(&b.tap, b.branch_id)));
        Ok(Catalog { scope:cache.into(), directories:directories.into_iter().collect(), files:files.into_values().collect(), sources:sources.into_iter().collect(), collections:collections.into_iter().collect(), bindings })
    }
}
impl Catalog {
    /// Host observations may only complete known file descriptors. They cannot
    /// add paths or change registry bindings, types, or Definitions.
    pub fn files_present(&mut self, observed: &[File]) -> Result<(), Error> {
        let known: BTreeMap<_,_> = self.files.iter().map(|f| (f.path.as_str(), f)).collect();
        let mut observations = BTreeMap::new();
        for file in observed {
            if !known.contains_key(file.path.as_str()) || observations.insert(&file.path, file).is_some() {
                return Err("Unregistered or duplicate inventory path".into());
            }
            let expected = known[file.path.as_str()];
            if expected.branch_id != file.branch_id { return Err("Inventory changed registered branch".into()); }
            if expected.kind == "state" {
                let mut roots=BTreeSet::new();
                for definition in &file.definitions {
                    if !roots.insert(definition) || !expected.definitions.contains(definition) {
                        return Err("State root Definition is not registered to its type".into());
                    }
                }
                if file.definition.is_some() { return Err("State file uses version roots".into()); }
            }
            if expected.kind == "definition" && (file.size != expected.size || file.digest != expected.digest) {
                return Err("Definition resource changed during inventory".into());
            }
            if expected.bootstrap && file.digest.as_ref().is_none_or(|d| d.len()!=64 || !d.bytes().all(|b| b.is_ascii_hexdigit())) {
                return Err("Missing inventory content digest".into());
            }
        }
        self.files.retain_mut(|file| {
            if let Some(observed) = observations.get(&file.path) {
                file.size = observed.size;
                if file.bootstrap { file.digest = observed.digest.clone(); }
                if file.kind == "state" { file.definitions = observed.definitions.clone(); }
                true
            } else { false }
        });
        Ok(())
    }
}

impl Trunk {
    /// Resolve a shared state resource from registered taxonomy only. A branch
    /// selector may narrow the request; the returned file still contains roots.
    pub fn state_path(&self, source: &str, definition: Option<&str>) -> Result<String, Error> {
        let registry=self.definitions()?;
        let mut paths = BTreeSet::new();
        for resource in registry.resources() {
            let route=self.route(resource.branch)?;
            let storage=route.storage()?;
            if (source==storage.directory || source==route.source()?) && definition.is_none_or(|d|d==route.digest) {
                paths.insert(route.leaf_path()?);
            }
        }
        if paths.len() == 1 { Ok(paths.into_iter().next().unwrap()) }
        else if paths.is_empty() { Err("State resource is not registered".into()) }
        else { Err("Select a registered branch for this state resource".into()) }
    }
}
