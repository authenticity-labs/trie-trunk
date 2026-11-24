//! Native registration adapter. The host publishes snapshots through its
//! existing locked/durable file path; Rust owns the Definition and registry.
use crate::seed_native::{SeedSlice, input, invalid, output};
use pil_codec::doc;
use serde::Deserialize;
use std::{collections::BTreeMap, ffi::c_void};
use trie_trunk_taxonomy::{
    Trunk,
    registration::{Registration, Selection},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    cache: String,
    service: String,
    resource: String,
    resource_type: String,
    level: String,
    file: String,
    binding: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    selection: Option<Selection>,
}

/// `.rx` is read without modification. All resource-specific values are
/// persisted in the existing registration footer of the returned trunk image.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_register_resource(
    trie: SeedSlice,
    rx: SeedSlice,
    target: SeedSlice,
) -> *mut c_void {
    output((|| {
        let trie = Trunk::open(unsafe { input(trie.data, trie.len)? }).map_err(invalid)?;
        let document = doc::read(unsafe { input(rx.data, rx.len)? })?;
        let target: Target = serde_json::from_slice(unsafe { input(target.data, target.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let (next, _) = trie
            .registration_selected(
                Registration {
                    cache: &target.cache,
                    service: &target.service,
                    resource: &target.resource,
                    resource_type: &target.resource_type,
                    level: &target.level,
                    file: &target.file,
                },
                &document,
                Some(target.binding),
                target.selection.as_ref(),
            )
            .map_err(invalid)?;
        Ok(next.bytes().to_vec())
    })())
}

/// A host projection of a persisted binding, not a host-side schema decoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_resource_binding(trie: SeedSlice, branch: u16) -> *mut c_void {
    output((|| {
        let trie = Trunk::open(unsafe { input(trie.data, trie.len)? }).map_err(invalid)?;
        let registry = trie.definitions().map_err(invalid)?;
        let resource = registry
            .resources()
            .iter()
            .find(|r| r.branch == branch)
            .ok_or_else(|| invalid("Resource has no Definition registration"))?;
        serde_json::to_vec(resource).map_err(|e| invalid(e.to_string()))
    })())
}

/// Resolve an independently requested .rx resource through the authoritative
/// registry. The host never interprets the registration footer or .rx schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_definition_file(trie: SeedSlice, file: SeedSlice) -> *mut c_void {
    output((|| {
        let trie = Trunk::open(unsafe { input(trie.data, trie.len)? }).map_err(invalid)?;
        let file = std::str::from_utf8(unsafe { input(file.data, file.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let registry = trie.definitions().map_err(invalid)?;
        serde_json::to_vec(registry.artifact(file).map_err(invalid)?)
            .map_err(|e| invalid(e.to_string()))
    })())
}

/// Detect a resource replacement racing a separate trie download. This is a
/// file revision check, not repeated schema digest validation at prescription.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_definition_file_matches(artifact: SeedSlice, bytes: SeedSlice) -> *mut c_void {
    output((|| {
        let artifact: trie_trunk_taxonomy::registration::Artifact =
            serde_json::from_slice(unsafe { input(artifact.data, artifact.len)? })
                .map_err(|e| invalid(e.to_string()))?;
        if !artifact.matches(unsafe { input(bytes.data, bytes.len)? }) {
            return Err(invalid("Definition resource changed during registration publication"));
        }
        Ok(vec![])
    })())
}

/// A lineage bundle may grow or change its default while retaining every
/// previously published schema. Application annotation edits remain allowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_definition_replacement(
    before: SeedSlice,
    after: SeedSlice,
) -> *mut c_void {
    output((|| {
        let before = doc::read(unsafe { input(before.data, before.len)? })?;
        let after = doc::read(unsafe { input(after.data, after.len)? })?;
        for member in before.definitions() {
            let h = &member.header;
            let next = after.member(&h.namespace, &h.rx_name, h.rx_major, h.rx_minor)?;
            if after.definitions()[next].header.rx_hash != h.rx_hash {
                return Err(invalid(
                    "Definition replacement would change a published schema",
                ));
            }
        }
        Ok(vec![])
    })())
}

/// Bind the reusable summary.rx library during existing Definition publication.
/// The host persists the returned opaque resources separately before the trie.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_prepare_summaries(trie: SeedSlice, rx: SeedSlice, target: SeedSlice, previous: SeedSlice) -> *mut c_void {
    output((|| {
        let trie = Trunk::open(unsafe { input(trie.data, trie.len)? }).map_err(invalid)?;
        let document = doc::read(unsafe { input(rx.data, rx.len)? })?;
        let target: Target = serde_json::from_slice(unsafe { input(target.data, target.len)? }).map_err(|e| invalid(e.to_string()))?;
        let selected = match &target.selection {
            Some(s) => document.member(&s.namespace, &s.name, s.major, s.minor)?, None => 0,
        };
        let source = data_seeds::SeedDefinition::prescribe_member(&document, selected)?;
        let previous = unsafe { input(previous.data, previous.len)? };
        let header = &document.definitions()[selected].header;
        let branch = trie.definitions().map_err(invalid)?.resources().iter().find_map(|resource| {
            let route = trie.route(resource.branch).ok()?;
            (route.cache == target.cache && route.service == target.service && route.resource == target.resource
                && route.resource_type == target.resource_type && route.level == target.level
                && route.major == header.rx_major && route.minor == header.rx_minor).then_some(resource.branch)
        }).ok_or_else(|| invalid("summary source is not registered"))?;
        if source.summary_fields().is_empty() {
            if trie.definitions().map_err(invalid)?.resources().iter().any(|r| r.branch == branch && !r.summaries.is_empty()) { return Err(invalid("removing all summary annotations requires an explicit summary retirement")); }
            return serde_json::to_vec(&serde_json::json!({"trie":trie.bytes(),"rx":null})).map_err(|e|invalid(e.to_string()));
        }
        let mut definitions = if previous.is_empty() { data_seeds::summaries::SummaryDefinitions::new() }
            else { data_seeds::summaries::SummaryDefinitions::read(previous)? };
        definitions.prescribe(source, data_seeds::summaries::SUMMARY_DAY_ROWS)?;
        let summaries = definitions.document()?.ok_or_else(|| invalid("missing summary Definitions"))?;
        let file = "summary.rx";
        let next = trie.register_summaries(branch, file, &summaries).map_err(invalid)?;
        serde_json::to_vec(&serde_json::json!({"trie":next.bytes(),"rx":summaries.bytes()?})).map_err(|e|invalid(e.to_string()))
    })())
}
