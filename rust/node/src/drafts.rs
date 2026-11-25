//! A draft owns one disposable storage directory. The registry selects the live
//! generation. A single registry publication switches readers before reclamation.
use crate::{
    Node, Result,
    ingress::{self, Runtime},
    registration::{Action, Registered, Target, filename, invalid, optional},
};
use data_seeds::{Document, doc, registration::Request};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use trie_trunk_taxonomy::registration::{Draft, Selection};

use data_seeds::registration::stamp;

pub(super) fn release(candidate: Document, previous: &[u8]) -> Result<(Document, Selection)> {
    let h = &candidate.definitions()[0].header;
    let mut selection = Selection {
        namespace: h.namespace.clone(),
        name: h.rx_name.clone(),
        major: 1,
        minor: 0,
    };
    if previous.is_empty() {
        return Ok((stamp(candidate, 1, 0)?, selection));
    }
    let before = doc::read(previous)?;
    let (i, head) = before
        .definitions()
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.header.namespace == selection.namespace && d.header.rx_name == selection.name
        })
        .max_by_key(|(_, d)| (d.header.rx_major, d.header.rx_minor))
        .ok_or("Wrong Definition lineage")?;
    let (major, minor) = (head.header.rx_major, head.header.rx_minor);
    if major == 0 {
        return Err(invalid("Published lineage contains reserved major zero").into());
    }
    let at_version = stamp(candidate.clone(), major, minor)?;
    if at_version.bytes()? == before.clone().retain_members(&[i])?.bytes()? {
        selection.major = major;
        selection.minor = minor;
        return Ok((before, selection));
    }
    if candidate.compare_member(0, &before, i)?.choice() == doc::VersionChange::Major {
        selection.major = major.checked_add(1).ok_or("Major version exhausted")?;
    } else {
        selection.major = major;
        selection.minor = minor.checked_add(1).ok_or("Minor version exhausted")?;
    }
    let next = stamp(candidate, selection.major, selection.minor)?;
    Ok((before.compound(next.bytes()?).sign()?, selection))
}

fn source(target: &Target) -> Result<String> {
    let parts = [
        &target.cache,
        &target.service,
        &target.resource,
        &target.resource_type,
    ];
    if target.branch.is_some()
        || !target.split.is_empty()
        || parts.iter().any(|p| {
            p.is_empty()
                || p.starts_with('.')
                || p.contains(['/', '\\', '\0'])
                || p.chars().any(char::is_control)
        })
    {
        return Err(invalid("Draft requires cache, service, resource and type").into());
    }
    Ok(parts.map(String::as_str).join("/"))
}
fn token() -> Result<String> {
    let mut bytes = [0; 16];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(crate::presentation::hex(&bytes))
}
impl Node {
    fn drafts_dir(&self) -> PathBuf {
        self.root.join(".trunk/drafts")
    }
    fn draft_root(&self, draft: &Draft) -> PathBuf {
        self.drafts_dir().join(&draft.generation)
    }
    fn draft_rx(&self, draft: &Draft) -> Result<Vec<u8>> {
        crate::read::bounded(
            &crate::read::within(&self.draft_root(draft).join("definitions"), &draft.file)?,
            16 << 20,
        )
    }
    fn retire(&self, root: &Path) -> Result<()> {
        fs::remove_dir_all(root)?;
        crate::sync_directory(&self.drafts_dir())?;
        Ok(())
    }
    pub(super) fn cleanup_drafts(&self) -> Result<()> {
        let dir = self.drafts_dir();
        if !dir.try_exists()? {
            return Ok(());
        }
        if !fs::symlink_metadata(&dir)?.is_dir() {
            return Err("Invalid draft directory".into());
        }
        let trie = self.trie();
        let registry = trie.definitions()?;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() == 32
                && name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && entry.file_type()?.is_dir()
                && !registry.drafts.values().any(|d| d.generation == name)
            {
                self.retire(&entry.path())?;
            }
        }
        Ok(())
    }
    fn stage(
        &self,
        target: &Target,
        request: &Request,
        candidate: &Document,
        draft: &Draft,
    ) -> Result<()> {
        // The ordinary reader, spool, and commit code open this isolated taxonomy.
        let mut working = request.clone();
        working.file = draft.file.clone();
        let prepared = working.prepare(
            &trie_trunk_taxonomy::Trunk::new(&target.cache)?,
            candidate.bytes()?,
            &[],
            &[],
        )?;
        let dir = self.drafts_dir();
        fs::create_dir_all(&dir)?;
        if !fs::symlink_metadata(&dir)?.is_dir() {
            return Err("Invalid draft directory".into());
        }
        crate::sync_directory(&self.root.join(".trunk"))?;
        let root = self.draft_root(draft);
        fs::create_dir(&root)?;
        crate::sync_directory(&dir)?;
        let defs = root.join("definitions");
        fs::create_dir(&defs)?;
        ingress::atomic(&defs.join(&working.file), candidate.bytes()?)?;
        if let Some(summary) = prepared.summaries {
            ingress::atomic(
                &defs.join(data_seeds::registration::ingress::SUMMARY_FILE),
                summary.bytes()?,
            )?;
        }
        ingress::atomic(&root.join("trunk.trie"), prepared.trie.bytes())?;
        Ok(())
    }

    pub(super) fn with_draft<T>(
        &self,
        source: &str,
        generation: &str,
        use_node: impl FnOnce(Node) -> Result<T>,
    ) -> Result<T> {
        let _owner = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        let trie = self.trie();
        let draft = trie
            .definitions()?
            .drafts
            .get(source)
            .filter(|d| d.generation == generation)
            .ok_or_else(|| invalid("Draft changed. Reopen it before continuing."))?;
        let node = Node::open_scoped(
            &self.draft_root(draft),
            self.configuration.clone(),
            self.memory.clone(),
            ingress::now(),
        )?;
        use_node(node)
    }
    pub(super) fn draft_action(
        &self,
        runtime: &mut Runtime,
        target: &Target,
        action: &Action,
        bytes: &[u8],
    ) -> Result<Registered> {
        let source = source(target)?;
        let trie = self.trie();
        let registry = trie.definitions()?;
        let current = registry.drafts.get(&source);
        if current.map(|d| &d.generation) != target.generation.as_ref() {
            return Err(invalid("Draft changed. Reopen it before continuing.").into());
        }
        let response = |d: &Draft| Registered {
            branch: 0,
            file: d.file.clone(),
            generation: Some(d.generation.clone()),
            bytes: None,
            model: None,
        };
        if matches!(action, Action::Save) {
            let request = self.registration_request(target, bytes)?;
            filename(&request.file)?;
            let candidate = stamp(doc::read(bytes)?, 0, 0)?;
            if let Some(old) = current {
                if self.draft_rx(old)? == candidate.bytes()? {
                    return Ok(response(old));
                }
                if self.draft_root(old).join(&target.cache).try_exists()? && !target.discard {
                    return Err(invalid("Saving a changed Definition discards its draft data. Confirm discard first.").into());
                }
            }
            let draft = Draft {
                generation: token()?,
                basis: registry
                    .artifacts()
                    .get(&request.file)
                    .map(|a| a.etag.clone()),
                file: format!("{}.0.rx", target.resource_type),
            };
            if let Some(old) = current {
                if old.basis != draft.basis {
                    return Err(
                        invalid("Published Definition changed since this draft was saved").into(),
                    );
                }
            }
            let next = trie.with_draft(&source, Some(draft.clone()))?;
            self.stage(target, &request, &candidate, &draft)?;
            self.publish_registry(&next, None)?;
            if let Some(old) = current {
                self.retire(&self.draft_root(old))?;
            }
            return Ok(response(&draft));
        }
        let draft = current.ok_or_else(|| invalid("No saved draft for this type"))?;
        let saved = self.draft_rx(draft)?;
        match action {
            Action::Read => Ok(Registered { bytes: Some(saved), ..response(draft) }),
            Action::Inspect => Ok(Registered {
                model: Some(data_seeds::ports::model::model(
                    doc::read(saved.as_slice())?,
                    0,
                )?),
                ..response(draft)
            }),
            Action::Ingest => {
                let clock = ingress::now();
                let working = Node::open_scoped(
                    &self.draft_root(draft),
                    self.configuration.clone(),
                    self.memory.clone(),
                    clock,
                )?;
                let branch = working.trie().definitions()?.resources()[0].branch;
                working.ingest(branch, bytes, clock)?;
                working.commit(clock)?;
                Ok(response(draft))
            }
            Action::Promote => {
                let request = self.registration_request(target, &saved)?;
                filename(&request.file)?;
                if registry.artifacts().get(&request.file).map(|a| &a.etag) != draft.basis.as_ref()
                {
                    return Err(
                        invalid("Published Definition changed since this draft was saved").into(),
                    );
                }
                let previous = optional(&self.root.join("definitions").join(&request.file))?;
                let (document, selection) = release(doc::read(saved.as_slice())?, &previous)?;
                let request = Request {
                    selection: Some(selection),
                    ..request
                };
                let branch = self.publish_type(
                    runtime,
                    &request,
                    document.bytes()?,
                    &previous,
                    Some(&source),
                )?;
                self.retire(&self.draft_root(draft)).map_err(|e| {
                    format!("Definition promoted; draft cleanup will retry on restart: {e}")
                })?;
                Ok(Registered {
                    branch,
                    file: request.file,
                    generation: None,
                    bytes: None,
                    model: None,
                })
            }
            Action::Save => unreachable!(),
        }
    }
}
