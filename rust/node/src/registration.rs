//! Durable publication of the existing RX resource and registered taxonomy.
use crate::{
    Node, Result,
    read::{bounded, within},
};
use data_seeds::{Document, registration::Trunk};
use std::sync::Arc;

/// Placement supplied by an adapter; the RX body contains only the Definition.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub cache: String,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub resource: String,
    #[serde(default)]
    pub resource_type: String,
    #[serde(default)]
    pub split: String,
    pub branch: Option<u16>,
    pub action: Option<Action>,
    pub generation: Option<String>,
    #[serde(default)]
    pub discard: bool,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Save,
    Promote,
    Inspect,
    Read,
    Ingest,
}

#[derive(serde::Serialize)]
pub struct Registered {
    #[serde(skip_serializing_if = "is_zero")]
    pub branch: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<Vec<u8>>,
    pub file: String,
}

fn is_zero(branch: &u16) -> bool {
    *branch == 0
}

pub(crate) fn invalid(message: impl Into<String>) -> crate::InputError {
    crate::InputError(data_seeds::Error::Definition(message.into()))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    pub cache: String,
    pub service: String,
    pub resource: String,
    pub resource_type: String,
    pub file: String,
}

impl Node {
    pub fn registration(&self, target: &Target, bytes: &[u8]) -> Result<Registered> {
        if bytes.len() > 16 << 20 {
            return Err(invalid("Definition exceeds node limit").into());
        }
        let mut runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        if let Some(action) = &target.action {
            return self.draft_action(&mut runtime, target, action, bytes);
        }
        let request = self.registration_request(target, bytes)?;
        let branch = self.register_locked(&mut runtime, &request, bytes)?;
        Ok(Registered {
            branch,
            file: request.file,
            generation: None,
            model: None,
            bytes: None,
        })
    }

    pub(crate) fn registration_request(
        &self,
        target: &Target,
        bytes: &[u8],
    ) -> Result<data_seeds::registration::Request> {
        let trie = self.trie();
        let registry = trie.definitions()?;
        let request = if let Some(branch) = target.branch {
            let route = trie.route(branch).map_err(invalid)?;
            if route.cache != target.cache
                || !target.service.is_empty() && target.service != route.service
                || !target.resource.is_empty() && target.resource != route.resource
                || !target.resource_type.is_empty() && target.resource_type != route.resource_type
                || !target.split.is_empty() && target.split != route.level
            {
                return Err(invalid("Trunk-Branch does not match registration placement").into());
            }
            let definition = registry
                .definitions()
                .get(route.definition as usize)
                .ok_or_else(|| invalid("Missing registered Definition"))?;
            let document = data_seeds::doc::read(bytes).map_err(crate::InputError)?;
            let head = &document
                .definitions()
                .first()
                .ok_or_else(|| invalid("Empty RX Definition"))?
                .header;
            if head.namespace != definition.namespace || head.rx_name != definition.name {
                return Err(
                    invalid("Trunk-Branch update must preserve Definition identity").into(),
                );
            }
            let resource = registry
                .resources()
                .iter()
                .find(|r| r.branch == branch)
                .ok_or_else(|| invalid("Missing registered branch"))?;
            data_seeds::registration::Request {
                cache: route.cache,
                service: route.service,
                resource: route.resource,
                resource_type: route.resource_type,
                level: route.level,
                file: definition.file.clone(),
                binding: resource.binding.clone(),
                selection: None,
            }
        } else {
            if target.service.is_empty()
                || target.resource.is_empty()
                || target.resource_type.is_empty()
            {
                return Err(invalid(
                    "Resource-Service, Resource-Name and Resource-Type are required",
                )
                .into());
            }
            let document = data_seeds::doc::read(bytes).map_err(crate::InputError)?;
            let header = &document
                .definitions()
                .first()
                .ok_or_else(|| invalid("Empty RX Definition"))?
                .header;
            let file = if let Some(existing) = registry
                .definitions()
                .iter()
                .find(|d| d.namespace == header.namespace && d.name == header.rx_name)
            {
                existing.file.clone()
            } else if header.namespace.is_empty() {
                format!("{}.rx", header.rx_name)
            } else {
                format!("{}.{}.rx", header.namespace, header.rx_name)
            };
            // Resolve the lineage from its taxonomy placement. Internal branch
            // handles are optional; a new RX revision must retain the current
            // placement's configuration rather than installing empty defaults.
            let mut current = None;
            let mut has_splits = false;
            for resource in registry.resources() {
                let route = trie.route(resource.branch)?;
                if route.cache != target.cache
                    || route.service != target.service
                    || route.resource != target.resource
                    || route.resource_type != target.resource_type
                {
                    continue;
                }
                has_splits |= !route.level.is_empty();
                if route.level != target.split {
                    continue;
                }
                let definition = &registry.definitions()[resource.definition as usize];
                if definition.namespace != header.namespace || definition.name != header.rx_name {
                    return Err(
                        invalid("Registration update must preserve Definition identity").into(),
                    );
                }
                if current
                    .as_ref()
                    .is_none_or(|(version, _)| (definition.major, definition.minor) > *version)
                {
                    current = Some(((definition.major, definition.minor), resource));
                }
            }
            if current.is_none() && has_splits && target.split.is_empty() {
                return Err(
                    invalid("Resource-Split is required to select a configured split").into(),
                );
            }
            data_seeds::registration::Request {
                cache: target.cache.clone(),
                service: target.service.clone(),
                resource: target.resource.clone(),
                resource_type: target.resource_type.clone(),
                level: target.split.clone(),
                file,
                binding: current
                    .map(|(_, resource)| resource.binding.clone())
                    .unwrap_or_default(),
                selection: None,
            }
        };
        Ok(request)
    }
    /// Metadata editors publish against the snapshot they inspected. The same
    /// node lock protects this transaction and event/summary registrations.
    pub fn publish(&self, expected: &[u8], next: &[u8]) -> Result<()> {
        let _runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        if self.trie().bytes() != expected {
            return Err("Registry changed during publication".into());
        }
        let next = trie_trunk_taxonomy::Trunk::open(next)?;
        if next.definitions()?.drafts != self.trie().definitions()?.drafts {
            return Err(invalid("Draft ownership changes require save or promotion").into());
        }
        self.publish_registry(&next, None)
    }
    pub fn register(&self, destination: &Destination, bytes: &[u8]) -> Result<u16> {
        self.register_request(
            &data_seeds::registration::Request {
                cache: destination.cache.clone(),
                service: destination.service.clone(),
                resource: destination.resource.clone(),
                resource_type: destination.resource_type.clone(),
                file: destination.file.clone(),
                level: String::new(),
                binding: Default::default(),
                selection: None,
            },
            bytes,
        )
    }
    pub fn register_request(
        &self,
        request: &data_seeds::registration::Request,
        bytes: &[u8],
    ) -> Result<u16> {
        if bytes.len() > 16 << 20 {
            return Err("Definition exceeds node limit".into());
        }
        let mut runtime = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        self.register_locked(&mut runtime, request, bytes)
    }
    pub(crate) fn register_locked(
        &self,
        runtime: &mut crate::ingress::Runtime,
        request: &data_seeds::registration::Request,
        bytes: &[u8],
    ) -> Result<u16> {
        filename(&request.file).map_err(|e| invalid(e.to_string()))?;
        let previous = optional(&self.root.join("definitions").join(&request.file))?;
        let document = data_seeds::doc::read(bytes).map_err(crate::InputError)?;
        let root = &document
            .definitions()
            .first()
            .ok_or_else(|| invalid("Empty RX"))?
            .header;
        if document.definitions().iter().skip(1).any(|d| {
            d.header.rx_name == root.rx_name
                && d.header.namespace == root.namespace
                && d.header.rx_major == 0
        }) {
            return Err(invalid("Published lineage cannot contain draft revisions").into());
        }
        let placement = format!(
            "{}/{}/{}/{}",
            request.cache, request.service, request.resource, request.resource_type
        );
        if self.trie().definitions()?.drafts.contains_key(&placement) {
            return Err(invalid("Use Promote for the saved draft at this placement").into());
        }
        if request.selection.as_ref().is_some_and(|s| s.major == 0) {
            return Err(invalid("Published Definitions require major version 1 or higher").into());
        }
        if root.rx_major == 0 && request.selection.is_none() {
            if root.rx_minor != 0 {
                return Err(invalid("Draft must use 0.0").into());
            }
            let (release, selection) = crate::drafts::release(document, &previous)?;
            let resolved = data_seeds::registration::Request {
                selection: Some(selection),
                ..request.clone()
            };
            return self.publish_type(runtime, &resolved, release.bytes()?, &previous, None);
        }
        self.publish_type(runtime, request, bytes, &previous, None)
    }
    pub(super) fn publish_type(
        &self,
        runtime: &mut crate::ingress::Runtime,
        request: &data_seeds::registration::Request,
        bytes: &[u8],
        previous: &[u8],
        retiring: Option<&str>,
    ) -> Result<u16> {
        let summaries = optional(&self.root.join("definitions/summary.rx"))?;
        let prepared = request
            .prepare(&self.trie(), bytes, previous, &summaries)
            .map_err(crate::InputError)?;
        let next = match retiring {
            Some(source) => prepared.trie.with_draft(source, None)?,
            None => prepared.trie,
        };
        if self.trie().bytes() == next.bytes() {
            return Ok(prepared.branch);
        }
        let mut documents = vec![(request.file.as_str(), &prepared.document)];
        if let Some(summary) = &prepared.summaries {
            documents.push((data_seeds::registration::ingress::SUMMARY_FILE, summary));
        }
        self.publish_documents(&next, &documents)?;
        runtime.refresh(self, prepared.branch)?;
        Ok(prepared.branch)
    }
    // The ingress owner serializes registry publications with commits. Queries
    // pin an Arc and never retain the registry publication lock during scans.
    pub(crate) fn publish_registry(
        &self,
        next: &trie_trunk_taxonomy::Trunk,
        document: Option<(&str, &Document)>,
    ) -> Result<()> {
        let documents: Vec<_> = document.into_iter().collect();
        self.publish_documents(next, &documents)
    }
    pub(crate) fn publish_documents(
        &self,
        next: &trie_trunk_taxonomy::Trunk,
        documents: &[(&str, &Document)],
    ) -> Result<()> {
        for (name, _) in documents {
            filename(name)?;
        }
        if self.trie().bytes() == next.bytes() {
            return Ok(());
        }
        let mut registered = Trunk::open(next.bytes())?;
        let names = registered
            .trie()
            .definitions()?
            .artifacts()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in names {
            if let Some((_, doc)) = documents.iter().find(|(file, _)| *file == name) {
                registered.prescribe_file(&name, doc.bytes()?)?;
            } else {
                registered.prescribe_file(
                    &name,
                    &bounded(&within(&self.root.join("definitions"), &name)?, 16 << 20)?,
                )?;
            }
        }
        for (name, document) in documents {
            let path = self.root.join("definitions").join(name);
            if std::fs::read(&path).ok().as_deref() != Some(document.bytes()?) {
                crate::ingress::atomic(&path, document.bytes()?)?;
            }
        }
        crate::ingress::atomic(&self.root.join("trunk.trie"), next.bytes())?;
        *self
            .trunk
            .write()
            .map_err(|_| "Registry publication poisoned")? = Arc::new(registered);
        self.roots.refresh(next);
        self.registry_generation
            .store(self.changed(), std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

pub(crate) fn filename(name: &str) -> Result<()> {
    if name.is_empty()
        || name.contains('/')
        || std::path::Path::new(name)
            .components()
            .any(|p| !matches!(p, std::path::Component::Normal(_)))
    {
        return Err("Invalid Definition filename".into());
    }
    Ok(())
}
pub(crate) fn optional(path: &std::path::Path) -> Result<Vec<u8>> {
    match bounded(path, 16 << 20) {
        Ok(bytes) => Ok(bytes),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}
