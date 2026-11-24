//! One registration transaction for every node entry point.
use crate::{Document, Error, SeedDefinition, doc, error};
use std::collections::BTreeMap;
use trie_trunk_taxonomy::{
    Trunk,
    registration::{Registration, Selection},
};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub cache: String,
    pub service: String,
    pub resource: String,
    pub resource_type: String,
    #[serde(default)]
    pub level: String,
    pub file: String,
    #[serde(default)]
    pub binding: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub selection: Option<Selection>,
}

pub struct Registered {
    pub trie: Trunk,
    pub document: Document,
    pub summaries: Option<Document>,
    pub branch: u16,
}

impl Request {
    pub fn prepare(
        &self,
        trie: &Trunk,
        bytes: &[u8],
        previous: &[u8],
        summaries: &[u8],
    ) -> Result<Registered, Error> {
        let document = doc::read(bytes)?;
        if !previous.is_empty() {
            let before = doc::read(previous)?;
            for member in before.definitions() {
                let h = &member.header;
                let next = document.member(&h.namespace, &h.rx_name, h.rx_major, h.rx_minor)?;
                if document.definitions()[next].header.rx_hash != h.rx_hash {
                    return Err(error(
                        "Definition replacement would change a published schema",
                    ));
                }
            }
        }
        let (mut trie, registered) = trie
            .registration_selected(
                Registration {
                    cache: &self.cache,
                    service: &self.service,
                    resource: &self.resource,
                    resource_type: &self.resource_type,
                    level: &self.level,
                    file: &self.file,
                },
                &document,
                Some(self.binding.clone()),
                self.selection.as_ref(),
            )
            .map_err(error)?;
        let selected = match &self.selection {
            Some(s) => document.member(&s.namespace, &s.name, s.major, s.minor)?,
            None => 0,
        };
        if trie.route(registered.branch).map_err(error)?.base == Some(trie_trunk_taxonomy::base::Base::Object) {
            return Ok(Registered { trie, document, summaries: None, branch: registered.branch });
        }
        let source = SeedDefinition::prescribe_member(&document, selected)?;
        if let Some(path)=self.binding.get("path") {
            super::ingress::StatePaths::compile(&source,path)?;
        }
        let summaries = if source.summary_fields().is_empty() {
            if trie
                .definitions()
                .map_err(error)?
                .resources()
                .iter()
                .any(|r| r.branch == registered.branch && !r.summaries.is_empty())
            {
                return Err(error(
                    "removing all summary annotations requires an explicit summary retirement",
                ));
            }
            None
        } else {
            let mut definitions = if summaries.is_empty() {
                crate::summaries::SummaryDefinitions::new()
            } else {
                crate::summaries::SummaryDefinitions::read(summaries)?
            };
            definitions.prescribe(source, crate::summaries::SUMMARY_DAY_ROWS)?;
            let document = definitions
                .document()?
                .ok_or_else(|| error("missing summary Definitions"))?;
            trie = trie
                .register_summaries(registered.branch, super::ingress::SUMMARY_FILE, &document)
                .map_err(error)?;
            Some(document)
        };
        Ok(Registered {
            trie,
            document,
            summaries,
            branch: registered.branch,
        })
    }
}
