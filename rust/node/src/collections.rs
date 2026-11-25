//! Published object revisions use the UUID index and the existing hour directory.
pub(crate) mod writer;
use crate::{
    Node, Result,
    read::{bounded, within},
    registration::invalid,
};
use data_seeds::{TIME_HOUR_MS, TimeTrie, reader::Selection, writer::NativeFiles};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    sync::Arc,
};
use trunk_uuid_index::collection::Index;

struct RevisionFields {
    digest: String,
    minor: u32,
    branch: u16,
    object: String,
    revision: String,
    time: String,
}
impl RevisionFields {
    fn new(
        definition: &data_seeds::SeedDefinition,
        digest: String,
        minor: u32,
        branch: u16,
    ) -> Result<Self> {
        let fields = definition.prescription().definition().fields();
        let name = |role: &str| -> Result<String> {
            let mut matches = fields.iter().enumerate().filter(|(i, _)| {
                definition
                    .annotations(*i)
                    .unwrap_or_default()
                    .iter()
                    .any(|tag| tag == role || (role == "@time" && tag == "@time(ms)"))
            });
            let field = matches
                .next()
                .ok_or_else(|| format!("Missing revision field {role}"))?;
            if matches.next().is_some() {
                return Err(format!("Ambiguous revision field {role}").into());
            }
            Ok(field.1.name.clone())
        };
        Ok(Self {
            digest,
            minor,
            branch,
            object: match definition.change_key() {
                Some(key) => key.to_owned(),
                None => name("@object")?,
            },
            revision: name("@revision")?,
            time: name("@time")?,
        })
    }
}

impl Node {
    pub fn history(
        &self,
        source: &str,
        id: &str,
        from: u64,
        to: u64,
        state_only: bool,
    ) -> Result<Value> {
        let uuid = uuid::Uuid::parse_str(id).map_err(|_| invalid("Invalid object UUID"))?;
        if from >= to {
            return Err(invalid("Invalid history window").into());
        }
        let _publication = self
            .object_publication
            .read()
            .map_err(|_| "Object publication poisoned")?;
        let trie = self.trie();
        let mut branches = Vec::new();
        let mut types = HashMap::new();
        let mut file = None;
        for entry in trie.definitions()?.resources() {
            let route = trie.route(entry.branch)?;
            if route.source()? != source {
                continue;
            }
            let Some(path) = route.pods()? else { continue };
            if file.as_ref().is_some_and(|previous| previous != &path) {
                return Err("History spans different object stores".into());
            }
            file = Some(path);
            types.insert(
                route.digest.clone(),
                (route.minor, entry.branch),
            );
            branches.push(entry.branch);
        }
        let path = file.ok_or_else(|| invalid("Unknown object collection"))?;
        let stem = path.strip_suffix(".pods").ok_or("Invalid PODS path")?;
        let index_file = format!("{stem}.indx");
        let time_file = format!("{stem}.time");
        let mut token = Sha256::new();
        token.update(source.as_bytes());
        token.update(uuid.as_bytes());
        let mut rows = BTreeMap::new();
        let mut definitions = HashMap::new();
        if self.root.join(&index_file).try_exists()? {
            let snapshot = Index::open(File::open(within(&self.root, &index_file)?)?)?.snapshot();
            let time = TimeTrie::open(Arc::new(bounded(
                &within(&self.root, &time_file)?,
                16 << 20,
            )?))?;
            token.update(time.header().generation);
            token.update(snapshot.sequence().to_le_bytes());
            if !state_only {
                let mut hours = BTreeMap::<u64, Vec<(u32, usize, u32)>>::new();
                let mut count = 0;
                for (binding, _, _) in snapshot.sources() {
                    if binding.generation != time.header().generation {
                        return Err("Object index generation differs from Time".into());
                    }
                    let mut next = None;
                    loop {
                        next = snapshot.history_page(
                            binding,
                            *uuid.as_bytes(),
                            next,
                            |revision, address| {
                                let hour = time.header().origin + (address >> 32) * TIME_HOUR_MS;
                                if hour < time.header().sealed
                                    && hour < to
                                    && from < hour + TIME_HOUR_MS
                                {
                                    hours.entry(hour).or_default().push((
                                        revision,
                                        address as u32 as usize,
                                        binding.minor,
                                    ));
                                    count += 1;
                                }
                            },
                        )?;
                        if count > 65536 {
                            return Err("Too many revisions in this history window".into());
                        }
                        if next.is_none() {
                            break;
                        }
                    }
                }
                if !hours.is_empty() {
                    let mut scope = trie.branches().select(branches).pods()?;
                    let pods = File::open(within(&self.root, &path)?)?;
                    let (footer, length, _lease) = crate::publication::footer(&pods, &self.memory)?;
                    scope.box_footer(footer, length)?;
                    let addresses = hours
                        .keys()
                        .map(|&hour| {
                            time.leaf(hour)?
                                .filter(|leaf| leaf.count > 0)
                                .map(|leaf| leaf.beds)
                                .ok_or_else(|| {
                                    data_seeds::Error::Definition(
                                        "Missing indexed object hour".into(),
                                    )
                                })
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let reader = scope.reader(
                        Arc::new(NativeFiles(HashMap::from([(1, pods)]))),
                        Selection {
                            file: 1,
                            length,
                            addresses,
                        },
                        32 << 20,
                        self.memory.clone(),
                    )?;
                    let mut size = 0;
                    let mut plans = HashMap::<Vec<u8>, RevisionFields>::new();
                    for (i, (hour, posts)) in hours.into_iter().enumerate() {
                        let pack = reader.pack(i)?;
                        for (revision, record, minor) in posts {
                            let ordinal = record
                                .checked_sub(1)
                                .ok_or("Invalid object record address")?;
                            let (definition, object) = pack.revision_object(ordinal)?;
                            let plan = match plans.get(definition.digest()) {
                                Some(plan) => plan,
                                None => {
                                    let digest = crate::presentation::hex(definition.digest());
                                    let &(minor, branch) =
                                        types.get(&digest).ok_or("Unknown object Definition")?;
                                    let fields =
                                        RevisionFields::new(definition, digest, minor, branch)?;
                                    plans.entry(definition.digest().to_vec()).or_insert(fields)
                                }
                            };
                            if plan.minor != minor {
                                return Err("Object posting Definition mismatch".into());
                            }
                            let value = |name: &str| {
                                object.iter().find(|(key, _)| key == name).map(|(_, v)| v)
                            };
                            let object_id = if definition.change_key().is_some() {
                                value(data_seeds::registration::revision::PAYLOAD)
                                    .and_then(|v| v.as_map())
                                    .and_then(|m| {
                                        m.iter()
                                            .find(|(k, _)| {
                                                k.as_text() == Some(plan.object.as_str())
                                            })
                                            .map(|(_, v)| v)
                                    })
                            } else {
                                value(&plan.object)
                            };
                            if object_id.and_then(|v| v.as_bytes()).map(Vec::as_slice)
                                != Some(uuid.as_bytes().as_slice())
                                || value(&plan.revision)
                                    .and_then(|v| v.as_integer())
                                    .map(i128::from)
                                    != Some(i128::from(revision))
                            {
                                return Err(
                                    "Object posting does not match the addressed revision".into()
                                );
                            }
                            let timestamp = value(&plan.time)
                                .and_then(|v| v.as_integer())
                                .and_then(|n| u64::try_from(n).ok())
                                .ok_or("Missing revision time")?;
                            if timestamp / TIME_HOUR_MS != hour / TIME_HOUR_MS {
                                return Err("Object posting hour mismatch".into());
                            }
                            if !(from..to).contains(&timestamp) {
                                continue;
                            }
                            if !definitions.contains_key(&plan.digest) {
                                definitions.insert(plan.digest.clone(), definition.field_descriptions());
                            }
                            let values = crate::service::result_row(object, &mut size)?;
                            if rows.insert(revision,json!({"revision":revision,"definition":plan.digest,"stream":plan.branch,"values":values})).is_some() {
                                return Err("Duplicate indexed revision".into());
                            }
                        }
                    }
                }
            }
        } else if self.root.join(&path).try_exists()? || self.root.join(time_file).try_exists()? {
            return Err("Missing published object index".into());
        }
        Ok(
            json!({"source":source,"id":uuid.to_string(),"token":crate::presentation::hex(&token.finalize()),
            "definitions":definitions,"rows":rows.into_values().collect::<Vec<_>>() }),
        )
    }
}
