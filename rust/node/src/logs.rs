//! Transport observations use the existing log.http model and ordinary ingress.
use crate::{Destination, Node, Result};
use data_seeds::SeedDefinition;
use std::{collections::BTreeMap, fs::File, io::Read, sync::{Arc, Mutex}};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub resource: String,
    pub time: u64,
    pub duration: u64,
    pub method: String,
    pub route: String,
    pub status: u16,
    pub bytes: u64,
    pub protocol: String,
    pub selection: String,
}

pub struct Logs {
    node: Arc<Node>,
    branches: BTreeMap<String, u16>,
    definition: Arc<SeedDefinition>,
    entropy: Mutex<File>,
}
impl Node {
    pub fn logs(self: &Arc<Self>, cache: &str, resources: &[String]) -> Result<Logs> {
        let bytes = data_seeds::seed::log::HTTP;
        let definition = SeedDefinition::read(bytes)?;
        let mut branches = BTreeMap::new();
        for resource in resources {
            let branch = self.register(&Destination {
                cache: cache.into(), service: "trie-trunk".into(), resource: resource.clone(),
                resource_type: "http".into(), file: "log.http.rx".into(),
            }, bytes)?;
            branches.insert(resource.clone(), branch);
        }
        Ok(Logs { node: self.clone(), branches, definition, entropy: Mutex::new(File::open("/dev/urandom")?) })
    }
}
impl Logs {
    pub fn write(&self, observation: Observation) -> Result<()> {
        let branch = *self.branches.get(&observation.resource).ok_or("Unknown resource observation")?;
        let duration = u32::try_from(observation.duration)?;
        let mut id = [0; 10];
        self.entropy.lock().map_err(|_| "Entropy reader poisoned")?.read_exact(&mut id)?;
        let seed = self.definition.ingest(&observation, |event, seed| {
            seed.set_uint("time", event.time)?
                .set_byte_string("id", &id)?
                .set_uint("level", if event.status >= 400 { 17u8 } else { 9 })?
                .set_text_string("method", &event.method)?
                .set_text_string("route", &event.route)?
                .set_uint("status", event.status)?
                .set_uint("duration", duration)?;
            seed.set_array("attributes", vec![ciborium::Value::Map(vec![
                ("response_bytes".into(), event.bytes.into()),
                ("protocol".into(), event.protocol.clone().into()),
                ("selection".into(), event.selection.clone().into()),
            ])])?;
            Ok(())
        })?;
        seed.cap()?;
        self.node.ingest(branch, &seed.bytes()?, crate::ingress::now())?;
        Ok(())
    }
}
