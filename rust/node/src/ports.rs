//! Native upload storage. Parsing and Definition authoring remain Seed Ports.
use crate::{Destination, Node, Result};
use data_seeds::{TIME_HOUR_MS, ports::import::Mapping, registration::ingress::recovery};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::PathBuf,
};

pub(crate) struct Upload {
    root: PathBuf,
    file: File,
    expected: u64,
    received: u64,
    destination: Destination,
    mapping: Mapping,
}
impl Drop for Upload {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
pub(crate) type Uploads = HashMap<String, Upload>;
pub(crate) struct ImportJob {
    result:
        std::sync::Arc<std::sync::Mutex<Option<std::result::Result<serde_json::Value, String>>>>,
    thread: std::thread::JoinHandle<()>,
}
pub(crate) type Jobs = HashMap<String, ImportJob>;

impl Node {
    pub fn start_import(self: &std::sync::Arc<Self>, token: String) -> Result<()> {
        let mut jobs = self.jobs.lock().map_err(|_| "Import owner poisoned")?;
        if jobs.len() >= 8 || jobs.contains_key(&token) {
            return Err("Import already active or completed results need collection".into());
        }
        if !self
            .uploads
            .lock()
            .map_err(|_| "Upload owner poisoned")?
            .contains_key(&token)
        {
            return Err("Upload is not active".into());
        }
        let result = std::sync::Arc::new(std::sync::Mutex::new(None));
        let node = self.clone();
        let key = token.clone();
        let output = result.clone();
        let thread = std::thread::spawn(move || {
            let value = node.import(&token).map_err(|e| e.to_string());
            *output.lock().unwrap() = Some(value);
        });
        jobs.insert(key, ImportJob { result, thread });
        Ok(())
    }
    pub fn progress(&self, token: &str) -> Result<serde_json::Value> {
        let mut jobs = self.jobs.lock().map_err(|_| "Import owner poisoned")?;
        let result = jobs
            .get(token)
            .ok_or("Import result is not available")?
            .result
            .clone();
        let mut result = result.lock().map_err(|_| "Import failed")?;
        let Some(value) = result.take() else {
            return Ok(serde_json::json!({"done":false}));
        };
        let job = jobs.remove(token).unwrap();
        drop(result);
        drop(jobs);
        job.thread.join().map_err(|_| "Import worker failed")?;
        Ok(match value {
            Ok(value) => serde_json::json!({"done":true,"result":value}),
            Err(error) => serde_json::json!({"done":true,"error":error}),
        })
    }
    pub(crate) fn finish_imports(&self) {
        let jobs = std::mem::take(&mut *self.jobs.lock().expect("Import owner poisoned"));
        for (_, job) in jobs {
            let _ = job.thread.join();
        }
    }
    pub fn upload(&self, destination: Destination, mapping: Mapping, bytes: u64) -> Result<String> {
        if bytes == 0 {
            return Err("Choose a nonempty event file".into());
        }
        mapping.prepare()?;
        let mut uploads = self.uploads.lock().map_err(|_| "Upload owner poisoned")?;
        if uploads.len() >= 2 {
            return Err("Finish or cancel the active imports first".into());
        }
        let mut random = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        let token = crate::presentation::hex(&random);
        let root = self.root.join(".trunk").join(format!("import-{token}"));
        fs::create_dir(&root)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join("source"))?;
        uploads.insert(
            token.clone(),
            Upload {
                root,
                file,
                expected: bytes,
                received: 0,
                destination,
                mapping,
            },
        );
        Ok(token)
    }
    pub fn append(&self, token: &str, offset: u64, bytes: &[u8]) -> Result<u64> {
        if bytes.is_empty() || bytes.len() > 256 << 10 {
            return Err("Upload chunk must contain 1–262144 bytes".into());
        }
        let mut uploads = self.uploads.lock().map_err(|_| "Upload owner poisoned")?;
        let job = uploads.get_mut(token).ok_or("Upload is not active")?;
        if offset != job.received || bytes.len() as u64 > job.expected - job.received {
            return Err("Upload extent does not match the source file".into());
        }
        job.file.write_all(bytes)?;
        job.received += bytes.len() as u64;
        Ok(job.received)
    }
    pub fn cancel(&self, token: &str) -> Result<()> {
        self.uploads
            .lock()
            .map_err(|_| "Upload owner poisoned")?
            .remove(token);
        Ok(())
    }
    pub fn import(&self, token: &str) -> Result<serde_json::Value> {
        let job = self
            .uploads
            .lock()
            .map_err(|_| "Upload owner poisoned")?
            .remove(token)
            .ok_or("Upload is not active")?;
        if job.received != job.expected {
            return Err("Upload is incomplete".into());
        }
        let plan = job.mapping.prepare()?;
        // First validate and encode the complete file into bounded hourly spools.
        // No registration or event publication occurs for a malformed source.
        let mut source = job.mapping.format.reader(
            BufReader::new(File::open(job.root.join("source"))?),
            1 << 20,
        )?;
        let mut entropy = File::open("/dev/urandom")?;
        let clock = crate::ingress::now();
        let mut hours = BTreeMap::<u64, PathBuf>::new();
        let mut count = 0u64;
        let mut first = u64::MAX;
        let mut last = 0;
        loop {
            let position = source.position();
            let Some(event) = source
                .next()
                .map_err(|e| format!("Source byte {} / line {}: {e}", position.0, position.1))?
            else {
                break;
            };
            let mut random = [0u8; 10];
            entropy.read_exact(&mut random)?;
            let (time, pill) = plan
                .encode_event(&event, random)
                .map_err(|e| format!("Record {}: {e}", count + 1))?;
            if time > clock {
                return Err(format!("Record {} is in the future", count + 1).into());
            }
            let hour = time / TIME_HOUR_MS * TIME_HOUR_MS;
            if hours.len() >= 100_000 && !hours.contains_key(&hour) {
                return Err("Import exceeds 100000 hourly partitions".into());
            }
            let path = hours
                .entry(hour)
                .or_insert_with(|| job.root.join(format!("{hour}.pack")));
            let mut output = OpenOptions::new().create(true).append(true).open(path)?;
            // Fits the existing open-hour budget; the file itself can span any
            // number of hours without keeping its events resident together.
            if output.metadata()?.len() + pill.len() as u64 + recovery::PREFIX as u64 > 24 << 20 {
                return Err(format!("Hour {hour} exceeds this node's import memory budget").into());
            }
            output.write_all(&recovery::prefix(1, &pill)?)?;
            output.write_all(&pill)?;
            count += 1;
            first = first.min(time);
            last = last.max(time);
        }
        if count == 0 {
            return Err("No event records in the source file".into());
        }
        let source = format!(
            "{}/{}/{}/{}",
            job.destination.cache,
            job.destination.service,
            job.destination.resource,
            job.destination.resource_type
        );
        if self
            .resolve(&source)?
            .as_array()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(
                "Quick Branch Setup requires a new branch; existing sealed history is immutable"
                    .into(),
            );
        }
        let branch = self.register(&job.destination, plan.document.bytes()?)?;
        self.runtime
            .lock()
            .map_err(|_| "Native writer poisoned")?
            .import(self, branch, hours, clock)?;
        Ok(
            serde_json::json!({"branch":branch,"source":source,"count":count,"from":first,"to":last+1,"durable":true}),
        )
    }
}
