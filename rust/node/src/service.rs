//! Node socket transport. Rust owns registry, reads and evaluation independently
//! of the connected client. Storage behavior comes from node configuration.
use crate::{Node, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use std::os::unix::{
    fs::PermissionsExt,
    net::{UnixListener, UnixStream},
};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub const REQUEST_BYTES: usize = 8 << 20;
pub const RESPONSE_BYTES: usize = 8 << 20;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Request {
    DraftRead {scope:String,generation:String,request:Box<Request>},
    Design { draft: data_seeds::ports::design::Design },
    Review { mapping: data_seeds::ports::import::Mapping },
    Upload {
        destination: crate::Destination,
        mapping: data_seeds::ports::import::Mapping,
        bytes: u64,
    },
    Append {
        token: String,
        offset: u64,
        bytes: Vec<u8>,
    },
    Import {
        token: String,
    },
    Progress {
        token: String,
    },
    Cancel {
        token: String,
    },
    Register {
        target: crate::RegistrationTarget,
        bytes: Vec<u8>,
    },
    Ingest {
        branch: u16,
        bytes: Vec<u8>,
    },
    Status,
    Catalog,
    Bootstrap,
    Presentation,
    Model { definition:u32 },
    Resolve {
        scope: String,
    },
    SummaryTable {
        branches: Vec<u16>,
        from: u64,
        to: u64,
        plan: Value,
        #[serde(default)]
        context: Value,
    },
    Language {
        query: Value,
    },
    Timeline {
        branches: Vec<u16>,
        from: u64,
        to: u64,
    },
    Summaries {
        branches: Vec<u16>,
        from: u64,
        to: u64,
        step: u64,
        #[serde(default)]
        metric: String,
        #[serde(default)]
        filter: Option<Value>,
    },
    Fetch {
        cursor: u64,
    },
    Release {
        cursor: u64,
    },
    Trie {
        query: Value,
    },
    Inspect {
        source: String,
        source_bytes: Option<u64>,
    },
    Join {
        branches: Vec<u16>,
        from: u64,
        to: u64,
        plan: data_seeds::relational::join::Plan,
        limit: Option<usize>,
        #[serde(default = "page_size")]
        page_size: usize,
    },
    Query {
        #[serde(default)]
        branch: Option<u16>,
        #[serde(default)]
        branches: Vec<u16>,
        from: u64,
        to: u64,
        #[serde(default)]
        query: Value,
        #[serde(default = "page_size")]
        page_size: usize,
    },
}
fn page_size() -> usize {
    100
}

enum Rows {
    Events(data_seeds::Cursor, Vec<u16>),
    Joined(data_seeds::relational::join::Join),
}
struct Pending {
    rows: Rows,
    page_size: usize,
    touched: std::time::Instant,
}
pub(crate) struct Session {
    node: Arc<Node>,
    cursors: std::collections::HashMap<u64, Pending>,
    next: u64,
}
impl Session {
    pub(crate) fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            cursors: Default::default(),
            next: 0,
        }
    }
    pub(crate) fn request(&mut self, request: Request) -> Result<Value> {
        self.cursors
            .retain(|_, c| c.touched.elapsed() < Duration::from_secs(60));
        match request {
            Request::DraftRead {scope,generation,request} => {
                if !matches!(request.as_ref(),Request::Query{..}|Request::Join{..}|Request::Timeline{..}|Request::SummaryTable{..}|Request::Bootstrap|Request::Resolve{..}){
                    return Err("Expected a draft query".into());
                }
                let owner=self.node.clone();
                owner.with_draft(&scope,&generation,|working|{
                    let previous=std::mem::replace(&mut self.node,Arc::new(working));
                    let result=self.request(*request);
                    self.node=previous;
                    result
                })
            }
            Request::Design { draft } => Ok(draft.run()?),
            Request::Review { mapping } => {
                Ok(serde_json::to_value(data_seeds::ports::Ports.review(&mapping)?)?)
            }
            Request::Upload {
                destination,
                mapping,
                bytes,
            } => Ok(json!({"token":self.node.upload(destination,mapping,bytes)?})),
            Request::Append {
                token,
                offset,
                bytes,
            } => Ok(json!({"received":self.node.append(&token,offset,&bytes)?})),
            Request::Import { token } => {
                self.node.start_import(token)?;
                Ok(json!({"started":true}))
            }
            Request::Progress { token } => self.node.progress(&token),
            Request::Cancel { token } => {
                self.node.cancel(&token)?;
                Ok(Value::Null)
            }
            Request::Register { target, bytes } => {
                Ok(serde_json::to_value(self.node.registration(&target, &bytes)?)?)
            }
            Request::Ingest { branch, bytes } => Ok(
                json!({"time":self.node.ingest(branch,&bytes,crate::ingress::now())?,"durable":true}),
            ),
            Request::Status => Ok(
                json!({"configuration":self.node.configuration(), "runtime":"native", "storage":self.node.configuration().storage.suffix().trim_start_matches('.')}),
            ),
            Request::Catalog => self.node.catalog(),
            Request::Bootstrap => self.node.bootstrap(),
            Request::Model {definition:id} => {
                let trie=self.node.trie();
                let registered=trie.definition(id)?;
                let bytes=crate::read::bounded(&self.node.root().join("definitions").join(&registered.file),16<<20)?;
                let document=data_seeds::doc::read(bytes.as_slice())?;
                let member=trie.definitions()?.member(id,&document)?;
                Ok(data_seeds::ports::model::model(document,member)?)
            }
            Request::Presentation => self.node.presentation(),
            Request::Resolve { scope } => self.node.resolve(&scope),
            Request::SummaryTable {
                branches,
                from,
                to,
                plan,
                context,
            } => self
                .node
                .summary_table(&branches, from, to, &plan, &context),
            Request::Language { query } => {
                data_seeds::language::request(&query).map_err(Into::into)
            }
            Request::Timeline { branches, from, to } => self.node.timeline(&branches, from, to),
            Request::Summaries {
                branches,
                from,
                to,
                step,
                metric,
                filter,
            } => {
                let predicate = filter
                    .as_ref()
                    .map(data_seeds::request::predicate)
                    .transpose()?
                    .unwrap_or(data_seeds::Predicate::All);
                self.node
                    .summaries(&branches, from, to, step, &metric, &predicate)
            }
            Request::Trie { query } => Ok(self.node.trie().query(&query)?),
            Request::Inspect {
                source,
                source_bytes,
            } => Ok(serde_json::to_value(self.node.ports().sample(
                source.as_bytes(),
                Default::default(),
                source_bytes,
            )?)?),
            Request::Release { cursor } => {
                self.cursors.remove(&cursor);
                Ok(Value::Null)
            }
            Request::Fetch { cursor } => self.page(cursor),
            Request::Join {
                branches,
                from,
                to,
                plan,
                limit,
                page_size,
            } => {
                self.admit(page_size)?;
                let rows = self.node.join(&branches, from, to, &plan, limit)?;
                self.start(Rows::Joined(rows), page_size)
            }
            Request::Query {
                branch,
                mut branches,
                from,
                to,
                query,
                page_size,
            } => {
                self.admit(page_size)?;
                let query = data_seeds::request::query(&query)?;
                if let Some(branch) = branch {
                    branches.push(branch);
                }
                let (cursor, origins) = self.node.query_many(&branches, from, to, query)?;
                self.start(Rows::Events(cursor,origins),page_size)
            }
        }
    }
    fn admit(&self, page_size: usize) -> Result<()> {
        if page_size == 0 || page_size > 1000 {
            return Err("Page size must be 1–1000".into());
        }
        if self.cursors.len() >= 8 {
            return Err("Close an existing result cursor before starting another query".into());
        }
        Ok(())
    }
    fn start(&mut self, rows: Rows, page_size: usize) -> Result<Value> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or("Cursor identifier exhausted")?;
        self.cursors.insert(
            self.next,
            Pending {
                rows,
                page_size,
                touched: std::time::Instant::now(),
            },
        );
        self.page(self.next)
    }
    fn page(&mut self, id: u64) -> Result<Value> {
        let pending = self
            .cursors
            .get_mut(&id)
            .ok_or("Result cursor expired or was released")?;
        let (cancelled, other) = match &pending.rows {
            Rows::Events(c, _) => (c.cancellation(), None),
            Rows::Joined(j) => {
                let [a, b] = j.cancellation();
                (a, Some(b))
            }
        };
        let (finished, wait) = std::sync::mpsc::channel();
        let deadline = std::thread::spawn(move || {
            if matches!(
                wait.recv_timeout(Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                cancelled.store(true, Ordering::Relaxed);
                if let Some(b) = other {
                    b.store(true, Ordering::Relaxed);
                }
            }
        });
        let result = (|| -> Result<(Value, bool)> {
            pending.touched = std::time::Instant::now();
            let (cursor, sources) = match &mut pending.rows {
                Rows::Events(cursor, sources) => (cursor, sources),
                Rows::Joined(join) => {
                    let mut rows = Vec::new();
                    let mut size = 0;
                    while rows.len() < pending.page_size {
                        let Some(row) = join.next_row()? else {
                            break;
                        };
                        rows.push(result_row(row, &mut size)?);
                    }
                    let exhausted = rows.len() < pending.page_size;
                    return Ok((
                        json!({"rows":rows,"columns":join.columns(),"cursor":if exhausted {None}else{Some(id)},"runtime":"native"}),
                        exhausted,
                    ));
                }
            };
            let batch = cursor.fetch(pending.page_size)?;
            let exhausted = batch.len() < pending.page_size;
            let mut rows = Vec::with_capacity(batch.len());
            let mut origins = Vec::with_capacity(batch.len());
            let mut size = 0;
            let mut reader = batch.reader();
            for ordinal in 0..batch.len() {
                let (pack, row, digest) = batch.origin(ordinal)?;
                origins.push(json!({"branch":sources[pack],"pack":pack,"row":row,
                    "definition":crate::presentation::hex(digest)}));
                let object = reader.view(ordinal)?.object()?;
                rows.push(result_row(object, &mut size)?);
            }
            pending.touched = std::time::Instant::now();
            Ok((
                json!({"rows":rows,"origins":origins,"cursor":if exhausted {None}else{Some(id)},"runtime":"native"}),
                exhausted,
            ))
        })();
        let _ = finished.send(());
        let _ = deadline.join();
        match result {
            Ok((value, exhausted)) => {
                if exhausted {
                    self.cursors.remove(&id);
                }
                Ok(value)
            }
            Err(error) => {
                self.cursors.remove(&id);
                Err(error)
            }
        }
    }
}

fn result_row(object: data_seeds::Object, size: &mut usize) -> Result<Value> {
    let row = Value::Object(
        object
            .into_iter()
            .map(|(name, value)| Ok((name, project(value)?)))
            .collect::<Result<_>>()?,
    );
    *size += serde_json::to_vec(&row)?.len();
    if *size > RESPONSE_BYTES - 4096 {
        return Err("Result page exceeds node limit; select fewer fields or rows".into());
    }
    Ok(row)
}
// JSON transport preserves integers outside JS's safe range and binary values.
// Snoot only renders these tags; it never decodes a PIL or evaluates a predicate.
fn project(value: ciborium::Value) -> Result<Value> {
    use ciborium::Value as D;
    Ok(match value {
        D::Null => Value::Null,
        D::Bool(v) => json!(v),
        D::Text(v) => json!(v),
        D::Integer(v) => {
            let n: i128 = v.into();
            if n.unsigned_abs() <= 9_007_199_254_740_991 {
                json!(n as i64)
            } else {
                json!({"$integer":n.to_string()})
            }
        }
        D::Float(v) if v.is_finite() => json!(v),
        D::Float(v) => json!({"$float":v.to_string()}),
        D::Bytes(v) => json!({"$bytes":v}),
        D::Array(v) => Value::Array(v.into_iter().map(project).collect::<Result<_>>()?),
        D::Map(v) if v.iter().all(|(k, _)| matches!(k, D::Text(_))) => {
            let mut out = serde_json::Map::new();
            for (k, v) in v {
                let D::Text(k) = k else { unreachable!() };
                out.insert(k, project(v)?);
            }
            Value::Object(out)
        }
        D::Map(v) => {
            json!({"$map":v.into_iter().map(|(k,v)|Ok([project(k)?,project(v)?])).collect::<Result<Vec<_>>>()?})
        }
        D::Tag(tag, v) => json!({"$tag":tag,"value":project(*v)?}),
        _ => return Err("Unsupported result value".into()),
    })
}

pub struct Service {
    path: PathBuf,
    stopped: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    commits: Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
}
impl Service {
    pub fn start(node: Arc<Node>, path: &Path) -> Result<Self> {
        // Bind exclusively; never unlink a socket belonging to another owner.
        let listener = UnixListener::bind(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let stopped = Arc::new(AtomicBool::new(false));
        let shutdown = stopped.clone();
        let (stop, wait) = std::sync::mpsc::channel();
        let owner = node.clone();
        let commits = std::thread::spawn(move || {
            while matches!(
                wait.recv_timeout(Duration::from_secs(30)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                if let Err(error) = owner.commit(crate::ingress::now()) {
                    eprintln!("Trunk commit failed: {error}");
                    break;
                }
            }
        });
        let thread = std::thread::spawn(move || {
            let mut session = Session {
                node,
                cursors: Default::default(),
                next: 0,
            };
            for stream in listener.incoming() {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                let Ok(mut stream) = stream else { break };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                let response = handle(&mut session, &mut stream);
                let response = match response {
                    Ok(value) => json!({"ok":true,"value":value}),
                    Err(error) => json!({"ok":false,"error":error.to_string()}),
                };
                if let Ok(mut bytes) = serde_json::to_vec(&response) {
                    if bytes.len() > RESPONSE_BYTES {
                        bytes = br#"{"ok":false,"error":"Node response exceeds limit"}"#.to_vec();
                    }
                    let _ = stream
                        .write_all(&(bytes.len() as u32).to_le_bytes())
                        .and_then(|_| stream.write_all(&bytes));
                }
            }
            session.node.finish_imports();
        });
        Ok(Self {
            path: path.into(),
            stopped,
            thread: Some(thread),
            commits: Some((stop, commits)),
        })
    }
}
fn handle(session: &mut Session, stream: &mut UnixStream) -> Result<Value> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > REQUEST_BYTES {
        return Err("Node request exceeds limit".into());
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    session.request(serde_json::from_slice(&bytes)?)
}
impl Drop for Service {
    fn drop(&mut self) {
        if let Some((stop, thread)) = self.commits.take() {
            let _ = stop.send(());
            let _ = thread.join();
        }
        self.stopped.store(true, Ordering::Release);
        let _ = UnixStream::connect(&self.path);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
