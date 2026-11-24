//! Optional read-only PostgreSQL transport on the existing node.
mod catalog;
mod metadata;
mod query;
use crate::{Node, Result};
use async_trait::async_trait;
use ciborium::Value as D;
use futures::stream;
use pgwire::{
    api::{
        ClientInfo, PgWireServerHandlers, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            cleartext::CleartextPasswordAuthStartupHandler,
        },
        cancel::CancelHandler,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::*,
        stmt::{NoopQueryParser, StoredStatement},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{cancel::CancelRequest, startup::SecretKey},
};
use query::{Output, Rows};
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

fn fail(e: impl ToString) -> PgWireError {
    let msg = e.to_string();
    if msg.contains("query cancelled") {
        PgWireError::QueryCanceled
    } else {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            "0A000".into(),
            msg,
        )))
    }
}
struct Login {
    node: Arc<Node>,
    password: String,
}
impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Trunk login")
    }
}
#[async_trait]
impl AuthSource for Login {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some("trunk") {
            return Err(PgWireError::InvalidPassword("unknown role".into()));
        }
        Ok(Password::new(None, self.password.as_bytes().to_vec()))
    }
    async fn verify_password(
        &self,
        login: &LoginInfo,
        presented: &[u8],
    ) -> PgWireResult<Option<()>> {
        if self.get_password(login).await?.password() != presented {
            return Err(PgWireError::InvalidPassword("trunk".into()));
        }
        let db = login.database().unwrap_or("postgres");
        if !catalog::Catalog::read(&self.node, db)
            .map_err(fail)?
            .database_exists(db)
        {
            return Err(fail(format!("Unknown cache {db}")));
        }
        Ok(None)
    }
}
#[derive(Default)]
struct Active(Mutex<HashMap<i32, (SecretKey, Vec<Arc<AtomicBool>>)>>);
impl Active {
    fn stop(&self, pid: i32, secret: Option<&SecretKey>) -> bool {
        let map = self.0.lock().unwrap();
        if let Some((key, flags)) = map.get(&pid) {
            if secret.is_none_or(|s| key.eq_value(s)) {
                for f in flags {
                    f.store(true, Ordering::Relaxed)
                }
                return true;
            }
        }
        false
    }
}
#[async_trait]
impl CancelHandler for Active {
    async fn on_cancel_request(&self, r: CancelRequest) {
        self.stop(r.pid, Some(&r.secret_key));
    }
}
struct Backend {
    node: Arc<Node>,
    login: Arc<CleartextPasswordAuthStartupHandler<Login, DefaultServerParameterProvider>>,
    active: Arc<Active>,
}
impl PgWireServerHandlers for ArcBackend {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.0.login.clone()
    }
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.0.active.clone()
    }
}
#[derive(Clone)]
struct ArcBackend(Arc<Backend>);
pub struct Service {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    pub address: SocketAddr,
}
impl Service {
    pub fn start(node: Arc<Node>, address: &str, password: String) -> Result<Self> {
        let address: SocketAddr = address.parse()?;
        if !address.ip().is_loopback() || password.is_empty() {
            return Err("PostgreSQL requires a loopback address and TRUNK_PG_PASSWORD".into());
        }
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let (stop, mut wait) = tokio::sync::oneshot::channel();
        let mut settings = DefaultServerParameterProvider::default();
        settings.is_superuser = false;
        settings.default_transaction_read_only = true;
        let login = Arc::new(CleartextPasswordAuthStartupHandler::new(
            Login {
                node: node.clone(),
                password,
            },
            settings,
        ));
        let backend = ArcBackend(Arc::new(Backend {
            node,
            login,
            active: Default::default(),
        }));
        let thread = std::thread::spawn(move || {
            rt.block_on(async move {
                let Ok(l) = tokio::net::TcpListener::from_std(listener) else {
                    return;
                };
                let connections = Arc::new(tokio::sync::Semaphore::new(32));
                loop {
                    let accept = async {
                        let permit = connections.clone().acquire_owned().await.ok()?;
                        let (s, _) = l.accept().await.ok()?;
                        Some((permit, s))
                    };
                    match futures::future::select(Box::pin(accept), &mut wait).await {
                        futures::future::Either::Left((Some((permit, s)), _)) => {
                            let b = backend.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let _ = pgwire::tokio::process_socket(s, None, b).await;
                            });
                        }
                        _ => break,
                    }
                }
            })
        });
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
            address,
        })
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn params(p: &Portal<String>) -> PgWireResult<Vec<Option<String>>> {
    (0..p.parameters.len())
        .map(|i| {
            if let Format::Individual(v) = &p.parameter_format {
                if v.len() != p.parameters.len() {
                    return Err(fail("Invalid parameter format count"));
                }
            }
            if p.parameter_format.is_text(i) {
                return p.parameters[i]
                    .as_deref()
                    .map(|b| std::str::from_utf8(b).map(str::to_owned).map_err(fail))
                    .transpose();
            }
            let t = p
                .statement
                .parameter_types
                .get(i)
                .and_then(Option::as_ref)
                .unwrap_or(&Type::UNKNOWN);
            macro_rules! read {
                ($v:ty) => {
                    p.parameter::<$v>(i, t)?.map(|n| n.to_string())
                };
            }
            Ok(match *t {
                Type::INT2 => read!(i16),
                Type::INT4 => read!(i32),
                Type::INT8 => read!(i64),
                Type::FLOAT4 => read!(f32),
                Type::FLOAT8 => read!(f64),
                Type::BOOL => read!(bool),
                Type::VARCHAR | Type::TEXT => read!(String),
                _ => return Err(fail("Unsupported binary parameter type")),
            })
        })
        .collect()
}
fn fields(o: &Output, f: &Format) -> PgWireResult<Vec<FieldInfo>> {
    if let Format::Individual(v) = f {
        if v.len() != o.columns.len() {
            return Err(fail("Invalid result format count"));
        }
    }
    Ok(o.columns
        .iter()
        .enumerate()
        .map(|(i, (n, t))| FieldInfo::new(n.clone(), None, None, t.clone(), f.format_for(i)))
        .collect())
}
fn text(v: &D, ty: &Type) -> Result<Option<String>> {
    if *ty == Type::JSON && !matches!(v, D::Null) {
        return Ok(Some(serde_json::to_string(v)?));
    }
    Ok(Some(match v {
        D::Null => return Ok(None),
        D::Bool(b) => b.to_string(),
        D::Text(s) => s.clone(),
        D::Integer(n) => i128::from(*n).to_string(),
        D::Float(f) => f.to_string(),
        D::Bytes(b) if *ty == Type::UUID && b.len() == 16 => {
            let h = crate::presentation::hex(b);
            format!(
                "{}-{}-{}-{}-{}",
                &h[..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..]
            )
        }
        D::Bytes(b) if *ty == Type::BYTEA => format!("\\x{}", crate::presentation::hex(b)),
        D::Tag(t @ (2 | 3), b) => {
            let D::Bytes(b) = &**b else {
                return Err("Invalid integer magnitude".into());
            };
            let mut raw = [0; 16];
            if b.len() > 16 {
                return Err("Integer exceeds RX width".into());
            }
            raw[16 - b.len()..].copy_from_slice(b);
            let n = u128::from_be_bytes(raw);
            if *t == 2 {
                n.to_string()
            } else {
                format!("-{}", n.checked_add(1).ok_or("Invalid signed magnitude")?)
            }
        }
        _ => return Err(format!("Cannot encode value as {}", ty.name()).into()),
    }))
}
fn encode(
    schema: &Arc<Vec<FieldInfo>>,
    row: Vec<Option<String>>,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut e = DataRowEncoder::new(schema.clone());
    for (f, v) in schema.iter().zip(row) {
        if v.is_none() || f.format() == FieldFormat::Text {
            e.encode_field_with_type_and_format(&v, &Type::TEXT, f.format(), f.format_options())?;
            continue;
        }
        let s = v.unwrap();
        match *f.datatype() {
            Type::TEXT | Type::VARCHAR => e.encode_field(&s)?,
            Type::INT2 => e.encode_field(&s.parse::<i16>().map_err(fail)?)?,
            Type::INT4 => e.encode_field(&s.parse::<i32>().map_err(fail)?)?,
            Type::INT8 => e.encode_field(&s.parse::<i64>().map_err(fail)?)?,
            Type::BOOL => e.encode_field(&(s == "true" || s == "t"))?,
            Type::FLOAT4 => e.encode_field(&s.parse::<f32>().map_err(fail)?)?,
            Type::FLOAT8 => e.encode_field(&s.parse::<f64>().map_err(fail)?)?,
            _ => {
                return Err(fail(format!(
                    "Use text results for {}",
                    f.datatype().name()
                )));
            }
        }
    }
    Ok(e.take_row())
}
struct Running {
    active: Arc<Active>,
    pid: i32,
    flags: Vec<Arc<AtomicBool>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        for f in &self.flags {
            f.store(true, Ordering::Relaxed)
        }
        self.active.0.lock().unwrap().remove(&self.pid);
    }
}
impl Backend {
    fn prepare<C: ClientInfo>(
        &self,
        c: &C,
        sql: &str,
        p: &[Option<String>],
        describe: bool,
    ) -> PgWireResult<Output> {
        query::prepare(
            &self.node,
            c.metadata()
                .get("database")
                .map_or("postgres", String::as_str),
            c.pid_and_secret_key().0,
            sql,
            p,
            describe,
            &self.active,
        )
        .map_err(fail)
    }
    fn response<C: ClientInfo>(
        &self,
        c: &C,
        sql: &str,
        p: &[Option<String>],
        f: &Format,
    ) -> PgWireResult<Response> {
        let mut out = self.prepare(c, sql, p, false)?;
        let schema = Arc::new(fields(&out, f)?);
        let flags = match &out.rows {
            Rows::Aggregate { cancel, .. } => vec![cancel.clone()],
            Rows::Scan(s) => vec![s.cancellation()],
            Rows::Join(j) => j.cancellation().to_vec(),
            _ => vec![],
        };
        let (pid, key) = c.pid_and_secret_key();
        let guard = if flags.is_empty() {
            None
        } else {
            let mut sessions = self.active.0.lock().unwrap();
            if sessions.contains_key(&pid) {
                return Err(fail(
                    "Close the previous result cursor before opening another",
                ));
            }
            sessions.insert(pid, (key, flags.clone()));
            Some(Running {
                active: self.active.clone(),
                pid,
                flags,
            })
        };
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let output_schema = schema.clone();
        let node = self.node.clone();
        tokio::task::spawn_blocking(move || {
            let produce = || -> Result<()> {
                let send = |r| -> Result<bool> {
                    Ok(tx.blocking_send(Ok(encode(&output_schema, r)?)).is_ok())
                };
                let project = |row: data_seeds::Object| -> Result<Vec<Option<String>>> {
                    out.keys
                        .iter()
                        .zip(&out.columns)
                        .map(|(k, (_, ty))| match row.iter().find(|(n, _)| n == k) {
                            Some((_, v)) => text(v, ty),
                            None => Ok(None),
                        })
                        .collect()
                };
                match &mut out.rows {
                    Rows::Aggregate {
                        branches,
                        plan,
                        offset,
                        limit,
                        cancel,
                    } => {
                        let result = node.aggregate(
                            branches,
                            0,
                            u64::MAX,
                            plan,
                            &serde_json::json!({}),
                            || {
                                if cancel.load(Ordering::Relaxed) || tx.is_closed() {
                                    Err("query cancelled".into())
                                } else {
                                    Ok(())
                                }
                            },
                        )?;
                        for row in result["rows"]
                            .as_array()
                            .ok_or("Missing aggregate rows")?
                            .iter()
                            .skip(*offset)
                            .take(limit.unwrap_or(usize::MAX))
                        {
                            if cancel.load(Ordering::Relaxed) {
                                return Err("query cancelled".into());
                            }
                            let cells = row.as_array().ok_or("Invalid aggregate row")?;
                            let values = cells
                                .iter()
                                .zip(&out.columns)
                                .map(|(v, (_, ty))| {
                                    if v.is_null() || v.get("$absent").is_some() {
                                        return Ok(None);
                                    }
                                    if let Some(bytes) = v.get("$bytes") {
                                        return text(
                                            &D::Bytes(serde_json::from_value(bytes.clone())?),
                                            ty,
                                        );
                                    }
                                    Ok(Some(
                                        match v
                                            .get("$integer")
                                            .and_then(|n| n.as_str())
                                            .or_else(|| v.as_str())
                                        {
                                            Some(s) => s.to_owned(),
                                            None => v.to_string(),
                                        },
                                    ))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            if !send(values)? {
                                break;
                            }
                        }
                    }
                    Rows::Catalog(rows) => {
                        for row in rows {
                            if !send(row)? {
                                break;
                            }
                        }
                    }
                    Rows::Scan(cursor) => {
                        while !tx.is_closed() {
                            let b = cursor.fetch(64)?;
                            if b.is_empty() {
                                break;
                            }
                            let mut reader = b.reader();
                            for i in 0..b.len() {
                                if !send(project(reader.view(i)?.object()?)?)? {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Rows::Join(j) => {
                        while !tx.is_closed() {
                            let Some(row) = j.next_row()? else { break };
                            if out.skip > 0 {
                                out.skip -= 1;
                                continue;
                            }
                            if !send(project(row)?)? {
                                break;
                            }
                        }
                    }
                    Rows::Describe => {}
                }
                Ok(())
            };
            let mut produce = produce;
            if let Err(e) = produce() {
                let _ = tx.blocking_send(Err(fail(e)));
            }
        });
        Ok(Response::Query(QueryResponse::new(
            schema,
            stream::unfold((rx, guard), |(mut rx, g)| async move {
                rx.recv().await.map(|r| (r, (rx, g)))
            }),
        )))
    }
}
#[async_trait]
impl SimpleQueryHandler for Backend {
    async fn do_query<C: ClientInfo + Unpin + Send + Sync>(
        &self,
        c: &mut C,
        q: &str,
    ) -> PgWireResult<Vec<Response>> {
        Ok(vec![self.response(c, q, &[], &Format::UnifiedText)?])
    }
}
#[async_trait]
impl ExtendedQueryHandler for Backend {
    type Statement = String;
    type QueryParser = NoopQueryParser;
    fn query_parser(&self) -> Arc<NoopQueryParser> {
        Arc::new(NoopQueryParser::new())
    }
    async fn do_query<C: ClientInfo + Unpin + Send + Sync>(
        &self,
        c: &mut C,
        p: &Portal<String>,
        _: usize,
    ) -> PgWireResult<Response> {
        self.response(
            c,
            &p.statement.statement,
            &params(p)?,
            &p.result_column_format,
        )
    }
    async fn do_describe_statement<C: ClientInfo + Unpin + Send + Sync>(
        &self,
        c: &mut C,
        s: &StoredStatement<String>,
    ) -> PgWireResult<DescribeStatementResponse> {
        let out = self.prepare(c, &s.statement, &[], true)?;
        Ok(DescribeStatementResponse::new(
            s.parameter_types
                .iter()
                .map(|t| t.clone().unwrap_or(Type::TEXT))
                .collect(),
            fields(&out, &Format::UnifiedText)?,
        ))
    }
    async fn do_describe_portal<C: ClientInfo + Unpin + Send + Sync>(
        &self,
        c: &mut C,
        p: &Portal<String>,
    ) -> PgWireResult<DescribePortalResponse> {
        let out = self.prepare(c, &p.statement.statement, &params(p)?, true)?;
        Ok(DescribePortalResponse::new(fields(
            &out,
            &p.result_column_format,
        )?))
    }
}
