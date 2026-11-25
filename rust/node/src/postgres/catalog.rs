//! A relational projection of the registered taxonomy for PostgreSQL clients.
use super::*;
use rusqlite::{Connection, params};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};

pub struct Table {
    pub path: Vec<String>,
    pub branches: Vec<u16>,
    pub fields: Vec<(String, Type)>,
    pub selectors: Vec<(String, Type)>,
}
pub struct Catalog {
    pub tables: BTreeMap<(String, String), Table>,
    databases: BTreeSet<String>,
}

fn field_type(f: &Json) -> Type {
    let kind = f["type"].as_str().unwrap_or("");
    match kind {
        "BOOL" => Type::BOOL,
        "UUID" => Type::UUID,
        "F32" => Type::FLOAT4,
        "F64" => Type::FLOAT8,
        "I8" | "U8" | "I16" => Type::INT2,
        "U16" | "U24" | "I32" | "REF16" => Type::INT4,
        "I64" | "U32" => Type::INT8,
        "U64" => Type::NUMERIC,
        s if s.starts_with("UINT(") || s.starts_with("INT(") => {
            let n = s
                .split_once('(')
                .and_then(|(_, n)| n.trim_end_matches(')').parse::<u8>().ok())
                .unwrap_or(16);
            let signed = s.starts_with("INT");
            match n {
                1 => Type::INT2,
                2 if signed => Type::INT2,
                2 | 3 => Type::INT4,
                4 if signed => Type::INT4,
                4..=7 => Type::INT8,
                8 if signed => Type::INT8,
                _ => Type::NUMERIC,
            }
        }
        s if s.starts_with("TIMESTAMP_") => Type::INT8,
        s if s.starts_with("BYTES") => Type::BYTEA,
        s if s.starts_with("STRING") => Type::TEXT,
        _ => Type::JSON,
    }
}
fn merge_type(fields: &mut Vec<(String, Type)>, name: &str, ty: &Type) {
    if let Some((_, old)) = fields.iter_mut().find(|(n, _)| n == name) {
        if *old != *ty {
            *old = if [Type::INT2, Type::INT4, Type::INT8, Type::NUMERIC].contains(old)
                && [Type::INT2, Type::INT4, Type::INT8, Type::NUMERIC].contains(ty)
            {
                Type::NUMERIC
            } else {
                Type::JSON
            };
        }
    } else {
        fields.push((name.to_owned(), ty.clone()));
    }
}
impl Catalog {
    pub fn read(node: &Node, database: &str) -> Result<Self> {
        let trie = node.trie();
        let mut tables = BTreeMap::<(String, String), Table>::new();
        let mut databases = BTreeSet::new();
        for r in trie.definitions()?.resources() {
            let route = trie.route(r.branch)?;
            databases.insert(route.cache.clone());
            if route.cache != database || route.pods()?.is_some() {
                continue;
            }
            let table = format!(
                "{}.{}",
                route.resource,
                route.resource_type.replace('.', "-")
            );
            let path = vec![
                route.cache,
                route.service.clone(),
                route.resource,
                route.resource_type,
            ];
            let t = tables
                .entry((route.service, table.clone()))
                .or_insert_with(|| Table {
                    path: path.clone(),
                    branches: vec![],
                    fields: vec![],
                    selectors: vec![],
                });
            if t.path != path {
                return Err(format!(
                    "Ambiguous PostgreSQL relation {table} for {:?} and {:?}",
                    t.path, path
                )
                .into());
            }
            t.branches.push(r.branch);
            let definition = trie.registered().definition(r.definition)?;
            for f in trie.registered().describe(r.definition)?["fields"]
                .as_array()
                .ok_or("Missing fields")?
            {
                let name = f["name"].as_str().ok_or("Missing field name")?;
                let ty = if definition.is_event_id(name) {
                    Type::UUID
                } else if definition.timestamp_unit(name) == Some("ms") {
                    Type::TIMESTAMPTZ
                } else {
                    field_type(f)
                };
                merge_type(&mut t.fields, name, &ty);
                for selector in f["selectors"].as_array().into_iter().flatten().filter_map(Json::as_str) {
                    merge_type(&mut t.selectors, selector, &ty);
                }
            }
        }
        Ok(Self { tables, databases })
    }
    pub fn schema(&self) -> &str {
        self.tables.keys().next().map_or("public", |(s, _)| s)
    }
    pub fn database_exists(&self, name: &str) -> bool {
        name == "postgres" || self.databases.contains(name)
    }
    // SQLite evaluates only catalog expressions. Event rows always use the Seed reader.
    pub fn metadata(&self, database: &str, pid: i32) -> Result<Connection> {
        let c = Connection::open_in_memory()?;
        c.execute_batch(include_str!("catalog.sql"))?;
        for db in &self.databases {
            c.execute("insert into pg_database values(?,0)", [db])?;
        }
        let mut schemas = BTreeMap::new();
        for ((schema, name), t) in &self.tables {
            let next = schemas.len() as i64 + 1;
            let ns = *schemas.entry(schema.clone()).or_insert(next);
            c.execute(
                "insert or ignore into pg_namespace values(?,?)",
                params![ns, schema],
            )?;
            let oid = 65536 + i64::from(*t.branches.iter().min().ok_or("Missing branch")?);
            c.execute(
                "insert into pg_class values(?,?,?,'r',-1)",
                params![oid, name, ns],
            )?;
            for (i, (field, ty)) in t.fields.iter().enumerate() {
                c.execute(
                    "insert into pg_attribute values(?,?,?,?,0,0,-1)",
                    params![oid, i as i64 + 1, field, ty.oid()],
                )?;
                c.execute("insert or ignore into pg_type(oid,typname,typnamespace,typrelid,typelem,typarray) values(?,?,0,0,0,0)",params![ty.oid(),ty.name()])?;
            }
        }
        let flags = rusqlite::functions::FunctionFlags::SQLITE_UTF8;
        for (name, value) in [
            ("version", "PostgreSQL 16.0 (Trie Trunk)"),
            ("current_schema", self.schema()),
            ("current_database", database),
        ] {
            let value = value.to_string();
            c.create_scalar_function(name, 0, flags, move |_| Ok(value.clone()))?;
        }
        c.create_scalar_function("pg_backend_pid", 0, flags, move |_| Ok(pid))?;
        c.create_scalar_function("col_description", 2, flags, |_| Ok(None::<String>))?;
        c.create_scalar_function("quote_ident", 1, flags, |ctx| {
            let s = ctx.get::<String>(0)?;
            Ok(quote(&s))
        })?;
        c.create_scalar_function("format_type", 2, flags, |ctx| {
            Ok(Type::from_oid(ctx.get::<u32>(0)?).map(|ty| ty.name().to_owned()))
        })?;
        c.create_scalar_function("format", 3, flags, |ctx| {
            let template = ctx.get::<String>(0)?;
            if template != "%I.%I" {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(format!(
                "{}.{}",
                quote(&ctx.get::<String>(1)?),
                quote(&ctx.get::<String>(2)?)
            ))
        })?;
        c.execute_batch("pragma query_only=on")?;
        c.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, 1 << 20)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        c.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
        Ok(c)
    }
}
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
