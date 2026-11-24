//! Two-input hash equijoin. Both inputs are ordinary filtered Seed cursors.
use crate::{
    Cell, Cursor, Error, MemoryBudget, MemoryLease, Object, OwnedBatchReader, Predicate, Query,
    SeedDefinition, error,
};
use ciborium::Value as Data;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Left,
    Right,
}
impl Side {
    pub fn index(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    Inner,
    Left,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Input {
    pub path: Vec<String>,
    pub alias: String,
    pub filters: Vec<serde_json::Value>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub side: Side,
    pub field: String,
    pub name: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub inputs: [Input; 2],
    pub kind: Kind,
    pub keys: Vec<[String; 2]>,
    pub columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Eq)]
enum Domain<'a> {
    Boolean,
    Text,
    Bytes,
    Number(Option<&'a str>),
    Timestamp(&'a str),
}
fn key_type<'a>(d: &'a SeedDefinition, name: &str) -> Result<Option<Domain<'a>>, Error> {
    use pil_codec::{Scalar::*, Type};
    let Some(i) = d.query_ordinal(name) else {
        return Ok(None);
    };
    if let Some(unit) = d.time_unit(i) {
        return Ok(Some(Domain::Timestamp(unit)));
    }
    let f = &d.prescription().definition().fields()[i];
    let kind = match d.enum_kinds[i].map(Type::Scalar).unwrap_or_else(|| f.kind) {
        Type::Scalar(Bool) => Domain::Boolean,
        Type::Scalar(String | FixedString(_)) => Domain::Text,
        Type::Scalar(Bytes | FixedBytes(_) | Uuid) => Domain::Bytes,
        Type::Scalar(
            U8 | U16 | U24 | U32 | U64 | Uint(_) | I8 | I16 | I32 | I64 | Int(_) | F32 | F64,
        ) => Domain::Number(d.annotation(i, "@unit")),
        _ => return Err(error(format!("Unsupported join key type for {name}"))),
    };
    Ok(Some(kind))
}

struct Slot {
    side: Side,
    index: usize,
    name: String,
}
pub struct Bound {
    queries: [Query; 2],
    slots: Vec<Slot>,
    keys: [Vec<usize>; 2],
    kind: Kind,
}
impl Plan {
    pub fn bind(&self, defs: [&[Arc<SeedDefinition>]; 2]) -> Result<Bound, Error> {
        if self.keys.is_empty() || self.inputs[0].alias == self.inputs[1].alias {
            return Err(error("Expected join keys and distinct aliases"));
        }
        let mut fields: [Vec<String>; 2] = Default::default();
        let mut keys: [Vec<usize>; 2] = Default::default();
        let mut slot = |side: usize, name: &str| {
            if let Some(i) = fields[side].iter().position(|n| n == name) {
                return i;
            }
            fields[side].push(name.to_owned());
            fields[side].len() - 1
        };
        for pair in &self.keys {
            let mut domain = None;
            for side in 0..2 {
                let mut exists = false;
                for def in defs[side] {
                    if let Some(ty) = key_type(def, &pair[side])? {
                        if domain.as_ref().is_some_and(|expected| expected != &ty) {
                            return Err(error(format!(
                                "Incompatible join keys {} and {}",
                                pair[0], pair[1]
                            )));
                        }
                        domain = Some(ty);
                        exists = true;
                    }
                }
                if !exists {
                    return Err(error(format!("Unknown join key {}", pair[side])));
                }
                keys[side].push(slot(side, &pair[side]));
            }
        }
        let mut slots = vec![];
        let mut names = HashSet::new();
        let mut project = |side: Side, field: &str, name: String| -> Result<(), Error> {
            if !names.insert(name.clone()) {
                return Err(error(format!("Duplicate result column {name}")));
            }
            slots.push(Slot {
                side,
                index: slot(side.index(), field),
                name,
            });
            Ok(())
        };
        if self.columns.is_empty() {
            for side in [Side::Left, Side::Right] {
                let i = side.index();
                let mut seen = HashSet::new();
                for field in defs[i].iter().flat_map(|d| d.query_fields()) {
                    if seen.insert(field.clone()) {
                        project(side, &field, format!("{}.{}", self.inputs[i].alias, field))?;
                    }
                }
            }
        } else {
            for c in &self.columns {
                project(c.side, &c.field, c.name.clone())?;
            }
        }
        let query = |i: usize| -> Result<Query, Error> {
            let filters = self.inputs[i]
                .filters
                .iter()
                .map(crate::request::predicate)
                .collect::<Result<Vec<_>, _>>()?;
            let mut query = Query::new().select(fields[i].clone());
            if !filters.is_empty() {
                query = query.filter(Predicate::And(filters));
            }
            crate::query::validate_definition_names(defs[i], &query)?;
            Ok(query)
        };
        Ok(Bound {
            queries: [query(0)?, query(1)?],
            keys,
            slots,
            kind: self.kind,
        })
    }
}

#[derive(Hash, PartialEq, Eq)]
enum Atom {
    Integer(bool, u128),
    Float(u64),
    Text(String),
    Bytes(Vec<u8>),
    Bool(bool),
}
type Row = Vec<Cell>;
fn key(row: &Row, slots: &[usize]) -> Result<Option<Vec<Atom>>, Error> {
    let mut key = Vec::with_capacity(slots.len());
    for &i in slots {
        let Cell::Value(v) = &row[i] else {
            return Ok(None);
        };
        key.push(match v {
            Data::Null => return Ok(None),
            Data::Integer(_) | Data::Tag(2 | 3, _) => {
                let (neg, n) = crate::value::magnitude(v)?;
                Atom::Integer(neg, n)
            }
            Data::Float(f) if f.is_nan() => return Ok(None),
            Data::Float(f) if f.fract() == 0.0 && f.abs() < 2.0f64.powi(128) => {
                Atom::Integer(*f < 0.0, f.abs() as u128)
            }
            Data::Float(f) => Atom::Float(f.to_bits()),
            Data::Text(v) => Atom::Text(v.clone()),
            Data::Bytes(v) => Atom::Bytes(v.clone()),
            Data::Bool(v) => Atom::Bool(*v),
            _ => return Err(error("Unsupported join key value")),
        });
    }
    Ok(Some(key))
}
fn heap(v: &Data) -> usize {
    match v {
        Data::Text(v) => v.capacity(),
        Data::Bytes(v) => v.capacity(),
        Data::Array(a) => a.capacity() * size_of::<Data>() + a.iter().map(heap).sum::<usize>(),
        Data::Map(m) => {
            m.capacity() * size_of::<(Data, Data)>()
                + m.iter().map(|(k, v)| heap(k) + heap(v)).sum::<usize>()
        }
        Data::Tag(_, v) => size_of::<Data>() + heap(v),
        _ => 0,
    }
}
struct Rows {
    cursor: Cursor,
    batch: Option<OwnedBatchReader>,
    next: usize,
}
impl Rows {
    fn new(cursor: Cursor) -> Self {
        Self {
            cursor,
            batch: None,
            next: 0,
        }
    }
    fn next(&mut self) -> Result<Option<Row>, Error> {
        if self
            .batch
            .as_ref()
            .is_none_or(|b| self.next == b.batch().len())
        {
            self.batch = None;
            let b = self.cursor.fetch(64)?;
            if b.is_empty() {
                return Ok(None);
            }
            self.batch = Some(b.into_reader());
            self.next = 0;
        }
        let view = self.batch.as_mut().unwrap().view(self.next)?;
        self.next += 1;
        (0..view.fields().count())
            .map(|i| crate::value::own(view.native_at(i)?))
            .collect::<Result<Row, _>>()
            .map(Some)
    }
}
pub struct Join {
    left: Option<Rows>,
    right: Option<Rows>,
    keys: [Vec<usize>; 2],
    slots: Vec<Slot>,
    kind: Kind,
    groups: HashMap<Vec<Atom>, usize>,
    buckets: Vec<Vec<(Row, MemoryLease)>>,
    current: Option<Row>,
    bucket: Option<usize>,
    position: usize,
    remaining: usize,
    budget: MemoryBudget,
    cancel: [Arc<AtomicBool>; 2],
}
impl Bound {
    pub fn open(
        self,
        budget: MemoryBudget,
        limit: Option<usize>,
        left: impl FnOnce(Query) -> Result<Cursor, Error>,
        right: impl FnOnce(Query) -> Result<Cursor, Error>,
    ) -> Result<Join, Error> {
        let [left_query, right_query] = self.queries;
        let left = left(left_query)?;
        let right = right(right_query)?;
        let cancel = [left.cancellation(), right.cancellation()];
        let remaining = limit.unwrap_or(usize::MAX);
        Ok(Join {
            left: (remaining > 0).then(|| Rows::new(left)),
            right: (remaining > 0).then(|| Rows::new(right)),
            keys: self.keys,
            slots: self.slots,
            kind: self.kind,
            groups: HashMap::new(),
            buckets: vec![],
            current: None,
            bucket: None,
            position: 0,
            remaining,
            budget,
            cancel,
        })
    }
}
impl Join {
    pub fn cancellation(&self) -> [Arc<AtomicBool>; 2] {
        self.cancel.clone()
    }
    pub fn columns(&self) -> Vec<&str> {
        self.slots.iter().map(|s| s.name.as_str()).collect()
    }
    fn check(&self) -> Result<(), Error> {
        if self.cancel.iter().any(|c| c.load(Ordering::Relaxed)) {
            return Err(error("query cancelled"));
        }
        Ok(())
    }
    fn build(&mut self) -> Result<(), Error> {
        let Some(mut right) = self.right.take() else {
            return Ok(());
        };
        while let Some(row) = right.next()? {
            self.check()?;
            let Some(k) = key(&row, &self.keys[1])? else {
                continue;
            };
            // Charge owned values, keys and conservative map/vector growth before retention.
            let bytes = 256
                + row.capacity() * size_of::<Cell>()
                + row
                    .iter()
                    .map(|v| match v {
                        Cell::Value(v) => heap(v),
                        _ => 0,
                    })
                    .sum::<usize>()
                + k.capacity() * size_of::<Atom>()
                + k.iter()
                    .map(|a| match a {
                        Atom::Text(v) => v.capacity(),
                        Atom::Bytes(v) => v.capacity(),
                        _ => 0,
                    })
                    .sum::<usize>();
            let lease = self.budget.reserve(bytes)?;
            let group = *self.groups.entry(k).or_insert_with(|| {
                self.buckets.push(vec![]);
                self.buckets.len() - 1
            });
            self.buckets[group].push((row, lease));
        }
        Ok(())
    }
    pub fn next_row(&mut self) -> Result<Option<Object>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let result = self.advance();
        if self.remaining == 0 || !matches!(result, Ok(Some(_))) {
            self.remaining = 0;
            self.groups = HashMap::new();
            self.buckets = Vec::new();
            self.current = None;
            self.right = None;
            self.left = None;
        }
        result
    }
    fn advance(&mut self) -> Result<Option<Object>, Error> {
        self.check()?;
        loop {
            if self.current.is_none() {
                let Some(row) = self.left.as_mut().unwrap().next()? else {
                    return Ok(None);
                };
                self.check()?;
                self.build()?;
                if self.groups.is_empty() && self.kind == Kind::Inner {
                    return Ok(None);
                }
                self.bucket = key(&row, &self.keys[0])?.and_then(|k| self.groups.get(&k).copied());
                if self.bucket.is_none() && self.kind == Kind::Inner {
                    continue;
                }
                self.current = Some(row);
                self.position = 0;
            }
            let right = self.bucket.map(|g| &self.buckets[g][self.position].0);
            let row = self
                .slots
                .iter()
                .filter_map(|s| {
                    let value = match s.side {
                        Side::Left => &self.current.as_ref().unwrap()[s.index],
                        Side::Right => match right {
                            Some(r) => &r[s.index],
                            None => return Some((s.name.clone(), Data::Null)),
                        },
                    };
                    match value {
                        Cell::Value(v) => Some((s.name.clone(), v.clone())),
                        Cell::Absent => None,
                    }
                })
                .collect();
            self.position += 1;
            self.remaining -= 1;
            if self
                .bucket
                .is_none_or(|i| self.position == self.buckets[i].len())
            {
                self.current = None;
            }
            return Ok(Some(row));
        }
    }
}
