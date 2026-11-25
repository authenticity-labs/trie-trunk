mod semantics;
pub(crate) mod coercion;
mod byte;
#[cfg(feature = "registration")]
mod time;
#[cfg(feature = "registration")]
pub(crate) use time::TimeBounds;
use semantics::Access;
use crate::seed::identity::Identity;
pub(crate) mod pack;
pub(crate) mod pruning;
pub use pack::ReadPack;
use pack::ReadCursor;
use crate::storage::{BoundRow, BindingReader};
use crate::{Cell, Error, Object, SeedPackSnapshot, MemoryBudget, MemoryLease, error, value};
use ciborium::Value as Data;
use pil_codec::api::{NativeValue, PillView};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
};

#[derive(Clone, Copy, Debug, PartialEq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
impl Compare {
    fn matches(self, order: Ordering) -> bool {
        match self {
            Self::Eq => order.is_eq(),
            Self::Ne => !order.is_eq(),
            Self::Lt => order.is_lt(),
            Self::Le => order.is_le(),
            Self::Gt => order.is_gt(),
            Self::Ge => order.is_ge(),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub enum Predicate {
    All,
    Compare {
        field: String,
        op: Compare,
        value: Data,
    },
    Cast { field: String, cast: crate::cast::Spec, op: Compare, value: Data },
    CastNull { field: String, cast: crate::cast::Spec },
    Contains { field: String, text: String },
    IsNull(String),
    IsAbsent(String),
    IsPresent(String),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}
impl Predicate {
    pub fn compare(field: impl Into<String>, op: Compare, value: impl Into<Data>) -> Self {
        Self::Compare {
            field: field.into(),
            op,
            value: value.into(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub enum Direction {
    Ascending,
    Descending,
}
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub struct Query {
    predicate: Predicate,
    projection: Option<Vec<String>>,
    casts: Vec<(String,String,crate::cast::Spec)>,
    version: Option<(u32, u32)>,
    order: Option<(String, Direction)>,
    limit: Option<usize>,
    offset: usize,
    context: crate::cast::Context,
    checkpoint: Option<std::ops::Range<u64>>,
}
/// Immutable predicate programs already compiled by the cursor for this query.
pub struct SourceFilter<'a> {
    #[cfg(all(feature = "planner-lab", feature = "registration"))]
    pub(crate) access: crate::planner_lab::Access,
    query: &'a Query,
    compiled: &'a HashMap<usize,Arc<Plan>>,
    work: &'a mut usize,
    pending: &'a mut bool,
    #[cfg(feature="registration")]
    goal: usize,
    #[cfg(feature = "registration")]
    pub(crate) time: Option<&'a TimeBounds>,
    #[cfg(feature="registration")]
    pub(crate) window: &'a mut Option<crate::reader::IndexWindow>,
}
#[cfg(feature="registration")]
impl SourceFilter<'_> {
    pub(crate) fn continues(&self)->bool{*self.pending}
    pub(crate) fn slice(&mut self, remaining: &mut std::vec::IntoIter<std::ops::Range<usize>>) -> Vec<std::ops::Range<usize>> {
        if *self.work == usize::MAX {
            *self.pending = false;
            return std::mem::take(remaining).collect();
        }
        let mut selected = Vec::new();
        while *self.work > 0 {
            let Some(range) = remaining.as_mut_slice().first_mut() else { break };
            let n = range.len().min(*self.work);
            selected.push(range.start..range.start + n);
            *self.work -= n;
            range.start += n;
            if range.start == range.end { remaining.next(); }
        }
        *self.pending = !remaining.as_slice().is_empty();
        selected
    }
    pub(crate) fn expression(&self, definition: &crate::SeedDefinition, sought: bool) -> Option<Arc<Expression>> {
        let plan = self.compiled.get(&(definition as *const _ as usize))?;
        if sought && let Some(rest) = &plan.after_seek { return Some(rest.clone()); }
        Some(plan.predicate.clone())
    }
    pub(crate) fn filtered(&self)->bool { !matches!(self.query.predicate,Predicate::All) }
    pub(crate) fn scan_goal(&self)->usize {
        self.goal
    }
}
impl Default for Query {
    fn default() -> Self {
        Self {
            predicate: Predicate::All,
            projection: None,
            casts: Vec::new(),
            version: None,
            order: None,
            limit: None,
            offset: 0,
            context: crate::cast::Context::default(),
            checkpoint: None,
        }
    }
}
impl Query {
    #[cfg(feature = "planner-lab")]
    pub(crate) fn lab_order(&self) -> Option<(&str, Direction, Option<usize>, usize)> {
        self.order
            .as_ref()
            .map(|(field, direction)| (field.as_str(), *direction, self.limit, self.offset))
    }
    pub fn new() -> Self {
        Self::default()
    }
    /// Interpret text literals using one clock/timezone snapshot for this query.
    pub fn context(mut self, context: crate::cast::Context) -> Self { self.context=context; self }
    /// Restrict Definition versions before field planning or record traversal.
    pub fn version(mut self, major: u32, minor: u32) -> Self {
        self.version = Some((major, minor));
        self
    }
    /// Track in-window event times seen by the scan, before matching or ordering.
    pub fn checkpoint(mut self, window: std::ops::Range<u64>) -> Self {
        self.checkpoint = Some(window);
        self
    }
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = predicate;
        self
    }
    pub fn select(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.projection = Some(names.into_iter().map(Into::into).collect());
        self
    }
    /// Add an explicitly converted result column. Conversion runs in the reader.
    pub fn cast(mut self, field:impl Into<String>, alias:impl Into<String>, spec:crate::cast::Spec)->Self {
        self.casts.push((field.into(),alias.into(),spec));self
    }
    pub fn order_by(mut self, field: impl Into<String>, direction: Direction) -> Self {
        self.order = Some((field.into(), direction));
        self
    }
    /// Applied once by the source coordinator, never once per Pack.
    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }
    pub fn offset(mut self, count: usize) -> Self {
        self.offset = count;
        self
    }
}

/// Views and materialized objects share predicate semantics. The view adapter
/// reads only fields reached by the expression's short-circuit evaluation.
pub(crate) trait PredicateRow {
    fn cast_at(&self,access:Access,plan:&crate::cast::Plan)->Result<NativeValue<'_>,Error>;
    fn byte_at(&self, ordinal: usize) -> Result<byte::Cell, Error>;
    fn contains_at(&self, ordinal: usize, text: &str) -> Result<Option<bool>, Error>;
    fn compare_at(&self, ordinal: usize, rhs: &value::Operand) -> Result<Option<Ordering>, Error>;
    // Object rows already contain the projected UUID.
    fn compare_id(&self, id: &Identity, rhs: &value::Operand) -> Result<Option<Ordering>, Error> {
        self.compare_at(id.id, rhs)
    }
    fn null_at(&self, ordinal: usize) -> Result<bool, Error>;
    fn present_at(&self, ordinal: usize) -> Result<bool, Error>;
}
pub(crate) trait NativeRow {
    fn native_value(&self, field:usize)->Result<NativeValue<'_>,Error>;
    fn is_present(&self, field:usize)->Result<bool,Error>;
}
impl NativeRow for PillView<'_> {
    fn native_value(&self, field: usize) -> Result<NativeValue<'_>, Error> {
        PillView::native_value(self, field)
    }
    fn is_present(&self, field: usize) -> Result<bool, Error> {
        PillView::is_present(self, field)
    }
}
impl NativeRow for BoundRow<'_> {
    fn native_value(&self, field:usize)->Result<NativeValue<'_>,Error>{BoundRow::native_value(self,field)}
    fn is_present(&self, field:usize)->Result<bool,Error>{BoundRow::is_present(self,field)}
}
impl<T:NativeRow> PredicateRow for T {
    fn compare_id(&self, id: &Identity, rhs: &value::Operand) -> Result<Option<Ordering>, Error> {
        value::compare_operand(id.read(|i| self.native_value(i))?, rhs)
    }
    fn cast_at(&self,access:Access,plan:&crate::cast::Plan)->Result<NativeValue<'_>,Error> {plan.value(access.value(self.native_value(access.ordinal)?))}
    fn byte_at(&self, ordinal: usize) -> Result<byte::Cell, Error> {
        byte::Cell::native(self.native_value(ordinal)?)
    }
    fn contains_at(&self, ordinal: usize, text: &str) -> Result<Option<bool>, Error> {
        Ok(match self.native_value(ordinal)? {
            NativeValue::Scalar(pil_codec::Value::String(v)) => Some(v.contains(text)),
            NativeValue::Composite(Data::Text(v)) => Some(v.contains(text)),
            _ => None,
        })
    }

    fn compare_at(&self, ordinal: usize, rhs: &value::Operand) -> Result<Option<Ordering>, Error> {
        value::compare_operand(self.native_value(ordinal)?, rhs)
    }
    fn null_at(&self, ordinal: usize) -> Result<bool, Error> {
        Ok(matches!(
            self.native_value(ordinal)?,
            NativeValue::Scalar(pil_codec::Value::Null) | NativeValue::Composite(Data::Null)
        ))
    }
    fn present_at(&self, ordinal: usize) -> Result<bool, Error> {
        self.is_present(ordinal)
    }
}

pub(crate) struct ObjectRow<'a> {
    pub rx: &'a pil_codec::api::Prescription,
    pub object: &'a Object,
}
impl ObjectRow<'_> {
    fn get(&self, ordinal: usize) -> Option<&Data> {
        let name = &self.rx.definition().fields().get(ordinal)?.name;
        // Full objects retain Definition order; projections/absent fields may
        // differ. Never reinterpret an object position as a field identity.
        self.object
            .get(ordinal)
            .filter(|(key, _)| key == name)
            .or_else(|| self.object.iter().find(|(key, _)| key == name))
            .map(|(_, v)| v)
    }
}
impl PredicateRow for ObjectRow<'_> {
    fn cast_at(&self,access:Access,plan:&crate::cast::Plan)->Result<NativeValue<'_>,Error> {match self.get(access.ordinal) {Some(v)=>plan.value(access.value(NativeValue::Composite(v.clone()))),None=>Ok(NativeValue::Scalar(pil_codec::Value::Absent))}}
    fn byte_at(&self, ordinal: usize) -> Result<byte::Cell, Error> {
        byte::Cell::data(self.get(ordinal))
    }
    fn contains_at(&self, ordinal: usize, text: &str) -> Result<Option<bool>, Error> {
        Ok(match self.get(ordinal) { Some(Data::Text(v)) => Some(v.contains(text)), _ => None })
    }

    fn compare_at(&self, ordinal: usize, rhs: &value::Operand) -> Result<Option<Ordering>, Error> {
        self.get(ordinal)
            .map_or(Ok(None), |v| value::compare_object(v, rhs))
    }
    fn null_at(&self, ordinal: usize) -> Result<bool, Error> {
        Ok(matches!(self.get(ordinal), Some(Data::Null)))
    }
    fn present_at(&self, ordinal: usize) -> Result<bool, Error> {
        Ok(self.get(ordinal).is_some())
    }
}

#[derive(Clone)]
pub(crate) enum Expression {
    All,
    Byte(Box<byte::Plan>),
    Compare(Option<usize>, Compare, value::Operand),
    EventId(Identity, Compare, value::Operand),
    Cast(Option<Access>, crate::cast::Plan, Compare, value::Operand),
    CastNull(Option<Access>, crate::cast::Plan),
    Contains(Option<usize>, String),
    Null(Option<usize>),
    Absent(Option<usize>),
    Present(Option<usize>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}
impl Expression {
    pub(crate) fn field(&self) -> Option<usize> {
        match self {
            Self::Byte(plan) => Some(plan.ordinal),
            Self::Cast(access, ..) | Self::CastNull(access, _) => access.map(|a|a.ordinal),
            Self::Compare(i, ..) | Self::Contains(i, _) | Self::Null(i)
            | Self::Absent(i) | Self::Present(i) => *i,
            _ => None,
        }
    }
    pub(crate) fn fields(&self, out:&mut Vec<usize>) {
        match self {
            Self::And(parts) | Self::Or(parts) => {for p in parts {p.fields(out)}}
            Self::Not(p)=>p.fields(out),
            _=>out.extend(self.field()),
        }
    }
    pub(crate) fn compile(p: &Predicate, rx: &pil_codec::api::Prescription) -> Self {
        if let Some(plan)=byte::Plan::compile(p,rx) {return Self::Byte(Box::new(plan));}
        let ordinal = |name: &str| rx.field_ordinal(name);
        match p {
            Predicate::All => Self::All,
            Predicate::Compare { field, op, value } => {
                Self::Compare(ordinal(field), *op, value::Operand::new(value.clone()))
            }
            Predicate::Cast{..}|Predicate::CastNull{..}=>unreachable!("casts use bound compilation"),
            Predicate::Contains { field, text } => Self::Contains(ordinal(field), text.clone()),
            Predicate::IsNull(f) => Self::Null(ordinal(f)),
            Predicate::IsAbsent(f) => Self::Absent(ordinal(f)),
            Predicate::IsPresent(f) => Self::Present(ordinal(f)),
            Predicate::And(p) => Self::And(p.iter().map(|p| Self::compile(p, rx)).collect()),
            Predicate::Or(p) => Self::Or(p.iter().map(|p| Self::compile(p, rx)).collect()),
            Predicate::Not(p) => Self::Not(Box::new(Self::compile(p, rx))),
        }
    }
    fn bound(p:&Predicate,definition:&crate::SeedDefinition)->Result<Self,Error> {
        if let Some(plan)=byte::Plan::compile(p,&definition.rx){return Ok(Self::Byte(Box::new(plan)));}
        let ordinal=|name:&str|definition.access(name);
        Ok(match p {
            Predicate::Compare { field, op, value } => {
                let field = definition.rx.field_ordinal(field);
                let rhs = value::Operand::new(value.clone());
                match definition.identity.filter(|id| field == Some(id.id)) {
                    Some(id) => Self::EventId(id, *op, rhs),
                    None => Self::Compare(field, *op, rhs),
                }
            }
            Predicate::Cast{field,cast,op,value}=>Self::Cast(ordinal(field),definition.cast_plan(field,*cast)?,*op,value::Operand::new(value.clone())),
            Predicate::CastNull{field,cast}=>Self::CastNull(ordinal(field),definition.cast_plan(field,*cast)?),
            Predicate::And(parts)=>Self::And(parts.iter().map(|p|Self::bound(p,definition)).collect::<Result<_,_>>()?),
            Predicate::Or(parts)=>Self::Or(parts.iter().map(|p|Self::bound(p,definition)).collect::<Result<_,_>>()?),
            Predicate::Not(p)=>Self::Not(Box::new(Self::bound(p,definition)?)),
            _=>Self::compile(p,&definition.rx),
        })
    }
    // Unknown propagates through boolean expressions, rather than turning NULL
    // or missing into a value that can accidentally satisfy NOT(comparison).
    pub(crate) fn evaluate(&self, row: &impl PredicateRow) -> Result<Option<bool>, Error> {
        Ok(match self {
            Self::All => Some(true),
            Self::EventId(id, op, rhs) => row.compare_id(id, rhs)?.map(|order| op.matches(order)),
            Self::Byte(plan) => plan.evaluate(row.byte_at(plan.ordinal)?),
            Self::Cast(None,_,_,_)=>None,
            Self::Cast(Some(i),plan,op,rhs)=>value::compare_operand(row.cast_at(*i,plan)?,rhs)?.map(|c|match op {Compare::Eq=>c==Ordering::Equal,Compare::Ne=>c!=Ordering::Equal,Compare::Lt=>c==Ordering::Less,Compare::Le=>c!=Ordering::Greater,Compare::Gt=>c==Ordering::Greater,Compare::Ge=>c!=Ordering::Less}),
            Self::CastNull(None,_)=>Some(false),
            Self::CastNull(Some(i),plan)=>Some(matches!(row.cast_at(*i,plan)?,NativeValue::Scalar(pil_codec::Value::Null)|NativeValue::Composite(Data::Null))),
            Self::Contains(None, _) => None,
            Self::Contains(Some(i), text) => row.contains_at(*i, text)?,
            Self::Compare(None, _, _) => None,
            Self::Compare(Some(i), op, rhs) => row.compare_at(*i, rhs)?.map(|c| op.matches(c)),
            Self::Null(None) => Some(false),
            Self::Absent(None) => Some(true),
            Self::Present(None) => Some(false),
            Self::Null(Some(i)) => Some(row.null_at(*i)?),
            Self::Absent(Some(i)) => Some(!row.present_at(*i)?),
            Self::Present(Some(i)) => Some(row.present_at(*i)?),
            Self::Not(p) => p.evaluate(row)?.map(|v| !v),
            Self::And(parts) => {
                let mut result = Some(true);
                for p in parts {
                    match p.evaluate(row)? {
                        Some(false) => return Ok(Some(false)),
                        None => result = None,
                        _ => {}
                    }
                }
                result
            }
            Self::Or(parts) => {
                let mut result = Some(false);
                for p in parts {
                    match p.evaluate(row)? {
                        Some(true) => return Ok(Some(true)),
                        None => result = None,
                        _ => {}
                    }
                }
                result
            }
        })
    }
}
struct Plan {
    definition: Arc<crate::SeedDefinition>,
    program: Arc<Program>,
}
struct Program {
    clock: Option<(usize, u64)>,
    #[cfg(feature = "registration")]
    after_seek: Option<Arc<Expression>>,
    #[cfg(feature = "registration")]
    time: Option<TimeBounds>,
    predicate: Arc<Expression>,
    projection: Vec<(String, Option<Access>)>,
    casts: Vec<Option<crate::cast::Plan>>,
    order: Option<Access>,
}
impl std::ops::Deref for Plan {
    type Target=Program;
    fn deref(&self)->&Program { &self.program }
}
/// Registration-scoped plans contain no Pack data or owner references. An
/// immutable prescription owns this bounded cache; cursors own all read state.
#[derive(Default)]
pub(crate) struct Plans(std::sync::Mutex<std::collections::VecDeque<(Query,Arc<Program>,usize)>>);
impl Plans {
    fn get(&self, definition:&crate::SeedDefinition, query:&Query)->Result<Arc<Program>,Error> {
        let clocked=definition.clocked(&query.predicate);
        let mut entries=self.0.lock().unwrap_or_else(|e|e.into_inner());
        if let Some(index)=entries.iter().position(|(stored,_,_)|stored.predicate==query.predicate
            && (!clocked || stored.context==query.context)
            && stored.casts==query.casts
            && stored.projection==query.projection
            && stored.order.as_ref().map(|(name,_)|name)==query.order.as_ref().map(|(name,_)|name)) {
            let entry=entries.remove(index).unwrap();let program=entry.1.clone();entries.push_back(entry);return Ok(program);
        }
        let mut names=query.projection.clone().unwrap_or_else(||definition.query_fields());
        for (_,alias,_) in &query.casts {if !names.contains(alias){names.push(alias.clone());}}
        let casts=if query.casts.is_empty(){Vec::new()}else{names.iter().map(|name|query.casts.iter().find(|(_,alias,_)|alias==name).map(|(field,_,spec)|definition.cast_plan(field,*spec)).transpose()).collect::<Result<Vec<_>,_>>()?};
        let predicate=definition.prepare(&query.predicate,&query.context)?;
        #[cfg(feature = "registration")]
        let time = TimeBounds::bind(definition, &predicate);
        #[cfg(feature = "registration")]
        let rest = if matches!(time, Some(TimeBounds::Candidates(_))) {
            time::after_seek(definition, &predicate)?
        } else { None };
        #[cfg(feature = "registration")]
        let residual_bytes = rest.as_ref().map_or(0, |(_, bytes)| *bytes);
        #[cfg(not(feature = "registration"))]
        let residual_bytes = 0;
        let program=Arc::new(Program {
            clock: crate::seed::identity::clock(&definition.annotations),
            #[cfg(feature = "registration")]
            time,
            #[cfg(feature = "registration")]
            after_seek: rest.map(|(expr, _)| Arc::new(expr)),
            predicate:Arc::new(Expression::bound(&predicate,definition)?),
            casts,
            projection:names.into_iter().map(|name|{let field=query.casts.iter().find(|(_,alias,_)|alias==&name).map_or(name.as_str(),|(field,_,_)|field.as_str());let access=definition.access(field);(name,access)}).collect(),
            order:query.order.as_ref().and_then(|(name,_)|definition.access(name)),
        });
        // Conservative retained-allocation estimate, including both authored
        // predicate and compiled expression. Oversized plans execute uncached.
        let bytes=512usize.saturating_add(residual_bytes).saturating_add(query.context.timezone.len()).saturating_add(predicate_bytes(&query.predicate)).saturating_add(predicate_bytes(&predicate))
            .saturating_add(query.casts.iter().map(|(field,alias,_)|field.len()+alias.len()+256).sum::<usize>())
            .saturating_add(program.projection.iter().map(|(n,_)|n.len()+128).sum::<usize>())
            .saturating_add(query.order.as_ref().map_or(0,|(name,_)|name.len()+64));
        if bytes<=64*1024 {
            while entries.len()>=32 || entries.iter().map(|e|e.2).sum::<usize>()+bytes>64*1024 {entries.pop_front();}
            let mut key=query.clone();key.limit=None;key.offset=0;key.version=None;
            if let Some((_,direction))=&mut key.order {*direction=Direction::Ascending;}
            entries.push_back((key,program.clone(),bytes));
        }
        Ok(program)
    }
}
fn predicate_bytes(predicate:&Predicate)->usize {
    fn data(value:&Data)->usize {128+match value {
        Data::Text(s)=>s.len(),Data::Bytes(b)=>b.len(),Data::Array(a)=>a.iter().map(data).sum(),
        Data::Map(m)=>m.iter().map(|(k,v)|data(k)+data(v)).sum(),Data::Tag(_,v)=>data(v),_=>0,
    }}
    128+match predicate {
        Predicate::All=>0,Predicate::CastNull{field,..}=>field.len(),Predicate::Cast{field,value,..}|Predicate::Compare{field,value,..}=>field.len()+data(value),
        Predicate::Contains{field,text}=>field.len()+text.len(),
        Predicate::IsNull(f)|Predicate::IsAbsent(f)|Predicate::IsPresent(f)=>f.len(),
        Predicate::And(p)|Predicate::Or(p)=>p.iter().map(predicate_bytes).sum(),Predicate::Not(p)=>predicate_bytes(p),
    }
}

// Ordinary Packs retain one plan. Mixed Packs share the query's existing
// compiled map and select by the record's registered prescription.
#[derive(Clone)]
struct PackPlans {
    base: Arc<Plan>,
    mixed: Option<Arc<HashMap<usize, Arc<Plan>>>>,
}
impl PackPlans {
    fn select<'a>(&'a self, source: &ReadPack, ordinal: usize) -> Result<Option<&'a Arc<Plan>>, Error> {
        match &self.mixed {
            None => Ok(Some(&self.base)),
            Some(plans) => Ok(plans.get(&(Arc::as_ptr(source.definition_at(ordinal)?) as usize))),
        }
    }
    fn required<'a>(&'a self, source: &ReadPack, ordinal: usize) -> Result<&'a Arc<Plan>, Error> {
        self.select(source, ordinal)?.ok_or_else(|| error("result row is outside the query Definition scope"))
    }
}

struct ScanProgress<'a> {
    pack: &'a ReadPack,
    plans: &'a PackPlans,
    window: &'a std::ops::Range<u64>,
    latest: &'a mut Option<u64>,
}

/// Pack-local filtering. Fetch size is a delivery batch size, not a query LIMIT.
pub struct PackScan {
    reader: ReadCursor,
    storage: Option<BindingReader>,
    predicate: Arc<Expression>,
    dispatch: Option<(Arc<ReadPack>, Arc<PackPlans>)>,
    next: usize,
    end: usize,
    exclude_matches: bool,
    _memory: Option<Arc<MemoryLease>>,
}
impl SeedPackSnapshot {
    pub fn filter(self: &Arc<Self>, predicate: Predicate) -> Result<PackScan, Error> {
        validate_names(
            std::slice::from_ref(self),
            &Query::new().filter(predicate.clone()),
        )?;
        let query=Query::new().filter(predicate.clone()).select(std::iter::empty::<String>());
        let program=self.definition.plans.get(&self.definition,&query)?;
        Ok(PackScan {
            _memory: self.memory.clone(),
            reader: ReadCursor::Resident(self.pack.reader()),
            storage: self.storage.as_ref().map(BindingReader::new),
            dispatch: if let Some(lineage) = &self.lineage {
                let compiled: HashMap<_, _> = lineage.values().map(|definition| {
                    Ok((Arc::as_ptr(definition) as usize, Arc::new(Plan { definition: definition.clone(),
                        program: definition.plans.get(definition, &query)? })))
                }).collect::<Result<_,Error>>()?;
                let base = compiled.values().next().unwrap().clone();
                Some((ReadPack::resident(self.clone()), Arc::new(PackPlans { base, mixed: Some(Arc::new(compiled)) })))
            } else { None },
            predicate: program.predicate.clone(),
            next: 0,
            end: self.len(),
            exclude_matches: false,
        })
    }
}
impl PackScan {
    pub(crate) fn excluding(pack: &SeedPackSnapshot, predicate: Arc<Expression>) -> Self {
        Self {
            _memory: pack.memory.clone(),
            reader: ReadCursor::Resident(pack.pack.reader()),
            storage: pack.storage.as_ref().map(BindingReader::new),
            predicate,
            dispatch: None,
            next: 0,
            end: pack.len(),
            exclude_matches: true,
        }
    }
    pub fn next_ordinal(&mut self) -> Result<Option<usize>, Error> {
        self.next_checked(None)
    }
    fn next_checked(&mut self, cancelled: Option<&AtomicBool>) -> Result<Option<usize>, Error> {
        let mut work = usize::MAX;
        self.next_bounded(cancelled, &mut work, None)
    }
    fn next_bounded(&mut self, cancelled: Option<&AtomicBool>, work: &mut usize, mut progress: Option<ScanProgress<'_>>) -> Result<Option<usize>, Error> {
        while self.next < self.end && *work > 0 {
            *work -= 1;
            if self.next % 64 == 0 && cancelled.is_some_and(|c| c.load(AtomicOrdering::Relaxed)) {
                return Err(error("query cancelled"));
            }
            let ordinal = self.next;
            self.next += 1;
            let predicate = if let Some((source, plans)) = &self.dispatch {
                let Some(plan) = plans.select(source, ordinal)? else { continue; };
                &plan.predicate
            } else { &self.predicate };
            let mut pill = None;
            if let Some(p) = &mut progress {
                if let Some((field, scale)) = p.plans.required(p.pack, ordinal)?.clock {
                    let stored = self.storage.as_ref().map(|s| s.value(ordinal, field)).transpose()?.flatten();
                    let value = match stored {
                        Some(value) => value,
                        None => pill.insert(self.reader.get(ordinal)?).native_value(field)?,
                    };
                    if let Some(ms) = crate::seed::identity::ticks(value)
                        .and_then(|t| u64::try_from(t / u128::from(scale)).ok())
                        .filter(|ms| p.window.contains(ms))
                    {
                        *p.latest = (*p.latest).max(Some(ms));
                    }
                }
            }
            // Exact indexed reads do not need the member payload.
            if matches!(predicate.as_ref(), Expression::All) {
                if !self.exclude_matches { return Ok(Some(ordinal)); }
                continue;
            }
            let row = BoundRow {
                row: match pill { Some(row) => row, None => self.reader.get(ordinal)? },
                ordinal,
                storage: self.storage.as_ref(),
            };
            if (predicate.evaluate(&row)? == Some(true)) != self.exclude_matches {
                return Ok(Some(ordinal));
            }
        }
        Ok(None)
    }
    pub fn fetch(&mut self, count: usize) -> Result<Vec<usize>, Error> {
        let mut rows = Vec::new();
        while rows.len() < count {
            if let Some(row) = self.next_ordinal()? {
                rows.push(row);
            } else {
                break;
            }
        }
        Ok(rows)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    pack: usize,
    row: usize,
}
/// Immutable Pack selection supplied by the storage owner. Loading one ordinal
/// must return the same published Pack and index/state bindings on every call.
/// Implementations pin the file publication, not all of its payloads, and may
/// reuse their existing bounded resident cache. No host predicate work occurs.
pub trait PackSource: Send {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }
    fn definitions(&self) -> Vec<Arc<crate::SeedDefinition>>;
    fn load(&mut self, ordinal: usize) -> Result<Arc<SeedPackSnapshot>, Error>;
    /// Query-only ownership can keep member bytes in positional storage.
    fn read(&mut self, ordinal: usize) -> Result<Arc<ReadPack>, Error> {
        self.load(ordinal).map(ReadPack::resident)
    }
    /// Optional source-index narrowing, evaluated before loading payload bytes.
    /// An empty range skips a Pack; None selects all rows. Ordinals stay intact.
    fn rows(&mut self, _ordinal:usize)->Result<Option<std::ops::Range<usize>>,Error> {Ok(None)}
    /// The boolean indicates that the source proved the entire predicate for all retained rows.
    fn filter_rows(&mut self, ordinal: usize, _query: &mut SourceFilter<'_>, _cancel: &AtomicBool) -> Result<(RowSelection, bool), Error> {
        Ok((self.row_selection(ordinal)?, false))
    }
    fn row_selection(&mut self, ordinal:usize)->Result<RowSelection,Error> {
        Ok(match self.rows(ordinal)? { Some(rows)=>RowSelection::Range(rows), None=>RowSelection::All })
    }
}

/// Source-index selections retain the original Pack and row ordinals. The
/// single-range path needs no allocation; disjoint periods share one reader.
pub enum RowSelection {
    All,
    Range(std::ops::Range<usize>),
    Ranges(Vec<std::ops::Range<usize>>),
}

enum Catalog {
    Resident(Vec<Arc<ReadPack>>),
    Paged { source: Box<dyn PackSource>, cached: Vec<Weak<ReadPack>> },
}
impl Catalog {
    fn rows(&mut self,ordinal:usize, query:&mut SourceFilter<'_>,cancel:&AtomicBool)->Result<(RowSelection,bool),Error> {
        match self {Self::Resident(_)=>Ok((RowSelection::All,false)),Self::Paged {source,..}=>source.filter_rows(ordinal,query,cancel)}
    }
    fn len(&self) -> usize {
        match self { Self::Resident(packs) => packs.len(), Self::Paged { cached, .. } => cached.len() }
    }
    fn load(&mut self, ordinal: usize) -> Result<Arc<ReadPack>, Error> {
        match self {
            Self::Resident(packs) => Ok(packs[ordinal].clone()),
            Self::Paged { source, cached } => {
                if let Some(pack) = cached[ordinal].upgrade() { return Ok(pack); }
                let pack = source.read(ordinal)?;
                cached[ordinal] = Arc::downgrade(&pack);
                Ok(pack)
            }
        }
    }
}
/// A cursor bounds delivery and enforces one global offset/limit. Unordered
/// queries stop scanning as soon as that global limit is satisfied.
pub struct Cursor {
    catalog: Catalog,
    #[cfg(feature = "registration")]
    time: Option<TimeBounds>,
    plans: Vec<Option<PackPlans>>,
    compiled: Arc<HashMap<usize, Arc<Plan>>>,
    current: Option<Arc<ReadPack>>,
    page_packs: HashMap<usize, Arc<ReadPack>>,
    page_last: Option<usize>,
    query: Query,
    pack: usize,
    reader: Option<PackScan>,
    index_pending:bool,
    #[cfg(feature="registration")]
    index_window:Option<crate::reader::IndexWindow>,
    row_ranges: std::vec::IntoIter<std::ops::Range<usize>>,
    skipped: usize,
    delivered: usize,
    sorted: Option<std::vec::IntoIter<Position>>,
    cancelled: Arc<AtomicBool>,
    slice_sort: Option<SliceSort>,
    slice_sorted: Option<Vec<Position>>,
    slice_positions_memory: Option<MemoryLease>,
    slice_memory: MemoryBudget,
    slice_started: bool,
    pub scanned: usize,
    pub matched: usize,
    pub checkpoint_ms: Option<u64>,
    #[cfg(feature = "planner-lab")]
    pub(crate) lab: crate::planner_lab::Execution,
}
impl Cursor {
    pub(crate) fn consuming(packs:Vec<Arc<SeedPackSnapshot>>,query:Query)->Result<Self,Error> {
        // Ordered queries can revisit Packs after their key pass. Preserve the
        // existing ordering path; streaming queries relinquish each load.
        if query.order.is_some() {return Self::new(packs,query);}
        struct Drain {packs:Vec<Option<Arc<SeedPackSnapshot>>>}
        impl PackSource for Drain {
            fn len(&self)->usize {self.packs.len()}
            fn definitions(&self)->Vec<Arc<crate::SeedDefinition>> {self.packs.iter().flatten().flat_map(|p|p.definitions().cloned()).collect()}
            fn load(&mut self,i:usize)->Result<Arc<SeedPackSnapshot>,Error> {
                self.packs.get_mut(i).and_then(Option::take).ok_or_else(||error("consumed Stalk source"))
            }
        }
        Self::from_source(Drain{packs:packs.into_iter().map(Some).collect()},query)
    }
    pub fn stalks(self,budget:MemoryBudget,row_limit:usize)->crate::stalks::Growth {
        crate::stalks::Growth::new(self,budget,row_limit)
    }
    pub(crate) fn new(mut packs: Vec<Arc<SeedPackSnapshot>>, query: Query) -> Result<Self, Error> {
        if let Some(version) = query.version {
            packs.retain(|pack| pack.definitions().any(|d| d.version == version));
        }
        let definitions = packs.iter().flat_map(|p| p.definitions().cloned()).collect();
        Self::create(Catalog::Resident(packs.into_iter().map(ReadPack::resident).collect()), definitions, query)
    }
    /// Use the existing evaluator with on-demand, immutable storage. Sorting
    /// retains keys and ordinals only; selected Packs are reopened for delivery.
    /// Result batches retain their own owners. No complete dataset is loaded.
    pub fn from_source(source: impl PackSource + 'static, query: Query) -> Result<Self, Error> {
        let definitions = source.definitions();
        let mut cached = Vec::new();
        cached.try_reserve_exact(source.len()).map_err(|_| error("source catalog allocation failed"))?;
        cached.resize_with(source.len(), Weak::new);
        Self::create(Catalog::Paged { source: Box::new(source), cached }, definitions, query)
    }
    fn create(catalog: Catalog, mut definitions: Vec<Arc<crate::SeedDefinition>>, query: Query) -> Result<Self, Error> {
        if let Some(version) = query.version { definitions.retain(|d| d.version == version); }
        validate_definition_names(&definitions, &query)?;
        if query.order.is_some() && query.limit.is_none() {
            return Err(error("ordered query requires a global limit in this implementation"));
        }
        query.offset.checked_add(query.limit.unwrap_or(0)).ok_or(Error::Overflow)?;
        let mut compiled = HashMap::new();
        for definition in &definitions {
            let identity = Arc::as_ptr(definition) as usize;
            if let std::collections::hash_map::Entry::Vacant(entry)=compiled.entry(identity) {
                entry.insert(Arc::new(Plan { definition:definition.clone(), program:definition.plans.get(definition,&query)? }));
            }
        }
        // Retain the prescriptions in plans through the source's immutable
        // registration scope. Every loaded Pack must use that exact scope.
        let mut plans = Vec::new();
        plans.try_reserve_exact(catalog.len()).map_err(|_| error("query plan catalog allocation failed"))?;
        plans.resize_with(catalog.len(), || None);
        // One physical time seek is valid across mixed Packs only when every
        // participating Definition binds the same bounds and proof.
        #[cfg(feature = "registration")]
        let time = {
            let mut bounds = compiled.values().map(|p| p.time.as_ref());
            bounds.next().flatten().filter(|first| bounds.all(|b| b == Some(*first))).cloned()
        };
        Ok(Self {
            catalog,
            #[cfg(feature = "registration")]
            time,
            plans,
            compiled: Arc::new(compiled),
            current: None,
            page_packs: HashMap::new(),
            page_last: None,
            query,
            pack: 0,
            reader: None,
            index_pending:false,
            #[cfg(feature="registration")]
            index_window:None,
            row_ranges: Vec::new().into_iter(),
            skipped: 0,
            delivered: 0,
            sorted: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            slice_sort: None,
            slice_sorted: None,
            slice_positions_memory: None,
            slice_memory: MemoryBudget::new(32 * 1024 * 1024),
            slice_started: false,
            scanned: 0,
            matched: 0,
            checkpoint_ms: None,
            #[cfg(feature = "planner-lab")]
            lab: crate::planner_lab::Execution::default(),
        })
    }
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Relaxed);
    }
    /// Share this accounting pool across a node's cursors. It charges the
    /// resumable top-K arrays and owned sort keys, not all query/codec memory.
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Result<Self, Error> {
        if self.slice_started || self.scanned != 0 || self.delivered != 0 {
            return Err(error("set query budget before execution"));
        }
        self.slice_memory = budget;
        Ok(self)
    }
    /// Owner-side cancellation releases cursor-held snapshots immediately.
    /// Result batches already delivered retain their own independent leases.
    pub fn abort(&mut self) {
        self.cancel();
        self.release_execution();
    }
    fn release_execution(&mut self) {
        self.reader = None;
        self.row_ranges = Vec::new().into_iter();
        self.index_pending=false;
        #[cfg(feature="registration")]
        {self.index_window=None;}
        self.slice_sort = None;
        self.slice_sorted = None;
        self.slice_positions_memory = None;
        self.sorted = None;
        self.current = None;
        self.page_packs.clear();
        self.page_last = None;
        self.catalog = Catalog::Resident(Vec::new());
        self.plans = Vec::new();
        self.compiled = Arc::new(HashMap::new());
    }
    /// Yield after at most `work` row/transition/heap operations. Hosts must
    /// yield their event loop between calls; a microtask loop is insufficient.
    /// This is cooperative work bounding, not a hard wall-clock deadline.
    pub fn poll(&mut self, count: usize, work: usize) -> Result<QuerySlice, Error> {
        if count == 0 || count > 4096 || work == 0 {
            return Err(error("poll requires 1..4096 result rows and positive work"));
        }
        if !self.slice_started && (self.scanned != 0 || self.sorted.is_some() || self.delivered != 0) {
            return Err(error("choose poll before synchronous fetch"));
        }
        #[cfg(feature = "planner-lab")]
        if self.lab.selection == crate::planner_lab::Selection::FullSortReference {
            return Err(error("reference-sort experiment uses synchronous fetch"));
        }
        self.slice_started = true;
        let result = self.poll_inner(count, work);
        if result.is_err() { self.abort(); }
        result
    }
    fn poll_inner(&mut self, count: usize, budget: usize) -> Result<QuerySlice, Error> {
        if self.cancelled.load(AtomicOrdering::Relaxed) { return Err(error("query cancelled")); }
        let mut work = budget;
        if self.query.limit.is_some_and(|n| self.delivered >= n) || self.catalog.len() == 0 {
            let batch = self.batch(Vec::new())?;
            self.release_execution();
            return Ok(QuerySlice { batch, done: true, work: 0 });
        }
        if self.query.order.is_some() && self.slice_sorted.is_none() && !self.sort_slice(&mut work)? {
            return Ok(QuerySlice { batch: self.batch(Vec::new())?, done: false, work: budget - work });
        }
        let mut positions = Vec::new();
        while positions.len() < count && work > 0 && self.query.limit.is_none_or(|n| self.delivered < n) {
            if self.cancelled.load(AtomicOrdering::Relaxed) { return Err(error("query cancelled")); }
            let position = if let Some(sorted) = &mut self.slice_sorted {
                work -= 1;
                sorted.pop()
            } else { self.next_match_slice(&mut work)? };
            let Some(position) = position else { break; };
            if self.slice_sorted.is_none() && self.skipped < self.query.offset {
                self.skipped += 1;
                continue;
            }
            self.retain_page_pack(position);
            positions.push(position);
            self.delivered += 1;
        }
        let done = self.query.limit.is_some_and(|n| self.delivered >= n)
            || self.slice_sorted.as_ref().is_some_and(Vec::is_empty)
            || (self.query.order.is_none() && self.scan_finished());
        let batch = self.batch(positions)?;
        if done { self.release_execution(); }
        Ok(QuerySlice { batch, done, work: budget - work })
    }
    fn scan_finished(&self) -> bool {
        self.pack >= self.catalog.len() || (self.pack + 1 == self.catalog.len()
            && self.reader.as_ref().is_some_and(|r| r.next == r.end) && self.row_ranges.len() == 0 && !self.index_pending)
    }
    fn next_match_slice(&mut self, work: &mut usize) -> Result<Option<Position>, Error> {
        while *work > 0 && self.pack < self.catalog.len() {
            if self.cancelled.load(AtomicOrdering::Relaxed) { return Err(error("query cancelled")); }
            if self.reader.is_none() && !self.start_pack(work)? {
                *work=work.saturating_sub(1);
                if !self.index_pending{self.pack+=1;}
                continue;
            }
            let reader = self.reader.as_mut().unwrap();
            let before = reader.next;
            let progress = self.query.checkpoint.as_ref().map(|window| ScanProgress {
                window, pack: self.current.as_ref().unwrap(), plans: self.plans[self.pack].as_ref().unwrap(),
                latest: &mut self.checkpoint_ms,
            });
            let result = reader.next_bounded(Some(&self.cancelled), work, progress);
            self.scanned += reader.next - before;
            if let Some(row) = result? {
                self.matched += 1;
                return Ok(Some(Position { pack: self.pack, row }));
            }
            if reader.next == reader.end {
                // Empty Packs consume work too, so they cannot bypass yielding.
                if *work == 0 { break; }
                *work -= 1;
                if let Some(rows) = self.row_ranges.next() {
                    reader.next = rows.start; reader.end = rows.end;
                    continue;
                }
                if !self.index_pending{self.pack+=1;}
                self.reader = None;
                self.current = None;
            }
        }
        Ok(None)
    }
    fn sort_slice(&mut self, work: &mut usize) -> Result<bool, Error> {
        let count = self.query.offset.checked_add(self.query.limit.unwrap()).ok_or(Error::Overflow)?;
        let mut state = match self.slice_sort.take() {
            Some(state) => state,
            None => {
                #[cfg(feature = "planner-lab")]
                if count > self.lab.max_candidates { return Err(error("planner lab: top-K exceeds candidate budget")); }
                let bytes = count.checked_mul(std::mem::size_of::<SliceCandidate>() + std::mem::size_of::<Position>()).ok_or(Error::Overflow)?;
                let memory = self.slice_memory.reserve(bytes)?;
                let mut heap = BinaryHeap::new();
                let mut positions = Vec::new();
                heap.try_reserve_exact(count).map_err(|_| error("sort allocation failed"))?;
                positions.try_reserve_exact(count).map_err(|_| error("sort allocation failed"))?;
                SliceSort { heap, positions, kind: None, draining: false, _memory: memory }
            }
        };
        while *work > 0 && !state.draining {
            let Some(position) = self.next_match_slice(work)? else {
                state.draining = self.scan_finished();
                break;
            };
            let reader = self.reader.as_mut().unwrap();
            let row = BoundRow { row: reader.reader.get(position.row)?,
                ordinal: position.row, storage: reader.storage.as_ref() };
            let plan = self.plans[position.pack].as_ref().unwrap().required(self.current.as_ref().unwrap(), position.row)?;
            let (key, lease) = match plan.order {
                Some(i) => {
                    let native = i.read(&row, plan.definition.identity.as_ref())?;
                    // Charge borrowed text/bytes before owning them. Composite
                    // scalar storage may already own its value in the codec.
                    let length = match &native {
                        NativeValue::Scalar(pil_codec::Value::String(s)) => s.len(),
                        NativeValue::Scalar(pil_codec::Value::Bytes(s)) => s.len(),
                        NativeValue::Scalar(pil_codec::Value::Uuid(_)) => 16,
                        NativeValue::Composite(Data::Text(s)) => s.capacity(),
                        NativeValue::Composite(Data::Bytes(s)) => s.capacity(),
                        NativeValue::Scalar(pil_codec::Value::Array(_) | pil_codec::Value::Object(_)) => return Err(error("ordering requires a scalar")),
                        _ => 0,
                    };
                    let lease = self.slice_memory.reserve(length)?;
                    (SortKey::new(value::own(native)?)?, lease)
                }
                None => (SortKey::Missing, self.slice_memory.reserve(0)?),
            };
            if let Some(kind) = key.kind() {
                if state.kind.is_some_and(|old| old != kind) { return Err(error("ordering across incompatible field types requires an explicit cast")); }
                state.kind = Some(kind);
            }
            let candidate = SliceCandidate { candidate: Candidate { position, key, direction: self.query.order.as_ref().unwrap().1 }, _key: lease };
            if state.heap.len() < count { state.heap.push(candidate); }
            else if state.heap.peek().is_some_and(|worst| candidate < *worst) { *state.heap.peek_mut().unwrap() = candidate; }
            #[cfg(feature = "planner-lab")]
            { self.lab.peak_candidates = self.lab.peak_candidates.max(state.heap.len()); }
        }
        while state.draining && *work > 0 {
            if self.cancelled.load(AtomicOrdering::Relaxed) { return Err(error("query cancelled")); }
            let Some(candidate) = state.heap.pop() else { break; };
            state.positions.push(candidate.candidate.position);
            *work -= 1;
        }
        if state.draining && state.heap.is_empty() {
            // Worst first: pop delivers best first, without an unbounded
            // reverse/sort pass at the end of the scan.
            state.positions.truncate(state.positions.len().saturating_sub(self.query.offset));
            self.slice_sorted = Some(state.positions);
            self.slice_positions_memory = Some(state._memory);
            self.reader = None;
            Ok(true)
        } else { self.slice_sort = Some(state); Ok(false) }
    }
    fn next_match(&mut self) -> Result<Option<Position>, Error> {
        while self.pack < self.catalog.len() {
            if self.cancelled.load(AtomicOrdering::Relaxed) {
                return Err(error("query cancelled"));
            }
            if self.reader.is_none() && !self.start_pack(&mut {usize::MAX})? {
                self.pack += 1;
                continue;
            }
            let reader = self.reader.as_mut().unwrap();
            let before = reader.next;
            let progress = self.query.checkpoint.as_ref().map(|window| ScanProgress {
                window, pack: self.current.as_ref().unwrap(), plans: self.plans[self.pack].as_ref().unwrap(),
                latest: &mut self.checkpoint_ms,
            });
            let mut work = usize::MAX;
            let result = reader.next_bounded(Some(&self.cancelled), &mut work, progress);
            self.scanned += reader.next - before;
            if let Some(row) = result? {
                self.matched += 1;
                return Ok(Some(Position {
                    pack: self.pack,
                    row,
                }));
            }
            if let Some(rows) = self.row_ranges.next() {
                reader.next = rows.start; reader.end = rows.end;
                continue;
            }
            self.pack += 1;
            self.reader = None;
            self.current = None;
        }
        Ok(None)
    }
    fn sort_top(&mut self) -> Result<(), Error> {
        let count = self
            .query
            .offset
            .checked_add(self.query.limit.unwrap())
            .ok_or(Error::Overflow)?;
        let direction = self.query.order.as_ref().unwrap().1;
        let mut heap = BinaryHeap::<Candidate>::new();
        #[cfg(feature = "planner-lab")]
        let mut reference = Vec::<Candidate>::new();
        #[cfg(feature = "planner-lab")]
        if self.lab.selection == crate::planner_lab::Selection::BoundedTopK
            && count > self.lab.max_candidates
        {
            return Err(error("planner lab: top-K exceeds candidate budget"));
        }
        let mut kind = None;
        if self.query.limit != Some(0) {
            while let Some(position) = self.next_match()? {
                let reader = self.reader.as_mut().unwrap();
                let row = BoundRow {
                    row: reader.reader.get(position.row)?,
                    ordinal: position.row,
                    storage: reader.storage.as_ref(),
                };
                let plan = self.plans[position.pack].as_ref().unwrap().required(self.current.as_ref().unwrap(), position.row)?;
                let key = match plan.order {
                    Some(i) => SortKey::new(value::own(i.read(&row, plan.definition.identity.as_ref())?)?)?,
                    None => SortKey::Missing,
                };
                if let Some(k) = key.kind() {
                    if kind.is_some_and(|old| old != k) {
                        return Err(error(
                            "ordering across incompatible field types requires an explicit cast",
                        ));
                    }
                    kind = Some(k);
                }
                let candidate = Candidate {
                    position,
                    key,
                    direction,
                };
                #[cfg(feature = "planner-lab")]
                if self.lab.selection == crate::planner_lab::Selection::FullSortReference {
                    if reference.len() == self.lab.max_candidates {
                        return Err(error(
                            "planner lab: reference sort exceeds candidate budget",
                        ));
                    }
                    reference.push(candidate);
                    self.lab.peak_candidates = self.lab.peak_candidates.max(reference.len());
                    continue;
                }
                if heap.len() < count {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                    *heap.peek_mut().unwrap() = candidate;
                }
                #[cfg(feature = "planner-lab")]
                {
                    self.lab.peak_candidates = self.lab.peak_candidates.max(heap.len());
                }
            }
        }
        self.reader = None;
        #[cfg(feature = "planner-lab")]
        if self.lab.selection == crate::planner_lab::Selection::FullSortReference {
            reference.sort_unstable();
            self.sorted = Some(
                reference
                    .into_iter()
                    .take(count)
                    .skip(self.query.offset)
                    .map(|v| v.position)
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
            return Ok(());
        }
        self.sorted = Some(
            heap.into_sorted_vec()
                .into_iter()
                .skip(self.query.offset)
                .map(|v| v.position)
                .collect::<Vec<_>>()
                .into_iter(),
        );
        Ok(())
    }
    pub fn fetch(&mut self, count: usize) -> Result<Batch, Error> {
        // Once a caller chooses resumable execution it cannot restart sorting
        // through the synchronous path and lose already scanned candidates.
        if self.slice_started {
            return Err(error("resumable query must continue through poll"));
        }
        let result = self.fetch_inner(count);
        if result.is_err() { self.abort(); }
        result
    }
    fn fetch_inner(&mut self, count: usize) -> Result<Batch, Error> {
        if self.cancelled.load(AtomicOrdering::Relaxed) {
            return Err(error("query cancelled"));
        }
        let mut positions = Vec::new();
        if count > 0 && self.query.order.is_some() && self.sorted.is_none() {
            self.sort_top()?;
        }
        while positions.len() < count && self.query.limit.is_none_or(|n| self.delivered < n) {
            let next = if let Some(sorted) = &mut self.sorted {
                sorted.next()
            } else {
                self.next_match()?
            };
            let Some(position) = next else {
                break;
            };
            if self.sorted.is_none() && self.skipped < self.query.offset {
                self.skipped += 1;
                continue;
            }
            self.retain_page_pack(position);
            positions.push(position);
            self.delivered += 1;
        }
        let done = self.query.limit.is_some_and(|n| self.delivered >= n)
            || self.sorted.as_ref().is_some_and(|rows| rows.len() == 0)
            || (self.query.order.is_none() && self.scan_finished());
        let batch = self.batch(positions)?;
        if done { self.release_execution(); }
        Ok(batch)
    }
    fn start_pack(&mut self,work:&mut usize) -> Result<bool, Error> {
        let mut filter=SourceFilter{query:&self.query,compiled:&self.compiled,work,pending:&mut self.index_pending,
            #[cfg(feature="registration")]
            time:self.time.as_ref(),
            #[cfg(all(feature = "planner-lab", feature = "registration"))]
            access:self.lab.access,
            #[cfg(feature="registration")]
            window:&mut self.index_window,
            #[cfg(feature="registration")]
            goal: match (self.query.order.as_ref(), self.query.limit) {
                (None, Some(limit)) => limit.saturating_sub(self.delivered)
                    .saturating_add(self.query.offset.saturating_sub(self.skipped)),
                _ => usize::MAX,
            },
        };
        let (selected,proven)=self.catalog.rows(self.pack,&mut filter,&self.cancelled)?;
        self.row_ranges = Vec::new().into_iter();
        if matches!(&selected, RowSelection::Range(rows) if rows.is_empty())
            || matches!(&selected, RowSelection::Ranges(rows) if rows.is_empty()) { return Ok(false); }
        let source = self.catalog.load(self.pack)?;
        if source.lineage.is_none() && self.query.version.is_some_and(|v| source.definition.version != v) { return Ok(false); }
        let base = if let Some(lineage) = &source.lineage {
            let Some(plan) = lineage.values().find_map(|d| self.compiled.get(&(Arc::as_ptr(d) as usize))) else { return Ok(false); };
            plan.clone()
        } else {
            self.compiled.get(&(Arc::as_ptr(&source.definition) as usize))
                .ok_or_else(|| error("Pack prescription is outside the query source scope"))?.clone()
        };
        if source.lineage.is_none() && self.plans[self.pack].as_ref().is_some_and(|old| !Arc::ptr_eq(&old.base, &base)) {
            return Err(error("query source changed its Pack prescription"));
        }
        let plan = PackPlans { base, mixed: source.lineage.as_ref().map(|_| self.compiled.clone()) };
        self.plans[self.pack] = Some(plan.clone());
        let rows=match selected {
            RowSelection::All=>0..source.len(),
            RowSelection::Range(rows)=>rows,
            RowSelection::Ranges(rows)=>{
                let mut prior=0;
                for row in &rows {
                    if row.start < prior || row.start >= row.end || row.end > source.len() {
                        return Err(error("source row ranges must be ordered, disjoint and inside Pack"));
                    }
                    prior=row.end;
                }
                self.row_ranges=rows.into_iter();
                self.row_ranges.next().unwrap()
            }
        };
        if rows.start>rows.end || rows.end>source.len() {return Err(error("source row range exceeds Pack"));}
        // A mixed Pack still uses its per-row Definition for projection.
        // An explicit version restriction also needs that dispatch while filtering.
        let exact = proven && source.lineage.as_ref().is_none_or(|members|
            members.values().all(|d| self.compiled.contains_key(&(Arc::as_ptr(d) as usize))));
        let dispatch = if exact { None } else {
            plan.mixed.as_ref().map(|_| (source.clone(), Arc::new(plan.clone())))
        };
        self.reader = Some(PackScan { _memory: source.memory.clone(), reader: source.pack.reader(), storage: source.storage.as_ref().map(BindingReader::new),
            predicate: if exact { Arc::new(Expression::All) } else {plan.base.predicate.clone()}, dispatch, next: rows.start, end: rows.end, exclude_matches: false });
        self.current = Some(source);
        Ok(true)
    }
    fn retain_page_pack(&mut self, position: Position) {
        // Resident sources already own their payloads. For on-demand scans,
        // pin once per contributing Pack, not once per matching row.
        if matches!(self.catalog, Catalog::Resident(_)) || self.page_last == Some(position.pack) {
            return;
        }
        if position.pack == self.pack {
            if let Some(pack) = &self.current {
                self.page_packs.entry(position.pack).or_insert_with(|| pack.clone());
                self.page_last = Some(position.pack);
            }
        }
    }
    fn batch(&mut self, mut positions: Vec<Position>) -> Result<Batch, Error> {
        let mut selected = HashMap::new();
        let mut origins = Vec::new();
        let mut packs = Vec::new();
        let mut plans = Vec::new();
        for position in &mut positions {
            let original = position.pack;
            let index = match selected.get(&original) {
                Some(index) => *index,
                None => {
                    let pack = match self.page_packs.remove(&original) {
                        Some(pack) => pack,
                        None => self.catalog.load(original)?,
                    };
                    let plan = self.plans[original].as_ref().unwrap();
                    if matches!(self.catalog, Catalog::Paged { .. })
                        && plan.mixed.is_none()
                        && !self.compiled.get(&(Arc::as_ptr(&pack.definition) as usize))
                            .is_some_and(|actual| Arc::ptr_eq(actual, &plan.base)) {
                        return Err(error("query source changed its Pack prescription"));
                    }
                    let index = packs.len();
                    origins.push(original);
                    packs.push(pack); plans.push(plan.clone()); selected.insert(original, index);
                    index
                }
            };
            position.pack = index;
        }
        self.page_packs.clear();
        self.page_last = None;
        Ok(Batch { packs, plans, positions, origins })
    }
}

/// A slice may be empty without being exhausted. `done` is authoritative.
/// Work counts visited rows, Pack transitions, and heap-drain operations, not
/// elapsed time. A single field operation remains indivisible in the codec.
pub struct QuerySlice { pub batch: Batch, pub done: bool, pub work: usize }
struct SliceSort {
    heap: BinaryHeap<SliceCandidate>,
    positions: Vec<Position>,
    kind: Option<u8>,
    draining: bool,
    _memory: MemoryLease,
}
struct SliceCandidate { candidate: Candidate, _key: MemoryLease }
impl PartialEq for SliceCandidate { fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal } }
impl Eq for SliceCandidate {}
impl PartialOrd for SliceCandidate { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for SliceCandidate { fn cmp(&self, other: &Self) -> Ordering { self.candidate.cmp(&other.candidate) } }

pub struct Batch {
    origins: Vec<usize>,
    packs: Vec<Arc<ReadPack>>,
    plans: Vec<PackPlans>,
    positions: Vec<Position>,
}
impl Batch {
    pub(crate) fn shape(&self,row:usize)->Result<usize,Error> {
        let p=self.positions.get(row).ok_or_else(||error("result ordinal outside batch"))?;
        Ok(self.plans[p.pack].required(&self.packs[p.pack],p.row)?.projection.as_ptr() as usize)
    }
    pub fn origin(&self, row: usize) -> Result<(usize, usize, &[u8]), Error> {
        let p = self.positions.get(row).ok_or_else(|| error("result ordinal outside batch"))?;
        Ok((self.origins[p.pack], p.row, &self.packs[p.pack].definition_at(p.row)?.digest))
    }
    pub fn len(&self) -> usize {
        self.positions.len()
    }
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
    /// Unique backing allocations, including an entire Box retained by a child.
    /// This excludes Definition, addressing-plan, and storage-binding allocations.
    pub fn retained_pack_bytes(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        self.packs
            .iter()
            .filter(|p| seen.insert(p.allocation))
            .map(|p| p.retained_bytes())
            .sum()
    }
    pub fn reader(&self) -> BatchReader<'_> {
        BatchReader {
            batch: self,
            active: None,
        }
    }
    pub fn into_reader(self) -> OwnedBatchReader {
        OwnedBatchReader {
            batch: self,
            active: None,
        }
    }
    pub fn objects(&self) -> Result<Vec<Object>, Error> {
        let mut reader = self.reader();
        (0..self.len()).map(|i| reader.view(i)?.object()).collect()
    }
}
struct BatchPackReader {
    ordinal: usize,
    reader: ReadCursor,
    storage: Option<BindingReader>,
}
pub struct BatchReader<'a> {
    batch: &'a Batch,
    active: Option<BatchPackReader>,
}
impl BatchReader<'_> {
    pub fn view(&mut self, row: usize) -> Result<SeedView<'_>, Error> {
        result_view(self.batch, &mut self.active, row)
    }
}
/// One reader per consumer, shared by all lazy field accesses in its batch.
/// There is no traversal cursor allocated for each returned Seed.
pub struct OwnedBatchReader {
    // Drop the codec cursor before the batch releases its payload admission.
    active: Option<BatchPackReader>,
    batch: Batch,
}
impl OwnedBatchReader {
    pub fn batch(&self) -> &Batch {
        &self.batch
    }
    pub fn view(&mut self, row: usize) -> Result<SeedView<'_>, Error> {
        result_view(&self.batch, &mut self.active, row)
    }
}
fn result_view<'a>(
    batch: &'a Batch,
    active: &'a mut Option<BatchPackReader>,
    row: usize,
) -> Result<SeedView<'a>, Error> {
    let position = batch
        .positions
        .get(row)
        .ok_or_else(|| error("result ordinal outside batch"))?;
    if active.as_ref().is_none_or(|p| p.ordinal != position.pack) {
        let pack = &batch.packs[position.pack];
        #[allow(unused_mut)]
        let mut reader=pack.pack.reader();
        #[cfg(feature="registration")]
        if let ReadCursor::Paged(paged)=&mut reader {
            let last=batch.positions[row..].iter().take_while(|p|p.pack==position.pack)
                .map(|p|p.row).max().unwrap_or(position.row);
            paged.through(last)?;
        }
        *active = Some(BatchPackReader { ordinal: position.pack, reader,
            storage: pack.storage.as_ref().map(BindingReader::new) });
    }
    let active = active.as_mut().unwrap();
    let definition=batch.packs[position.pack].definition_at(position.row)?;
    Ok(SeedView {
        identity: definition.identity.as_ref(),
        definition,
        row: BoundRow {
            row: active.reader.get(position.row)?,
            ordinal: position.row,
            storage: active.storage.as_ref(),
        },
        fields: &batch.plans[position.pack].required(&batch.packs[position.pack], position.row)?.projection,
        casts: &batch.plans[position.pack].required(&batch.packs[position.pack], position.row)?.casts,
    })
}
pub struct SeedView<'a> {
    identity: Option<&'a crate::seed::identity::Identity>,
    definition: &'a Arc<crate::SeedDefinition>,
    row: BoundRow<'a>,
    fields: &'a [(String, Option<Access>)],
    casts: &'a [Option<crate::cast::Plan>],
}
impl SeedView<'_> {
    pub fn definition(&self)->&Arc<crate::SeedDefinition> {self.definition}
    pub(crate) fn scalar_kind_at(&self,projected:usize)->Option<pil_codec::Scalar> {
        if let Some(plan)=self.casts.get(projected).and_then(Option::as_ref) {return Some(plan.spec.scalar());}
        let ordinal=self.fields.get(projected)?.1?.ordinal;
        if let Some(kind)=self.definition.enum_kinds.get(ordinal).copied().flatten() {return Some(kind);}
        match self.row.row.prescription().definition().fields().get(ordinal)?.kind {
            pil_codec::Type::Scalar(kind)=>Some(kind),_=>None,
        }
    }
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|(name, _)| name.as_str())
    }
    pub fn shape_id(&self) -> usize {
        self.fields.as_ptr() as usize
    }
    pub fn is_present_at(&self, projected: usize) -> Result<bool, Error> {
        match self.fields.get(projected) {
            Some((_, Some(i))) => self.row.is_present(i.ordinal),
            Some((_, None)) => Ok(false),
            None => Err(error("projected field ordinal outside range")),
        }
    }
    /// Untransformed field value for storage/accumulator tooling. Public query
    /// views use native_at/get and apply their annotation-derived access plan.
    pub fn stored_at(&self, projected: usize) -> Result<NativeValue<'_>, Error> {
        match self.fields.get(projected) {
            Some((_, Some(access))) => self.row.native_value(access.ordinal),
            Some((_, None)) => Ok(NativeValue::Scalar(pil_codec::Value::Absent)),
            None => Err(error("projected field ordinal outside range")),
        }
    }
    pub fn native_at(&self, projected: usize) -> Result<NativeValue<'_>, Error> {
        match self.fields.get(projected) {
            Some((_, Some(i))) => {let value=self.value(*i)?;match self.casts.get(projected).and_then(Option::as_ref) {Some(plan)=>plan.value(value),None=>Ok(value)}},
            Some((_, None)) => Ok(NativeValue::Scalar(pil_codec::Value::Absent)),
            None => Err(error("projected field ordinal outside range")),
        }
    }
    fn value(&self, access: Access) -> Result<NativeValue<'_>, Error> {
        access.read(&self.row, self.identity)
    }
    pub fn get(&self, name: &str) -> Result<Cell, Error> {
        let i=self.fields.iter().position(|(n,_)|n==name).ok_or_else(||error(format!("field not projected: {name}")))?;
        value::own(self.native_at(i)?)
    }
    /// Read the underlying value without applying annotation views. Used when
    /// rebuilding accumulators from stored events, never to change query meaning.
    pub fn stored(&self, name: &str) -> Result<Cell, Error> {
        let projected=self.fields.iter().position(|(field,_)|field==name)
            .ok_or_else(||error(format!("field not projected: {name}")))?;
        value::own(self.stored_at(projected)?)
    }
    pub fn object(&self) -> Result<Object, Error> {
        let mut object = Vec::with_capacity(self.fields.len());
        for (ordinal,(name, i)) in self.fields.iter().enumerate() {
            if i.is_some() {
                if let Cell::Value(v) = value::own(self.native_at(ordinal)?)? {
                    object.push((name.clone(), v));
                }
            }
        }
        Ok(object)
    }
}

fn validate_names(packs: &[Arc<SeedPackSnapshot>], query: &Query) -> Result<(), Error> {
    validate_definition_names(&packs.iter().flat_map(|p| p.definitions().cloned()).collect::<Vec<_>>(), query)
}
pub(crate) fn validate_definition_names(definitions: &[Arc<crate::SeedDefinition>], query: &Query) -> Result<(), Error> {
    fn names<'a>(p: &'a Predicate, out: &mut Vec<&'a str>) {
        match p {
            Predicate::All => {}
            Predicate::Cast{field,..}|Predicate::CastNull{field,..}
            | Predicate::Contains { field, .. }
            | Predicate::Compare { field, .. }
            | Predicate::IsNull(field)
            | Predicate::IsAbsent(field)
            | Predicate::IsPresent(field) => out.push(field),
            Predicate::And(v) | Predicate::Or(v) => {
                for p in v {
                    names(p, out);
                }
            }
            Predicate::Not(p) => names(p, out),
        }
    }
    let mut fields = Vec::new();
    names(&query.predicate, &mut fields);
    if let Some(v) = &query.projection {
        fields.extend(v.iter().map(|name|query.casts.iter().find(|(_,alias,_)|alias==name).map_or(name.as_str(),|(field,_,_)|field.as_str())));
    }
    fields.extend(query.casts.iter().map(|(field,_,_)|field.as_str()));
    if let Some((f, _)) = &query.order {
        fields.push(f);
    }
    for name in fields {
        if name.starts_with('@') {
            for definition in definitions {
                if definition.rx.field_ordinal(name).is_none() {
                    definition.annotation_field(name)?;
                }
            }
        }
        if !definitions.is_empty()
            && !definitions
                .iter()
                .any(|d| d.query_ordinal(name).is_some())
        {
            return Err(error(format!("unknown query field: {name}")));
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum SortKey {
    Missing,
    Null,
    Bool(bool),
    Integer(i128),
    Unsigned(u128),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}
impl SortKey {
    fn new(cell: Cell) -> Result<Self, Error> {
        Ok(match cell {
            Cell::Absent => Self::Missing,
            Cell::Value(Data::Null) => Self::Null,
            Cell::Value(Data::Bool(v)) => Self::Bool(v),
            Cell::Value(Data::Integer(v)) => Self::Integer(v.into()),
            Cell::Value(v @ Data::Tag(2, _)) => Self::Unsigned(pil_codec::unsigned_native(&v)?),
            Cell::Value(v @ Data::Tag(3, _)) => Self::Integer(pil_codec::signed_native(&v)?),
            Cell::Value(Data::Float(v)) if !v.is_nan() => Self::Float(v),
            Cell::Value(Data::Text(v)) => Self::Text(v),
            Cell::Value(Data::Bytes(v)) => Self::Bytes(v),
            _ => return Err(error("ordering requires a non-NaN scalar")),
        })
    }
    fn kind(&self) -> Option<u8> {
        match self {
            Self::Missing | Self::Null => None,
            Self::Bool(_) => Some(0),
            Self::Integer(_) | Self::Unsigned(_) | Self::Float(_) => Some(1),
            Self::Text(_) => Some(3),
            Self::Bytes(_) => Some(4),
        }
    }
    fn cmp(&self, other: &Self) -> Ordering {
        use crate::comparison::Number as N;
        match (self, other) {
            (Self::Missing, Self::Missing) | (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Missing, _) => Ordering::Greater,
            (_, Self::Missing) => Ordering::Less,
            (Self::Null, _) => Ordering::Greater,
            (_, Self::Null) => Ordering::Less,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Integer(a), Self::Integer(b)) => N::Signed(*a).compare(N::Signed(*b)).unwrap(),
            (Self::Unsigned(a), Self::Unsigned(b)) => N::Unsigned(*a).compare(N::Unsigned(*b)).unwrap(),
            (Self::Unsigned(a), Self::Integer(b)) => N::Unsigned(*a).compare(N::Signed(*b)).unwrap(),
            (Self::Integer(a), Self::Unsigned(b)) => N::Signed(*a).compare(N::Unsigned(*b)).unwrap(),
            (Self::Unsigned(a), Self::Float(b)) => N::Unsigned(*a).compare(N::Float(*b)).unwrap(),
            (Self::Float(a), Self::Unsigned(b)) => N::Float(*a).compare(N::Unsigned(*b)).unwrap(),
            (Self::Integer(a), Self::Float(b)) => N::Signed(*a).compare(N::Float(*b)).unwrap(),
            (Self::Float(a), Self::Integer(b)) => N::Float(*a).compare(N::Signed(*b)).unwrap(),
            (Self::Float(a), Self::Float(b)) => N::Float(*a).compare(N::Float(*b)).unwrap(),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            _ => self.kind().cmp(&other.kind()),
        }
    }
}
#[derive(Debug)]
struct Candidate {
    position: Position,
    key: SortKey,
    direction: Direction,
}
impl PartialEq for Candidate {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Candidate {
    fn cmp(&self, o: &Self) -> Ordering {
        let mut c = self.key.cmp(&o.key);
        // Missing/null sort last in either direction; ties use source/member
        // ordinals only as a stable execution tie-breaker, not event identity.
        if self.direction == Direction::Descending
            && self.key.kind().is_some()
            && o.key.kind().is_some()
        {
            c = c.reverse();
        }
        c.then_with(|| self.position.pack.cmp(&o.position.pack))
            .then_with(|| self.position.row.cmp(&o.position.row))
    }
}

#[cfg(test)]
mod plan_cache_tests {
    use super::*;
    fn definition(reverse:bool)->Arc<crate::SeedDefinition>{
        let fields=if reverse {["second","first"]}else{["first","second"]};
        let mut document=crate::doc::new("cache");
        for field in fields {document=document.field(crate::Field::new(field).r#type("uint").size(4));}
        crate::SeedDefinition::prescribe(&document.sign().unwrap()).unwrap()
    }
    #[test]
    fn plans_share_only_immutable_definition_bindings() {
        let d=definition(false);
        let q=Query::new().filter(Predicate::compare("first",Compare::Gt,1)).select(["first"]);
        let a=d.plans.get(&d,&q).unwrap();let b=d.plans.get(&d,&q.clone().limit(10)).unwrap();
        assert!(Arc::ptr_eq(&a,&b));
        let different=d.plans.get(&d,&q.clone().filter(Predicate::compare("first",Compare::Gt,2))).unwrap();
        assert!(!Arc::ptr_eq(&a,&different));
        let reverse=definition(true);let c=reverse.plans.get(&reverse,&q).unwrap();
        assert_eq!(a.projection[0].1.map(|a|a.ordinal),Some(0));assert_eq!(c.projection[0].1.map(|a|a.ordinal),Some(1));
        let weak=Arc::downgrade(&d);drop(d);assert!(weak.upgrade().is_none(),"cached programs must not retain Definition owners");
    }
    #[test]
    fn cache_is_bounded_and_large_literals_remain_uncached() {
        let d=definition(false);
        for n in 0..100 {d.plans.get(&d,&Query::new().filter(Predicate::compare("first",Compare::Eq,n))).unwrap();}
        let entries=d.plans.0.lock().unwrap();assert!(entries.len()<=32);assert!(entries.iter().map(|e|e.2).sum::<usize>()<=64*1024);drop(entries);
        let query=Query::new().filter(Predicate::Contains{field:"first".into(),text:"a".repeat(70_000)});
        assert!(!Arc::ptr_eq(&d.plans.get(&d,&query).unwrap(),&d.plans.get(&d,&query).unwrap()));
        fn shareable<T:Send+Sync>(){} shareable::<crate::SeedDefinition>();
    }
}
