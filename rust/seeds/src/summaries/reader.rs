//! Prepared summary reads shared by native nodes and WASM. Row schemas come
//! from stored summary metadata, not the current event Definition's annotations.
use super::*;
use crate::{Predicate, SeedPackSnapshot};
use std::{collections::HashMap, ops::Range};

enum Source {
    Box { owner: Arc<Vec<u8>>, reader: SummaryBoxReader<'static> },
    Pack { layout: Arc<SummaryLayout>, pack: Arc<SeedPackSnapshot> },
}
pub struct SummaryReader {
    source: Source,
    first: u64,
    projections: HashMap<usize, SummaryProjection>,
}
impl SummaryReader {
    #[cfg(feature = "registration")]
    pub fn registered_box(trunk: &crate::registration::Trunk, branch: u16, bytes: Vec<u8>, first: u64) -> Result<Self, Error> {
        let mut result = Self::open(bytes, first)?;
        let Source::Box { reader, .. } = &mut result.source else { unreachable!() };
        let trie = trunk.trie();
        let registry = trie.definitions().map_err(Error::Definition)?;
        let branches = trie.storage_versions(branch).map_err(Error::Definition)?;
        // Check the footer's schema catalog once, never every field access.
        for layout in &mut reader.versions.layouts {
            if !layout.context.is_empty() {
                let mut plans = Vec::new();
                for &[definition, revision] in &layout.context {
                    if !registry.resources().iter().any(|r| branches.contains(&r.branch) && r.definition == definition) {
                        return Err(error("summary source is outside the selected branch"));
                    }
                    plans.push(SummaryPlan::new(trunk.definition_at(definition, revision as usize)?, layout.rows())?);
                }
                if plans.len() == 1 {
                    if plans[0].layout().shape != layout.shape {return Err(error("summary context does not match stored shape"));}
                    *layout = plans[0].layout().clone();
                } else {
                    let mixed = MixedSummaryPlan::new(&plans)?;
                    if mixed.layout().shape != layout.shape { return Err(error("mixed summary context does not match stored shape")); }
                    *layout = mixed.layout().clone();
                }
            }
            for definition in layout.definitions() {
                if !registry.resources().iter().filter(|r| branches.contains(&r.branch))
                    .flat_map(|r| &r.summaries).any(|id| registry.definitions()[*id as usize].digest == definition.digest) {
                    return Err(error("summary Box Definition is outside the selected branch"));
                }
            }
        }
        Ok(result)
    }
    #[cfg(feature = "registration")]
    pub fn registered(trunk: &crate::registration::Trunk, branch: u16, bytes: Vec<u8>, first: u64) -> Result<Self, Error> {
        Self::calendar(first)?;
        #[cfg(feature = "compression")]
        let bytes = crate::expand_owned(bytes)?;
        let trie = trunk.trie();
        let context = trie.summary_context(branch, first).map_err(Error::Definition)?;
        if (first - context.first) % SUMMARY_DAY_MS != 0 || first.checked_add(SUMMARY_DAY_MS).is_none_or(|end| end > context.end) {
            return Err(error("summary Pack is outside its dataset day boundary"));
        }
        let plans = context.sources.iter().map(|&[id, revision]| {
            SummaryPlan::new(trunk.definition_at(id, revision as usize)?, SUMMARY_DAY_ROWS)
        }).collect::<Result<Vec<_>, Error>>()?;
        let projection = if plans.len() == 1 { SummaryProjection::Single(plans.into_iter().next().unwrap()) }
            else { SummaryProjection::Mixed(MixedSummaryPlan::new(&plans)?) };
        // The branch's historical selection and the Box use the same plans.
        // Dimensions in the PAC are checked by open_range; they cannot select
        // field names or substitute today's annotations for an older day.
        let layout = projection.layout().clone();
        let key = Arc::as_ptr(&layout) as usize;
        let mut reader = Self::pack(layout, bytes, first)?;
        reader.projections.insert(key, projection);
        Ok(reader)
    }
    pub fn open(bytes: Vec<u8>, first: u64) -> Result<Self, Error> {
        Self::calendar(first)?;
        let length = bytes.len() as u64;
        let footer = PackBox::footer_range(&bytes, length)?;
        let reader = SummaryBoxReader::open_footer_owned(bytes[footer.start as usize..].to_vec(), length)?;
        Ok(Self { source: Source::Box { owner: Arc::new(bytes), reader }, first, projections: HashMap::new() })
    }
    pub fn pack(layout: Arc<SummaryLayout>, bytes: Vec<u8>, first: u64) -> Result<Self, Error> {
        Self::calendar(first)?;
        #[cfg(feature = "compression")]
        let bytes = crate::expand_owned(bytes)?;
        let length = bytes.len();
        let pack = layout.open_range(Arc::new(bytes), 0..length)?;
        Ok(Self { source: Source::Pack { layout, pack }, first, projections: HashMap::new() })
    }
    fn calendar(first: u64) -> Result<(), Error> {
        if first % SUMMARY_PERIOD_MS != 0 { return Err(error("invalid summary calendar start")); }
        Ok(())
    }
    pub fn preview(&mut self, window: &SummaryWindow, predicate: &Predicate) -> Result<SummaryWindowResult, Error> {
        self.preview_with(window,predicate,&crate::cast::Context::default())
    }
    pub fn preview_with(&mut self, window: &SummaryWindow, predicate: &Predicate, context: &crate::cast::Context) -> Result<SummaryWindowResult, Error> {
        let first = self.first;
        let projections = &mut self.projections;
        let mut results = Vec::new();
        self.source.selected(first, window, |layout, pack, at| {
            let key = Arc::as_ptr(layout) as usize;
            if let std::collections::hash_map::Entry::Vacant(entry) = projections.entry(key) {
                entry.insert(SummaryProjection::from_layout(layout.clone())?);
            }
            results.push(projections[&key].preview_pack_with(&pack, at, window, predicate,context)?);
            Ok(())
        })?;
        if results.len() == 1 { return Ok(results.pop().unwrap()); }
        SummaryWindowResult::combine(window.clone(), results)
    }
    /// Materialize selected aggregate rows only. Event payloads never leave the
    /// Pack reader. Native tools and browser adapters share this traversal.
    pub fn rows(&mut self, window: &SummaryWindow, mut visit: impl FnMut(u64, u64, crate::Object) -> Result<(), Error>) -> Result<(), Error> {
        self.projected_rows(window,None,&mut visit)
    }
    /// Resolve requested class objects once per Definition; unrelated objects
    /// are not decoded just to be discarded by a reduction.
    pub fn projected_rows(&mut self, window: &SummaryWindow, fields:Option<&[String]>, mut visit: impl FnMut(u64,u64,crate::Object)->Result<(),Error>) -> Result<(),Error> {
        self.bound_rows(window,fields,|_,time,count,object|visit(time,count,object))
    }
    pub(super) fn bound_rows(&mut self, window:&SummaryWindow, fields:Option<&[String]>, mut visit:impl FnMut(&SummaryPlan,u64,u64,crate::Object)->Result<(),Error>)->Result<(),Error> {
        let projections = &mut self.projections;
        self.source.selected(self.first, window, |layout, pack, at| {
            let key = Arc::as_ptr(layout) as usize;
            if let std::collections::hash_map::Entry::Vacant(entry) = projections.entry(key) {
                entry.insert(SummaryProjection::from_layout(layout.clone())?);
            }
            let periods = projections[&key].periods(pack.pack.headers())?;
            let plans=projections[&key].plans();
            let selected=fields.map(|fields|plans.iter().map(|plan|fields.iter().filter_map(|name|plan.field_index(name).map(|i|(name,i))).collect::<Vec<_>>()).collect::<Vec<_>>());
            let mut cursor = pack.pack.reader();
            for ordinal in window.rows(at, pack.len())? {
                let row = cursor.get(ordinal)?;
                let NativeValue::Scalar(pil_codec::Value::U64(count)) = row.native_value(0)? else { return Err(error("invalid summary count")); };
                let index=if plans.len()==1 {0} else {plans.iter().position(|p|std::ptr::eq(row.prescription(),&**p.layout().definition().prescription()) || row.prescription().bytes()==p.layout().definition().prescription().bytes()).ok_or_else(||error("unregistered summary row Definition"))?};
                let bound=if let Some(periods)=&periods {periods.period(ordinal)?}else{plans[index]};
                let object=if let Some(periods)=&periods {
                    let plan=periods.period(ordinal)?;
                    if let Some(fields)=fields { fields.iter().filter_map(|name| plan.field_index(name).map(|i|(name,i)))
                        .map(|(name,i)| Ok((name.clone(),plan.project_field(&row,i)?))).collect::<Result<_,Error>>()? }
                    else {plan.object(&row)?}
                } else if let Some(selected)=&selected {
                    selected[index].iter().map(|(name,i)|Ok(((*name).clone(),plans[index].project_field(&row,*i)?))).collect::<Result<_,Error>>()?
                } else {projections[&key].object(&row)?};
                visit(bound,at + ordinal as u64 * SUMMARY_PERIOD_MS, count, object)?;
            }
            Ok(())
        })
    }
    /// Borrow one bounded Pill at a time; no analytic objects are materialized.
    #[cfg(feature = "registration")]
    pub(super) fn views(&mut self, window: &SummaryWindow, mut visit: impl FnMut(&SummaryPlan, u64, u64, &pil_codec::api::PillView<'_>) -> Result<(), Error>) -> Result<(), Error> {
        let projections = &mut self.projections;
        self.source.selected(self.first, window, |layout, pack, at| {
            let key = Arc::as_ptr(layout) as usize;
            if let std::collections::hash_map::Entry::Vacant(entry) = projections.entry(key) {
                entry.insert(SummaryProjection::from_layout(layout.clone())?);
            }
            let periods = projections[&key].periods(pack.pack.headers())?;
            let plans = projections[&key].plans();
            let mut cursor = pack.pack.reader();
            for ordinal in window.rows(at, pack.len())? {
                let row = cursor.get(ordinal)?;
                let NativeValue::Scalar(Value::U64(count)) = row.native_value(0)? else { return Err(error("invalid summary count")); };
                let plan = if let Some(periods) = &periods { periods.period(ordinal)? }
                    else if plans.len() == 1 { plans[0] }
                    else { *plans.iter().find(|p| std::ptr::eq(row.prescription(), &**p.layout().definition().prescription()) || row.prescription().bytes() == p.layout().definition().prescription().bytes()).ok_or_else(||error("unregistered summary row Definition"))? };
                visit(plan, at + ordinal as u64 * SUMMARY_PERIOD_MS, count, &row)?;
            }
            Ok(())
        })
    }
    pub fn extent(&self) -> Result<Range<u64>, Error> {
        let rows = match &self.source { Source::Box { reader, .. } => reader.len().checked_mul(reader.layout().rows()).ok_or(Error::Overflow)?, Source::Pack { pack, .. } => pack.len() };
        Ok(self.first..self.first.checked_add((rows as u64).checked_mul(SUMMARY_PERIOD_MS).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?)
    }
    /// Report projection: resolve the requested field once per row Definition,
    /// skip empty periods, and leave every unrelated summary object unread.
    pub(super) fn metric_rows(&mut self, window: &SummaryWindow, name: &str,
        mut visit: impl FnMut(u64, u64, Option<super::annotated::SummaryMetric>) -> Result<(), Error>) -> Result<(), Error> {
        let projections = &mut self.projections;
        self.source.selected(self.first, window, |layout, pack, at| {
            let key = Arc::as_ptr(layout) as usize;
            if let std::collections::hash_map::Entry::Vacant(entry) = projections.entry(key) {
                entry.insert(SummaryProjection::from_layout(layout.clone())?);
            }
            let plans = projections[&key].plans();
            let fields = plans.iter().map(|plan| plan.field_index(name)).collect::<Vec<_>>();
            let periods = projections[&key].periods(pack.pack.headers())?;
            let mut cursor = pack.pack.reader();
            for ordinal in window.rows(at, pack.len())? {
                let row = cursor.get(ordinal)?;
                let NativeValue::Scalar(pil_codec::Value::U64(count)) = row.native_value(0)? else { return Err(error("invalid summary count")); };
                let metric = if count == 0 { None } else {
                    let index = if plans.len() == 1 { 0 } else {
                        plans.iter().position(|plan| std::ptr::eq(row.prescription(), &**plan.layout().definition().prescription())
                            || row.prescription().bytes() == plan.layout().definition().prescription().bytes())
                            .ok_or_else(|| error("unregistered summary row Definition"))?
                    };
                    let plan = if let Some(periods)=&periods { periods.period(ordinal)? } else { plans[index] };
                    match fields[index] {Some(field)=>plan.metric(&row, field)?,None=>None}
                };
                visit(at + ordinal as u64 * SUMMARY_PERIOD_MS, count, metric)?;
            }
            Ok(())
        })
    }
    pub fn sealed(&self) -> bool { match &self.source { Source::Pack { pack, .. } => pack.is_sealed(), Source::Box { .. } => true } }
    pub fn hour_counts(&self) -> Result<Vec<(u64, u64)>, Error> {
        let range = self.extent()?;
        if range.is_empty() { return Ok(Vec::new()); }
        let window = SummaryWindow::new(range)?;
        let mut hours = Vec::<(u64,u64)>::new();
        self.source.selected(self.first, &window, |_, pack, at| {
            let mut cursor = pack.pack.reader();
            for ordinal in 0..pack.len() {
                let time = at + ordinal as u64 * SUMMARY_PERIOD_MS;
                let hour = time / 3_600_000 * 3_600_000;
                let NativeValue::Scalar(pil_codec::Value::U64(count)) = cursor.get(ordinal)?.native_value(0)? else { return Err(error("invalid summary count")); };
                if hours.last().is_none_or(|h| h.0 != hour) { hours.push((hour, 0)); }
                let last = hours.last_mut().unwrap();
                last.1 = last.1.checked_add(count).ok_or(Error::Overflow)?;
            }
            Ok(())
        })?;
        Ok(hours)
    }
    /// Keep the existing homogeneous aggregate path. Event-count series also
    /// work across revisions because every row retains the same count field.
    pub fn series(&self, window: &SummaryWindow, span: u64, measures: &[SummaryMeasure]) -> Result<Vec<SummarySeries>, Error> {
        let rows = match &self.source { Source::Box { reader, .. } => reader.len().checked_mul(reader.layout().rows()).ok_or(Error::Overflow)?, Source::Pack { pack, .. } => pack.len() };
        let end = self.first.checked_add((rows as u64).checked_mul(SUMMARY_PERIOD_MS).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
        if window.covered().start < self.first || window.covered().end > end { return Err(error("incomplete summary coverage")); }
        let layout = self.source.layout();
        let counts_only = measures.iter().all(|m| m.field.is_none());
        if !counts_only && !self.source.uniform() {
            return Err(error("field aggregates across summary Definitions require a compatible rollup plan"));
        }
        let plan = SummaryPlan::from_layout(layout.clone())?;
        let mut builder = plan.series(window, span, measures)?;
        self.source.selected(self.first, window, |_, pack, at| {
            if !counts_only && pack.pack.headers().get("summary.unknown").is_some_and(|bits| bits.iter().any(|b| *b != 0)) {
                return Err(error("field series over mixed exceptions requires the summary report reader"));
            }
            let mut cursor = pack.pack.reader();
            for ordinal in window.rows(at, pack.len())? {
                let time = at.checked_add((ordinal as u64).checked_mul(SUMMARY_PERIOD_MS).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
                builder.observe(time, &cursor.get(ordinal)?)?;
            }
            Ok(())
        })?;
        builder.finish()
    }
}
impl Source {
    fn layout(&self) -> &Arc<SummaryLayout> {
        match self { Self::Box { reader, .. } => reader.layout(), Self::Pack { layout, .. } => layout }
    }
    fn uniform(&self) -> bool {
        !self.layout().is_versioned() && match self { Self::Box { reader, .. } => reader.is_uniform(), _ => true }
    }
    fn selected(&self, first: u64, window: &SummaryWindow, mut visit: impl FnMut(&Arc<SummaryLayout>, Arc<SeedPackSnapshot>, u64) -> Result<(), Error>) -> Result<(), Error> {
        match self {
            Self::Pack { layout, pack } => {
                if !window.rows(first, pack.len())?.is_empty() { visit(layout, pack.clone(), first)?; }
            }
            Self::Box { owner, reader } => {
                let rows = reader.layout().rows();
                let selected = window.rows(first, reader.len().checked_mul(rows).ok_or(Error::Overflow)?)?;
                for day in selected.start / rows..selected.end.div_ceil(rows) {
                    let extent = reader.storage_range(day)?;
                    let range = Range { start: usize::try_from(extent.start).map_err(|_| Error::Overflow)?, end: usize::try_from(extent.end).map_err(|_| Error::Overflow)? };
                    let layout = reader.layout_at(day)?;
                    let pack = layout.open_range(owner.clone(), range)?;
                    let at = first.checked_add((day as u64).checked_mul(rows as u64).and_then(|n| n.checked_mul(SUMMARY_PERIOD_MS)).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
                    visit(layout, pack, at)?;
                }
            }
        }
        Ok(())
    }
}
