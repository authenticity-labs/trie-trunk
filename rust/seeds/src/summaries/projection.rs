use super::*;
use crate::Object;
use ciborium::Value;
use pil_codec::api::PillView;
/// Typed access to the prepared daily row shape. Only mixed source days need
/// the field-name union and native-null exception path.
pub enum SummaryProjection {
    Single(SummaryPlan),
    Mixed(MixedSummaryPlan),
    Stored {
        layout: Arc<SummaryLayout>,
        plans: Vec<SummaryPlan>,
    },
}
/// Prepared per-period exception plans. Only distinct bitmap rows need a plan;
/// bindings and codec ownership remain shared. Ordinary Packs skip this path.
pub struct SummaryPeriods {
    plans: Vec<SummaryPlan>,
    ordinals: Vec<usize>,
}
impl SummaryPeriods {
    pub fn period(&self, ordinal: usize) -> Result<&SummaryPlan, Error> {
        self.ordinals.get(ordinal).map(|&i| &self.plans[i])
            .ok_or_else(|| error("summary period outside Pack"))
    }
}
impl SummaryProjection {
    pub fn periods(&self, headers: &std::collections::BTreeMap<String, Vec<u8>>) -> Result<Option<SummaryPeriods>, Error> {
        let Some(bits) = headers.get("summary.unknown") else {
            if self.layout().mode() == crate::EncodingMode::FixedSparse {
                return Err(error("mixed summary Pack is missing its exception context"));
            }
            return Ok(None)
        };
        let plans = self.plans();
        if plans.len() != 1 { return Err(error("exception bitmap requires one bound summary shape")); }
        let base = plans[0];
        let fields = base.field_count();
        let total = fields.checked_mul(self.layout().rows()).ok_or(Error::Overflow)?;
        if bits.len() != total.div_ceil(8) || (total % 8 != 0 && bits.last().is_some_and(|b| *b >> (total % 8) != 0)) {
            return Err(error("invalid summary exception bitmap"));
        }
        let mut unique = std::collections::BTreeMap::<Vec<u8>, usize>::new();
        let mut result = SummaryPeriods { plans: Vec::new(), ordinals: Vec::with_capacity(self.layout().rows()) };
        for ordinal in 0..self.layout().rows() {
            let mut mask = vec![0; fields.div_ceil(8)];
            for field in 0..fields {
                let bit = ordinal * fields + field;
                if bits[bit / 8] & (1 << (bit % 8)) != 0 { mask[field / 8] |= 1 << (field % 8); }
            }
            let next = unique.len();
            let index = *unique.entry(mask.clone()).or_insert_with(|| {
                result.plans.push(base.with_unknown(mask)); next
            });
            result.ordinals.push(index);
        }
        Ok(Some(result))
    }
    pub fn from_layout(layout: Arc<SummaryLayout>) -> Result<Self, Error> {
        if !layout.is_versioned() { return Ok(Self::Single(SummaryPlan::from_layout(layout)?)); }
        let mut plans = Vec::new();
        for definition in layout.definitions() {
            let document = crate::doc::read(definition.prescription().bytes())?;
            let mut single = SummaryLayout::with_mode(&document, layout.rows(), layout.mode())?;
            Arc::make_mut(&mut single).definition = definition.clone();
            plans.push(SummaryPlan::from_layout(single)?);
        }
        Ok(Self::Stored { layout, plans })
    }
    pub(super) fn select<'a>(plans: &'a [SummaryPlan], row: &PillView<'_>) -> Result<&'a SummaryPlan, Error> {
        plans
            .iter()
            .find(|plan| {
                std::ptr::eq(
                    row.prescription(),
                    &**plan.layout().definition().prescription(),
                ) || row.prescription().bytes() == plan.layout().definition().prescription().bytes()
            })
            .ok_or_else(|| crate::error("unregistered summary row Definition"))
    }

    pub fn layout(&self) -> &Arc<SummaryLayout> {
        match self {
            Self::Single(p) => p.layout(),
            Self::Mixed(p) => p.layout(),
            Self::Stored { layout, .. } => layout,
        }
    }
    pub fn field(&self, row: &PillView<'_>, name: &str) -> Result<Value, Error> {
        match self {
            Self::Single(p) => p.field(row, name),
            Self::Mixed(p) => p.field(row, name),
            Self::Stored { plans, .. } => Self::select(plans, row)?.field(row, name),
        }
    }
    pub(super) fn plans(&self) -> Vec<&SummaryPlan> {
        match self {
            Self::Single(plan) => vec![plan],
            Self::Stored { plans, .. } => plans.iter().collect(),
            Self::Mixed(plan) => plan.plans().collect(),
        }
    }
    pub fn comparison(
        &self,
        row: &PillView<'_>,
        name: &str,
        op: crate::Compare,
        rhs: &Value,
    ) -> Result<CountBounds, Error> {
        let plan = match self {
            Self::Mixed(plan) => return plan.comparison(row, name, op, rhs),
            Self::Single(plan) => plan,
            Self::Stored { plans, .. } => Self::select(plans, row)?,
        };
        let index = plan
            .field_index(name)
            .ok_or_else(|| crate::error("unknown summary field"))?;
        if plan.is_null(row, index)? {
            let NativeValue::Scalar(pil_codec::Value::U64(count)) = row.native_value(0)? else {
                return Err(crate::error("invalid summary count"));
            };
            return Ok(CountBounds {
                lower: 0,
                upper: count,
            });
        }
        plan.comparison(row, index, op, rhs)
    }
    /// Evaluate a selected Pack against its actual row Definitions. Plans are
    /// compiled once per Definition; mixed nulls cannot prove a period empty.
    pub fn preview_pack(&self, pack: &crate::SeedPackSnapshot, first: u64, window: &SummaryWindow, predicate: &crate::Predicate) -> Result<SummaryWindowResult, Error> {
        self.preview_pack_with(pack,first,window,predicate,&crate::cast::Context::default())
    }
    pub fn preview_pack_with(&self, pack: &crate::SeedPackSnapshot, first: u64, window: &SummaryWindow, predicate: &crate::Predicate, context: &crate::cast::Context) -> Result<SummaryWindowResult, Error> {
        let periods = self.periods(pack.pack.headers())?;
        if let Some(periods) = &periods {
            let previews = periods.plans.iter().map(|p| p.preview_with(predicate,context)).collect::<Result<Vec<_>,_>>()?;
            let mut windows = previews.iter().map(|p| p.window(window.requested())).collect::<Result<Vec<_>, _>>()?;
            let mut cursor = pack.pack.reader();
            for ordinal in window.rows(first, pack.len())? {
                let row = cursor.get(ordinal)?;
                let time = first.checked_add((ordinal as u64).checked_mul(SUMMARY_PERIOD_MS).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
                windows[periods.ordinals[ordinal]].observe(time, &row, pack.is_sealed())?;
            }
            return SummaryWindowResult::combine(window.clone(), windows.into_iter().map(|w| w.finish()).collect::<Result<Vec<_>, _>>()?);
        }
        let plans: Vec<&SummaryPlan> = match self {
            Self::Single(plan) => vec![plan],
            Self::Stored { plans, .. } => plans.iter().collect(),
            Self::Mixed(plan) => plan.plans().collect(),
        };
        let previews = plans.iter().map(|p| p.preview_with(predicate,context)).collect::<Result<Vec<_>,_>>()?;
        let mut windows = previews.iter().map(|p| p.window(window.requested())).collect::<Result<Vec<_>, _>>()?;
        let mut cursor = pack.pack.reader();
        for ordinal in window.rows(first, pack.len())? {
            let row = cursor.get(ordinal)?;
            let index = if plans.len() == 1 { 0 } else {
                plans.iter().position(|p| std::ptr::eq(row.prescription(), &**p.layout().definition().prescription())
                    || row.prescription().bytes() == p.layout().definition().prescription().bytes())
                    .ok_or_else(|| error("unregistered summary row Definition"))?
            };
            let time = first.checked_add((ordinal as u64).checked_mul(SUMMARY_PERIOD_MS).ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
            windows[index].observe(time, &row, pack.is_sealed())?;
        }
        if windows.len() == 1 { return windows.pop().unwrap().finish(); }
        SummaryWindowResult::combine(window.clone(), windows.into_iter().map(|w| w.finish()).collect::<Result<Vec<_>, _>>()?)
    }
    pub fn object(&self, row: &PillView<'_>) -> Result<Object, Error> {
        match self {
            Self::Single(p) => p.object(row),
            Self::Mixed(p) => p.object(row),
            Self::Stored { plans, .. } => Self::select(plans, row)?.object(row),
        }
    }
}
