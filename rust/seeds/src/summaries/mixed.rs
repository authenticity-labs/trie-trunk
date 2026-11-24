//! Mixed-Definition exception only. Source accumulators remain unchanged.
//! A prepared union shape stores exactly one fixed row per period. Different
//! widths share generic class arrays, never extra period records.
use super::*;
use crate::summaries::CountBounds;
use std::collections::BTreeMap;

struct Group {
    name: String,
    class: SummaryClass,
    kind: &'static str,
    width: u16,
    values: Vec<Data>,
    semantics: Vec<String>,
}
struct Mapping {
    slot: usize,
    categories: Vec<usize>,
}
struct Variant {
    normalized: SummaryPlan,
    // One optional mapping per field in each source. Uncollected fields have
    // no mapping and become unknown; source ordinals never reach stored names.
    mappings: Vec<Vec<Option<Mapping>>>,
}
pub struct MixedSummaryPlan {
    sources: Vec<SummaryPlan>,
    variants: Vec<Variant>,
    layout: Arc<SummaryLayout>,
}
pub struct MixedSummaryBucket {
    counts: Vec<u64>,
    contributions: Vec<Option<SummaryBucket>>,
    count: u64,
}
impl MixedSummaryBucket {
    pub fn count(&self) -> u64 {
        self.count
    }
}
impl MixedSummaryPlan {
    pub fn new(plans: &[SummaryPlan]) -> Result<Self, Error> {
        Self::with_definitions(plans, &mut crate::summaries::SummaryDefinitions::new())
    }
    pub(crate) fn with_definitions(plans: &[SummaryPlan], definitions: &mut crate::summaries::SummaryDefinitions) -> Result<Self, Error> {
        if plans.len() < 2 {
            return Err(error("mixed summaries require multiple source plans"));
        }
        let rows = plans[0].layout.rows();
        if plans.iter().any(|p| p.layout.rows() != rows) {
            return Err(error("mixed summary period counts differ"));
        }
        let mut groups: Vec<Group> = Vec::new();
        let mut source_slots = Vec::new();
        let mut names: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for plan in plans {
            let mut slots = Vec::new();
            for binding in plan.bindings.iter() {
                let f = &binding.field;
                let (kind, width) = match f.class {
                    SummaryClass::Numeric | SummaryClass::Temporal => {
                        number_type(binding.scalar.unwrap())?
                    }
                    SummaryClass::Boolean => ("bool", 1),
                    _ => (literal_kind(&f.values[0]), 0),
                };
                // Only identical non-analytic annotations are known to have
                // identical semantics. Never infer unit conversion from a name.
                let semantics = crate::summaries::definitions::semantic_annotations(&plan.source, f.ordinal);
                let found = groups.iter().position(|g| {
                    g.name == f.name
                        && g.class == f.class
                        && g.kind == kind
                        && g.semantics == semantics
                        && (f.class != SummaryClass::Ordinal || g.values == f.values)
                });
                let slot = if let Some(i) = found {
                    let g = &mut groups[i];
                    g.width = g.width.max(width);
                    if f.class == SummaryClass::Categorical {
                        for value in &f.values {
                            if !g.values.contains(value) {
                                g.values.push(value.clone());
                            }
                        }
                    }
                    i
                } else {
                    let i = groups.len();
                    groups.push(Group {
                        name: f.name.clone(),
                        class: f.class,
                        kind,
                        width,
                        values: f.values.clone(),
                        semantics,
                    });
                    names.entry(f.name.clone()).or_default().push(i);
                    i
                };
                slots.push(slot);
            }
            source_slots.push(slots);
        }
        let fields: Vec<_> = names.into_iter().collect();
        // TODO(custom log schemas): support a field changing statistical class,
        // signedness or units across versions. Event.log schemas are managed by
        // Trunk today. Until that policy is designed, reject incompatible plans
        // rather than reinterpret values or publish partial numeric evidence.
        // Compatible source versions share one bound union layout. A class or
        // unit conflict cannot be represented by silently reinterpreting slots.
        if fields.iter().any(|(_, groups)| groups.len() != 1) {
            return Err(error("mixed summary class or unit changes are not supported"));
        }
        let mut variants = Vec::with_capacity(1);
        for winner in 0..1 {
            let selected: Vec<_> = fields
                .iter()
                .map(|(_, slots)| {
                    slots
                        .iter()
                        .copied()
                        .find(|slot| source_slots[winner].contains(slot))
                        .unwrap_or(slots[0])
                })
                .collect();
            let source = &plans[0].source;
            let mut document = doc::new(&source.name).namespace(&source.namespace);
            for &slot in &selected {
                let g = &groups[slot];
                let field = Field::new(&g.name);
                let field = if matches!(g.class, SummaryClass::Categorical | SummaryClass::Ordinal)
                {
                    let labels = g
                        .values
                        .iter()
                        .cloned()
                        .map(pil_codec::json_view::data_json)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| error(e.to_string()))?;
                    let kind = if g.kind == "integer" {
                        if g.values
                            .iter()
                            .any(|v| v.as_integer().is_some_and(|n| i128::from(n) < 0))
                        {
                            "int"
                        } else {
                            "uint"
                        }
                    } else {
                        g.kind
                    };
                    field.closed_enum(kind, labels)
                } else {
                    field.r#type(g.kind).size(g.width)
                };
                let class = match g.class {
                    SummaryClass::Numeric => "numeric",
                    SummaryClass::Temporal => "temporal",
                    SummaryClass::Boolean => "boolean",
                    SummaryClass::Categorical => "categorical",
                    SummaryClass::Ordinal => "ordinal",
                };
                document = document.field(field.annotations(g.semantics.clone()).annotation(format!("#summ({class})")));
            }
            let mut normalized = definitions.prescribe(SeedDefinition::prescribe(&document.sign()?)?, rows)?;
            normalized.layout = SummaryLayout::compiled(&normalized.document, rows, crate::EncodingMode::FixedSparse,
                normalized.layout.definition().clone(),None)?
                .with_bindings(normalized.layout.bindings.clone())?
                .with_source(normalized.source.clone())?;
            Arc::make_mut(&mut normalized.layout).class_bindings=Some(normalized.bindings.clone());
            if plans.iter().all(|p| p.source.registration.is_some()) {
                normalized.layout = normalized.layout.with_context(plans.iter().map(|p| p.source.registration.unwrap()).collect())?;
            }
            let mappings = plans
                .iter()
                .zip(&source_slots)
                .map(|(plan, slots)| {
                    plan.bindings
                        .iter()
                        .zip(slots)
                        .map(|(binding, group)| {
                            selected
                                .iter()
                                .position(|selected| selected == group)
                                .map(|slot| Mapping {
                                    slot,
                                    categories: binding
                                        .field
                                        .values
                                        .iter()
                                        .map(|v| {
                                            groups[*group]
                                                .values
                                                .iter()
                                                .position(|n| n == v)
                                                .unwrap()
                                        })
                                        .collect(),
                                })
                        })
                        .collect()
                })
                .collect();
            variants.push(Variant {
                normalized,
                mappings,
            });
        }
        let layout = SummaryLayout::family(
            variants
                .iter()
                .map(|v| v.normalized.layout.clone())
                .collect(),
        )?;
        // Use exactly the handlers registered in the Pack, including its base.
        Ok(Self {
            sources: plans.to_vec(),
            variants,
            layout,
        })
    }
    pub fn layout(&self) -> &Arc<SummaryLayout> {
        &self.layout
    }
    pub fn bucket(&self) -> MixedSummaryBucket {
        MixedSummaryBucket {
            counts: vec![0; self.sources.len()],
            contributions: vec![None; self.sources.len()],
            count: 0,
        }
    }
    pub fn merge(
        &self,
        target: &mut MixedSummaryBucket,
        plan: &SummaryPlan,
        bucket: &SummaryBucket,
    ) -> Result<(), Error> {
        plan.check(bucket)?;
        let index = self
            .sources
            .iter()
            .position(|s| Arc::ptr_eq(&s.identity, &plan.identity))
            .ok_or_else(|| error("unregistered mixed summary source"))?;
        if target.contributions.len() != self.sources.len() {
            return Err(error("mixed summary bucket shape"));
        }
        let total = target
            .count
            .checked_add(bucket.count)
            .ok_or(Error::Overflow)?;
        let count = target.counts[index]
            .checked_add(bucket.count)
            .ok_or(Error::Overflow)?;
        if bucket.count == 0 {
            return Ok(());
        }
        if let Some(previous) = &mut target.contributions[index] {
            plan.merge(previous, bucket)?;
        } else {
            target.contributions[index] = Some(bucket.clone());
        }
        target.count = total;
        target.counts[index] = count;
        Ok(())
    }
    /// Highest event population wins. Ties retain the first prepared source;
    /// observation order, field population and field values do not break ties.
    pub fn winner(&self, bucket: &MixedSummaryBucket) -> usize {
        bucket
            .counts
            .iter()
            .enumerate()
            .max_by_key(|(i, count)| (**count, std::cmp::Reverse(*i)))
            .map_or(0, |(i, _)| i)
    }
    /// Append the row and its Seeds-owned exception bits together. Fixed slots
    /// remain present; incomplete contributions are never encoded as zero facts.
    pub fn load<'a>(&'a self, pack: &mut super::super::SummaryPackBuilder<'a>, bucket: &MixedSummaryBucket) -> Result<(), Error> {
        let fields = self.variants[0].normalized.bindings.len();
        let mut unknown = vec![false; fields];
        for (source, contribution) in bucket.contributions.iter().enumerate() {
            if contribution.is_none() { continue; }
            for (slot, missing) in unknown.iter_mut().enumerate() {
                *missing |= !self.variants[0].mappings[source].iter().flatten().any(|m| m.slot == slot);
            }
        }
        pack.exceptions(&unknown)?;
        pack.load(self.seed(bucket)?)?;
        Ok(())
    }
    pub fn seed(&self, bucket: &MixedSummaryBucket) -> Result<Seed<'_>, Error> {
        if bucket.contributions.len() != self.sources.len() {
            return Err(error("mixed summary bucket shape"));
        }
        let variant = &self.variants[0];
        let plan = &variant.normalized;
        let mut combined = plan.bucket();
        for (source, contribution) in bucket.contributions.iter().enumerate() {
            let Some(contribution) = contribution else {
                continue;
            };
            self.sources[source].check(contribution)?;
            let mut normalized = plan.bucket();
            normalized.count = contribution.count;
            for (state, map) in contribution.fields.iter().zip(&variant.mappings[source]) {
                let Some(map) = map else { continue };
                let mut state = state.clone();
                if let Statistics::Categories(counts) = &mut state.stats {
                    let mut reordered = vec![0; plan.bindings[map.slot].field.values.len()];
                    for (count, to) in counts.iter().zip(&map.categories) {
                        reordered[*to] = *count;
                    }
                    *counts = reordered;
                }
                normalized.fields[map.slot] = state;
            }
            plan.merge(&mut combined, &normalized)?;
        }
        let mut seed = plan.seed(&combined)?;
        seed.sparse()?;
        Ok(seed)
    }
    pub(crate) fn plans(&self) -> impl Iterator<Item = &SummaryPlan> {
        self.variants.iter().map(|variant| &variant.normalized)
    }
    fn plan(&self, row: &PillView<'_>) -> Result<&SummaryPlan, Error> {
        self.variants
            .iter()
            .map(|v| &v.normalized)
            .find(|p| {
                std::ptr::eq(row.prescription(), &**p.layout.definition().prescription())
                    || row.prescription().bytes() == p.layout.definition().prescription().bytes()
            })
            .ok_or_else(|| error("mixed summary Definition mismatch"))
    }
    pub fn field(&self, row: &PillView<'_>, name: &str) -> Result<Data, Error> {
        self.plan(row)?.field(row, name)
    }
    pub fn object(&self, row: &PillView<'_>) -> Result<Object, Error> {
        self.plan(row)?.object(row)
    }
    pub fn comparison(
        &self,
        row: &PillView<'_>,
        name: &str,
        op: crate::Compare,
        rhs: &Data,
    ) -> Result<CountBounds, Error> {
        let plan = self.plan(row)?;
        let index = plan
            .field_index(name)
            .ok_or_else(|| error("unknown summary field"))?;
        if plan.is_null(row, index)? {
            return Ok(CountBounds {
                lower: 0,
                upper: view_uint(row, 0)?,
            });
        }
        plan.comparison(row, index, op, rhs)
    }
}

fn literal_kind(value: &Data) -> &'static str {
    match value {
        Data::Text(_) => "tstr",
        Data::Bool(_) => "bool",
        _ => "integer",
    }
}
