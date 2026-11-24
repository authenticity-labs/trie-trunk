//! Application-owned semantic mappings shared by views and query planning.
//! These never change the codec, close a value domain, or select storage routes.
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Integer(i64),
    Text(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Mapping {
    pub value: Value,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Group {
    pub min: i64,
    pub max: i64,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    pub provider: String,
    pub field: String,
    pub kind: String,
    pub open: bool,
    pub sources: Vec<String>,
    pub values: Vec<Mapping>,
    pub groups: Vec<Group>,
    /// Application views over the same stored scalar. An omitted name uses the source name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub views: Vec<View>,
    #[serde(skip)]
    fixed_groups: OnceLock<Option<(i64, i64, u32)>>,
}
impl Profile {
    pub fn value(&self, value: &Value) -> Option<&Mapping> {
        self.values.iter().find(|entry| &entry.value == value)
    }
    pub fn group(&self, value: i64) -> Option<&Group> {
        self.groups
            .iter()
            .find(|group| (group.min..=group.max).contains(&value))
    }
    fn accepts(&self, kind: &str) -> bool {
        match self.kind.as_str() {
            "string" => kind == "STRING" || kind.strip_prefix("STRING(").and_then(|s|s.strip_suffix(')')).is_some_and(|n|n.parse::<u32>().is_ok_and(|n|n>0)),
            "integer" => kind.strip_prefix(['U', 'I']).is_some_and(|width| {
                !width.is_empty() && width.bytes().all(|c| c.is_ascii_digit())
            }),
            _ => false,
        }
    }
}

/// Shared, explicit provider profiles, loaded only when metadata is requested.
/// Field spelling and Definition namespace do not imply a mapping.
pub fn resolve(annotations: &[String], kind: &str) -> Option<&'static Profile> {
    static PROFILES: OnceLock<Vec<Profile>> = OnceLock::new();
    let profiles = PROFILES.get_or_init(|| {
        serde_json::from_str(include_str!("metadata.json"))
            .expect("checked-in field metadata must be valid")
    });
    let mut matched = annotations.iter().filter_map(|annotation| {
        let field = annotation.strip_prefix("@otel(")?.strip_suffix(')')?;
        profiles
            .iter()
            .find(|profile| profile.field == field && profile.accepts(kind))
    });
    let profile = matched.next()?;
    // Conflicting annotations cannot truthfully provide one completion domain.
    matched
        .all(|other| other.name == profile.name)
        .then_some(profile)
}

/// Prepared once per Definition/annotation snapshot; no event bytes are retained.
pub(crate) fn bindings(
    rx: &pil_codec::api::Prescription,
    annotations: &[Vec<String>],
) -> Vec<(usize, &'static Profile)> {
    rx.definition()
        .fields()
        .iter()
        .zip(annotations)
        .enumerate()
        .filter_map(|(ordinal, (field, annotations))| {
            resolve(annotations, &field.kind.to_string()).map(|p| (ordinal, p))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ViewKind {
    Group,
    Position,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub kind: ViewKind,
}
impl Profile {
    pub(crate) fn view(&self, name: &str, stored: &str) -> Option<ViewKind> {
        self.views
            .iter()
            .find(|v| v.name.as_deref() == Some(name))
            .or_else(|| {
                (name == stored)
                    .then(|| self.views.iter().find(|v| v.name.is_none()))
                    .flatten()
            })
            .map(|v| v.kind)
    }
    pub(crate) fn number(&self, value: i64, kind: ViewKind) -> i64 {
        // Uniform power-of-two groups need only a range check and shift/mask.
        let fixed = self.fixed_groups.get_or_init(|| {
            let first = self.groups.first()?;
            let width = first.max.checked_sub(first.min)?.checked_add(1)?;
            let width = u64::try_from(width).ok()?;
            if !width.is_power_of_two()
                || !self.groups.iter().enumerate().all(|(i, g)| {
                    g.min == first.min + i as i64 * width as i64
                        && g.max - g.min + 1 == width as i64
                })
            {
                return None;
            }
            Some((first.min, self.groups.last()?.max, width.trailing_zeros()))
        });
        if let Some((min, max, shift)) = *fixed {
            if (min..=max).contains(&value) {
                let delta = value - min;
                return match kind {
                    ViewKind::Group => (delta >> shift) + 1,
                    ViewKind::Position => (delta & ((1i64 << shift) - 1)) + 1,
                };
            }
        } else if let Some((i, g)) = self
            .groups
            .iter()
            .enumerate()
            .find(|(_, g)| (g.min..=g.max).contains(&value))
        {
            return match kind {
                ViewKind::Group => i as i64 + 1,
                ViewKind::Position => value - g.min + 1,
            };
        }
        // Open profiles preserve unspecified and provider-specific numbers.
        value
    }
    pub(crate) fn presentation(&self, view: ViewKind) -> Self {
        let mut result = self.clone();
        result.name = format!(
            "{}.{}",
            self.name,
            match view {
                ViewKind::Group => "group",
                ViewKind::Position => "position",
            }
        );
        result.groups.clear();
        result.views.clear();
        result.values.clear();
        result.values.push(Mapping {
            value: Value::Integer(0),
            label: "Unspecified".into(),
            safe: None,
            idempotent: None,
        });
        match view {
            ViewKind::Group => {
                for (i, g) in self.groups.iter().enumerate() {
                    result.values.push(Mapping {
                        value: Value::Integer(i as i64 + 1),
                        label: g.label.clone(),
                        safe: None,
                        idempotent: None,
                    });
                }
            }
            ViewKind::Position => {
                for i in 1..=self
                    .groups
                    .iter()
                    .map(|g| g.max - g.min + 1)
                    .max()
                    .unwrap_or(0)
                {
                    result.values.push(Mapping {
                        value: Value::Integer(i),
                        label: i.to_string(),
                        safe: None,
                        idempotent: None,
                    });
                }
            }
        }
        result
    }
    pub(crate) fn comparison(
        &self,
        field: &str,
        view: Option<ViewKind>,
        op: crate::Compare,
        value: &ciborium::Value,
    ) -> Option<crate::Predicate> {
        use crate::{Compare::*, Predicate};
        use ciborium::Value as Data;
        if self.kind != "integer" {
            return None;
        }
        let logical = view.map(|v| self.presentation(v));
        let profile = logical.as_ref().unwrap_or(self);
        let value = if let Data::Text(label) = value {
            if view.is_none() {
                if let Some(group) = self
                    .groups
                    .iter()
                    .find(|g| g.label.eq_ignore_ascii_case(label))
                {
                    let compare = |op, n| Predicate::compare(field, op, n);
                    return Some(match op {
                        Eq => Predicate::And(vec![compare(Ge, group.min), compare(Le, group.max)]),
                        Ne => Predicate::Or(vec![compare(Lt, group.min), compare(Gt, group.max)]),
                        Lt => compare(Lt, group.min),
                        Le => compare(Le, group.max),
                        Gt => compare(Gt, group.max),
                        Ge => compare(Ge, group.min),
                    });
                }
            }
            let mapping = profile
                .values
                .iter()
                .find(|v| v.label.eq_ignore_ascii_case(label))?;
            match &mapping.value {
                Value::Integer(n) => Data::from(*n),
                Value::Text(v) => Data::Text(v.clone()),
            }
        } else {
            value.clone()
        };
        let compare = |op, n: i64| Predicate::compare(field, op, n);
        let Some(view) = view else {
            return Some(Predicate::compare(field, op, value));
        };
        let accepts = |n: i64| {
            crate::value::compare_data(&Data::from(n), &value)
                .ok()
                .flatten()
                .is_some_and(|order| {
                    use std::cmp::Ordering::*;
                    match op {
                        Eq => order == Equal,
                        Ne => order != Equal,
                        Lt => order == Less,
                        Le => order != Greater,
                        Gt => order == Greater,
                        Ge => order != Less,
                    }
                })
        };
        let mut ranges = Vec::new();
        let mut outside = Vec::new();
        for (index, group) in self.groups.iter().enumerate() {
            outside.push(Predicate::Or(vec![
                compare(Lt, group.min),
                compare(Gt, group.max),
            ]));
            match view {
                ViewKind::Group if accepts(index as i64 + 1) => ranges.push((group.min, group.max)),
                ViewKind::Position => {
                    // Compile contiguous accepted positions as ranges, never per-row modulo.
                    let mut first = None;
                    for n in group.min..=group.max {
                        if accepts(n - group.min + 1) {
                            if first.is_none() {
                                first = Some(n)
                            }
                        } else if let Some(start) = first.take() {
                            ranges.push((start, n - 1));
                        }
                    }
                    if let Some(start) = first {
                        ranges.push((start, group.max));
                    }
                }
                _ => {}
            }
        }
        // Keep zero and values outside the provider's known groups lossless.
        // Null and absent still follow the executor's existing three-valued logic.
        ranges.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::new();
        for (start, end) in ranges {
            if let Some(last) = merged
                .last_mut()
                .filter(|last| last.1.saturating_add(1) >= start)
            {
                last.1 = last.1.max(end)
            } else {
                merged.push((start, end));
            }
        }
        let mut selected: Vec<_> = merged
            .into_iter()
            .map(|(start, end)| {
                if start == end {
                    compare(Eq, start)
                } else {
                    Predicate::And(vec![compare(Ge, start), compare(Le, end)])
                }
            })
            .collect();
        let literal_inside = if let Data::Integer(n) = &value {
            i64::try_from(i128::from(*n))
                .ok()
                .is_some_and(|n| self.group(n).is_some())
        } else {
            false
        };
        if op != Eq || !literal_inside || selected.is_empty() {
            if let (Some(first), Some(last)) = (self.groups.first(), self.groups.last()) {
                if self
                    .groups
                    .windows(2)
                    .all(|g| g[0].max.checked_add(1) == Some(g[1].min))
                {
                    outside = vec![Predicate::Or(vec![
                        compare(Lt, first.min),
                        compare(Gt, last.max),
                    ])];
                }
            }
            outside.push(Predicate::compare(field, op, value));
            selected.push(Predicate::And(outside));
        }
        Some(if selected.len() == 1 {
            selected.pop().unwrap()
        } else {
            Predicate::Or(selected)
        })
    }
}
