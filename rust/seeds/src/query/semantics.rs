//! Resolve application view names and literals once, before ordinal planning.
//! The resulting predicate reads the original fields through the existing executor.
use crate::{Predicate, SeedDefinition};
use std::borrow::Cow;

impl SeedDefinition {
    /// Whether a query result reconstructs this field as a full event UUID.
    pub fn is_event_id(&self, name: &str) -> bool {
        self.identity.is_some_and(|id| self.query_ordinal(name) == Some(id.id))
    }

    pub(crate) fn selectors(tags: &[String]) -> impl Iterator<Item = &str> {
        tags.iter()
            .map(|tag| tag.split_once('(').map_or(tag.as_str(), |(name, _)| name))
            .filter(|name| matches!(*name, "@time" | "@duration" | "@event" | "@object"))
    }

    // Match annotation names at prescription binding time, including parameterized tags.
    pub(crate) fn annotation_field(&self, selector: &str) -> Result<Option<usize>, crate::Error> {
        let mut matches = self.annotations.iter().enumerate().filter(|(_, tags)| {
            Self::selectors(tags).any(|name| name == selector)
        });
        let first = matches.next().map(|(ordinal, _)| ordinal);
        if matches.next().is_some() {
            return Err(crate::error(format!("ambiguous annotation {selector} in {}: use a field name", self.name)));
        }
        Ok(first)
    }

    /// Physical fields keep precedence. An ambiguous annotation alias is unavailable.
    pub fn query_ordinal(&self, name: &str) -> Option<usize> {
        if let Some(ordinal) = self.rx.field_ordinal(name) {
            return Some(ordinal);
        }
        if name.starts_with('@') { return self.annotation_field(name).ok().flatten(); }
        #[cfg(any(feature = "registration", feature = "query-language"))]
        {
            let mut matches = self
                .semantics
                .iter()
                .filter(|(_, p)| p.views.iter().any(|v| v.name.as_deref() == Some(name)));
            let ordinal = matches.next()?.0;
            return matches.next().is_none().then_some(ordinal);
        }
        #[cfg(not(any(feature = "registration", feature = "query-language")))]
        None
    }
    pub fn query_fields(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .rx
            .definition()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        #[cfg(any(feature = "registration", feature = "query-language"))]
        for (ordinal, profile) in &self.semantics {
            for alias in profile.views.iter().filter_map(|v| v.name.as_ref()) {
                if !names.contains(alias) && self.query_ordinal(alias) == Some(*ordinal) {
                    names.push(alias.clone());
                }
            }
        }
        names
    }
    pub(crate) fn access(&self, name: &str) -> Option<Access> {
        let ordinal = self.query_ordinal(name)?;
        Some(Access {
            ordinal,
            #[cfg(any(feature = "registration", feature = "query-language"))]
            view: self.metadata(ordinal).and_then(|p| {
                let stored = &self.rx.definition().fields()[ordinal].name;
                p.view(if name.starts_with('@') { stored } else { name }, stored)
                    .map(|v| (p, v))
            }),
        })
    }
    pub fn predicate<'a>(&self, predicate: &'a Predicate) -> Cow<'a, Predicate> {
        self.lower(predicate)
    }
    fn lower<'a>(&self, predicate: &'a Predicate) -> Cow<'a, Predicate> {
        use Predicate::*;
        let field = |name: &str| {
            self.query_ordinal(name).map(|ordinal| {
                (
                    ordinal,
                    self.rx.definition().fields()[ordinal].name.as_str(),
                )
            })
        };
        let rename = |name: &str| {
            field(name).and_then(|(_, stored)| (stored != name).then(|| stored.to_owned()))
        };
        match predicate {
            Compare {
                field: name,
                op,
                value,
            } => {
                if let Some((_ordinal, stored)) = field(name) {
                    #[cfg(any(feature = "registration", feature = "query-language"))]
                    if let Some(profile) = self.metadata(_ordinal) {
                        if let Some(mapped) =
                            profile.comparison(stored, profile.view(if name.starts_with('@') { stored } else { name }, stored), *op, value)
                        {
                            return Cow::Owned(mapped);
                        }
                    }
                    if stored != name {
                        return Cow::Owned(Compare {
                            field: stored.into(),
                            op: *op,
                            value: value.clone(),
                        });
                    }
                }
            }
            Contains { field, text } => {
                if let Some(field) = rename(field) {
                    return Cow::Owned(Contains {
                        field,
                        text: text.clone(),
                    });
                }
            }
            IsNull(name) => {
                if let Some(name) = rename(name) {
                    return Cow::Owned(IsNull(name));
                }
            }
            IsAbsent(name) => {
                if let Some(name) = rename(name) {
                    return Cow::Owned(IsAbsent(name));
                }
            }
            IsPresent(name) => {
                if let Some(name) = rename(name) {
                    return Cow::Owned(IsPresent(name));
                }
            }
            And(parts) | Or(parts) => {
                let mut changed = None;
                for (i, part) in parts.iter().enumerate() {
                    match (self.lower(part), &mut changed) {
                        (Cow::Borrowed(_), None) => {}
                        (item, Some(items)) => Vec::push(items, item.into_owned()),
                        (Cow::Owned(item), None) => {
                            let mut items = parts[..i].to_vec();
                            items.push(item);
                            changed = Some(items);
                        }
                    }
                }
                if let Some(parts) = changed {
                    return Cow::Owned(if matches!(predicate, And(_)) { And(parts) } else { Or(parts) });
                }
            }
            Not(inner) => {
                if let Cow::Owned(inner) = self.lower(inner) {
                    return Cow::Owned(Not(Box::new(inner)));
                }
            }
            Cast{..}|CastNull{..}|All => {}
        }
        Cow::Borrowed(predicate)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Access {
    pub ordinal: usize,
    #[cfg(any(feature = "registration", feature = "query-language"))]
    view: Option<(&'static crate::metadata::Profile, crate::metadata::ViewKind)>,
}
impl Access {
    pub(super) fn read<'a>(&self, row: &'a crate::storage::BoundRow<'_>, identity: Option<&super::Identity>) -> Result<pil_codec::api::NativeValue<'a>, crate::Error> {
        if let Some(id) = identity.filter(|id| id.id == self.ordinal) {
            return id.read(|i| row.native_value(i));
        }
        Ok(self.value(row.native_value(self.ordinal)?))
    }

    pub(crate) fn value<'a>(
        &self,
        value: pil_codec::api::NativeValue<'a>,
    ) -> pil_codec::api::NativeValue<'a> {
        #[cfg(any(feature = "registration", feature = "query-language"))]
        if let Some((profile, kind)) = self.view {
            use pil_codec::{Value, api::NativeValue::Scalar};
            let number = match &value {
                Scalar(Value::U8(v)) => Some(*v as i64),
                Scalar(Value::U16(v)) => Some(*v as i64),
                Scalar(Value::U24(v) | Value::U32(v)) => Some(*v as i64),
                Scalar(Value::U64(v)) => i64::try_from(*v).ok(),
                Scalar(Value::U128(v)) => i64::try_from(*v).ok(),
                Scalar(Value::I8(v)) => Some(*v as i64),
                Scalar(Value::I16(v)) => Some(*v as i64),
                Scalar(Value::I32(v)) => Some(*v as i64),
                Scalar(Value::I64(v)) => Some(*v),
                Scalar(Value::I128(v)) => i64::try_from(*v).ok(),
                _ => None,
            };
            if let Some(number) = number {
                let mapped = profile.number(number, kind);
                if mapped != number {
                    return Scalar(match &value {
                        Scalar(
                            Value::U8(_)
                            | Value::U16(_)
                            | Value::U24(_)
                            | Value::U32(_)
                            | Value::U64(_)
                            | Value::U128(_),
                        ) => Value::U64(mapped as u64),
                        _ => Value::I64(mapped),
                    });
                }
            }
        }
        value
    }
}
