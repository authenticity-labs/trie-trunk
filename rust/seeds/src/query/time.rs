//! Half-open millisecond bounds for the published hour index. Only an entire
//! predicate proved by the index may bypass row evaluation.
use super::{Compare, Expression, Predicate, predicate_bytes};
use crate::SeedDefinition;
use ciborium::Value;
use std::ops::Range;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TimeBounds {
    Exact(Range<u64>),
    Candidates(Range<u64>),
}

impl TimeBounds {
    pub(crate) fn range(&self) -> &Range<u64> {
        match self {
            Self::Exact(range) | Self::Candidates(range) => range,
        }
    }

    pub(super) fn bind(definition: &SeedDefinition, predicate: &Predicate) -> Option<Self> {
        match predicate {
            Predicate::Compare { field, op: Compare::Eq, value: Value::Bytes(bytes) } => {
                let id = definition.identity.as_ref()?;
                if definition.query_ordinal(field)? != id.id { return None; }
                let ms = crate::seed::identity::millis(bytes)?;
                Some(Self::Candidates(ms..ms + 1))
            }
            Predicate::Compare { field, op, value: Value::Integer(n) } => {
                let ordinal = definition.prescription().field_ordinal(field)?;
                if !definition.annotations(ordinal)?.iter().any(|a| a == "@time" || a == "@time(ms)") {
                    return None;
                }
                let n = u64::try_from(*n).ok()?;
                // Published millisecond hours use 48-bit addresses, so MAX is
                // strictly beyond every representable timestamp in this index.
                let after = n.saturating_add(1);
                Some(Self::Exact(match op {
                    Compare::Eq => n..after,
                    Compare::Lt => 0..n,
                    Compare::Le => 0..after,
                    Compare::Gt => after..u64::MAX,
                    Compare::Ge => n..u64::MAX,
                    Compare::Ne => return None,
                }))
            }
            Predicate::And(parts) => {
                let mut bound = None::<Range<u64>>;
                let mut exact = true;
                for part in parts {
                    let Some(child) = Self::bind(definition, part) else {
                        exact = false;
                        continue;
                    };
                    exact &= matches!(child, Self::Exact(_));
                    let range = child.range();
                    bound = Some(match bound {
                        None => range.clone(),
                        Some(r) => r.start.max(range.start)..r.end.min(range.end),
                    });
                }
                bound.map(|r| if exact { Self::Exact(r) } else { Self::Candidates(r) })
            }
            _ => None,
        }
    }
}

// Only positive conjunctions inherit a seek's proof. OR, NOT and UUIDv7
// equality stay intact: the UUID timestamp by itself identifies candidates.
pub(super) fn after_seek(
    definition: &SeedDefinition,
    predicate: &Predicate,
) -> Result<Option<(Expression, usize)>, crate::Error> {
    fn remaining<'a>(def: &SeedDefinition, p: &'a Predicate, terms: &mut Vec<&'a Predicate>) -> bool {
        if let Predicate::And(parts) = p {
            let mut removed = false;
            for child in parts { removed |= remaining(def, child, terms); }
            removed
        } else if matches!(TimeBounds::bind(def, p), Some(TimeBounds::Exact(_))) {
            true
        } else {
            terms.push(p);
            false
        }
    }
    let mut terms = Vec::new();
    if !matches!(predicate, Predicate::And(_)) || !remaining(definition, predicate, &mut terms) {
        return Ok(None);
    }
    let bytes = terms.iter().map(|p| predicate_bytes(p)).sum();
    let mut parts = terms.into_iter().map(|p| Expression::bound(p, definition))
        .collect::<Result<Vec<_>, _>>()?;
    let expr = match parts.len() {
        0 => Expression::All,
        1 => parts.pop().unwrap(),
        _ => Expression::And(parts),
    };
    Ok(Some((expr, bytes)))
}
