//! Compile compound predicates over a single byte-sized integer into a truth
//! table. The table belongs to the immutable query plan, never to a record.
use super::{Compare, Predicate};
use crate::{Error, error};
use ciborium::Value as Data;
use pil_codec::{
    Scalar, Type, Value,
    api::{NativeValue, Prescription},
};

pub(crate) enum Cell {
    Number(u8),
    Null,
    Absent,
}
impl Cell {
    pub(super) fn native(value: NativeValue<'_>) -> Result<Self, Error> {
        match value {
            NativeValue::Scalar(Value::U8(n)) => Ok(Self::Number(n)),
            NativeValue::Scalar(Value::I8(n)) => Ok(Self::Number(n as u8)),
            NativeValue::Scalar(Value::Null) => Ok(Self::Null),
            NativeValue::Scalar(Value::Absent) => Ok(Self::Absent),
            NativeValue::Composite(v) => Self::data(Some(&v)),
            _ => Err(error("byte predicate requires a one-byte integer")),
        }
    }
    pub(super) fn data(value: Option<&Data>) -> Result<Self, Error> {
        match value {
            None => Ok(Self::Absent),
            Some(Data::Null) => Ok(Self::Null),
            Some(Data::Integer(n)) => {
                let n = i128::from(*n);
                let byte = u8::try_from(n)
                    .ok()
                    .or_else(|| i8::try_from(n).ok().map(|n| n as u8));
                byte.map(Self::Number)
                    .ok_or_else(|| error("byte predicate integer outside range"))
            }
            _ => Err(error("byte predicate requires an integer")),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Plan {
    pub ordinal: usize,
    bits: [u64; 4],
    null: Option<bool>,
    absent: Option<bool>,
}
impl Plan {
    pub fn evaluate(&self, value: Cell) -> Option<bool> {
        match value {
            Cell::Number(n) => Some(self.bits[(n >> 6) as usize] & (1u64 << (n & 63)) != 0),
            Cell::Null => self.null,
            Cell::Absent => self.absent,
        }
    }
    pub fn compile(predicate: &Predicate, rx: &Prescription) -> Option<Self> {
        if !matches!(
            predicate,
            Predicate::And(_) | Predicate::Or(_) | Predicate::Not(_)
        ) {
            return None;
        }
        fn first(p: &Predicate) -> Option<&str> {
            match p {
                Predicate::Compare { field, .. }
                | Predicate::IsNull(field)
                | Predicate::IsAbsent(field)
                | Predicate::IsPresent(field) => Some(field),
                Predicate::And(parts) | Predicate::Or(parts) => first(parts.first()?),
                Predicate::Not(p) => first(p),
                _ => None,
            }
        }
        // Reject other declared types before walking the compound expression.
        let ordinal = rx.field_ordinal(first(predicate)?)?;
        let signed = match rx.definition().fields()[ordinal].kind {
            Type::Scalar(Scalar::U8) => false,
            Type::Scalar(Scalar::I8) => true,
            _ => return None,
        };
        // Check shape before allocating or constructing tables. A different
        // field/type or a non-integer literal retains the ordinary evaluator.
        fn shape<'a>(
            p: &'a Predicate,
            field: &mut Option<&'a str>,
            count: &mut usize,
            numeric: &mut bool,
        ) -> bool {
            let name = match p {
                Predicate::Compare {
                    field,
                    value: Data::Integer(_),
                    ..
                } => {
                    *numeric = true;
                    field
                }
                Predicate::IsNull(field)
                | Predicate::IsAbsent(field)
                | Predicate::IsPresent(field) => field,
                Predicate::And(parts) | Predicate::Or(parts) => {
                    return parts.iter().all(|p| shape(p, field, count, numeric));
                }
                Predicate::Not(p) => return shape(p, field, count, numeric),
                _ => return false,
            };
            if field.is_some_and(|f| f != name) {
                return false;
            }
            *field = Some(name);
            *count += 1;
            true
        }
        let (mut field, mut count, mut numeric) = (None, 0, false);
        // Presence-only queries keep using masks without reading value bytes.
        if !shape(predicate, &mut field, &mut count, &mut numeric) || count < 2 || !numeric {
            return None;
        }
        Some(Self::build(predicate, ordinal, signed))
    }
    fn build(p: &Predicate, ordinal: usize, signed: bool) -> Self {
        let mut out = Self {
            ordinal,
            bits: [0; 4],
            null: None,
            absent: None,
        };
        match p {
            Predicate::Compare {
                op,
                value: Data::Integer(rhs),
                ..
            } => {
                let rhs = i128::from(*rhs);
                for n in 0..=255u16 {
                    let value = if signed {
                        n as u8 as i8 as i128
                    } else {
                        n as i128
                    };
                    let yes = match op {
                        Compare::Eq => value == rhs,
                        Compare::Ne => value != rhs,
                        Compare::Lt => value < rhs,
                        Compare::Le => value <= rhs,
                        Compare::Gt => value > rhs,
                        Compare::Ge => value >= rhs,
                    };
                    if yes {
                        out.bits[(n >> 6) as usize] |= 1u64 << (n & 63);
                    }
                }
            }
            Predicate::IsNull(_) => {
                out.null = Some(true);
                out.absent = Some(false);
            }
            Predicate::IsAbsent(_) => {
                out.null = Some(false);
                out.absent = Some(true);
            }
            Predicate::IsPresent(_) => {
                out.bits = [u64::MAX; 4];
                out.null = Some(true);
                out.absent = Some(false);
            }
            Predicate::Not(p) => {
                out = Self::build(p, ordinal, signed);
                out.bits = out.bits.map(|v| !v);
                out.null = out.null.map(|v| !v);
                out.absent = out.absent.map(|v| !v);
            }
            Predicate::And(parts) | Predicate::Or(parts) => {
                let and = matches!(p, Predicate::And(_));
                out.bits = [if and { u64::MAX } else { 0 }; 4];
                out.null = Some(and);
                out.absent = Some(and);
                for part in parts {
                    let part = Self::build(part, ordinal, signed);
                    for (value, other) in out.bits.iter_mut().zip(part.bits) {
                        *value = if and { *value & other } else { *value | other };
                    }
                    let merge = |a, b| match (and, a, b) {
                        (true, Some(false), _) | (true, _, Some(false)) => Some(false),
                        (false, Some(true), _) | (false, _, Some(true)) => Some(true),
                        (_, None, _) | (_, _, None) => None,
                        _ => Some(and),
                    };
                    out.null = merge(out.null, part.null);
                    out.absent = merge(out.absent, part.absent);
                }
            }
            _ => unreachable!("byte predicate shape checked before compilation"),
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Expression, ObjectRow, PredicateRow};
    use super::*;
    use crate::{Field, doc};
    use std::{cell::Cell as Counter, cmp::Ordering};

    fn reference(p: &Predicate, rx: &Prescription) -> Expression {
        match p {
            Predicate::And(parts) => {
                Expression::And(parts.iter().map(|p| reference(p, rx)).collect())
            }
            Predicate::Or(parts) => {
                Expression::Or(parts.iter().map(|p| reference(p, rx)).collect())
            }
            Predicate::Not(p) => Expression::Not(Box::new(reference(p, rx))),
            _ => Expression::compile(p, rx),
        }
    }
    #[test]
    fn exhaustive_domain_matches_original_evaluator_with_three_valued_logic() {
        for signed in [false, true] {
            let doc = doc::new("byte-test")
                .field(
                    Field::new("n")
                        .r#type(if signed { "int" } else { "uint" })
                        .size(1),
                )
                .sign()
                .unwrap();
            let rx = doc.prescribe().unwrap();
            let mut predicates = vec![];
            for op in [
                Compare::Eq,
                Compare::Ne,
                Compare::Lt,
                Compare::Le,
                Compare::Gt,
                Compare::Ge,
            ] {
                for rhs in [i64::MIN, -129, -128, -1, 0, 1, 127, 128, 255, 256, i64::MAX] {
                    let a = Predicate::compare("n", op, rhs);
                    for b in [
                        Predicate::compare("n", Compare::Eq, 1),
                        Predicate::IsNull("n".into()),
                        Predicate::IsAbsent("n".into()),
                        Predicate::IsPresent("n".into()),
                    ] {
                        predicates.push(Predicate::And(vec![a.clone(), b.clone()]));
                        predicates.push(Predicate::Or(vec![a.clone(), b]));
                    }
                }
            }
            for p in predicates {
                for p in [p.clone(), Predicate::Not(Box::new(p))] {
                    let fast = Expression::compile(&p, &rx);
                    assert!(matches!(fast, Expression::Byte(_)));
                    let slow = reference(&p, &rx);
                    for n in 0..258 {
                        let value = if n < 256 {
                            Some(Data::from(if signed {
                                n as u8 as i8 as i64
                            } else {
                                n as i64
                            }))
                        } else if n == 256 {
                            Some(Data::Null)
                        } else {
                            None
                        };
                        let object = value.map(|v| vec![("n".into(), v)]).unwrap_or_default();
                        let row = ObjectRow {
                            rx: &rx,
                            object: &object,
                        };
                        assert_eq!(
                            fast.evaluate(&row).unwrap(),
                            slow.evaluate(&row).unwrap(),
                            "signed={signed} slot={n} predicate={p:?}"
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn compound_predicate_reads_the_field_once() {
        struct Row(Counter<usize>);
        impl PredicateRow for Row {
            fn cast_at(&self,_:crate::query::Access,_:&crate::cast::Plan)->Result<pil_codec::api::NativeValue<'_>,Error>{panic!("unexpected cast")}
            fn byte_at(&self, _: usize) -> Result<Cell, Error> {
                self.0.set(self.0.get() + 1);
                Ok(Cell::Number(20))
            }
            fn contains_at(&self, _: usize, _: &str) -> Result<Option<bool>, Error> {
                panic!("unexpected text read")
            }
            fn compare_at(&self, _: usize, _: &crate::value::Operand) -> Result<Option<Ordering>, Error> {
                panic!("repeated comparison")
            }
            fn null_at(&self, _: usize) -> Result<bool, Error> {
                panic!("separate null read")
            }
            fn present_at(&self, _: usize) -> Result<bool, Error> {
                panic!("separate presence read")
            }
        }
        let rx = doc::new("byte-test")
            .field(Field::new("n").r#type("uint").size(1))
            .sign()
            .unwrap()
            .prescribe()
            .unwrap();
        let p = Predicate::Or(
            [4, 8, 12, 16, 20, 24]
                .map(|n| Predicate::compare("n", Compare::Eq, n))
                .to_vec(),
        );
        let expression = Expression::compile(&p, &rx);
        let row = Row(Counter::new(0));
        assert_eq!(expression.evaluate(&row).unwrap(), Some(true));
        assert_eq!(row.0.get(), 1);
        let mut fields = Vec::new();
        expression.fields(&mut fields);
        assert_eq!(fields, vec![0]);
    }
    #[test]
    fn other_domains_keep_the_existing_evaluator() {
        let rx = doc::new("other-types")
            .field(Field::new("n").r#type("uint").size(2))
            .field(Field::new("b").r#type("uint").size(1))
            .sign()
            .unwrap()
            .prescribe()
            .unwrap();
        for p in [
            Predicate::Or(vec![
                Predicate::IsNull("b".into()),
                Predicate::IsAbsent("b".into()),
            ]),
            Predicate::And(vec![
                Predicate::compare("n", Compare::Ge, 1),
                Predicate::compare("n", Compare::Le, 255),
            ]),
            Predicate::And(vec![
                Predicate::compare("b", Compare::Ge, 1.5),
                Predicate::compare("b", Compare::Le, 4),
            ]),
            Predicate::Or(vec![
                Predicate::compare("n", Compare::Eq, 1),
                Predicate::compare("b", Compare::Eq, 2),
            ]),
            Predicate::And(vec![
                Predicate::compare("b", Compare::Eq, Data::Null),
                Predicate::compare("b", Compare::Eq, 2),
            ]),
        ] {
            assert!(Plan::compile(&p, &rx).is_none());
        }
    }
}
