use crate::{Error, error};
use ciborium::Value as Data;
use pil_codec::{Array, Value, api::NativeValue};
use std::cmp::Ordering;

/// Lossless application value. Missing and explicit null stay distinct.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Absent,
    Value(Data),
}
pub type Object = Vec<(String, Data)>;

pub(crate) fn own(value: NativeValue<'_>) -> Result<Cell, Error> {
    if let NativeValue::Composite(v) = value {
        return Ok(Cell::Value(v));
    }
    let NativeValue::Scalar(v) = value else {
        unreachable!()
    };
    let value = match v {
        Value::Absent => return Ok(Cell::Absent),
        Value::Null => Data::Null,
        Value::Bool(v) => Data::Bool(v),
        Value::I8(v) => Data::Integer(v.into()),
        Value::I16(v) => Data::Integer(v.into()),
        Value::I32(v) => Data::Integer(v.into()),
        Value::I64(v) => Data::Integer(v.into()),
        Value::U8(v) => Data::Integer(v.into()),
        Value::U16(v) | Value::Ref16(v) => Data::Integer(v.into()),
        Value::U24(v) | Value::U32(v) => Data::Integer(v.into()),
        Value::U64(v) => Data::Integer(v.into()),
        Value::U128(v) => pil_codec::native_unsigned(v),
        Value::I128(v) => pil_codec::native_signed(v),
        Value::TimestampMs(v) | Value::TimestampUs(v) | Value::TimestampNs(v) => {
            Data::Integer(v.into())
        }
        Value::F32(v) => Data::Float(v as f64),
        Value::F64(v) => Data::Float(v),
        Value::String(v) => Data::Text(v.to_owned()),
        Value::Bytes(v) => Data::Bytes(v.to_vec()),
        Value::Uuid(v) => Data::Bytes(v.to_vec()),
        Value::Object(v) => pil_codec::object::decode_value(v)?,
        Value::Array(v) => {
            let mut out = Vec::new();
            let mut push = |v| -> Result<(), Error> {
                match own(NativeValue::Scalar(v))? {
                    Cell::Value(v) => out.push(v),
                    Cell::Absent => return Err(error("absent array member")),
                }
                Ok(())
            };
            match v {
                Array::Items(v) => {
                    for &v in v {
                        push(v)?;
                    }
                }
                Array::Encoded(v) => {
                    for v in v.iter() {
                        push(v?)?;
                    }
                }
            }
            Data::Array(out)
        }
        Value::Mapped(_) => return Err(error("unresolved dictionary value")),
    };
    Ok(Cell::Value(value))
}

/// Compare integers exactly without rounding through floating point.
#[inline(always)]
pub(crate) fn compare(value: NativeValue<'_>, rhs: &Data) -> Result<Option<Ordering>, Error> {
    let v = match value {
        NativeValue::Composite(v) => return compare_data(&v, rhs),
        NativeValue::Scalar(v) => v,
    };
    if let Value::U128(v) = v {
        return compare_integer(false, v, rhs);
    }
    if let Value::I128(v) = v {
        return compare_integer(v < 0, v.unsigned_abs(), rhs);
    }
    let integer = match v {
        Value::I8(v) => Some(v as i128),
        Value::I16(v) => Some(v as i128),
        Value::I32(v) => Some(v as i128),
        Value::I64(v) | Value::TimestampMs(v) | Value::TimestampUs(v) | Value::TimestampNs(v) => {
            Some(v as i128)
        }
        Value::U8(v) => Some(v as i128),
        Value::U16(v) | Value::Ref16(v) => Some(v as i128),
        Value::U24(v) | Value::U32(v) => Some(v as i128),
        Value::U64(v) => Some(v as i128),
        _ => None,
    };
    if let Some(v) = integer {
        return compare_integer(v < 0, v.unsigned_abs(), rhs);
    }
    Ok(match (v, rhs) {
        (Value::Absent | Value::Null, _) | (_, Data::Null) => None,
        (Value::String(a), Data::Text(b)) => Some(a.cmp(b)),
        (Value::Bytes(a), Data::Bytes(b)) => Some(a.cmp(b)),
        (Value::Uuid(a), Data::Bytes(b)) => Some(a.as_slice().cmp(b)),
        (Value::Bool(a), Data::Bool(b)) => Some(a.cmp(b)),
        (Value::F32(a), Data::Float(b)) => (a as f64).partial_cmp(b),
        (Value::F64(a), Data::Float(b)) => a.partial_cmp(b),
        (Value::F32(a), Data::Integer(b)) => {
            integer_float((*b).into(), a as f64).map(Ordering::reverse)
        }
        (Value::F64(a), Data::Integer(b)) => integer_float((*b).into(), a).map(Ordering::reverse),
        (Value::F32(a), Data::Tag(2 | 3, _)) => {
            numeric_float(rhs, a as f64)?.map(Ordering::reverse)
        }
        (Value::F64(a), Data::Tag(2 | 3, _)) => numeric_float(rhs, a)?.map(Ordering::reverse),
        _ => {
            return Err(error(
                "comparison requires matching scalar types; cast explicitly",
            ));
        }
    })
}
#[inline(always)]
pub(crate) fn compare_data(a: &Data, b: &Data) -> Result<Option<Ordering>, Error> {
    if matches!(a, Data::Tag(2 | 3, _)) || matches!(b, Data::Tag(2 | 3, _)) {
        if matches!(a, Data::Null) || matches!(b, Data::Null) {
            return Ok(None);
        }
        if let Data::Float(f) = b {
            return Ok(numeric_float(a, *f)?);
        }
        if let Data::Float(f) = a {
            return Ok(numeric_float(b, *f)?.map(Ordering::reverse));
        }
        let (negative_a, a) = magnitude(a)?;
        let (negative_b, b) = magnitude(b)?;
        return Ok(Some(match (negative_a, negative_b) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, true) => b.cmp(&a),
            (false, false) => a.cmp(&b),
        }));
    }
    Ok(match (a, b) {
        (Data::Null, _) | (_, Data::Null) => None,
        (Data::Integer(a), Data::Integer(b)) => Some(i128::from(*a).cmp(&i128::from(*b))),
        (Data::Float(a), Data::Float(b)) => a.partial_cmp(b),
        (Data::Integer(a), Data::Float(b)) => integer_float((*a).into(), *b),
        (Data::Float(a), Data::Integer(b)) => integer_float((*b).into(), *a).map(Ordering::reverse),
        (Data::Text(a), Data::Text(b)) => Some(a.cmp(b)),
        (Data::Bytes(a), Data::Bytes(b)) => Some(a.cmp(b)),
        (Data::Bool(a), Data::Bool(b)) => Some(a.cmp(b)),
        _ => {
            return Err(error(
                "comparison requires matching scalar types; cast explicitly",
            ));
        }
    })
}

fn compare_integer(negative: bool, n: u128, rhs: &Data) -> Result<Option<Ordering>, Error> {
    if matches!(rhs, Data::Null) {
        return Ok(None);
    }
    if let Data::Float(f) = rhs {
        return Ok(if negative {
            unsigned_float(n, -f).map(Ordering::reverse)
        } else {
            unsigned_float(n, *f)
        });
    }
    let (other_negative, other) = magnitude(rhs)?;
    Ok(Some(match (negative, other_negative) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => other.cmp(&n),
        (false, false) => n.cmp(&other),
    }))
}
pub(crate) fn magnitude(value: &Data) -> Result<(bool, u128), Error> {
    match value {
        Data::Tag(2, _) => Ok((false, pil_codec::unsigned_native(value)?)),
        Data::Tag(3, _) | Data::Integer(_) => {
            let n = pil_codec::signed_native(value)?;
            Ok((n < 0, n.unsigned_abs()))
        }
        _ => Err(error(
            "comparison requires matching scalar types; cast explicitly",
        )),
    }
}
fn numeric_float(value: &Data, f: f64) -> Result<Option<Ordering>, Error> {
    let (negative, n) = magnitude(value)?;
    Ok(if negative {
        unsigned_float(n, -f).map(Ordering::reverse)
    } else {
        unsigned_float(n, f)
    })
}
// The existing native adapters retain their direct type matches. Numeric
// boundary handling is shared with sorting and relational reductions.
#[inline]
pub(crate) fn integer_float(n: i128, f: f64) -> Option<Ordering> {
    crate::comparison::Number::Signed(n).compare(crate::comparison::Number::Float(f))
}
#[inline]
pub(crate) fn unsigned_float(n: u128, f: f64) -> Option<Ordering> {
    crate::comparison::Number::Unsigned(n).compare(crate::comparison::Number::Float(f))
}

/// Cache expensive wide-literal decoding; ordinary literals retain the tuned
/// native comparison path rather than passing through a second scalar enum.
#[derive(Clone)]
pub(crate) struct Operand {
    value: Data,
    wide: Option<crate::comparison::Number>,
}
impl Operand {
    pub(crate) fn new(value: Data) -> Self {
        use crate::comparison::Number as N;
        let wide = match &value {
            Data::Tag(2, _) => pil_codec::unsigned_native(&value).ok().map(N::Unsigned),
            Data::Tag(3, _) => pil_codec::signed_native(&value).ok().map(N::Signed),
            _ => None,
        };
        Self { value, wide }
    }
}
#[inline(always)]
pub(crate) fn compare_operand(
    value: NativeValue<'_>,
    rhs: &Operand,
) -> Result<Option<Ordering>, Error> {
    use crate::comparison::Number as N;
    let Some(right) = rhs.wide else {
        return compare(value, &rhs.value);
    };
    let v = match value {
        NativeValue::Composite(v) => return compare_data(&v, &rhs.value),
        NativeValue::Scalar(v) => v,
    };
    let left = match v {
        Value::Absent | Value::Null => return Ok(None),
        Value::I8(n) => N::Signed(n.into()),
        Value::I16(n) => N::Signed(n.into()),
        Value::I32(n) => N::Signed(n.into()),
        Value::I64(n) | Value::TimestampMs(n) | Value::TimestampUs(n) | Value::TimestampNs(n) => {
            N::Signed(n.into())
        }
        Value::I128(n) => N::Signed(n),
        Value::U8(n) => N::Unsigned(n.into()),
        Value::U16(n) | Value::Ref16(n) => N::Unsigned(n.into()),
        Value::U24(n) | Value::U32(n) => N::Unsigned(n.into()),
        Value::U64(n) => N::Unsigned(n.into()),
        Value::U128(n) => N::Unsigned(n),
        Value::F32(n) => N::Float(n.into()),
        Value::F64(n) => N::Float(n),
        _ => {
            return Err(error(
                "comparison requires matching scalar types; cast explicitly",
            ));
        }
    };
    Ok(left.compare(right))
}
#[inline(always)]
pub(crate) fn compare_object(value: &Data, rhs: &Operand) -> Result<Option<Ordering>, Error> {
    compare_data(value, &rhs.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prepared_wide_literals_match_direct_comparison() {
        let literals = [
            pil_codec::native_unsigned(u128::MAX),
            pil_codec::native_signed(i128::MIN),
            pil_codec::native_signed(i128::MAX),
        ];
        for rhs in literals {
            let prepared = Operand::new(rhs.clone());
            assert!(prepared.wide.is_some());
            for value in [
                Value::Absent,
                Value::Null,
                Value::I8(-1),
                Value::I16(-3),
                Value::I32(-7),
                Value::I64(i64::MIN),
                Value::I128(i128::MIN),
                Value::I128(i128::MAX),
                Value::U8(1),
                Value::U16(3),
                Value::U24(7),
                Value::U32(u32::MAX),
                Value::U64(u64::MAX),
                Value::U128(u128::MAX),
                Value::F32(-0.5),
                Value::F64(2f64.powi(128)),
                Value::F64(-2f64.powi(127)),
                Value::F64(f64::NAN),
            ] {
                assert_eq!(
                    compare_operand(NativeValue::Scalar(value), &prepared).unwrap(),
                    compare(NativeValue::Scalar(value), &rhs).unwrap(),
                );
            }
            assert!(compare_operand(NativeValue::Scalar(Value::String("1")), &prepared).is_err());
        }
    }
    #[test]
    fn numeric_comparisons_preserve_128_bit_boundaries() {
        assert_eq!(
            unsigned_float(u128::MAX, 2f64.powi(128)),
            Some(Ordering::Less)
        );
        assert_eq!(
            integer_float(i128::MIN, -2f64.powi(127)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            integer_float(i128::MIN + 1, -2f64.powi(127)),
            Some(Ordering::Greater)
        );
        assert_eq!(unsigned_float(0, -0.0), Some(Ordering::Equal));
        assert_eq!(unsigned_float(3, 3.5), Some(Ordering::Less));
        assert_eq!(integer_float(-3, -3.5), Some(Ordering::Greater));
        assert_eq!(
            unsigned_float(u128::MAX, f64::INFINITY),
            Some(Ordering::Less)
        );
        assert_eq!(
            integer_float(i128::MIN, f64::NEG_INFINITY),
            Some(Ordering::Greater)
        );
        assert_eq!(unsigned_float(0, f64::NAN), None);
        for a in [i128::MIN, -1, 0, i128::MAX] {
            for b in [i128::MIN, -1, 0, i128::MAX] {
                assert_eq!(
                    compare(
                        NativeValue::Scalar(Value::I128(a)),
                        &pil_codec::native_signed(b)
                    )
                    .unwrap(),
                    Some(a.cmp(&b))
                );
            }
        }
        assert_eq!(
            compare(
                NativeValue::Scalar(Value::U128(u128::MAX)),
                &pil_codec::native_signed(i128::MAX)
            )
            .unwrap(),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare(
                NativeValue::Scalar(Value::F64(2f64.powi(128))),
                &pil_codec::native_unsigned(u128::MAX)
            )
            .unwrap(),
            Some(Ordering::Greater)
        );
    }
}
