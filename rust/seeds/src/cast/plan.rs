//! Explicit conversions are compiled once; only selected text values are parsed.
use super::number;
use crate::{Error, error};
use ciborium::Value as Data;
use pil_codec::{Value, api::NativeValue};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(
        feature = "registration",
        feature = "query-language",
        feature = "planner-lab"
    ),
    derive(serde::Serialize, serde::Deserialize)
)]
#[cfg_attr(
    any(
        feature = "registration",
        feature = "query-language",
        feature = "planner-lab"
    ),
    serde(rename_all = "lowercase")
)]
pub enum Target {
    Int,
    Uint,
    Float,
    Bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(
        feature = "registration",
        feature = "query-language",
        feature = "planner-lab"
    ),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct Spec {
    pub target: Target,
    pub bytes: u8,
    #[cfg_attr(
        any(
            feature = "registration",
            feature = "query-language",
            feature = "planner-lab"
        ),
        serde(default)
    )]
    pub tolerant: bool,
}
impl Spec {
    pub fn validate(self) -> Result<Self, Error> {
        let valid = match self.target {
            Target::Int | Target::Uint => (1..=16).contains(&self.bytes),
            Target::Float => matches!(self.bytes, 4 | 8),
            Target::Bool => self.bytes == 1,
        };
        if !valid {
            return Err(error("invalid cast width"));
        }
        Ok(self)
    }
    pub fn scalar(self) -> pil_codec::Scalar {
        match self.target {
            Target::Int => pil_codec::Scalar::Int(self.bytes.into()),
            Target::Uint => pil_codec::Scalar::Uint(self.bytes.into()),
            Target::Float => pil_codec::Scalar::F64,
            Target::Bool => pil_codec::Scalar::Bool,
        }
    }
}
#[derive(Clone)]
pub(crate) struct Plan {
    pub spec: Spec,
    unit: Option<String>,
}
impl Plan {
    pub fn new(spec: Spec, unit: Option<&str>) -> Result<Self, Error> {
        Ok(Self {
            spec: spec.validate()?,
            unit: unit.map(str::to_owned),
        })
    }
    pub fn value<'a>(&self, value: NativeValue<'a>) -> Result<NativeValue<'a>, Error> {
        if matches!(
            value,
            NativeValue::Scalar(Value::Absent | Value::Null) | NativeValue::Composite(Data::Null)
        ) {
            return Ok(value);
        }
        let result = self.convert(value);
        if self.spec.tolerant {
            result.or(Ok(NativeValue::Scalar(Value::Null)))
        } else {
            result
        }
    }
    fn convert<'a>(&self, value: NativeValue<'a>) -> Result<NativeValue<'a>, Error> {
        // Text borrows the original Pill; no object or whole-row materialization.
        let text = match &value {
            NativeValue::Scalar(Value::String(s)) => Some(*s),
            NativeValue::Composite(Data::Text(s)) => Some(s.as_str()),
            _ => None,
        };
        if self.spec.target == Target::Bool {
            let b = match (&value, text) {
                (NativeValue::Scalar(Value::Bool(b)), _)
                | (NativeValue::Composite(Data::Bool(b)), _) => *b,
                (_, Some(s)) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "yes" | "on" | "1" => true,
                    "false" | "no" | "off" | "0" => false,
                    _ => return Err(error("cannot cast value to Boolean")),
                },
                _ => return Err(error("cannot cast value to Boolean")),
            };
            return Ok(NativeValue::Scalar(Value::Bool(b)));
        }
        let n = if let Some(s) = text {
            number(s, self.unit.as_deref())?
        } else {
            use num_rational::BigRational as R;
            let data = crate::value::own(value)?;
            let r = match data {
                crate::Cell::Value(Data::Integer(n)) => R::from_integer(i128::from(n).into()),
                crate::Cell::Value(Data::Float(n)) => {
                    R::from_float(n).ok_or_else(|| error("cast requires finite number"))?
                }
                crate::Cell::Value(v @ Data::Tag(2, _)) => {
                    R::from_integer(pil_codec::unsigned_native(&v)?.into())
                }
                crate::Cell::Value(v @ Data::Tag(3, _)) => {
                    R::from_integer(pil_codec::signed_native(&v)?.into())
                }
                _ => return Err(error("cast requires numeric or text value")),
            };
            super::Number(r)
        };
        let bits = self.spec.bytes as u32 * 8;
        Ok(NativeValue::Scalar(match self.spec.target {
            Target::Int => {
                let n = n.int()?;
                if bits < 128 && (n < -(1i128 << (bits - 1)) || n > ((1i128 << (bits - 1)) - 1)) {
                    return Err(error("signed cast overflow"));
                }
                Value::I128(n)
            }
            Target::Uint => {
                let n = n.uint()?;
                if bits < 128 && n > ((1u128 << bits) - 1) {
                    return Err(error("unsigned cast overflow"));
                }
                Value::U128(n)
            }
            Target::Float => {
                let mut n = n.float()?;
                if self.spec.bytes == 4 {
                    n = (n as f32) as f64;
                }
                if !n.is_finite() {
                    return Err(error("float cast overflow"));
                }
                Value::F64(n)
            }
            Target::Bool => unreachable!(),
        }))
    }
}
