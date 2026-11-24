//! Allocation-free scalar comparison shared by query execution and reductions.
//! Numeric widths are erased losslessly, not converted through floating point.
//! Units, field identities and explicit casts belong to the query planner.
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Number {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

impl Number {
    /// Compare represented values exactly, including integers beyond 2^53.
    /// NaN is unordered. Infinities compare normally.
    #[inline(always)]
    pub fn compare(self, rhs: Self) -> Option<Ordering> {
        use Number::*;
        Some(match (self, rhs) {
            (Signed(a), Signed(b)) => a.cmp(&b),
            (Unsigned(a), Unsigned(b)) => a.cmp(&b),
            (Signed(a), Unsigned(b)) => signed_unsigned(a, b),
            (Unsigned(a), Signed(b)) => signed_unsigned(b, a).reverse(),
            (Signed(a), Float(b)) => return signed_float(a, b),
            (Float(a), Signed(b)) => return signed_float(b, a).map(Ordering::reverse),
            (Unsigned(a), Float(b)) => return unsigned_float(a, b),
            (Float(a), Unsigned(b)) => return unsigned_float(b, a).map(Ordering::reverse),
            (Float(a), Float(b)) => return a.partial_cmp(&b),
        })
    }
}

#[inline(always)]
fn signed_unsigned(a: i128, b: u128) -> Ordering {
    if a < 0 {
        Ordering::Less
    } else {
        (a as u128).cmp(&b)
    }
}

#[inline(always)]
fn signed_float(n: i128, f: f64) -> Option<Ordering> {
    if n < 0 {
        unsigned_float(n.unsigned_abs(), -f).map(Ordering::reverse)
    } else {
        unsigned_float(n as u128, f)
    }
}

#[inline(always)]
fn unsigned_float(n: u128, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f < 0.0 {
        return Some(Ordering::Greater);
    }
    // 2^128 is exactly representable; guard before the saturating Rust cast.
    if f >= 340282366920938463463374607431768211456.0 {
        return Some(Ordering::Less);
    }
    Some(n.cmp(&(f as u128)).then_with(|| {
        if f.fract() > 0.0 {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Absent,
    Null,
    Number,
    Bool,
    Text,
    Bytes,
    Composite,
}

/// Borrowed operands: comparing text or bytes never copies their contents.
#[derive(Clone, Copy, Debug)]
pub enum Scalar<'a> {
    Absent,
    Null,
    Number(Number),
    Bool(bool),
    Text(&'a str),
    Bytes(&'a [u8]),
    Composite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Incompatible {
    pub left: Kind,
    pub right: Kind,
}

impl std::fmt::Display for Incompatible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot compare {:?} with {:?}; cast explicitly",
            self.left, self.right
        )
    }
}
impl std::error::Error for Incompatible {}

impl Scalar<'_> {
    pub fn kind(self) -> Kind {
        match self {
            Self::Absent => Kind::Absent,
            Self::Null => Kind::Null,
            Self::Number(_) => Kind::Number,
            Self::Bool(_) => Kind::Bool,
            Self::Text(_) => Kind::Text,
            Self::Bytes(_) => Kind::Bytes,
            Self::Composite => Kind::Composite,
        }
    }

    /// Null/absent and NaN produce unknown; unrelated types are an error.
    #[inline(always)]
    pub fn compare(self, rhs: Self) -> Result<Option<Ordering>, Incompatible> {
        use Scalar::*;
        Ok(match (self, rhs) {
            (Absent | Null, _) | (_, Absent | Null) => None,
            (Number(a), Number(b)) => a.compare(b),
            (Bool(a), Bool(b)) => Some(a.cmp(&b)),
            (Text(a), Text(b)) => Some(a.cmp(b)),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            _ => {
                return Err(Incompatible {
                    left: self.kind(),
                    right: rhs.kind(),
                });
            }
        })
    }
}
