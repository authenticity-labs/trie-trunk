//! Explicit text-to-number casts. Formats are reusable query-plan inputs.
//! Affixes are matched exactly, not removed from arbitrary positions. Removing
//! a unit yields its magnitude; it does not convert units or exchange currency.
use std::num::IntErrorKind;
mod natural;
mod temporal;
pub use natural::{Number, number, duration};
pub use temporal::{Context, DateOrder, timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    Prefix,
    Suffix,
    Integer,
    Unsigned,
    Overflow,
    Float,
    NonFinite,
    Sign,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Empty => "numeric cast requires a value",
            Self::Prefix => "numeric cast prefix does not match",
            Self::Suffix => "numeric cast suffix does not match",
            Self::Integer => {
                "numeric cast requires integer digits; fractions and exponents are not truncated"
            }
            Self::Unsigned => "negative value cannot be cast to uint",
            Self::Overflow => "numeric cast exceeds the target range",
            Self::Float => "numeric cast requires a decimal number",
            Self::NonFinite => "numeric cast requires a finite float",
            Self::Sign => "numeric cast contains more than one sign",
        })
    }
}
impl std::error::Error for Error {}

/// Borrowed format settings; one instance can be reused for an entire column.
/// Decimal point is `.`. Grouping separators and locale inference are not used.
#[derive(Clone, Copy, Debug, Default)]
pub struct Format<'a> {
    prefix: &'a str,
    suffix: &'a str,
}

impl<'a> Format<'a> {
    pub const fn new() -> Self {
        Self {
            prefix: "",
            suffix: "",
        }
    }
    pub const fn prefix(mut self, prefix: &'a str) -> Self {
        self.prefix = prefix;
        self
    }
    pub const fn suffix(mut self, suffix: &'a str) -> Self {
        self.suffix = suffix;
        self
    }

    fn parts<'s>(&self, text: &'s str) -> Result<(bool, &'s str), Error> {
        fn sign(text: &str) -> (Option<bool>, &str) {
            match text.as_bytes().first() {
                Some(b'-') => (Some(true), &text[1..]),
                Some(b'+') => (Some(false), &text[1..]),
                _ => (None, text),
            }
        }
        let text = text.trim();
        if text.is_empty() {
            return Err(Error::Empty);
        }
        // Accept -$12 or $-12, but never both signs. The sign must touch the
        // prefix or number; whitespace may separate an affix from the number.
        let (outer_sign, text) = sign(text);
        let text = text.strip_prefix(self.prefix).ok_or(Error::Prefix)?;
        let text = if self.prefix.is_empty() {
            text
        } else {
            text.trim_start()
        };
        let (inner_sign, text) = sign(text);
        if outer_sign.is_some() && inner_sign.is_some() {
            return Err(Error::Sign);
        }
        let text = text.strip_suffix(self.suffix).ok_or(Error::Suffix)?;
        let text = if self.suffix.is_empty() {
            text
        } else {
            text.trim_end()
        };
        if text.is_empty() {
            return Err(Error::Empty);
        }
        // The parser must not interpret another sign after the one removed.
        if text.starts_with(['+', '-']) {
            return Err(Error::Sign);
        }
        Ok((outer_sign.or(inner_sign).unwrap_or(false), text))
    }

    /// Exact unsigned 128-bit result. No floating-point intermediate.
    pub fn uint(&self, text: &str) -> Result<u128, Error> {
        let (negative, digits) = self.parts(text)?;
        if negative {
            return Err(Error::Unsigned);
        }
        magnitude(digits)
    }

    /// Exact signed 128-bit result, including i128::MIN.
    pub fn int(&self, text: &str) -> Result<i128, Error> {
        let (negative, digits) = self.parts(text)?;
        let n = magnitude(digits)?;
        if negative {
            if n == 1u128 << 127 {
                return Ok(i128::MIN);
            }
            i128::try_from(n).map(|n| -n).map_err(|_| Error::Overflow)
        } else {
            i128::try_from(n).map_err(|_| Error::Overflow)
        }
    }

    /// IEEE float64 conversion. Decimal fractions may round to binary float.
    /// Decimal exponents are accepted; NaN and infinities are rejected.
    pub fn float(&self, text: &str) -> Result<f64, Error> {
        let (negative, digits) = self.parts(text)?;
        let n: f64 = digits.parse().map_err(|_| Error::Float)?;
        if !n.is_finite() {
            return Err(Error::NonFinite);
        }
        Ok(if negative { -n } else { n })
    }
}

fn magnitude(digits: &str) -> Result<u128, Error> {
    digits.parse::<u128>().map_err(|e| match e.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => Error::Overflow,
        _ => Error::Integer,
    })
}

pub fn uint(text: &str) -> Result<u128, Error> {
    Format::new().uint(text)
}
pub fn int(text: &str) -> Result<i128, Error> {
    Format::new().int(text)
}
pub fn float(text: &str) -> Result<f64, Error> {
    Format::new().float(text)
}

mod plan;
pub use plan::{Spec,Target};
pub(crate) use plan::Plan;
