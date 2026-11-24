//! Fixed distribution profiles and query-time percentile results.
use crate::{Error, error};
use ciborium::Value as Data;
use std::{cmp::Ordering, collections::BTreeMap};

/// Upper-inclusive boundaries. K boundaries allocate K+1 counters, including
/// both tails. Profiles are stored once in Box metadata, not in each period.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct SummaryProfile {
    pub(super) histograms: BTreeMap<String, Vec<Data>>,
}
impl SummaryProfile {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn histogram(
        mut self,
        field: impl Into<String>,
        boundaries: impl IntoIterator<Item = Data>,
    ) -> Result<Self, Error> {
        let field = field.into();
        let boundaries: Vec<_> = boundaries.into_iter().collect();
        if boundaries.is_empty() || boundaries.len() >= u16::MAX as usize {
            return Err(error("histogram needs 1–65534 finite boundaries"));
        }
        for (i, value) in boundaries.iter().enumerate() {
            let numeric = matches!(value, Data::Integer(_) | Data::Tag(2 | 3, _))
                || matches!(value, Data::Float(v) if v.is_finite());
            if !numeric
                || crate::value::compare_data(value, value)? != Some(Ordering::Equal)
                || i > 0
                    && crate::value::compare_data(&boundaries[i - 1], value)?
                        != Some(Ordering::Less)
            {
                return Err(error(
                    "histogram boundaries must be finite, numeric and strictly increasing",
                ));
            }
        }
        if self.histograms.insert(field, boundaries).is_some() {
            return Err(error("duplicate histogram profile for field"));
        }
        Ok(self)
    }
    pub fn boundaries(&self, field: &str) -> Option<&[Data]> {
        self.histograms.get(field).map(Vec::as_slice)
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        let fields = self
            .histograms
            .iter()
            .map(|(name, values)| {
                Ok((
                    Data::Text(name.clone()),
                    Data::Array(
                        values
                            .iter()
                            .map(encode_bound)
                            .collect::<Result<Vec<_>, Error>>()?,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        pil_codec::object::encode_value(&Data::Map(fields))
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let Data::Map(fields) = pil_codec::object::decode_value(bytes)? else {
            return Err(error("invalid summary distribution profile"));
        };
        let mut profile = Self::new();
        for (name, bounds) in fields {
            let (Data::Text(name), Data::Array(bounds)) = (name, bounds) else {
                return Err(error("invalid summary histogram profile"));
            };
            profile = profile.histogram(
                name,
                bounds
                    .iter()
                    .map(decode_bound)
                    .collect::<Result<Vec<_>, _>>()?,
            )?;
        }
        Ok(profile)
    }
}

// PIL's generic value profile has no CBOR bignum tags. Numeric profile literals
// therefore carry an explicit kind and decimal integer text in metadata only;
// planning converts them once to typed numeric boundaries. Event/bin counters
// continue to use the fixed Rust integer codec.
fn encode_bound(value: &Data) -> Result<Data, Error> {
    let (kind, value) = match value {
        Data::Float(n) => (2u64, Data::Float(*n)),
        Data::Integer(n) if i128::from(*n) < 0 => (1, Data::Text(i128::from(*n).to_string())),
        Data::Tag(3, _) => (1, Data::Text(pil_codec::signed_native(value)?.to_string())),
        _ => (
            0,
            Data::Text(pil_codec::unsigned_native(value)?.to_string()),
        ),
    };
    Ok(Data::Array(vec![kind.into(), value]))
}
fn decode_bound(value: &Data) -> Result<Data, Error> {
    let Data::Array(parts) = value else {
        return Err(error("invalid histogram boundary"));
    };
    if parts.len() != 2 {
        return Err(error("invalid histogram boundary"));
    }
    let kind = parts[0]
        .as_integer()
        .map(i128::from)
        .ok_or_else(|| error("invalid histogram boundary kind"))?;
    Ok(match kind {
        0 => pil_codec::native_unsigned(
            parts[1]
                .as_text()
                .and_then(|s| s.parse::<u128>().ok())
                .ok_or_else(|| error("invalid unsigned histogram boundary"))?,
        ),
        1 => pil_codec::native_signed(
            parts[1]
                .as_text()
                .and_then(|s| s.parse::<i128>().ok())
                .ok_or_else(|| error("invalid signed histogram boundary"))?,
        ),
        2 => Data::Float(
            parts[1]
                .as_float()
                .ok_or_else(|| error("invalid float histogram boundary"))?,
        ),
        _ => return Err(error("invalid histogram boundary kind")),
    })
}

/// Nearest-rank percentile. For histogram estimates, value is the upper bound
/// of the containing bin, capped by the observed maximum. Bounds retain native
/// integer precision. No uniform-within-bin interpolation is assumed.
#[derive(Clone, Debug, PartialEq)]
pub struct Percentile {
    pub value: Data,
    pub lower: Data,
    pub upper: Data,
    pub exact: bool,
}
impl Percentile {
    pub(super) fn bounded(lower: Data, upper: Data) -> Self {
        let exact = lower == upper;
        Self {
            value: upper.clone(),
            lower,
            upper,
            exact,
        }
    }
}

/// Compute ceil(n*p/100) without converting the population to f64. Every
/// finite f64 percentile is an integer significand times a power of two.
pub(super) fn rank(percentile: f64, count: u64) -> Result<u64, Error> {
    if !percentile.is_finite() || !(0.0..=100.0).contains(&percentile) {
        return Err(error("percentile must be finite and from 0 to 100"));
    }
    if count == 0 {
        return Ok(0);
    }
    if percentile == 0.0 {
        return Ok(1);
    }
    if percentile == 100.0 {
        return Ok(count);
    }
    let bits = percentile.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = (bits & ((1u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1u64 << 52 };
    let shift = 1075 - exponent.max(1);
    if shift > 121 {
        return Ok(1);
    }
    let numerator = u128::from(mantissa) * u128::from(count);
    let denominator = 100u128 << shift;
    u64::try_from(numerator.div_ceil(denominator)).map_err(|_| Error::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ranks_preserve_large_counts() {
        assert_eq!(rank(95.0, 1200).unwrap(), 1140);
        assert_eq!(rank(50.0, u64::MAX).unwrap(), 1u64 << 63);
        assert_eq!(rank(100.0, u64::MAX).unwrap(), u64::MAX);
        assert_eq!(rank(f64::from_bits(1), u64::MAX).unwrap(), 1);
        assert!(rank(f64::NAN, 1).is_err());
    }
}
