//! Recognition is separate from conversion. Never evaluate user text as code.
use crate::{Error, error};
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{ToPrimitive, Zero};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Exact decimal/rational magnitude in the requested unit.
#[derive(Clone, Debug)]
pub struct Number(pub(crate) BigRational);
impl Number {
    pub fn uint(&self) -> Result<u128, Error> {
        if !self.0.is_integer() {
            return Err(error(
                "fraction cannot be cast to an integer without rounding",
            ));
        }
        self.0
            .to_integer()
            .to_u128()
            .ok_or_else(|| error("value exceeds uint(16)"))
    }
    pub fn int(&self) -> Result<i128, Error> {
        if !self.0.is_integer() {
            return Err(error(
                "fraction cannot be cast to an integer without rounding",
            ));
        }
        self.0
            .to_integer()
            .to_i128()
            .ok_or_else(|| error("value exceeds int(16)"))
    }
    /// IEEE-754 rounding is explicit at the float boundary only.
    pub fn float(&self) -> Result<f64, Error> {
        self.0
            .to_f64()
            .filter(|v| v.is_finite())
            .ok_or_else(|| error("value exceeds finite float range"))
    }
}

/// Parse a number, optionally converting its units into the declared target.
/// Bare numbers already use the target unit. Currency symbols require a matching
/// currency target; this never performs exchange-rate conversions.
pub fn number(text: &str, target: Option<&str>) -> Result<Number, Error> {
    match basic(text, target) {
        Ok(n) => Ok(n),
        Err(e) => {
            let Some(target) = target else {
                return Err(e);
            };
            if !runits::Quantity::parse("1 s").is_ok_and(|q| q.to(target).is_ok()) {
                return Err(e);
            }
            duration(text, target).or(Err(e))
        }
    }
}
/// Clock notation or compound elapsed durations, interpreted in a time context.
pub fn duration(text: &str, target: &str) -> Result<Number, Error> {
    if text.len() > 512 {
        return Err(error("duration exceeds 512 bytes"));
    }
    let text = text.trim();
    let (sign, text) = if let Some(s) = text.strip_prefix('-') {
        (-1, s)
    } else {
        (1, text)
    };
    if text.contains(':') {
        let parts = text.split(':').collect::<Vec<_>>();
        if !(2..=3).contains(&parts.len()) {
            return Err(error("duration requires MM:SS or HH:MM:SS"));
        }
        let mut seconds = BigRational::from_integer(BigInt::from(0));
        for (i, p) in parts.iter().enumerate() {
            let n = decimal(p)?;
            if n < BigRational::from_integer(0.into())
                || (i > 0 && n >= BigRational::from_integer(60.into()))
                || (i + 1 < parts.len() && !n.is_integer())
            {
                return Err(error("invalid clock duration"));
            }
            seconds = seconds * BigInt::from(60) + n;
        }
        return scale_seconds(seconds * BigInt::from(sign), target);
    }
    let mut rest = text;
    let mut sum = BigRational::from_integer(0.into());
    let mut terms = 0;
    while !rest.is_empty() {
        let n = rest
            .bytes()
            .take_while(|c| c.is_ascii_digit() || *c == b'.')
            .count();
        if n == 0 {
            return Err(error("invalid duration quantity"));
        }
        let magnitude = &rest[..n];
        rest = rest[n..].trim_start();
        let end = rest
            .char_indices()
            .take_while(|(_, c)| c.is_alphabetic() || *c == 'µ')
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .ok_or_else(|| error("duration requires units"))?;
        let unit = &rest[..end];
        let unit = if unit == "m" { "min" } else { unit };
        sum += basic(&format!("{magnitude} {unit}"), Some("s"))?.0;
        rest = rest[end..].trim_start();
        terms += 1;
        if terms > 16 {
            return Err(error("duration exceeds 16 components"));
        }
    }
    if terms == 0 {
        return Err(error("empty duration"));
    }
    scale_seconds(sum * BigInt::from(sign), target)
}
fn scale_seconds(seconds: BigRational, target: &str) -> Result<Number, Error> {
    Ok(Number(seconds * basic("1 s", Some(target))?.0))
}
fn basic(text: &str, target: Option<&str>) -> Result<Number, Error> {
    if text.len() > 512 {
        return Err(error("numeric input exceeds 512 bytes"));
    }
    let normalized = text.trim().replace('−', "-");
    let mut s = normalized.as_str();
    let mut sign = "";
    if s.starts_with(['-', '+']) {
        sign = &s[..1];
        s = s[1..].trim_start();
    }
    let currency = s.chars().next().filter(|c| "$¥£€".contains(*c));
    if let Some(c) = currency {
        s = s[c.len_utf8()..].trim_start();
    }
    if s.starts_with(['-', '+']) {
        if !sign.is_empty() {
            return Err(error("more than one numeric sign"));
        }
        sign = &s[..1];
        s = s[1..].trim_start();
    }
    // Exponent notation belongs to the numeric token only when followed by digits.
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_digit()
            || matches!(c, '.' | ',' | '\u{202f}' | '\u{a0}')
            || (c == ' ' && s.as_bytes().get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return Err(error("text does not start with a number"));
    }
    if s[end..].starts_with(['e', 'E']) {
        let start = end;
        let mut n = end + 1;
        if s[n..].starts_with(['+', '-']) {
            n += 1;
        }
        let digits = n;
        while s.as_bytes().get(n).is_some_and(u8::is_ascii_digit) {
            n += 1;
        }
        if n > digits {
            end = n;
        } else {
            end = start;
        }
    }
    let value = decimal(&format!("{sign}{}", &s[..end]))?;
    let suffix = s[end..].trim();
    if currency.is_some() || currency_name(suffix).is_some() {
        let expected = target
            .and_then(currency_name)
            .ok_or_else(|| error("currency text requires a declared currency unit"))?;
        if currency.is_some_and(|c| !match c {
            '$' => expected == "USD",
            '¥' => matches!(expected, "JPY" | "CNY"),
            '£' => expected == "GBP",
            '€' => expected == "EUR",
            _ => false,
        }) || (!suffix.is_empty() && currency_name(suffix) != Some(expected))
        {
            return Err(error(
                "currency does not match the declared unit; exchange is not implicit",
            ));
        }
        return Ok(Number(value));
    }
    if suffix.is_empty() {
        return Ok(Number(value));
    }
    let target =
        target.ok_or_else(|| error("unit-bearing text requires a declared target unit"))?;
    let source = unit(suffix)?;
    let destination = unit(target)?;
    // runits supplies vocabulary/dimensional validation; its f64 magnitude is
    // deliberately never used. Fend supplies exact arithmetic for conversion.
    let q = runits::Quantity::parse(&format!("1 {source}")).map_err(|e| error(e.to_string()))?;
    q.to(&destination).map_err(|e| error(e.to_string()))?;
    if source == destination {
        return Ok(Number(value));
    }
    let expression = format!(
        "((({}/{}) {source}) to {destination}) to fraction",
        value.numer(),
        value.denom()
    );
    let result = fend_core::evaluate_with_interrupt(
        &expression,
        &mut fend_core::Context::new(),
        &Budget(AtomicUsize::new(0)),
    )
    .map_err(|e| error(format!("unit conversion: {e}")))?;
    let output = result.get_main_result();
    // The requested fraction formatter produces a single rational magnitude.
    // Reject approximate/symbolic output rather than silently losing precision.
    let magnitude = output
        .split_whitespace()
        .next()
        .ok_or_else(|| error("missing converted magnitude"))?;
    let value = if let Some((a, b)) = magnitude.split_once('/') {
        let a = a
            .parse::<BigInt>()
            .map_err(|_| error("conversion is not an exact rational"))?;
        let b = b
            .parse::<BigInt>()
            .map_err(|_| error("conversion is not an exact rational"))?;
        if b.is_zero() {
            return Err(error("invalid conversion divisor"));
        }
        BigRational::new(a, b)
    } else {
        decimal(magnitude)?
    };
    Ok(Number(value))
}
fn currency_name(s: &str) -> Option<&'static str> {
    match s {
        "USD" | "$" => Some("USD"),
        "GBP" | "£" => Some("GBP"),
        "EUR" | "€" => Some("EUR"),
        "JPY" => Some("JPY"),
        "CNY" => Some("CNY"),
        _ => None,
    }
}
struct Budget(AtomicUsize);
impl fend_core::Interrupt for Budget {
    fn should_interrupt(&self) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed) >= 50_000
    }
}
fn unit(s: &str) -> Result<String, Error> {
    let s = s.replace(" per ", "/").replace('μ', "µ");
    let mut out = String::new();
    let mut name = String::new();
    let flush = |name: &mut String, out: &mut String| -> Result<(), Error> {
        if name.is_empty() {
            return Ok(());
        }
        if matches!(
            name.as_str(),
            "month" | "months" | "year" | "years" | "yr" | "yrs"
        ) {
            return Err(error("calendar units require a reference date"));
        }
        runits::unit(name).map_err(|e| error(e.to_string()))?;
        out.push_str(match name.as_str() {
            "msec" => "millisecond",
            "usec" => "microsecond",
            "nsec" => "nanosecond",
            _ => name,
        });
        name.clear();
        Ok(())
    };
    for c in s.chars() {
        if c.is_alphabetic() || matches!(c, '°' | '%' | 'µ') {
            name.push(c);
        } else {
            flush(&mut name, &mut out)?;
            if c.is_whitespace() {
                out.push(' ');
            } else if matches!(c, '/' | '*' | '^' | '2' | '3' | '-' | '1') {
                out.push(c);
            } else {
                return Err(error("unsupported unit notation"));
            }
        }
    }
    flush(&mut name, &mut out)?;
    if out.is_empty() {
        return Err(error("missing unit"));
    }
    Ok(out)
}

fn decimal(s: &str) -> Result<BigRational, Error> {
    let (mantissa, exponent) = s
        .split_once(['e', 'E'])
        .map_or((s, Ok(0)), |(a, b)| (a, b.parse::<i32>()));
    let exponent = exponent.map_err(|_| error("invalid decimal exponent"))?;
    if exponent.unsigned_abs() > 128 {
        return Err(error("decimal exponent exceeds 128"));
    }
    let mut s = mantissa.to_owned();
    for group in ['\u{202f}', '\u{a0}', ' '] {
        if s.contains(group) {
            let end = s.find(['.', ',']).unwrap_or(s.len());
            groups(&s[..end], group)?;
            if s[end..].contains(group) {
                return Err(error("invalid fractional grouping"));
            }
            s = s.replace(group, "");
        }
    }
    if s.contains(',') {
        if s.contains('.') {
            let comma = s.rfind(',').unwrap();
            let dot = s.rfind('.').unwrap();
            let (group, decimal) = if comma > dot { ('.', ',') } else { (',', '.') };
            let split = s.rfind(decimal).unwrap();
            groups(&s[..split], group)?;
            s = s.replace(group, "").replace(decimal, ".");
        } else {
            if s.matches(',').count() > 1 {
                groups(&s, ',')?;
                s = s.replace(',', "");
            } else if s.len() - s.rfind(',').unwrap() - 1 == 3 {
                return Err(error(
                    "ambiguous separator: use an ungrouped number or an explicit decimal point",
                ));
            } else {
                s = s.replace(',', ".");
            }
        }
    }
    let (whole, fraction) = s.split_once('.').unwrap_or((&s, ""));
    if fraction.contains('.') || !fraction.bytes().all(|c| c.is_ascii_digit()) {
        return Err(error("invalid decimal"));
    }
    let digits = format!("{whole}{fraction}");
    if digits.trim_start_matches(['-', '+']).is_empty()
        || !digits
            .trim_start_matches(['-', '+'])
            .bytes()
            .all(|c| c.is_ascii_digit())
    {
        return Err(error("invalid decimal digits"));
    }
    let n = digits
        .parse::<BigInt>()
        .map_err(|_| error("invalid decimal"))?;
    let scale = fraction.len() as i32 - exponent;
    Ok(if scale >= 0 {
        BigRational::new(n, BigInt::from(10).pow(scale as u32))
    } else {
        BigRational::from_integer(n * BigInt::from(10).pow((-scale) as u32))
    })
}
fn groups(s: &str, separator: char) -> Result<(), Error> {
    let mut parts = s.trim_start_matches(['-', '+']).split(separator);
    let first = parts.next().unwrap_or("");
    if first.is_empty()
        || first.len() > 3
        || !first.bytes().all(|c| c.is_ascii_digit())
        || !parts.all(|p| p.len() == 3 && p.bytes().all(|c| c.is_ascii_digit()))
    {
        return Err(error("invalid digit grouping"));
    }
    Ok(())
}
