use crate::{Error, error};
use chrono::{
    DateTime, Datelike, Days, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub enum DateOrder {
    MonthFirst,
    DayFirst,
}

/// One clock snapshot per query. Browser adapters supply their clock explicitly.
/// UTC is the default; named zones retain daylight-saving rules.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "planner-lab", derive(serde::Serialize))]
pub struct Context {
    pub reference_ms: Option<i64>,
    pub timezone: String,
    pub date_order: Option<DateOrder>,
}
impl Default for Context {
    fn default() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let reference_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok());
        #[cfg(target_arch = "wasm32")]
        let reference_ms = None;
        Self {
            reference_ms,
            timezone: "UTC".into(),
            date_order: None,
        }
    }
}

/// Parse a timestamp into exact nanoseconds since Unix epoch. A local time must
/// resolve uniquely: a DST gap/fold requires an explicit offset instead.
pub fn timestamp(text: &str, context: &Context) -> Result<i128, Error> {
    if text.len() > 256 {
        return Err(error("time input exceeds 256 bytes"));
    }
    let text = text.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(text) {
        return nanos(t);
    }
    if let Ok(t) = DateTime::parse_from_rfc2822(text) {
        return nanos(t);
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f %:z",
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%d/%b/%Y:%H:%M:%S %z",
    ] {
        if let Ok(t) = DateTime::parse_from_str(text, format) {
            return nanos(t);
        }
    }
    let zone = context
        .timezone
        .parse::<chrono_tz::Tz>()
        .map_err(|_| error("unknown query time zone"))?;
    let local =
        |t: NaiveDateTime| -> Result<i128, Error> {
            nanos(zone.from_local_datetime(&t).single().ok_or_else(|| {
                error("local time is ambiguous or nonexistent; provide a UTC offset")
            })?)
        };
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(text, format) {
            return local(t);
        }
    }
    for format in ["%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y"] {
        if let Ok(t) = NaiveDate::parse_from_str(text, format) {
            return local(t.and_hms_opt(0, 0, 0).unwrap());
        }
    }
    if text.contains('/') && text.chars().all(|c| c.is_ascii_digit() || c == '/') {
        let format = match context.date_order {
            Some(DateOrder::MonthFirst) => "%m/%d/%Y",
            Some(DateOrder::DayFirst) => "%d/%m/%Y",
            None => {
                return Err(error(
                    "ambiguous date notation; specify date order or use YYYY-MM-DD",
                ));
            }
        };
        return local(
            NaiveDate::parse_from_str(text, format)
                .map_err(|_| error("invalid date"))?
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
    }
    let reference = context
        .reference_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| error("relative time requires a query reference clock"))?
        .with_timezone(&zone);
    let lower = text.to_lowercase();
    let lower = lower.trim();
    if lower == "now" {
        return nanos(reference);
    }
    let date = reference.date_naive();
    if ["today", "yesterday", "tomorrow"].contains(&lower) {
        let date = match lower {
            "yesterday" => date.checked_sub_days(Days::new(1)),
            "tomorrow" => date.checked_add_days(Days::new(1)),
            _ => Some(date),
        }
        .ok_or(Error::Overflow)?;
        return local(date.and_hms_opt(0, 0, 0).unwrap());
    }
    for format in ["%H:%M:%S%.f", "%H:%M", "%I:%M%P", "%I%P"] {
        if let Ok(time) = NaiveTime::parse_from_str(lower, format) {
            return local(date.and_time(time));
        }
    }
    let words = lower.split_whitespace().collect::<Vec<_>>();
    if words.len() == 2 && matches!(words[0], "next" | "last") {
        let weekdays = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ];
        if let Some(day) = weekdays.iter().position(|d| *d == words[1]) {
            let current = date.weekday().num_days_from_monday() as usize;
            let delta = if words[0] == "next" {
                (day + 7 - current) % 7
            } else {
                (current + 7 - day) % 7
            };
            let delta = if delta == 0 { 7 } else { delta } as u64;
            let date = if words[0] == "next" {
                date.checked_add_days(Days::new(delta))
            } else {
                date.checked_sub_days(Days::new(delta))
            }
            .ok_or(Error::Overflow)?;
            return local(date.and_hms_opt(0, 0, 0).unwrap());
        }
    }
    let (body, negative) = if let Some(s) = lower.strip_suffix(" ago") {
        (s, true)
    } else if let Some(s) = lower.strip_prefix("in ") {
        (s, false)
    } else {
        return Err(error("unrecognized timestamp or relative-time expression"));
    };
    let (n, unit) = body
        .split_once(' ')
        .ok_or_else(|| error("relative time requires a quantity and unit"))?;
    if matches!(
        unit,
        "month" | "months" | "year" | "years" | "day" | "days" | "week" | "weeks"
    ) {
        let n = n
            .parse::<u32>()
            .map_err(|_| error("calendar interval requires a positive whole number"))?;
        let shifted = if matches!(unit, "month" | "months" | "year" | "years") {
            let count = n
                .checked_mul(if unit.starts_with("year") { 12 } else { 1 })
                .ok_or(Error::Overflow)?;
            if negative {
                date.checked_sub_months(Months::new(count))
            } else {
                date.checked_add_months(Months::new(count))
            }
        } else {
            let count = u64::from(n) * if unit.starts_with("week") { 7 } else { 1 };
            if negative {
                date.checked_sub_days(Days::new(count))
            } else {
                date.checked_add_days(Days::new(count))
            }
        }
        .ok_or(Error::Overflow)?;
        return local(shifted.and_time(reference.time()));
    }
    let duration = super::number(body, Some("ns"))?.int()?;
    let base = nanos(reference)?;
    if negative {
        base.checked_sub(duration)
    } else {
        base.checked_add(duration)
    }
    .ok_or(Error::Overflow)
}
fn nanos<T: TimeZone>(t: DateTime<T>) -> Result<i128, Error> {
    if t.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(error(
            "leap-second timestamp cannot be represented as Unix time",
        ));
    }
    Ok(i128::from(t.timestamp()) * 1_000_000_000 + i128::from(t.timestamp_subsec_nanos()))
}
