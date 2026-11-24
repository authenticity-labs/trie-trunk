//! Calendar selection for persisted five-minute summary periods.
use crate::{Error, error};
use std::ops::Range;

pub const SUMMARY_PERIOD_MS: u64 = 300_000;
pub const SUMMARY_DAY_MS: u64 = 86_400_000;
pub const SUMMARY_DAY_ROWS: usize = (SUMMARY_DAY_MS / SUMMARY_PERIOD_MS) as usize;

/// The configured fixed UTC offset, shared by native and browser nodes.
/// This retains the existing -12..14 whole-hour policy; it is not a timezone
/// with daylight-saving transitions or a browser-local calendar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SummaryCalendar {
    offset_hours: i8,
}
impl SummaryCalendar {
    pub fn new(offset_hours: i8) -> Result<Self, Error> {
        if !(-12..=14).contains(&offset_hours) {
            return Err(error("summary UTC offset must be -12..14 whole hours"));
        }
        Ok(Self { offset_hours })
    }
    pub fn offset_hours(self) -> i8 { self.offset_hours }
    fn start(self, time: u64) -> i128 {
        let offset = i128::from(self.offset_hours) * 3_600_000;
        let day = i128::from(SUMMARY_DAY_MS);
        (i128::from(time) + offset).div_euclid(day) * day - offset
    }
    pub fn day(self, time: u64) -> Result<Range<u64>, Error> {
        let start = u64::try_from(self.start(time)).map_err(|_| Error::Overflow)?;
        Ok(start..start.checked_add(SUMMARY_DAY_MS).ok_or(Error::Overflow)?)
    }
}

/// Requested timestamps remain distinct from the complete buckets used by
/// summary analytics. Event readers retain the requested window unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryWindow {
    requested: Range<u64>,
    covered: Range<u64>,
}
impl SummaryWindow {
    pub fn new(requested: Range<u64>) -> Result<Self, Error> {
        if requested.start >= requested.end {
            return Err(error("summary window must increase"));
        }
        let start = requested.start / SUMMARY_PERIOD_MS * SUMMARY_PERIOD_MS;
        let end = requested
            .end
            .div_ceil(SUMMARY_PERIOD_MS)
            .checked_mul(SUMMARY_PERIOD_MS)
            .ok_or(Error::Overflow)?;
        Ok(Self {
            requested,
            covered: start..end,
        })
    }
    pub fn requested(&self) -> Range<u64> {
        self.requested.clone()
    }
    pub fn covered(&self) -> Range<u64> {
        self.covered.clone()
    }
    pub fn period_count(&self) -> u64 {
        (self.covered.end - self.covered.start) / SUMMARY_PERIOD_MS
    }
    /// Constant-time selection of overlapping rows in a fixed-period Pack.
    /// The caller supplies the Pack's calendar binding, not an event field.
    pub fn rows(&self, first: u64, count: usize) -> Result<Range<usize>, Error> {
        if first % SUMMARY_PERIOD_MS != 0 {
            return Err(error("summary Pack must start at a five-minute boundary"));
        }
        let length = u64::try_from(count)
            .map_err(|_| Error::Overflow)?
            .checked_mul(SUMMARY_PERIOD_MS)
            .ok_or(Error::Overflow)?;
        let end = first.checked_add(length).ok_or(Error::Overflow)?;
        let from = self.covered.start.max(first);
        let to = self.covered.end.min(end);
        if from >= to {
            return Ok(0..0);
        }
        Ok(
            usize::try_from((from - first) / SUMMARY_PERIOD_MS).map_err(|_| Error::Overflow)?
                ..usize::try_from((to - first) / SUMMARY_PERIOD_MS).map_err(|_| Error::Overflow)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arbitrary_windows_select_complete_overlapping_periods() {
        let minute = 60_000;
        let window = SummaryWindow::new(602 * minute..617 * minute).unwrap();
        assert_eq!(window.requested(), 602 * minute..617 * minute);
        assert_eq!(window.covered(), 600 * minute..620 * minute);
        assert_eq!(window.rows(600 * minute, 12).unwrap(), 0..4);
        assert_eq!(window.period_count(), 4);
        assert_eq!(
            SummaryWindow::new(1..2).unwrap().covered(),
            0..SUMMARY_PERIOD_MS
        );
        assert_eq!(
            SummaryWindow::new(0..SUMMARY_PERIOD_MS)
                .unwrap()
                .period_count(),
            1
        );
        assert_eq!(window.rows(620 * minute, 12).unwrap(), 0..0);
    }
    #[test]
    fn midnight_and_invalid_ranges() {
        let day = 86_400_000;
        let window = SummaryWindow::new(day - 1..day + 1).unwrap();
        assert_eq!(window.rows(0, 288).unwrap(), 287..288);
        assert_eq!(window.rows(day, 288).unwrap(), 0..1);
        assert!(window.rows(1, 288).is_err());
        assert!(SummaryWindow::new(1..1).is_err());
        assert!(SummaryWindow::new(u64::MAX - 1..u64::MAX).is_err());
        assert!(window.rows(0, usize::MAX).is_err());
    }
}
