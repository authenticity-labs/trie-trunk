//! Dashboard working set: copy selected scalar aggregates once, then reuse
//! contiguous columns for charts, totals, comparisons and grouped reductions.
use super::report::Metric;
use crate::{stalks::Column, Error, MemoryBudget, MemoryLease, error};

pub(super) struct Block {
    source: String,
    times: Column<u64>, events: Column<u64>, lower: Column<u64>, samples: Column<u64>,
    sums: Column<f64>, minima: Column<f64>, maxima: Column<f64>,
    _metadata: MemoryLease,
}
impl Block {
    pub fn new(source: &str, count: usize, budget: &MemoryBudget) -> Result<Self, Error> {
        let metadata = budget.reserve(std::mem::size_of::<Self>().checked_add(source.len()).ok_or(Error::Overflow)?)?;
        Ok(Self { source: source.to_owned(), times: Column::new(count,budget)?, events:Column::new(count,budget)?,
            lower:Column::new(count,budget)?, samples:Column::new(count,budget)?, sums:Column::new(count,budget)?,
            minima:Column::new(count,budget)?, maxima:Column::new(count,budget)?, _metadata:metadata })
    }
    pub fn push(&mut self, time:u64, value:Metric) -> Result<(),Error> {
        self.times.push(time)?; self.events.push(value.events)?; self.lower.push(value.lower)?;
        self.samples.push(value.samples)?; self.sums.push(value.sum)?;
        self.minima.push(value.min.unwrap_or(0.0))?; self.maxima.push(value.max.unwrap_or(0.0))?; Ok(())
    }
}
#[derive(Default)]
pub(super) struct Columns { blocks:Vec<Block>, count:usize }
impl Columns {
    pub fn len(&self)->usize {self.count}
    pub fn insert(&mut self, block:Block)->Result<(),Error> {
        let (Some(&first),Some(&last))=(block.times.values().first(),block.times.values().last()) else {return Ok(())};
        if self.blocks.iter().any(|b| b.source==block.source && b.times.values()[0]<=last && *b.times.values().last().unwrap()>=first) {
            return Err(error("duplicate summary source period"));
        }
        self.count=self.count.checked_add(block.times.len()).ok_or(Error::Overflow)?;
        self.blocks.push(block);
        // Match the original stable source/time reduction order, including
        // floating-point accumulation and independently delivered day ranges.
        self.blocks.sort_by(|a,b|a.source.cmp(&b.source).then(a.times.values()[0].cmp(&b.times.values()[0])));
        Ok(())
    }
    pub fn iter(&self)->impl Iterator<Item=(&str,u64,Metric)> {
        self.blocks.iter().flat_map(|b|(0..b.times.len()).map(move |i| {
            let samples=b.samples.values()[i];
            (b.source.as_str(),b.times.values()[i],Metric {events:b.events.values()[i],lower:b.lower.values()[i],
                samples,sum:b.sums.values()[i],min:(samples>0).then(||b.minima.values()[i]),max:(samples>0).then(||b.maxima.values()[i]),means:Vec::new()})
        }))
    }
}
