//! Summary lifecycle belongs to the same ingress owner on every target.
//! Hosts supply persisted bytes and publish returned artifacts; they neither
//! accumulate fields nor independently construct daily publication plans.
use super::OwnedIngress;
use crate::registration::{SummaryIngress, Trunk};
use crate::{
    Document, Error, TimeTrie, error,
    summaries::{SUMMARY_DAY_MS, SummaryBox, SummaryBoxReader, SummaryCalendar},
};

pub const SUMMARY_FILE: &str = "summary.rx";
pub struct SummaryStart {
    pub first: u64,
    pub bootstrap: Vec<u8>,
}
pub struct SummaryWrite {
    pub trie: trie_trunk_taxonomy::Trunk,
    pub document: Document,
    pub bytes: Vec<u8>,
    pub offset: u64,
    pub length: u64,
}

/// Durable summary publication carries only its replacement extent. Registry
/// publication precedes this checkpoint; replay cannot replace the registry.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SummaryCommit {
    pub bytes: std::sync::Arc<Vec<u8>>,
    pub offset: u64,
    pub length: u64,
}
impl SummaryCommit {
    pub fn validate(&self) -> Result<(), Error> {
        if self.offset.checked_add(self.bytes.len() as u64) != Some(self.length) {
            return Err(error("invalid summary publication extent"));
        }
        Ok(())
    }
    pub fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        super::recovery::checkpoint_bytes(
            &serde_json::to_vec(self).map_err(|e| error(e.to_string()))?,
        )
    }
    pub fn restore(bytes: &[u8], maximum: usize) -> Result<Self, Error> {
        let raw = super::recovery::restore_bytes(bytes, maximum)?;
        let commit: Self = serde_json::from_slice(&raw).map_err(|e| error(e.to_string()))?;
        commit.validate()?;
        Ok(commit)
    }
}
impl SummaryWrite {
    pub fn commit(self) -> SummaryCommit {
        SummaryCommit {
            bytes: std::sync::Arc::new(self.bytes),
            offset: self.offset,
            length: self.length,
        }
    }
}
impl OwnedIngress {
    pub fn has_summaries(&self) -> bool {
        self.summaries.is_some()
    }
    pub fn configure_summaries(
        &mut self,
        trunk: &Trunk,
        calendar: SummaryCalendar,
        rx: &[u8],
    ) -> Result<(), Error> {
        if self
            .scope
            .definition(self.branch)?
            .summary_fields()
            .is_empty()
        {
            self.summaries = None;
            return Ok(());
        }
        let mut scope = trunk.trie().branches().select([self.branch]).summaries()?;
        if !rx.is_empty() {
            scope.restore(rx)?;
        }
        self.summaries = Some(SummaryIngress::new(scope, calendar));
        Ok(())
    }
    pub fn summaries_start(
        &mut self,
        trunk: &Trunk,
        time: &TimeTrie,
        rx: &[u8],
        footer: &[u8],
        length: u64,
    ) -> Result<Option<SummaryStart>, Error> {
        self.healthy()?;
        self.configure_summaries(trunk, time.summary_calendar()?, rx)?;
        let Some(summaries) = &self.summaries else {
            return Ok(None);
        };
        let bootstrap = if length == 0 {
            SummaryBox::new(summaries.scope.plan().layout().clone())?
                .commit()?
                .bytes
                .to_vec()
        } else {
            Vec::new()
        };
        let count = if footer.is_empty() {
            0
        } else {
            SummaryBoxReader::open_footer(footer, length)?.len() as u64
        };
        let first = time
            .summary_calendar()?
            .day(time.header().first)?
            .start
            .checked_add(count.checked_mul(SUMMARY_DAY_MS).ok_or(Error::Overflow)?)
            .ok_or(Error::Overflow)?;
        Ok(Some(SummaryStart { first, bootstrap }))
    }
    pub fn summary_restore_snapshot(&mut self, pack:&crate::SeedPackSnapshot)->Result<(),Error>{
        self.healthy()?;
        if let Some(summary)=&mut self.summaries {summary.restore_snapshot(&self.scope,pack)?;}
        Ok(())
    }
    pub fn summary_restore(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        self.healthy()?;
        if let Some(summary) = &mut self.summaries {
            summary.restore_pack(&self.scope, bytes)?;
        }
        Ok(())
    }
    /// A fresh PAC with already-published context avoids retransmitting RX/trie.
    pub fn summary_snapshot(
        &mut self,
        registry: &[u8],
        day: u64,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.healthy()?;
        let live = self
            .summaries
            .as_mut()
            .ok_or_else(|| error("no annotated summary fields"))?
            .day(day)?;
        if let Some(bytes) = live.published_snapshot(registry, SUMMARY_FILE)? {
            return Ok(Some(bytes));
        }
        let trie = trie_trunk_taxonomy::Trunk::open(registry).map_err(error)?;
        let (next, _, bytes) = live.snapshot_registered(&trie, SUMMARY_FILE)?;
        Ok((next.bytes() == registry).then_some(bytes))
    }
    pub fn summary_prepare(
        &mut self,
        trie: &trie_trunk_taxonomy::Trunk,
        day: u64,
        through: u64,
        footer: &[u8],
        length: u64,
        seal: bool,
    ) -> Result<SummaryWrite, Error> {
        self.healthy()?;
        let live = self
            .summaries
            .as_mut()
            .ok_or_else(|| error("no annotated summary fields"))?
            .day(day)?;
        if seal {
            if through < live.end() {
                return Err(error("summary day still accepts events"));
            }
            let mut copy = live.clone();
            copy.complete(copy.end())?;
            let (trie, document, projection, pack) = copy.seal_registered(trie, SUMMARY_FILE)?;
            let mut container = if footer.is_empty() {
                SummaryBox::new(projection.layout().clone())?
            } else {
                SummaryBox::open_footer(footer.to_vec(), length)?
            };
            container.append(pack)?;
            let write = container.write()?;
            Ok(SummaryWrite {
                trie,
                document,
                bytes: write.bytes().to_vec(),
                offset: write.offset(),
                length: write.length(),
            })
        } else {
            let (trie, document, bytes) = live.snapshot_registered(trie, SUMMARY_FILE)?;
            Ok(SummaryWrite {
                trie,
                document,
                bytes,
                offset: 0,
                length: 0,
            })
        }
    }
    pub fn summary_published(&mut self, day: u64) {
        if let Some(summary) = &mut self.summaries {
            summary.published(day);
        }
    }
}
