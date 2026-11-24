//! Summary publication uses the existing Box replacement tail and node writer.
//! Its footer already includes every referenced row Definition and version span.
use crate::{
    Error, MemoryBudget, error,
    writer::{Buffer, FileId, WriteBatch, WriteCursor},
};
use pil_codec::r#box::{Box as Container, Commit};

/// A prepared tail, not a durable publication. Hold the writer lease until the
/// scheduler is Ready and the owner's availability metadata has been published.
/// The owner retains its previous footer snapshot across failure and recovery.
pub struct SummaryWrite<'a> {
    commit: Commit<'a>,
    footer: usize,
}
impl<'a> SummaryWrite<'a> {
    pub(super) fn new(commit: Commit<'a>) -> Result<Self, Error> {
        let footer = if commit.bytes.is_empty() {
            0
        } else {
            let range = Container::footer_range(commit.bytes, commit.length)?;
            usize::try_from(
                range
                    .start
                    .checked_sub(commit.offset)
                    .ok_or(Error::Overflow)?,
            )
            .map_err(|_| Error::Overflow)?
        };
        if footer > commit.bytes.len() {
            return Err(error("summary footer outside commit tail"));
        }
        Ok(Self { commit, footer })
    }
    pub fn offset(&self) -> u64 {
        self.commit.offset
    }
    pub fn length(&self) -> u64 {
        self.commit.length
    }
    pub fn is_empty(&self) -> bool {
        self.commit.bytes.is_empty()
    }
    pub fn bytes(&self) -> &[u8] {
        self.commit.bytes
    }
    /// New reader snapshot; an empty commit leaves the previous footer intact.
    /// Only publish this snapshot after scheduler Ready and owner publication.
    pub fn footer(&self) -> Option<&[u8]> {
        (!self.is_empty()).then(|| &self.commit.bytes[self.footer..])
    }
    /// One admitted copy of the replacement tail, shared by the payload and
    /// footer operations. Historical Pack bytes are never loaded or copied.
    /// Admission failure leaves this borrow retryable. Queue rejection returns
    /// the same WriteCursor through the scheduler's existing retry mechanism.
    pub fn start(
        &self,
        file: FileId,
        previous_length: u64,
        budget: &MemoryBudget,
    ) -> Result<WriteCursor, Error> {
        if self.is_empty() {
            if previous_length != self.length() {
                return Err(error(
                    "unchanged summary commit requires its published length",
                ));
            }
            return WriteBatch::new(0).start().map_err(|e| error(e.to_string()));
        }
        if previous_length < self.offset() {
            return Err(error("summary append starts beyond the published file"));
        }
        let buffer = Buffer::copy(self.commit.bytes, budget)?;
        let mut batch = WriteBatch::new(5);
        batch
            .write(file, self.offset(), buffer.clone(), 0..self.footer)
            .and_then(|b| b.barrier())
            .map_err(|e| error(e.to_string()))?;
        batch
            .write(
                file,
                self.offset() + self.footer as u64,
                buffer,
                self.footer..self.commit.bytes.len(),
            )
            .map_err(|e| error(e.to_string()))?;
        // Appends grow through positional writes. Only a shorter replacement
        // needs an explicit truncate so its footer remains at physical EOF.
        if self.length() < previous_length {
            batch
                .resize(file, self.length())
                .map_err(|e| error(e.to_string()))?;
        }
        batch.start().map_err(|e| error(e.to_string()))
    }
}
