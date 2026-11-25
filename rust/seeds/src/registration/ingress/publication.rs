//! Existing Seed commit preparation and recovery, shared by every native owner.
use super::SealedHour;
use crate::{Error, error};
use std::sync::Arc;
#[repr(C)]
#[derive(Default)]
pub struct PublicationPlan {
    pub offset: u64,
    pub length: u64,
    pub beds: u64,
    pub index: u64,
    pub hour: u64,
    pub count: u64,
}
pub struct Publication {
    pub tail: Arc<Vec<u8>>,
    pub time: Arc<Vec<u8>>,
    pub index: Arc<Vec<u8>>,
    pub info: PublicationPlan,
}
impl Publication {
    /// The append tail starts with the sealed plain PAC, followed by the BOX footer.
    pub fn open_pack(&self, scope: &crate::registration::Packs) -> Result<Arc<crate::SeedPackSnapshot>, Error> {
        let n = usize::try_from(pil_codec::pac_envelope::Plain::read_prefix(&self.tail)?.size).map_err(|_| Error::Overflow)?;
        let pack = scope.open_pack(self.tail.clone(), 0..n, self.tail.len())?;
        if pack.len() as u64 != self.info.count {return Err(error("publication Pack count mismatch"));}
        Ok(pack)
    }
    pub fn prepare(
        sealed: &SealedHour,
        time: &[u8],
        footer: &[u8],
        length: u64,
        compression: Option<crate::Compression>,
    ) -> Result<Self, Error> {

        let mut writer = crate::packs::Writer::resume(footer.to_vec(), length)?;
        let beds = writer.next_beds_address();

        if let Some(compression) = compression {
            writer.compression(compression)?;
        }
        writer.pack(sealed.pack.as_slice())?;
        Self::finish(writer,sealed.hour,sealed.count,sealed.index.clone(),time,beds)
    }
    pub(super) fn finish(mut writer:crate::packs::Writer<'_>,hour:u64,count:usize,index:Vec<u8>,
        time:&[u8],beds:u64) -> Result<Self,Error> {
        let timeline = crate::TimeTrie::open(Arc::new(time.to_vec()))?;
        let commit = writer.commit()?;
        let offset = commit.offset;
        let next = timeline.publish(
            hour + crate::TIME_HOUR_MS,
            Some(crate::TimeLeaf {
                hour: hour,
                start: timeline.header().tail,
                end: timeline
                    .header()
                    .tail
                    .checked_add(1 + count as u64)
                    .ok_or(Error::Overflow)?,
                packs: offset,
                beds,
                count: count as u64,
            }),
        )?;
        Ok(Publication {
            tail: Arc::new(commit.bytes.to_vec()),
            time: Arc::new(next.bytes().to_vec()),
            index: Arc::new(index),
            info: PublicationPlan {
                offset: commit.offset,
                length: commit.length,
                beds,
                index: timeline
                    .header()
                    .tail
                    .checked_mul(6)
                    .ok_or(Error::Overflow)?,
                hour: hour,
                count: count as u64,
            },
        })
    }
    pub fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value as D;
        let data = D::Array(vec![
            D::Integer(self.info.offset.into()),
            D::Integer(self.info.length.into()),
            D::Integer(self.info.beds.into()),
            D::Integer(self.info.index.into()),
            D::Integer(self.info.hour.into()),
            D::Integer(self.info.count.into()),
            D::Bytes(self.tail.as_ref().clone()),
            D::Bytes(self.index.as_ref().clone()),
            D::Bytes(self.time.as_ref().clone()),
        ]);
        let body = pil_codec::object::encode_value(&data)?;
        let header = crate::registration::ingress::recovery::checkpoint(&body)?;
        let mut bytes = Vec::with_capacity(header.len() + body.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }
    pub fn read(raw: &[u8]) -> Result<Self, Error> {
        let (branch, body) = crate::registration::ingress::recovery::read(raw, 256 << 20)?;
        if branch != 0 {
            return Err(error("expected commit checkpoint"));
        }
        let ciborium::Value::Array(mut fields) = pil_codec::object::decode_value(body)? else {
            return Err(error("invalid commit checkpoint"));
        };
        if fields.len() != 9 {
            return Err(error("invalid commit checkpoint fields"));
        }
        let mut number = |i: usize| -> Result<u64, Error> {
            match std::mem::replace(&mut fields[i], ciborium::Value::Null) {
                ciborium::Value::Integer(n) => u64::try_from(n).map_err(|_| Error::Overflow),
                _ => Err(error("invalid commit coordinate")),
            }
        };
        let info = PublicationPlan {
            offset: number(0)?,
            length: number(1)?,
            beds: number(2)?,
            index: number(3)?,
            hour: number(4)?,
            count: number(5)?,
        };
        let mut bytes = |i: usize| -> Result<Vec<u8>, Error> {
            match std::mem::replace(&mut fields[i], ciborium::Value::Null) {
                ciborium::Value::Bytes(b) => Ok(b),
                _ => Err(error("invalid commit bytes")),
            }
        };
        let plan = Publication {
            info,
            tail: Arc::new(bytes(6)?),
            index: Arc::new(bytes(7)?),
            time: Arc::new(bytes(8)?),
        };
        let time = crate::TimeTrie::open(plan.time.clone())?;
        let leaf = time
            .leaf(plan.info.hour)?
            .ok_or_else(|| error("commit hour outside Time"))?;
        if leaf.packs != plan.info.offset
            || leaf.beds != plan.info.beds
            || leaf.count != plan.info.count
            || leaf.start.checked_mul(6) != Some(plan.info.index)
            || plan.info.offset.checked_add(plan.tail.len() as u64) != Some(plan.info.length)
            || leaf.count.checked_add(1).and_then(|n| n.checked_mul(6))
                != Some(plan.index.len() as u64)
        {
            return Err(error("inconsistent commit checkpoint"));
        }
        Ok(plan)
    }
}
