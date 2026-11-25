//! UUID index publication for the ordinary durable Roots lane.
use crate::{
    Result,
    ingress::{atomic, file, suffix},
};
use data_seeds::{
    SeedDefinition, TIME_HOUR_MS, TimeTrie,
    registration::{Packs, ingress::publication::Publication},
};
use std::{
    collections::{BTreeMap, HashMap},
    os::unix::fs::FileExt,
    path::Path,
};
use trunk_uuid_index::collection::{Binding, Index, MAX_REV, Posting};

type Key = ([u8; 16], u32);
pub(crate) struct Pods {
    index: Index,
    pending: HashMap<Key, u64>,
}
impl Pods {
    pub fn open(prefix: &Path) -> Result<Self> {
        let path = suffix(prefix, ".indx");
        if suffix(prefix, ".time").try_exists()? && !path.try_exists()? {
            return Err("Published object collection is missing its UUID index".into());
        }
        Ok(Self {
            index: Index::open(file(&path)?)?,
            pending: Default::default(),
        })
    }

    pub fn pending<'a>(
        &self,
        definition: &SeedDefinition,
        pills: impl Iterator<Item = (u64, &'a [u8])>,
    ) -> Result<HashMap<Key, u64>> {
        let snap = self.index.snapshot();
        let mut keys = HashMap::new();
        for (time, pill) in pills {
            let key = definition
                .inspect_revision(pill)
                .map_err(crate::InputError)?;
            if key.1 > MAX_REV {
                return Err(crate::registration::invalid(format!(
                    "Object revision exceeds {MAX_REV}"
                ))
                .into());
            }
            if self.pending.contains_key(&key)
                || keys
                    .insert(key, time / TIME_HOUR_MS * TIME_HOUR_MS)
                    .is_some()
            {
                return Err(crate::registration::invalid("Duplicate object revision").into());
            }
            for (source, _, _) in snap.sources() {
                if snap.get(source, key.0, key.1)?.is_some() {
                    return Err(
                        crate::registration::invalid("Object revision already published").into(),
                    );
                }
            }
        }
        Ok(keys)
    }
    pub fn admit(&mut self, keys: HashMap<Key, u64>) {
        self.pending.extend(keys);
    }

    pub fn publish(&mut self, prefix: &Path, scope: &Packs, plan: &Publication) -> Result<()> {
        let time = TimeTrie::open(plan.time.clone())?;
        let leaf = time
            .leaf(plan.info.hour)?
            .ok_or("Missing object publication hour")?;
        let elapsed = plan
            .info
            .hour
            .checked_sub(time.header().origin)
            .ok_or("Object hour precedes its time index")?;
        let hour = u32::try_from(elapsed / TIME_HOUR_MS)?;
        let snapshot = self.index.snapshot();
        for (binding, _, end) in snapshot.sources() {
            if binding.generation != time.header().generation || end > leaf.end {
                return Err(
                    "Checkpoint does not match the object index generation or watermark".into(),
                );
            }
        }
        let pack = plan.open_pack(scope)?;
        let mut groups = BTreeMap::<u32, Vec<Posting>>::new();
        for i in 0..pack.len() {
            let (object, revision) = pack.revision_key(i)?;
            let minor = pack.definition_at(i)?.version().1;
            groups.entry(minor).or_default().push(Posting {
                object,
                revision,
                hour,
                record: (i + 1).try_into()?,
            });
        }
        let output = file(&suffix(prefix, ".pods"))?;
        output.write_all_at(&plan.tail, plan.info.offset)?;
        output.set_len(plan.info.length)?;
        output.sync_all()?;
        for (minor, posts) in groups {
            let binding = Binding {
                generation: time.header().generation,
                minor,
            };
            let prior = snapshot
                .sources()
                .find(|(k, _, _)| *k == binding)
                .map_or(0, |(_, _, n)| n);
            if prior == leaf.end {
                for post in posts {
                    if snapshot.get(binding, post.object, post.revision)? != Some(post.address()) {
                        return Err("Replayed object posting differs from its checkpoint".into());
                    }
                }
            } else {
                self.index.append(binding, 4, prior, leaf.end, &posts)?;
            }
        }
        atomic(&suffix(prefix, ".time"), &plan.time)?;
        self.pending.retain(|_, h| *h != plan.info.hour);
        Ok(())
    }
}
