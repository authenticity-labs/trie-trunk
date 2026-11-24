//! Environment I/O only; shared Rust plans own offsets, bytes, and barriers.
use super::*;
use data_seeds::{
    MemoryBudget,
    replication::{ReceivedPack, ReplicaBatch},
    writer::{CommitId, QueueLimits, QueueProgress, Storage, WriteCursor},
};
use std::io;

#[link(wasm_import_module = "seed_files")]
unsafe extern "C" {
    fn read(file: u64, at: u64, data: *mut u8, length: usize) -> i32;
    fn write(file: u64, at: u64, data: *const u8, length: usize) -> i32;
    fn resize(file: u64, length: u64) -> i32;
    fn flush(file: u64) -> i32;
}
struct HostFiles;
impl Storage for HostFiles {
    fn write_at(&mut self, file: u64, at: u64, bytes: &[u8]) -> io::Result<usize> {
        let length = bytes.len().min(i32::MAX as usize);
        let result = unsafe { write(file, at, bytes.as_ptr(), length) };
        if result < 0 {
            Err(io::Error::other("host storage write failed"))
        } else {
            Ok(result as usize)
        }
    }
    fn resize(&mut self, file: u64, length: u64) -> io::Result<()> {
        if unsafe { resize(file, length) } == 0 {
            Ok(())
        } else {
            Err(io::Error::other("host storage resize failed"))
        }
    }
    fn flush(&mut self, file: u64) -> io::Result<()> {
        if unsafe { flush(file) } == 0 {
            Ok(())
        } else {
            Err(io::Error::other("host storage flush failed"))
        }
    }
}
enum Plan {
    Building(ReplicaBatch),
    Queued(WriteCursor),
}
#[derive(serde::Deserialize)]
struct Selection {
    generation: [u8; 16],
    retained: u64,
    sealed: u64,
    ranges: Vec<[u64; 2]>,
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection(replica: usize, time: usize) -> u32 {
    run(|r| {
        let selection: Selection = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let collection = get(&r.replicas, replica)?.collect(
            get(&r.times, time)?.clone(),
            selection.ranges.into_iter().map(|[start, end]| start..end).collect(),
            data_seeds::replication::Window {
                generation: selection.generation,
                retained: selection.retained,
                sealed: selection.sealed,
            },
        )?;
        add(&mut r.collections, collection)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection_accept(collection: usize, pack: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, pack)?;
        let leaf = get_mut(&mut r.collections, collection)?.accept(pack)?;
        json_value(&serde_json::to_value(leaf).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection_load(collection: usize, batch: usize, pack: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, pack)?.clone();
        let leaf = get_mut(&mut r.collections, collection)?.load(
            get_mut(&mut r.writes, batch)?.building()?, pack,
        )?;
        json_value(&serde_json::to_value(leaf).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection_finish(collection: usize) -> u32 {
    run(|r| {
        let count = get_mut(&mut r.collections, collection)?.finish()?;
        json_value(&serde_json::json!(count))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection_load_selected(collection: usize, batch: usize, pack: usize, store: usize, ordinal: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, pack)?.clone();
        let Store::Reader(reader) = get(&r.stores, store)? else { return Err(fail("expected a storage reader")); };
        let leaf = get_mut(&mut r.collections, collection)?.load_selected(
            get_mut(&mut r.writes, batch)?.building()?, pack, reader, ordinal)?;
        json_value(&serde_json::to_value(leaf).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
pub(super) struct PendingWrite {
    trunk: usize,
    plan: Option<Plan>,
}
impl PendingWrite {
    fn building(&mut self) -> Result<&mut ReplicaBatch, Error> {
        match self.plan.as_mut() {
            Some(Plan::Building(batch)) => Ok(batch),
            _ => Err(fail("write batch is no longer editable")),
        }
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_writer_open(memory: usize, batches: usize, operations: usize) -> u32 {
    run(|r| {
        let trunk = data_seeds::trunk::Trunk::open(std::mem::take(&mut r.input))?.writer(
            MemoryBudget::new(memory),
            QueueLimits {
                batches,
                operations,
            },
        )?;
        add(&mut r.trunks, trunk)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reconstruct(store: usize, length: u64, count: u64) -> u32 {
    run(|r| {
        let replica = match get(&r.stores, store)? {
            Store::Registered(scope) => scope.reconstruct(length, count)?,
            Store::Unbound(_) => Packs::reconstruct(length, count)?,
            Store::Reader(_) => return Err(fail("read-only storage selection")),
        };
        add(&mut r.replicas, replica)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_publication(replica: usize, time: usize) -> u32 {
    #[derive(serde::Deserialize)]
    struct Publication { generation: [u8; 16], placements: Vec<[String; 6]>, file: Option<u64> }
    run(|r| {
        let input: Publication = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let placements = input.placements.into_iter().map(|values| {
            let mut parsed = [0; 6];
            for (out, text) in parsed.iter_mut().zip(values) {
                *out = text.parse::<u64>().map_err(|_| fail("invalid Time placement integer"))?;
            }
            let [hour, start, end, packs, beds, count] = parsed;
            Ok(data_seeds::TimeLeaf { hour, start, end, packs, beds, count })
        }).collect::<Result<Vec<_>, Error>>()?;
        let replica = get(&r.replicas, replica)?;
        let time = get(&r.times, time)?;
        let patches = if let Some(file) = input.file {
            replica.publication_with(time, input.generation, &placements, |mut at, mut out| {
                while !out.is_empty() {
                    let n = unsafe { read(file, at, out.as_mut_ptr(), out.len()) };
                    if n <= 0 || n as usize > out.len() { return Err(fail("short Time publication read")); }
                    at += n as u64;out = &mut out[n as usize..];
                }
                Ok(())
            })?
        } else { Some(replica.publication(time, input.generation, &placements)?) };
        json_value(&serde_json::to_value(patches).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_batch(
    replica: usize,
    trunk: usize,
    file: u64,
    operations: usize,
) -> u32 {
    run(|r| {
        let memory = get_mut(&mut r.trunks, trunk)?.commits()?.memory().clone();
        let batch = get(&r.replicas, replica)?.batch(file, &memory, operations);
        add(
            &mut r.writes,
            PendingWrite {
                trunk,
                plan: Some(Plan::Building(batch)),
            },
        )
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_load(batch: usize, at: u64) -> u32 {
    run(|r| {
        let owner = Arc::new(std::mem::take(&mut r.input));
        let length = owner.len();
        let received = ReceivedPack::bounded(owner, 0..length, length)?;
        get_mut(&mut r.writes, batch)?
            .building()?
            .load(at, received)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_footer(batch: usize) -> u32 {
    run(|r| {
        get_mut(&mut r.writes, batch)?.building()?.footer()?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_pack(batch: usize, at: u64, pack: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, pack)?.clone();
        get_mut(&mut r.writes, batch)?.building()?.pack(at, pack)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_pack_selected(batch: usize, store: usize, ordinal: usize, pack: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, pack)?.clone();
        let Store::Reader(reader) = get(&r.stores, store)? else { return Err(fail("expected a storage reader")); };
        get_mut(&mut r.writes, batch)?.building()?.pack_selected(reader, ordinal, pack)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_write_submit(batch: usize) -> u32 {
    run(|r| {
        let entry = get_mut(&mut r.writes, batch)?;
        let queue = get_mut(&mut r.trunks, entry.trunk)?.commits()?;
        let cursor = match entry
            .plan
            .take()
            .ok_or_else(|| fail("write already submitted"))?
        {
            Plan::Building(batch) => batch.start()?,
            Plan::Queued(cursor) => cursor,
        };
        match queue.submit(cursor) {
            Ok(id) => {
                json_value(&serde_json::json!(id.value().to_string()))?;
                Ok(0)
            }
            Err(rejected) => {
                let message = rejected.reason.to_string();
                entry.plan = Some(Plan::Queued(rejected.into_cursor()));
                Err(fail(message))
            }
        }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_commit_pending(trunk: usize) -> u32 {
    run(|r| Ok(get_mut(&mut r.trunks, trunk)?.commits()?.pending()))
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_commit_poll(trunk: usize, calls: usize, bytes: usize) -> u32 {
    run(|r| {
        let queue = get_mut(&mut r.trunks, trunk)?.commits()?;
        let result = match queue
            .poll(&mut HostFiles, calls, bytes)
            .map_err(|e| fail(e.to_string()))?
        {
            QueueProgress::Idle => serde_json::json!({"state":"idle"}),
            QueueProgress::Ready(id) => {
                serde_json::json!({"state":"ready","id":id.value().to_string()})
            }
            QueueProgress::Writing { id, progress } => {
                serde_json::json!({"state":"writing","id":id.value().to_string(),"calls":progress.calls,"bytes":progress.bytes})
            }
        };
        json_value(&result)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_commit_published(trunk: usize, id: u64) -> u32 {
    run(|r| {
        get_mut(&mut r.trunks, trunk)?
            .commits()?
            .published(CommitId::from_raw(id))
            .map_err(|e| fail(e.to_string()))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_commit_failed(trunk: usize, id: u64) -> u32 {
    run(|r| {
        get_mut(&mut r.trunks, trunk)?
            .commits()?
            .publication_failed(CommitId::from_raw(id))
            .map_err(|e| fail(e.to_string()))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_commit_recovered(trunk: usize) -> u32 {
    run(|r| {
        let ids = get_mut(&mut r.trunks, trunk)?
            .commits()?
            .recovered()
            .map_err(|e| fail(e.to_string()))?;
        json_value(&serde_json::json!(
            ids.into_iter()
                .map(|id| id.value().to_string())
                .collect::<Vec<_>>()
        ))?;
        Ok(0)
    })
}

impl data_seeds::reader::Storage for HostFiles {
    fn read_at(&self, file: u64, at: u64, target: &mut [u8]) -> io::Result<usize> {
        let length = target.len().min(i32::MAX as usize);
        let result = unsafe { read(file, at, target.as_mut_ptr(), length) };
        if result < 0 { Err(io::Error::other("host storage read failed")) } else { Ok(result as usize) }
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_resident_budget(bytes: usize) -> u32 {
    run(|r| {
        if r.resident_memory.is_some() || bytes == 0 { return Err(fail("resident budget must be configured before opening storage")); }
        r.resident_memory = Some(MemoryBudget::new(bytes)); Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader(store: usize, file: u64, length: u64, maximum: usize) -> u32 {
    run(|r| {
        let raw: Vec<String> = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let addresses = raw.into_iter().map(|s| s.parse::<u64>().map_err(|_| fail("invalid Pack address"))).collect::<Result<Vec<_>,_>>()?;
        let budget = r.resident_memory.get_or_insert_with(|| MemoryBudget::new(256 * 1024 * 1024)).clone();
        let reader = get_mut(&mut r.stores, store)?.registered()?.reader(Arc::new(HostFiles),
            data_seeds::reader::Selection { file, length, addresses }, maximum, budget)?;
        add(&mut r.stores, Store::Reader(reader))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_pack(store: usize, ordinal: usize) -> u32 {
    run(|r| {
        let Store::Reader(reader) = get(&r.stores, store)? else { return Err(fail("expected a storage reader")); };
        let pack = reader.pack(ordinal)?;
        add(&mut r.packs, pack)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_prepare(store: usize, first: usize, count: usize) -> u32 {
    run(|r| {
        let Store::Reader(reader) = get(&r.stores, store)? else { return Err(fail("expected a storage reader")); };
        let next = reader.prepare(first, count)?;
        json_value(&serde_json::json!({"next":next,"done":next == data_seeds::PackSource::len(reader),"bytes":reader.metadata_bytes()?}))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_retain(store: usize, ordinal: usize, pack: usize) -> u32 {
    run(|r| {
        let Store::Reader(reader) = get(&r.stores, store)? else { return Err(fail("expected a storage reader")); };
        reader.retain(ordinal, get(&r.packs, pack)?)?;
        Ok(0)
    })
}

#[derive(serde::Deserialize)]
struct IndexSelection {
    hours: Vec<String>,
    leaves: Vec<usize>,
    space: String,
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_index(store: usize, file: u64, length: u64, time: usize) -> u32 {
    run(|r| {
        let selection: IndexSelection = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let hours = selection.hours.into_iter().map(|s| s.parse::<u64>().map_err(|_| fail("invalid index hour"))).collect::<Result<Vec<_>,_>>()?;
        let leaves = selection.leaves.into_iter().map(|id| get(&r.leaves,id).cloned()).collect::<Result<Vec<_>,_>>()?;
        let time = get(&r.times,time)?.clone();
        let space = match selection.space.as_str() {
            "packs" => data_seeds::reader::AddressSpace::Packs,
            "beds" => data_seeds::reader::AddressSpace::Beds,
            _ => return Err(fail("invalid index address space")),
        };
        let Store::Reader(reader) = get_mut(&mut r.stores,store)? else { return Err(fail("expected a storage reader")); };
        reader.index(data_seeds::reader::IndexSelection { file, length, time, hours, space, leaves })?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_range(store: usize, from: u64, to: u64) -> u32 {
    run(|r| {
        let Store::Reader(reader) = get(&r.stores,store)? else { return Err(fail("expected a storage reader")); };
        let selected=reader.range(from,to)?;
        add(&mut r.stores,Store::Reader(selected))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_reader_ranges(store: usize) -> u32 {
    run(|r| {
        let values: Vec<[String; 2]> = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let ranges = values.into_iter().map(|[a,z]| {
            Ok(a.parse::<u64>().map_err(|_| fail("invalid period start"))?
                ..z.parse::<u64>().map_err(|_| fail("invalid period end"))?)
        }).collect::<Result<Vec<_>, Error>>()?;
        let Store::Reader(reader) = get(&r.stores,store)? else { return Err(fail("expected a storage reader")); };
        let selected = reader.ranges(ranges)?;
        add(&mut r.stores, Store::Reader(selected))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_reads() -> u32 {
    run(|r| {
        let budget=r.resident_memory.get_or_insert_with(||MemoryBudget::new(256*1024*1024)).clone();
        let owner=r.readiness.get_or_insert_with(||data_seeds::replication::Readiness::new(budget)).clone();
        add(&mut r.read_owners,owner)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_ready(owner:usize,op:u32,address:u64,value:usize,claim:u64)->u32{
    run(|r|{
        let source=std::str::from_utf8(&r.input).map_err(|_|fail("invalid readiness source"))?;
        let owner=get(&r.read_owners,owner)?;
        use data_seeds::replication::ReadState;
        let state=|s| match s {Some(ReadState::Queued)=>1,Some(ReadState::Ready)=>2,None=>0};
        match op {
            0=>Ok(state(Some(owner.queue(source,address)?))),
            1=>Ok(usize::from(owner.admit(source,address,value,claim)?)),
            2=>{owner.load(source,address,get(&r.packs,value)?.clone(),claim)?;Ok(0)},
            3=>Ok(state(owner.state(source,address)?)),
            4=>{owner.published(source,&[address])?;Ok(0)},
            5=>{owner.cancel(source,claim)?;Ok(0)},
            6=>{owner.clear(source)?;Ok(0)},
            7=>owner.used(),
            8=>{let pack=owner.get(source,address)?.ok_or_else(||fail("Pack is Queued"))?;add(&mut r.packs,pack)},
            9=>{let Store::Reader(reader)=get(&r.stores,value)? else{return Err(fail("expected a reader"))};reader.ready(owner,source)?;Ok(0)},
            10=>{owner.submitted(source,address)?;Ok(0)},
            11=>{owner.discard(source,address,claim)?;Ok(0)},
            12=>Ok(owner.coverage(source,address,u8::try_from(value).map_err(|_|fail("invalid coverage flags"))?)? as usize),
            _=>Err(fail("unknown readiness operation")),
        }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_replica_retain(id:usize)->u32{run(|r|{let value=get(&r.replicas,id)?.clone();add(&mut r.replicas,value)})}
#[unsafe(no_mangle)]
pub extern "C" fn seed_time_retain(id:usize)->u32{run(|r|{let value=get(&r.times,id)?.clone();add(&mut r.times,value)})}
#[unsafe(no_mangle)]
pub extern "C" fn seed_collection_peek(id:usize)->u32{run(|r|{let leaf=get_mut(&mut r.collections,id)?.peek()?;json_value(&serde_json::to_value(leaf).map_err(|e|fail(e.to_string()))?)?;Ok(0)})}
