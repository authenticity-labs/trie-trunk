//! Prior publication-gated ownership versus Ready handoff, using identical
//! registered bytes and the current query engine. Real local files, warm OS
//! cache; not HTTP, UI, or an atomic catalog-publication benchmark.
use data_seeds::reader::{AddressSpace, IndexSelection, Selection, Storage};
use data_seeds::registration::{Packs, Trunk};
use data_seeds::replication::{Readiness, ReceivedPack, Replica};
use data_seeds::{Compare, LeafTrie, MemoryBudget, Predicate, Query, SeedPackSnapshot, TimeTrie};
use std::{
    fs::{File, OpenOptions},
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAXIMUM: usize = 64 << 20;

struct Files {
    data: File,
    index: File,
    read_bytes: AtomicUsize,
    pack_read_bytes: AtomicUsize,
    index_read_bytes: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
    flushes: AtomicUsize,
}
impl Storage for Files {
    fn read_at(&self, id: u64, at: u64, out: &mut [u8]) -> io::Result<usize> {
        let file = if id == 1 { &self.data } else { &self.index };
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(file, out, at)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(file, out, at)?;
        if id == 1 {
            self.pack_read_bytes.fetch_add(n, Ordering::Relaxed);
        } else {
            self.index_read_bytes.fetch_add(n, Ordering::Relaxed);
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}
struct Writer(Arc<Files>);
impl data_seeds::writer::Storage for Writer {
    fn write_at(&mut self, id: u64, at: u64, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(id, 1);
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::write_at(&self.0.data, bytes, at)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_write(&self.0.data, bytes, at)?;
        self.0.writes.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
    fn resize(&mut self, _: u64, size: u64) -> io::Result<()> {
        self.0.data.set_len(size)
    }
    fn flush(&mut self, _: u64) -> io::Result<()> {
        self.0.flushes.fetch_add(1, Ordering::Relaxed);
        self.0.data.sync_all()
    }
}
struct Fixture {
    scope: Packs,
    time: Arc<TimeTrie>,
    roots: Vec<Arc<LeafTrie>>,
    hours: Vec<u64>,
    addresses: Vec<u64>,
    input: Vec<Vec<u8>>,
    length: u64,
    index_path: PathBuf,
    rows: usize,
    expected: Vec<u8>,
}
impl Fixture {
    fn open(path: PathBuf) -> Result<Self> {
        let info: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path.join("fixture.json"))?)?;
        let nums = |name: &str| {
            info[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_u64().unwrap())
                .collect::<Vec<_>>()
        };
        let mut trunk = Trunk::open(std::fs::read(path.join("trunk.trie"))?)?;
        trunk.prescribe_file("indexed.rx", &std::fs::read(path.join("indexed.rx"))?)?;
        let branches = nums("branches");
        let leaves = std::fs::read(path.join("sample.leaf"))?;
        let roots = branches
            .iter()
            .map(|&b| {
                Ok(trunk
                    .trie()
                    .branches()
                    .select([b as u16])
                    .leaves()?
                    .read(&leaves)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let scope = trunk
            .trie()
            .branches()
            .select(branches.into_iter().map(|b| b as u16))
            .packs()?;
        let data = std::fs::read(path.join("indexed.beds"))?;
        let addresses = nums("addresses");
        let input = addresses
            .iter()
            .map(|&a| {
                let bytes = &data[a as usize..];
                let size =
                    usize::try_from(pil_codec::pac_envelope::Plain::read_prefix(bytes)?.size)?;
                Ok(bytes[..size].to_vec())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            scope,
            roots,
            hours: nums("hours"),
            addresses,
            input,
            length: data.len() as u64,
            time: Arc::new(TimeTrie::open(Arc::new(std::fs::read(
                path.join("indexed.time"),
            )?))?),
            index_path: path.join("indexed.indx"),
            expected: data,
            rows: info["count"].as_u64().unwrap() as usize * 2,
        })
    }
    fn reader(
        &self,
        files: Arc<Files>,
        budget: MemoryBudget,
        ready: Option<&Readiness>,
    ) -> Result<data_seeds::reader::Reader> {
        let mut reader = self.scope.reader(
            files.clone(),
            Selection {
                file: 1,
                length: self.length,
                addresses: self.addresses.clone(),
            },
            MAXIMUM,
            budget,
        )?;
        reader.index(IndexSelection {
            file: 2,
            length: files.index.metadata()?.len(),
            time: self.time.clone(),
            hours: self.hours.clone(),
            space: AddressSpace::Beds,
            leaves: self.roots.clone(),
        })?;
        if let Some(owner) = ready {
            reader.ready(owner, "fixture")?;
        }
        Ok(reader.range(self.hours[0], self.hours.last().unwrap() + 3_600_000)?)
    }
}
fn publish(
    replica: &Replica,
    addresses: &[u64],
    packs: &[Arc<SeedPackSnapshot>],
    budget: &MemoryBudget,
    files: Arc<Files>,
) -> Result<()> {
    let mut batch = replica.batch(1, budget, 16);
    for (&address, pack) in addresses.iter().zip(packs) {
        batch.pack(address, pack.clone())?;
    }
    batch.footer()?;
    let mut cursor = batch.start()?;
    let mut writer = Writer(files);
    while !cursor.poll(&mut writer, 512, 4 << 20)?.ready {}
    Ok(())
}
fn plan(name: &str) -> Query {
    let q = Query::new().select(["time", "region", "sequence", "duration"]);
    match name {
        "first_100" => q.limit(100),
        "leaf_first_100" => q
            .filter(Predicate::compare("region", Compare::Eq, "east"))
            .limit(100),
        "zero_matches" => q.filter(Predicate::compare("sequence", Compare::Gt, 999999u64)),
        "duration_matches" => q.filter(Predicate::compare("duration", Compare::Ge, 10000u64)),
        "all_records" => q,
        _ => unreachable!(),
    }
}
fn fingerprint(objects: &[data_seeds::Object], hash: &mut u64) {
    for object in objects {
        for (_, value) in object {
            match value {
                ciborium::Value::Integer(v) => {
                    *hash = hash.wrapping_mul(1099511628211) ^ i128::from(*v) as u64;
                }
                ciborium::Value::Text(s) => {
                    for b in s.bytes() {
                        *hash = hash.wrapping_mul(1099511628211) ^ u64::from(b);
                    }
                }
                _ => panic!("unexpected projected type"),
            }
        }
    }
}
fn sample(
    f: &Fixture,
    path: &std::path::Path,
    mode: &str,
    workload: &str,
) -> Result<serde_json::Value> {
    let files = Arc::new(Files {
        data: OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?,
        index: File::open(&f.index_path)?,
        read_bytes: AtomicUsize::new(0),
        pack_read_bytes: AtomicUsize::new(0),
        index_read_bytes: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        flushes: AtomicUsize::new(0),
    });
    let budget = MemoryBudget::new(256 << 20);
    let ready = Readiness::new(budget.clone());
    let replica = data_seeds::Packs::reconstruct(f.length, f.input.len() as u64)?;
    let start = Instant::now();
    let mut received = Vec::new();
    for (i, bytes) in f.input.iter().enumerate() {
        if mode == "ready" {
            assert!(ready.admit("fixture", f.addresses[i], MAXIMUM * 2, 1)?);
        }
        // Received allocation, CRC verification and Pack opening are timed on
        // BOTH sides. These are reader inputs, not pre-encoded encoder inputs.
        let incoming = Arc::new(bytes.clone());
        let pack = ReceivedPack::bounded(incoming, 0..bytes.len(), MAXIMUM)?
            .open_registered_for_write(&f.scope, MAXIMUM)?;
        if mode == "ready" {
            ready.load("fixture", f.addresses[i], pack, 1)?;
            received.push(ready.get("fixture", f.addresses[i])?.unwrap());
        } else {
            received.push(pack);
        }
    }
    let collect_ms = start.elapsed().as_secs_f64() * 1000.;
    let mut write_ms = 0.;
    if mode == "publication_gated" {
        let at = Instant::now();
        publish(&replica, &f.addresses, &received, &budget, files.clone())?;
        write_ms = at.elapsed().as_secs_f64() * 1000.;
        received.clear();
    }
    let read_start = Instant::now();
    let reader = f.reader(
        files.clone(),
        budget.clone(),
        (mode == "ready").then_some(&ready),
    )?;
    let mut query = reader.query(plan(workload))?;
    let mut rows = 0;
    let mut checksum = 14695981039346656037u64;
    let mut first_ms = None;
    let mut sampled_memory = budget.used();
    loop {
        let batch = query.fetch(100)?;
        sampled_memory = sampled_memory.max(budget.used());
        let objects = batch.objects()?;
        if first_ms.is_none() {
            first_ms = Some(start.elapsed().as_secs_f64() * 1000.);
        }
        rows += objects.len();
        fingerprint(&objects, &mut checksum);
        if batch.is_empty() {
            break;
        }
    }
    let read_ms = read_start.elapsed().as_secs_f64() * 1000.;
    let query_done_ms = start.elapsed().as_secs_f64() * 1000.;
    let scanned = query.scanned;
    drop(query);
    drop(reader);
    if mode == "ready" {
        let at = Instant::now();
        publish(&replica, &f.addresses, &received, &budget, files.clone())?;
        write_ms = at.elapsed().as_secs_f64() * 1000.;
        ready.published("fixture", &f.addresses)?;
    }
    let all_done_ms = start.elapsed().as_secs_f64() * 1000.;
    received.clear();
    let cached_bytes = budget.used();
    ready.clear("fixture")?;
    assert_eq!(budget.used(), 0);
    assert_eq!(files.data.metadata()?.len(), f.length);
    assert_eq!(
        std::fs::read(path)?,
        f.expected,
        "publication bytes changed"
    );
    if mode == "ready" {
        assert_eq!(files.pack_read_bytes.load(Ordering::Relaxed), 0);
    }
    let result = serde_json::json!({"collect_ms":collect_ms,"write_ms":write_ms,"reader_ms":read_ms,"first_page_ms":first_ms.unwrap(),"query_done_ms":query_done_ms,"query_and_file_barriers_ms":all_done_ms,
        "returned":rows,"scanned":scanned,"checksum":checksum.to_string(),"read_bytes":files.read_bytes.load(Ordering::Relaxed),"read_calls":files.reads.load(Ordering::Relaxed),"pack_read_bytes":files.pack_read_bytes.load(Ordering::Relaxed),"index_read_bytes":files.index_read_bytes.load(Ordering::Relaxed),
        "written_bytes":files.writes.load(Ordering::Relaxed),"flushes":files.flushes.load(Ordering::Relaxed),"sampled_charged_bytes":sampled_memory,"cached_after_query_bytes":cached_bytes});
    drop(files);
    std::fs::remove_file(path)?;
    Ok(result)
}
fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/registered-indexed");
    let f = Fixture::open(root)?;
    let samples = std::env::args()
        .nth(1)
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(40);
    let base = std::env::temp_dir().join(format!("trunk-reader-comparison-{}", std::process::id()));
    std::fs::create_dir(&base)?;
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(base.clone());
    let mut report = serde_json::Map::new();
    for workload in [
        "first_100",
        "leaf_first_100",
        "zero_matches",
        "duration_matches",
        "all_records",
    ] {
        let mut results = [Vec::new(), Vec::new()];
        for iteration in 0..samples + 5 {
            let mut checks = Vec::new();
            for mode in if iteration % 2 == 0 { [0, 1] } else { [1, 0] } {
                let name = ["publication_gated", "ready"][mode];
                let value = sample(
                    &f,
                    &base.join(format!("{workload}-{iteration}-{mode}.beds")),
                    name,
                    workload,
                )?;
                checks.push((
                    value["returned"].clone(),
                    value["scanned"].clone(),
                    value["checksum"].clone(),
                    value["written_bytes"].clone(),
                    value["flushes"].clone(),
                ));
                if iteration >= 5 {
                    results[mode].push(value);
                }
            }
            assert_eq!(checks[0], checks[1], "different query or publication work");
        }
        let mut modes = serde_json::Map::new();
        for (name, values) in ["publication_gated", "ready"].into_iter().zip(results) {
            let mut stats = serde_json::Map::new();
            for key in [
                "collect_ms",
                "reader_ms",
                "write_ms",
                "first_page_ms",
                "query_done_ms",
                "query_and_file_barriers_ms",
                "read_bytes",
                "pack_read_bytes",
                "index_read_bytes",
                "read_calls",
                "written_bytes",
                "flushes",
                "sampled_charged_bytes",
                "cached_after_query_bytes",
            ] {
                let mut series = values
                    .iter()
                    .map(|s| s[key].as_f64().unwrap())
                    .collect::<Vec<_>>();
                series.sort_by(f64::total_cmp);
                stats.insert(key.into(),serde_json::json!({"median":series[series.len()/2],"p95":series[(series.len()*95/100).min(series.len()-1)]}));
            }
            modes.insert(
                name.into(),
                serde_json::json!({"stats":stats,"samples":values}),
            );
        }
        report.insert(workload.into(), modes.into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"platform":std::env::consts::OS,"arch":std::env::consts::ARCH,"scope":"native Rust; real files and sync_all; warm OS cache; fresh reader each sample; alternating order; no HTTP/UI/catalog publication", "scheduling":"publication_gated writes before querying; ready deliberately defers the same writes until after query completion (no concurrent writer); first-result latency is this controlled schedule, not measured UI latency", "rows":f.rows,"packs":f.input.len(),"pack_bytes":f.input.iter().map(Vec::len).sum::<usize>(),"logical_box_bytes":f.length,"samples_per_mode":samples,"workloads":report})
        )?
    );
    Ok(())
}
