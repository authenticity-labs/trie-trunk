//! Cold-Rust-reader I/O probe, using a real file and events created through Seed
//! ingestion. OS cache is intentionally not flushed; this is not a cold-disk
//! benchmark. Definition registration and fixture generation are timed separately.
use data_seeds::reader::{AddressSpace, IndexSelection, Selection, Storage};
use data_seeds::registration::Trunk;
use data_seeds::{
    Cursor, Field, HourIndexBuilder, IndexFields, MemoryBudget, PackSource, Query, TimeTrie, doc,
};
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

struct Files {
    data: File,
    index: File,
    bytes: AtomicUsize,
    calls: AtomicUsize,
}
impl Storage for Files {
    fn read_at(&self, id: u64, offset: u64, target: &mut [u8]) -> io::Result<usize> {
        let file = match id {
            1 => &self.data,
            2 => &self.index,
            _ => return Err(io::ErrorKind::NotFound.into()),
        };
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(file, target, offset)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(file, target, offset)?;
        self.bytes.fetch_add(n, Ordering::Relaxed);
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }
}
// Reference the former whole-Pack query ownership through the existing explicit
// pack() API. The same query engine, files, index and data feed both strategies.
struct Whole(data_seeds::reader::Reader);
impl PackSource for Whole {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn definitions(&self) -> Vec<Arc<data_seeds::SeedDefinition>> {
        self.0.definitions()
    }
    fn load(
        &mut self,
        ordinal: usize,
    ) -> Result<Arc<data_seeds::SeedPackSnapshot>, data_seeds::Error> {
        self.0.pack(ordinal)
    }
    fn rows(
        &mut self,
        ordinal: usize,
    ) -> Result<Option<std::ops::Range<usize>>, data_seeds::Error> {
        self.0.rows(ordinal)
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = 10000usize;
    let payload = std::env::args()
        .nth(1)
        .map(|n| n.parse::<usize>())
        .transpose()?
        .unwrap_or(2048);
    let selected_rows = std::env::args()
        .nth(2)
        .map(|n| n.parse::<usize>())
        .transpose()?
        .unwrap_or(2);
    if selected_rows == 0 || selected_rows > rows || payload < 8 {
        return Err("require 1..10000 selected rows and >=8 message bytes".into());
    }
    let first = if selected_rows <= 500 { 9500 } else { 0 };
    let began = Instant::now();
    let document = doc::new("reader-io")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("sequence").r#type("uint").size(4))
        .field(Field::new("message").r#type("tstr"))
        .sign()?;
    let (taxonomy, registered) = Taxonomy::new("Reader I/O")?.registration(
        Registration {
            cache: "test",
            service: "lab",
            resource: "probe",
            resource_type: "sample",
            level: "",
            file: "probe.rx",
        },
        &document,
    )?;
    let mut trunk = Trunk::open(taxonomy.bytes())?;
    let definition = trunk.prescribe(registered.definition, document.bytes()?)?;
    let scope = trunk.trie().branches().packs()?;
    let registration_us = began.elapsed().as_micros();
    let hour = 1_767_225_600_000u64;
    let mut index = HourIndexBuilder::new(hour, rows, IndexFields::from_definition(&definition)?)?;
    let mut pack = scope.fill(registered.branch, rows)?;
    pack.header("crc32c", vec![0; 4])?;
    let began = Instant::now();
    for row in 0..rows {
        let text = format!("{row:08}{}", "x".repeat(payload.saturating_sub(8)));
        pack.load(
            definition.ingest((row, text.as_str()), |(row, text), seed| {
                index.append(hour + row as u64, 0)?;
                seed.set_uint("sequence", row as u64)?
                    .set_text_string("message", text)?;
                Ok(())
            })?,
        )?;
    }
    pack.seal()?;
    let mut data = pack.into_bytes()?;
    data_seeds::integrity::stamp(&mut data)?;
    data_seeds::integrity::before_write(&data)?;
    let ingestion_us = began.elapsed().as_micros();
    let size = data.len();
    let index = index.finish();
    let index_size = index.bytes().len();
    let base = std::env::temp_dir().join(format!("seed-reader-io-{}", std::process::id()));
    std::fs::create_dir(&base)?;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(base.clone());
    let mut out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(base.join("probe.beds"))?;
    out.write_all(&data)?;
    out.sync_all()?;
    drop(out);
    drop(data);
    let mut out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(base.join("probe.indx"))?;
    out.write_all(index.bytes())?;
    out.sync_all()?;
    drop(out);
    drop(index);
    let files = Arc::new(Files {
        data: File::open(base.join("probe.beds"))?,
        index: File::open(base.join("probe.indx"))?,
        bytes: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let mut raw = vec![0u8; 64 + 366 * 24 * 16];
    raw[..8].copy_from_slice(b"TTTIME00");
    for (at, value) in [
        (8, hour),
        (16, hour),
        (24, (rows + 1) as u64),
        (32, hour + 3_600_000),
    ] {
        raw[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    raw[40..46].copy_from_slice(&[4, 5, 5, 2, 2, 2]);
    raw[78..80].copy_from_slice(&(rows as u16).to_le_bytes());
    let time = Arc::new(TimeTrie::open(Arc::new(raw))?);
    let mut timings = [Vec::new(), Vec::new()];
    let mut metrics = vec![serde_json::Value::Null; 2];
    for sample in 0..7 {
        for strategy in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
            files.bytes.store(0, Ordering::Relaxed);
            files.calls.store(0, Ordering::Relaxed);
            let budget = MemoryBudget::new(128 << 20);
            let began = Instant::now();
            let mut reader = scope.reader(
                files.clone(),
                Selection {
                    file: 1,
                    length: size as u64,
                    addresses: vec![0],
                },
                64 << 20,
                budget.clone(),
            )?;
            reader.index(IndexSelection {
                file: 2,
                length: index_size as u64,
                time: time.clone(),
                hours: vec![hour],
                space: AddressSpace::Beds,
                leaves: vec![],
            })?;
            let selected = reader.range(hour + first, hour + first + selected_rows as u64)?;
            let plan = Query::new()
                .select(["sequence", "message"])
                .limit(selected_rows);
            let mut query = if strategy == 0 {
                Cursor::from_source(Whole(selected.clone()), plan)?
            } else {
                selected.query(plan)?
            };
            let result = query.fetch(selected_rows)?;
            let retained = budget.used();
            let objects = result.objects()?;
            assert_eq!(query.scanned, selected_rows);
            assert_eq!(objects.len(), selected_rows);
            for (row, object) in objects.iter().enumerate() {
                assert_eq!(object[0].1, (first + row as u64).into());
                let ciborium::Value::Text(text) = &object[1].1 else {
                    panic!("text projection changed type")
                };
                assert_eq!(&text[..8], format!("{:08}", first + row as u64));
            }
            let output_bytes = objects
                .iter()
                .map(|o| match &o[1].1 {
                    ciborium::Value::Text(text) => text.len(),
                    _ => 0,
                })
                .sum::<usize>();
            timings[strategy].push(began.elapsed().as_micros());
            metrics[strategy] = serde_json::json!({"read_bytes":files.bytes.load(Ordering::Relaxed),"read_calls":files.calls.load(Ordering::Relaxed),"retained_after_fetch_bytes":retained,"selected_text_bytes":output_bytes});
            drop(result);
            drop(query);
            drop(selected);
            drop(reader);
            assert_eq!(budget.used(), 0);
        }
    }
    for (i, times) in timings.iter_mut().enumerate() {
        times.sort();
        metrics[i]["query_us_median"] = times[times.len() / 2].into();
        metrics[i]["query_us_samples"] = serde_json::json!(times);
    }
    println!(
        "{}",
        serde_json::json!({"scope":"real-file query through native Rust; fresh reader each sample; warm OS cache; not production UI", "rows":rows,"message_bytes":payload,"pack_bytes":size,"index_bytes":index_size,"selected_rows":selected_rows,"whole_pack":metrics[0],"bounded_windows":metrics[1],"registration_us":registration_us,"ingestion_and_seal_us":ingestion_us})
    );
    Ok(())
}
