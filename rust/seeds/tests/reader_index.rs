#![cfg(feature = "registration")]
use data_seeds::reader::{AddressSpace, IndexSelection, Reader, Selection, Storage};
use data_seeds::registration::{Packs, Trunk};
use data_seeds::{
    Compare, Direction, Field, HourIndexBuilder, IndexFields, LeafTrie, MemoryBudget, Predicate,
    Query, TIME_HOUR_MS, TimeTrie, doc,
};
use pil_codec::api::FieldBytes;
use std::{
    io,
    sync::{Arc, Mutex},
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

struct Files {
    packs: Vec<u8>,
    index: Vec<u8>,
    calls: Mutex<Vec<(u64, u64, usize)>>,
}
impl Storage for Files {
    fn read_at(&self, file: u64, at: u64, target: &mut [u8]) -> io::Result<usize> {
        self.calls.lock().unwrap().push((file, at, target.len()));
        let data = match file {
            1 => &self.packs,
            2 => &self.index,
            _ => return Err(io::ErrorKind::NotFound.into()),
        };
        let data = data.get(at as usize..).unwrap_or(&[]);
        let n = target.len().min(data.len());
        target[..n].copy_from_slice(&data[..n]);
        Ok(n)
    }
}
struct Fixture {
    scope: Packs,
    files: Arc<Files>,
    time: Arc<TimeTrie>,
    roots: Vec<Arc<LeafTrie>>,
    hours: Vec<u64>,
    addresses: Vec<u64>,
    count: usize,
}
impl Fixture {
    fn reader(&self, budget: MemoryBudget) -> Reader {
        self.scope
            .reader(
                self.files.clone(),
                Selection {
                    file: 1,
                    length: self.files.packs.len() as u64,
                    addresses: self.addresses.clone(),
                },
                1 << 20,
                budget,
            )
            .unwrap()
    }
    fn index(&self) -> IndexSelection {
        IndexSelection {
            file: 2,
            length: self.files.index.len() as u64,
            time: self.time.clone(),
            hours: self.hours.clone(),
            space: AddressSpace::Beds,
            leaves: self.roots.clone(),
        }
    }
}
fn fixture(count: usize) -> Fixture {
    fixture_step(count, 1)
}
fn fixture_step(count: usize, step: u64) -> Fixture { fixture_plan(count, step, false) }
fn fixture_plan(count: usize, step: u64, mixed: bool) -> Fixture {
    let time = || {
        Field::new("time")
            .r#type("uint")
            .size(6)
            .annotation("@time")
    };
    let leaf = || {
        Field::new("leaf_id")
            .r#type("uint")
            .size(2)
            .annotation("@leaf")
    };
    let region = || Field::new("region").r#type("tstr");
    let sequence = || Field::new("sequence").r#type("uint").size(4).annotation("#summ(numeric)");
    let duration = || Field::new("duration").r#type("uint").size(4).annotation("#summ(numeric)");
    let old = doc::new("indexed")
        .field(time())
        .field(leaf())
        .field(region())
        .field(sequence())
        .field(duration())
        .sign()
        .unwrap();
    let bundle = doc::new("indexed")
        .version(0, 1)
        .field(sequence())
        .field(region())
        .field(leaf())
        .field(time())
        .field(duration())
        .compound(old.bytes().unwrap())
        .sign()
        .unwrap();
    let fixture_cache = std::env::var("SEED_INDEXED_CACHE").unwrap_or_else(|_| "test".into());
    let registration = || Registration {
        cache: &fixture_cache,
        service: "lab",
        resource: "indexed",
        resource_type: "sample",
        level: "",
        file: "indexed.rx",
    };
    let (before, first) = Taxonomy::new("Indexed Reader")
        .unwrap()
        .registration(registration(), &old)
        .unwrap();
    let (taxonomy, second) = before.registration(registration(), &bundle).unwrap();
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    trunk
        .prescribe_file("indexed.rx", bundle.bytes().unwrap())
        .unwrap();
    let scope = trunk.trie().branches().packs().unwrap();
    let mut packs = Vec::new();
    let mut index = Vec::new();
    let mut roots = [LeafTrie::new(trunk.definition(first.definition).unwrap()),
        LeafTrie::new(trunk.definition(second.definition).unwrap())];
    let mut leaf_ids = [0; 2];
    let mut addresses = Vec::new();
    let mut starts = Vec::new();
    let epoch = 1_767_225_600_000u64; // 2026 UTC fixture year
    let first_hour = std::env::var("SEED_INDEXED_HOUR")
        .map(|value| value.parse::<u64>().unwrap()).unwrap_or(epoch);
    assert!(first_hour >= epoch && first_hour < epoch + 363 * 24 * TIME_HOUR_MS);
    assert_eq!(first_hour % TIME_HOUR_MS, 0);
    let hours = vec![first_hour, first_hour + TIME_HOUR_MS];
    let mut summary_day = if std::env::var_os("SEED_INDEXED_FIXTURE").is_some() {
        Some(trunk.trie().branches().select([first.branch]).summaries().unwrap()
            .accumulator_day(first_hour / 86_400_000 * 86_400_000, data_seeds::summaries::SummaryCalendar::new(0).unwrap()).unwrap())
    } else { None };
    for (version, entry) in [&first, &second].iter().enumerate() {
        let definition = trunk.definition(entry.definition).unwrap();

        let mut hour_index = HourIndexBuilder::new(
            hours[version],
            count,
            IndexFields::from_definition(&definition).unwrap(),
        )
        .unwrap();
        let mut pack = scope.fill(entry.branch, count).unwrap();
        pack.header(data_seeds::integrity::HEADER, vec![0;4]).unwrap();
        for row in 0..count {
            let row_version = if mixed { row % 2 } else { version };
            let row_definition = trunk.definition(if row_version == 0 { first.definition } else { second.definition }).unwrap();
            let transform = |row:usize, seed:&mut data_seeds::Seed<'_>| {
                    seed.set_uint("sequence", row as u64)?.set_uint("duration", row as u64 * 17 % 20000)?
                        .set_text_string("region", if row_version == 0 { "west" } else { "east" })?;
                    let ordinal = row_definition.prescription().field_ordinal("region").unwrap();
                    if leaf_ids[row_version] == 0 {
                        let FieldBytes::Bytes(raw) = seed.get(ordinal)? else {
                            panic!()
                        };
                        leaf_ids[row_version] = roots[row_version].append(0, ordinal as u16, raw.to_vec())?;
                    }
                    seed.set(ordinal, FieldBytes::Absent)?;
                    hour_index.append(hours[version] + row as u64 * step, leaf_ids[row_version])?;
                    Ok(())
                };
            let seed = if let Some(day) = summary_day.as_mut() {
                day.ingest(if row_version==0 {first.branch} else {second.branch}, hours[version]+row as u64*step,row,transform).unwrap()
            } else {row_definition.ingest(row,transform).unwrap()};
            pack.load(seed).unwrap();
        }
        pack.seal().unwrap();
        let mut bytes = pack.into_bytes().unwrap();
        data_seeds::integrity::stamp(&mut bytes).unwrap();
        let address = version * 262144;
        packs.resize(address, 0);
        addresses.push(address as u64);
        packs.extend(bytes);
        starts.push(index.len() as u64 / 6);
        index.extend_from_slice(hour_index.finish().bytes());

    }
    let roots: Vec<_> = roots.into_iter().map(Arc::new).collect();
    let length = packs.len() + 64;
    let replica = data_seeds::Beds::reconstruct(length as u64, 2).unwrap();
    packs.resize(length, 0);
    packs[length-replica.footer().len()..].copy_from_slice(replica.footer());
    // Time metadata uses the unchanged server file layout; events above all go
    // through typed Seed ingestion and the actual Pack writer.
    let mut raw = vec![0u8; 64 + 366 * 24 * 16];
    raw[..8].copy_from_slice(b"TTTIME00");
    for (at, value) in [
        (8, first_hour),
        (16, first_hour),
        (24, index.len() as u64 / 6),
        (32, first_hour + 2 * TIME_HOUR_MS),
    ] {
        raw[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    raw[40..46].copy_from_slice(&[4, 5, 5, 2, 2, 2]);
    if let Ok(value)=std::env::var("SEED_INDEXED_GENERATION") {
        assert_eq!(value.len(),32);
        for i in 0..16 {raw[48+i]=u8::from_str_radix(&value[i*2..i*2+2],16).unwrap();}
    }
    for i in 0..2 {
        let mut at = 64 + (((first_hour - epoch) / TIME_HOUR_MS) as usize + i) * 16;
        for (value, width) in [
            (starts[i], 4),
            (addresses[i], 5),
            (addresses[i], 5),
            (count as u64, 2),
        ] {
            raw[at..at + width].copy_from_slice(&value.to_le_bytes()[..width]);
            at += width;
        }
    }
    let time = Arc::new(TimeTrie::open(Arc::new(raw.clone())).unwrap());
    if let Ok(path) = std::env::var("SEED_INDEXED_FIXTURE") {
        let path = std::path::Path::new(&path);
        std::fs::create_dir_all(path).unwrap();
        let mut day=summary_day.take().unwrap();day.complete(day.end()).unwrap();
        let (taxonomy, summary_definition, projection, pack)=day.seal_registered(&taxonomy,"indexed.summary.rx").unwrap();
        let mut summaries=data_seeds::summaries::SummaryBox::new(projection.layout().clone()).unwrap();
        summaries.append(pack).unwrap().commit().unwrap();
        std::fs::write(path.join("indexed.summ"),summaries.bytes().unwrap()).unwrap();
        std::fs::write(path.join("indexed.summary.rx"),summary_definition.bytes().unwrap()).unwrap();
        for (name, bytes) in [
            ("trunk.trie", taxonomy.bytes()),
            ("indexed.rx", bundle.bytes().unwrap()),
            ("indexed.beds", packs.as_slice()),
            ("indexed.indx", index.as_slice()),
            ("indexed.time", raw.as_slice()),
        ] {
            std::fs::write(path.join(name), bytes).unwrap();
        }
        // The origin carries the replica extent in application metadata. Its
        // existing sparse Pack addresses are identical to this plain replica.
        let headers = pil_codec::api::encode_headers(&std::collections::BTreeMap::from([
            ("seed.beds.length".to_owned(), (packs.len() as u64).to_le_bytes().to_vec()),
        ])).unwrap();
        let payload = packs.len() - replica.footer().len();
        let (_, footer) = pil_codec::r#box::Box::external_footer_from_payload(
            payload as u64, 2, &headers, false,
        ).unwrap();
        let mut origin = packs[..payload].to_vec();
        origin.extend_from_slice(&footer);
        std::fs::write(path.join("indexed.packs"), origin).unwrap();
        let scope = trunk
            .trie()
            .branches()
            .select([first.branch])
            .leaves()
            .unwrap();
        let mut forest = scope.create().unwrap();
        forest.register(first.branch).unwrap();
        forest.append(&roots[0]).unwrap();
        forest.freeze(first.branch).unwrap();
        forest.register(second.branch).unwrap();
        forest.append(&roots[1]).unwrap();
        std::fs::write(path.join("sample.leaf"), forest.publish().unwrap()).unwrap();
        std::fs::write(
            path.join("fixture.json"),
            format!(
                "{{\"branches\":[{},{}],\"hours\":[{},{}],\"addresses\":[{},{}],\"count\":{}}}",
                first.branch, second.branch, hours[0], hours[1], addresses[0], addresses[1], count
            ),
        )
        .unwrap();
    }
    Fixture {
        scope,
        files: Arc::new(Files {
            packs,
            index,
            calls: Mutex::new(Vec::new()),
        }),
        time,
        roots,
        hours,
        addresses,
        count,
    }
}
#[test]
fn disk_index_restores_version_specific_fields_with_bounded_windows() {
    let fixture = fixture(10000);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    reader.index(fixture.index()).unwrap();
    assert!(fixture.files.calls.lock().unwrap().is_empty());
    let mut query = reader
        .query(
            Query::new()
                .filter(Predicate::compare("region", Compare::Eq, "east"))
                .select(["time", "leaf_id", "region", "sequence"])
                .limit(2),
        )
        .unwrap();
    let batch = query.fetch(2).unwrap_or_else(|e| {
        panic!(
            "{e:?}; scanned {}; reads {:?}",
            query.scanned,
            fixture.files.calls.lock().unwrap()
        )
    });
    assert_eq!(query.scanned, 2);
    let objects = batch.objects().unwrap();
    assert_eq!(objects.len(), 2);
    for (i, row) in objects.iter().enumerate() {
        for (name, value) in [
            ("time", (fixture.hours[1] + i as u64).into()),
            ("leaf_id", 1u64.into()),
            ("region", "east".into()),
            ("sequence", (i as u64).into()),
        ] {
            assert_eq!(&row.iter().find(|(n, _)| n == name).unwrap().1, &value);
        }
    }
    let calls = fixture.files.calls.lock().unwrap();
    let index_calls: Vec<_> = calls.iter().filter(|(file, _, _)| *file == 2).collect();
    assert!(index_calls.iter().all(|(_, _, length)| *length <= 16380));
    assert_eq!(
        index_calls.len(),
        10,
        "bounded index windows for the prefilter and the two projected rows"
    );
    drop(calls);
    drop(batch);
    drop(query);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}
#[test]
fn disk_index_rejects_mismatched_time_and_missing_version_root() {
    let fixture = fixture(4);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    let mut binding = fixture.index();
    binding.hours.reverse();
    reader.index(binding).unwrap();
    assert!(reader.pack(0).is_err());
    assert_eq!(budget.used(), 0);
    let mut reader = fixture.reader(budget.clone());
    let mut binding = fixture.index();
    binding.leaves.pop();
    reader.index(binding).unwrap();
    assert!(reader.pack(0).is_ok());
    assert!(reader.pack(1).is_err());
    assert_eq!(budget.used(), 0);
    let mut reader = fixture.reader(budget);
    reader.index(fixture.index()).unwrap();
    let cursor = reader.query(Query::new()).unwrap();
    assert!(reader.index(fixture.index()).is_err());
    drop(cursor);
}
#[test]
fn disk_index_sort_and_independent_cursors_preserve_original_ordinals() {
    let fixture = fixture(7);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    reader.index(fixture.index()).unwrap();
    let mut a = reader
        .query(
            Query::new()
                .select(["time"])
                .order_by("time", Direction::Descending)
                .limit(2),
        )
        .unwrap();
    let mut b = reader
        .query(Query::new().select(["time"]).limit(1))
        .unwrap();
    let batch = a.fetch(2).unwrap();
    let earlier = b.fetch(1).unwrap();
    assert_eq!(
        batch.objects().unwrap()[0][0].1,
        (fixture.hours[1] + 6).into()
    );
    assert_eq!(earlier.objects().unwrap()[0][0].1, fixture.hours[0].into());
    drop(batch);
    drop(earlier);
    drop(a);
    drop(b);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
}

#[test]
fn timestamp_seek_skips_payloads_and_scans_only_the_selected_rows() {
    let fixture = fixture(10000);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    reader.index(fixture.index()).unwrap();
    let from = fixture.hours[1] + 9500;
    let selected = reader.range(from, from + 3).unwrap();
    let mut cursor = selected
        .query(Query::new().select(["sequence", "time"]).limit(2))
        .unwrap();
    let batch = cursor.fetch(2).unwrap();
    assert_eq!(cursor.scanned, 2);
    let rows = batch.objects().unwrap();
    assert_eq!(rows[0][0].1, 9500u64.into());
    assert_eq!(rows[1][1].1, (from + 1).into());
    let calls = fixture.files.calls.lock().unwrap();
    assert!(
        calls
            .iter()
            .filter(|(file, _, _)| *file == 1)
            .all(|(_, at, _)| *at >= fixture.addresses[1])
    );
    // Probe windows are 4 KiB; they do not scan the 60 KiB hour index.
    let probes: usize = calls
        .iter()
        .filter(|(file, _, n)| *file == 2 && *n <= 4092)
        .map(|(_, _, n)| *n)
        .sum();
    assert!(probes < 30000);
    drop(calls);
    drop(batch);
    drop(cursor);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    fixture.files.calls.lock().unwrap().clear();
    let mut empty = reader
        .range(fixture.hours[0] + 20000, fixture.hours[0] + 21000)
        .unwrap()
        .query(Query::new())
        .unwrap();
    assert!(empty.fetch(1).unwrap().is_empty());
    assert_eq!(empty.scanned, 0);
    assert!(
        fixture
            .files
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(file, _, _)| *file == 2)
    );
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    let mut disjoint = reader
        .range(from, from + 5)
        .unwrap()
        .range(from + 10, from + 20)
        .unwrap()
        .query(Query::new())
        .unwrap();
    fixture.files.calls.lock().unwrap().clear();
    assert!(disjoint.fetch(1).unwrap().is_empty());
    assert!(fixture.files.calls.lock().unwrap().is_empty());
}

#[test]
fn metadata_preparation_respects_time_selected_pack_ranges() {
    let fixture = fixture(10000);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    reader.index(fixture.index()).unwrap();
    let from = fixture.hours[1] + 9500;
    let selected = reader.range(from, from + 3).unwrap();
    assert_eq!(selected.prepare(0, 2).unwrap(), 2);
    let start = fixture.addresses[1] as usize;
    let end = start + pil_codec::pac_envelope::Plain::read_prefix(&fixture.files.packs[start..]).unwrap().header_length as usize;
    assert!(fixture.files.calls.lock().unwrap().iter().filter(|(file, _, _)| *file == 1)
        .all(|(_, at, length)| *at as usize >= start && *at as usize + length <= end));
    assert_eq!(budget.used(), selected.metadata_bytes().unwrap());
    drop(reader);drop(selected);
    assert_eq!(budget.used(), 0);
}

#[test]
fn five_minute_periods_skip_rows_without_copying_packs_or_losing_origins() {
    let fixture = fixture_step(60, 60_000);
    let budget = MemoryBudget::new(1 << 20);
    let mut reader = fixture.reader(budget.clone());
    reader.index(fixture.index()).unwrap();
    let hour = fixture.hours[1];
    let periods = reader.ranges(vec![hour + 30*60_000..hour + 35*60_000,
        hour + 5*60_000..hour + 10*60_000]).unwrap();
    periods.prepare(0, 2).unwrap();
    for resumable in [false, true] {
        let mut cursor = periods.query(Query::new().select(["sequence"]).offset(3).limit(6)).unwrap();
        let mut values = Vec::new();
        loop {
            let (batch,done) = if resumable {
                let slice = cursor.poll(2, 1).unwrap(); (slice.batch, slice.done)
            } else { let batch=cursor.fetch(2).unwrap(); let done=batch.is_empty(); (batch,done) };
            for row in 0..batch.len() {
                let (pack, ordinal, _) = batch.origin(row).unwrap();
                assert_eq!(pack, 1, "original Pack identity survives period selection");
                values.push(ordinal);
            }
            if done { break; }
        }
        assert_eq!(values, [8,9,30,31,32,33]);
        assert_eq!(cursor.scanned, 9, "excluded periods are not scanned");
    }
    let mut sorted = periods.query(Query::new().select(["sequence"])
        .order_by("time", Direction::Descending).limit(3)).unwrap();
    let rows=sorted.fetch(3).unwrap().objects().unwrap();
    assert_eq!(rows.iter().map(|o| o[0].1.clone()).collect::<Vec<_>>(),
        vec![34u64.into(),33u64.into(),32u64.into()]);
    assert_eq!(sorted.scanned,10);
    assert!(fixture.files.calls.lock().unwrap().iter().filter(|(file,_,_)| *file==1)
        .all(|(_,at,_)| *at >= fixture.addresses[1]));
    fixture.files.calls.lock().unwrap().clear();
    let empty = reader.ranges(Vec::new()).unwrap();
    empty.prepare(0,2).unwrap();
    assert!(empty.query(Query::new()).unwrap().fetch(1).unwrap().is_empty());
    assert!(fixture.files.calls.lock().unwrap().is_empty(), "empty periods need no I/O");
    let narrowed = periods.range(hour + 8*60_000,hour + 32*60_000).unwrap()
        .ranges(vec![hour..hour+TIME_HOUR_MS]).unwrap();
    let mut cursor = narrowed.query(Query::new()).unwrap();
    assert_eq!(cursor.fetch(20).unwrap().len(),4, "later selection cannot widen previous bounds");
}

#[test]
fn mixed_rows_restore_fields_from_their_own_version_root() {
    let fixture = fixture_plan(10, 1, true);
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    reader.index(fixture.index()).unwrap();
    let query = Query::new().filter(Predicate::compare("region", Compare::Eq, "east"))
        .select(["sequence", "region", "time"]).order_by("time", Direction::Ascending).limit(20);
    let batch = data_seeds::Cursor::from_source(reader.clone(), query).unwrap().fetch(20).unwrap();
    let rows = batch.objects().unwrap();
    assert_eq!(rows.len(), 10);
    for (i, row) in rows.iter().enumerate() {
        let value = |name: &str| &row.iter().find(|(k, _)| k == name).unwrap().1;
        assert_eq!(value("region"), &ciborium::Value::Text("east".into()));
        let ordinal = (i % 5 * 2 + 1) as u64;
        assert_eq!(value("sequence"), &ciborium::Value::Integer(ordinal.into()));
        assert_eq!(value("time"), &ciborium::Value::Integer((fixture.hours[i / 5] + ordinal).into()));
        assert_eq!(batch.origin(i).unwrap().1, ordinal as usize);
    }
    // Resident loading uses the same per-version bindings as paged reads.
    use data_seeds::PackSource;
    let mut reader = reader;
    let pack = reader.load(0).unwrap();
    let mut source = data_seeds::Packs::new(); source.pack(pack);
    let rows = source.query(Query::new().filter(Predicate::compare("region", Compare::Eq, "west")))
        .unwrap().fetch(20).unwrap().objects().unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn leaf_predicates_preserve_residual_boolean_and_missing_field_semantics(){
    let fixture=fixture(10);
    let region=|s:&str|Predicate::compare("region",Compare::Eq,s);
    let last=Predicate::compare("sequence",Compare::Ge,8);
    for (filter,count,scanned) in [
        (Predicate::And(vec![region("east"),last.clone()]),2,10),
        (Predicate::And(vec![last.clone(),region("north")]),0,0),
        (Predicate::Or(vec![region("north"),last.clone()]),4,20),
        (Predicate::Not(Box::new(region("west"))),10,10),
    ] {
        let mut reader=fixture.reader(MemoryBudget::new(1<<20));
        reader.index(fixture.index()).unwrap();
        let q=Query::new().filter(filter).select(["region","sequence"]);
        let expected=data_seeds::Cursor::from_source(Scan(reader.clone()),q.clone()).unwrap().fetch(50).unwrap().objects().unwrap();
        fixture.files.calls.lock().unwrap().clear();
        let mut cursor=reader.query(q).unwrap();
        let batch=cursor.fetch(50).unwrap();
        assert_eq!(batch.objects().unwrap(),expected);
        assert_eq!((batch.len(),cursor.scanned),(count,scanned));
        if count==0{assert!(fixture.files.calls.lock().unwrap().iter().all(|c|c.0!=1));}
    }
}

#[test]
fn sliced_leaf_pruning_yields_and_reuses_the_index_window() {
    let fixture = fixture(1000);
    for (region, count) in [("north",0),("east",1000)] {
        fixture.files.calls.lock().unwrap().clear();
        let mut reader=fixture.reader(MemoryBudget::new(1<<20));
        reader.index(fixture.index()).unwrap();
        let mut cursor=reader.query(Query::new().filter(Predicate::compare("region",Compare::Eq,region)).select(["sequence"])).unwrap();
        let mut pages=0;
        let mut results=Vec::new();
        loop {
            let page=cursor.poll(128,19).unwrap();
            pages+=1;
            assert!(page.work<=19);
            results.extend(page.batch.objects().unwrap());
            if page.done {break}
            assert!(page.work>0);
        }
        assert!(pages>=100,"index rows count towards cooperative work");
        assert_eq!((results.len(),cursor.scanned),(count,count));
        for (i,row) in results.iter().enumerate(){ assert_eq!(row[0].1,(i as u64).into()); }
        if region=="north" {
            let calls=fixture.files.calls.lock().unwrap();
            assert!(calls.iter().all(|c|c.0==2));
            assert_eq!(calls.iter().map(|c|c.2).sum::<usize>(),12012);
        }
    }
}

#[test]
fn bounded_polls_seek_disjoint_periods_once() {
    let fixture = fixture(1000);
    let hour = fixture.hours[1];
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    reader.index(fixture.index()).unwrap();
    let reader = reader.ranges(vec![hour + 10..hour + 150, hour + 500..hour + 610]).unwrap();
    let query = Query::new().filter(Predicate::compare("region", Compare::Eq, "north"));
    let mut cursor = reader.query(query.clone()).unwrap();
    assert!(cursor.fetch(32).unwrap().is_empty());
    let expected_reads = std::mem::take(&mut *fixture.files.calls.lock().unwrap());
    assert!(expected_reads.iter().all(|c| c.0 == 2));
    let mut cursor = reader.query(query).unwrap();
    loop {
        let page = cursor.poll(8, 7).unwrap();
        assert!(page.batch.is_empty() && page.work <= 7);
        if page.done { break; }
        assert!(page.work > 0);
    }
    assert_eq!(*fixture.files.calls.lock().unwrap(), expected_reads);
}

#[test]
fn leaf_planning_spends_only_the_remaining_global_limit_in_the_next_hour() {
    let fixture = fixture(10_000);
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    reader.index(fixture.index()).unwrap();
    let query = Query::new().filter(Predicate::Or(vec![
        Predicate::compare("region", Compare::Eq, "east"),
        Predicate::compare("region", Compare::Eq, "west"),
    ])).select(["sequence"]).offset(9995).limit(10);
    let mut cursor = reader.query(query).unwrap();
    let mut positions = Vec::new();
    for _ in 0..2 {
        let page = cursor.fetch(5).unwrap();
        positions.extend((0..page.len()).map(|i| page.origin(i).unwrap().1));
    }
    assert_eq!(positions, [9995,9996,9997,9998,9999,0,1,2,3,4]);
    let second = (fixture.count as u64 + 1) * 6;
    let calls = fixture.files.calls.lock().unwrap();
    let bytes: usize = calls.iter().filter(|(id,at,_)| *id == 2 && *at >= second)
        .map(|(_,_,n)| n).sum();
    assert!(bytes <= 16384 + 12, "last five results need only one index window, read {bytes} bytes");
}

/// The reference delegates data reads to the same reader and leaves predicate
/// evaluation to the row scanner. Only index predicate pushdown differs.
struct Scan(Reader);

#[test]
fn time_predicates_seek_the_published_index_and_preserve_residuals() {
    let fixture = fixture_plan(32, 1, true);
    let hour = fixture.hours[1];
    let time = |op, delta| Predicate::compare("time", op, hour + delta);
    let window = Predicate::And(vec![time(Compare::Gt, 2u64), time(Compare::Le, 12)]);
    let last = Predicate::compare("sequence", Compare::Ge, 10);
    let east = Predicate::compare("region", Compare::Eq, "east");
    // The seek proves the outer window. Time tests under OR/NOT still
    // need the row even when another conjunct is fully covered by the leaf.
    let third = time(Compare::Eq, 3);
    for (filter, expected_count, expected_scanned) in [
        (window.clone(), 8, 8),
        (Predicate::And(vec![window.clone(), east.clone(), Predicate::Or(vec![
            third.clone(), Predicate::compare("region", Compare::Eq, "north"),
        ])]), 1, 4),
        (Predicate::And(vec![window.clone(), east, Predicate::Not(Box::new(third))]), 3, 4),
        (Predicate::And(vec![window.clone(), last.clone()]), 3, 8),
        (Predicate::And(vec![window.clone(), Predicate::compare("region", Compare::Eq, "east")]), 4, 4),
        (Predicate::And(vec![window.clone(), time(Compare::Ge, 13)]), 0, 0),
        (time(Compare::Eq, 44), 0, 0),
        (Predicate::Or(vec![time(Compare::Eq, 5), last]), 11, 18),
    ] {
        let open = || {
            let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
            reader.index(fixture.index()).unwrap();
            reader.ranges(vec![hour..hour+6, hour+8..hour+20]).unwrap()
        };
        let q = Query::new().filter(filter).select(["sequence", "region"]);
        let expected = data_seeds::Cursor::from_source(Scan(open()), q.clone())
            .unwrap().fetch(50).unwrap().objects().unwrap();
        assert_eq!(expected.len(), expected_count);
        fixture.files.calls.lock().unwrap().clear();
        let reader = open();
        let mut cursor = reader.query(q.clone()).unwrap();
        let mut rows = Vec::new();
        loop {
            let page = cursor.poll(3, 5).unwrap();
            rows.extend(page.batch.objects().unwrap());
            if page.done { break; }
        }
        assert_eq!(rows, expected);
        assert_eq!(cursor.scanned, expected_scanned);
        if expected_count == 0 {
            assert!(fixture.files.calls.lock().unwrap().iter().all(|c| c.0 != 1));
        }
        let sync = reader.query(q).unwrap().fetch(50).unwrap().objects().unwrap();
        assert_eq!(sync, expected);
    }
}

#[test]
fn exact_time_seek_preserves_timestamp_ties_and_explicit_mixed_version() {
    let fixture = fixture_plan(12, 0, true);
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    reader.index(fixture.index()).unwrap();
    let q = Query::new().filter(Predicate::compare("time", Compare::Eq, fixture.hours[1]))
        .version(0, 1).select(["sequence"]).offset(1).limit(3);
    let expected = data_seeds::Cursor::from_source(Scan(reader.clone()), q.clone())
        .unwrap().fetch(4).unwrap().objects().unwrap();
    let rows = reader.query(q).unwrap().fetch(4).unwrap().objects().unwrap();
    assert_eq!(rows, expected);
    let values: Vec<_> = rows.iter().map(|row| row[0].1.clone()).collect();
    assert_eq!(values, [3u64.into(), 5u64.into(), 7u64.into()]);
}

#[test]
fn indexed_time_cannot_exclude_rows_using_another_packs_hour() {
    let fixture = fixture(5);
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    let mut index = fixture.index();
    index.hours.reverse();
    reader.index(index).unwrap();
    let q = Query::new().filter(Predicate::compare("time", Compare::Eq, fixture.hours[0]));
    assert!(reader.query(q).unwrap().fetch(5).is_err());
}

impl data_seeds::PackSource for Scan {
    fn len(&self)->usize {self.0.len()}
    fn definitions(&self)->Vec<Arc<data_seeds::SeedDefinition>>{self.0.definitions()}
    fn load(&mut self,i:usize)->Result<Arc<data_seeds::SeedPackSnapshot>,data_seeds::Error>{self.0.load(i)}
    fn read(&mut self,i:usize)->Result<Arc<data_seeds::ReadPack>,data_seeds::Error>{self.0.read(i)}
    fn row_selection(&mut self,i:usize)->Result<data_seeds::RowSelection,data_seeds::Error>{self.0.row_selection(i)}
}
#[test]
#[ignore = "paired reader I/O and latency benchmark"]
fn leaf_pruning_baseline() {
    let fixture = fixture(10000);
    let time = Predicate::And(vec![
        Predicate::compare("time", Compare::Ge, fixture.hours[1] + 500),
        Predicate::compare("time", Compare::Lt, fixture.hours[1] + 600),
    ]);
    for (case, filter, matches) in [
        ("all", None, 2 * fixture.count),
        ("east", Some(Predicate::compare("region", Compare::Eq, "east")), fixture.count),
        ("north", Some(Predicate::compare("region", Compare::Eq, "north")), 0),
        ("time", Some(time.clone()), 100),
        ("time+sequence", Some(Predicate::And(vec![time, Predicate::compare("sequence", Compare::Ge, 575)])), 25),
    ] {
        for materialize in [false,true] {
            let mut runs=[Vec::new(),Vec::new()];
            let mut reference=None;
            for pair in 0..9 {
                for enabled in [pair%2!=0,pair%2==0] {
                    fixture.files.calls.lock().unwrap().clear();
                    let mut reader=fixture.reader(MemoryBudget::new(1<<20));
                    reader.index(fixture.index()).unwrap();
                    let mut query=Query::new().select(["sequence"]);
                    if let Some(predicate)=&filter {query=query.filter(predicate.clone());}
                    let start=std::time::Instant::now();
                    let mut cursor=if enabled {reader.query(query)} else {Reader::query_sources(vec![Box::new(Scan(reader))],query)}.unwrap();
                    let mut count=0;
                    let mut sequence=Vec::new();
                    loop {
                        let page=cursor.fetch(512).unwrap();
                        if page.is_empty(){break}
                        count+=page.len();
                        if materialize {sequence.extend(page.objects().unwrap());}
                    }
                    let micros=start.elapsed().as_secs_f64()*1e6;
                    let calls=fixture.files.calls.lock().unwrap();
                    let bytes=|file|calls.iter().filter(|(id,_,_)|*id==file).map(|(_,_,n)|n).sum::<usize>();
                    let result=(count,cursor.scanned,bytes(1),bytes(2));
                    assert_eq!(count,matches);
                    if enabled && case=="north"{assert_eq!((result.1,result.2),(0,0));}
                    if enabled && case=="east"{assert_eq!(result.1,fixture.count);}
                    if enabled && case.starts_with("time"){assert_eq!(result.1,100);}
                    if let Some(expected)=&reference {assert_eq!(expected,&sequence)}else{reference=Some(sequence);}
                    runs[enabled as usize].push((micros,result));
                }
            }
            for (enabled,mut samples) in runs.into_iter().enumerate(){
                samples.sort_by(|a,b|a.0.total_cmp(&b.0));
                let (us,(matched,scanned,pac,index))=samples[samples.len()/2];
                eprintln!("case={case} projected={materialize} pruning={enabled} matched={matched} scanned={scanned} PAC_bytes={pac} index_bytes={index} median_us={us:.1}");
            }
        }
    }
}

#[cfg(feature = "planner-lab")]
fn rank_reader_plans(count: usize, rounds: usize) -> data_seeds::planner_lab::Ranking {
    use data_seeds::planner_lab::{Access, PlanModel, Ranking, Selection};
    let fixture = fixture(count);
    let mut reader = fixture.reader(MemoryBudget::new(1 << 20));
    reader.index(fixture.index()).unwrap();
    // Last 5% of the west hour and first 5% of the east hour.
    let width = count / 20;
    let query = Query::new().filter(Predicate::And(vec![
        Predicate::compare("time", Compare::Ge, fixture.hours[0] + (count - width) as u64),
        Predicate::compare("time", Compare::Lt, fixture.hours[1] + width as u64),
        Predicate::compare("region", Compare::Eq, "east"),
    ])).select(["sequence"]).order_by("sequence", Direction::Descending).limit(10);
    let expected: Vec<_> = (0..width as u64).rev().take(10)
        .map(|n| vec![("sequence".into(), n.into())]).collect();
    let mut plans = Vec::new();
    for access in [Access::Scan, Access::Time, Access::Leaves, Access::TimeAndLeaves] {
        for selection in [Selection::FullSortReference, Selection::BoundedTopK] {
            plans.push(PlanModel::new(format!("{access:?}/{selection:?}"))
                .access(access).selection(selection).max_candidates(count));
        }
    }
    let json = serde_json::to_vec(&plans).unwrap();
    let plans: Vec<PlanModel> = serde_json::from_slice(&json).unwrap();
    let ranking = Ranking::run(&reader, &query, &plans, &expected, rounds, 42).unwrap();
    for score in &ranking.scores {
        let model = &score.plan.model;
        let scanned = match model.access {
            Access::Scan => 2 * count,
            Access::Leaves => count,
            Access::Time => 2 * width,
            Access::TimeAndLeaves => width,
        };
        let retained = if model.selection == Selection::BoundedTopK { width.min(10) } else { width };
        for sample in &score.samples {
            assert_eq!((sample.scanned_rows, sample.output_rows, sample.peak_sort_candidates),
                       (scanned, expected.len(), retained));
        }
    }
    ranking
}

#[cfg(feature = "planner-lab")]
#[test]
fn ranked_access_paths_preserve_residuals_order_and_limit() {
    let ranking = rank_reader_plans(400, 2);
    assert_eq!(ranking.scores.len(), 8);
    assert!(ranking.scores.windows(2).all(|w| w[0].median_ns <= w[1].median_ns));
}

#[cfg(feature = "planner-lab")]
#[test]
#[ignore = "warm registered reader strategy comparison"]
fn rank_hour_indexes() {
    let ranking = rank_reader_plans(10_000, 16);
    if let Some(path) = std::env::var_os("SEED_PLAN_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&ranking).unwrap()).unwrap();
    }
    for score in ranking.scores {
        let sample = &score.samples[0];
        eprintln!("{} median_us={:.1} p95_us={:.1} scanned={} retained={}",
            score.plan.model.name, score.median_ns / 1000.0, score.p95_ns / 1000.0,
            sample.scanned_rows, sample.peak_sort_candidates);
    }
}
