//! Reproducible native read benchmark. Dataset creation/encoding is outside all
//! query timers. Generated events use actual typed builder setters, not canned PILs.
use data_seeds::{
    Compare, Field, HourIndexBuilder, IndexFields, Predicate, Query, TIME_HOUR_MS, TimeLeaf,
    TimeTrie, doc,
};
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
    time::Instant,
};
use trie_trunk_taxonomy::{Trunk, registration::Registration};
use trunk_node::{Node, Result};
const HOURS: u64 = 24;
const PER_HOUR: u64 = 10_000;
const FIRST: u64 = 1_794_960_000_000; // 2026-11-18 UTC, exact hour boundary.
fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}
fn generate(root: &Path) -> Result<serde_json::Value> {
    drop(Node::install(root, "Native Performance Sample")?);
    let document = doc::new("http")
        .namespace("log")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(
            Field::new("level")
                .r#type("uint")
                .size(1)
                .annotation("@otel(SeverityNumber)"),
        )
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("@unit(ms)")
                .annotation("#summ(numeric)"),
        )
        .field(Field::new("status").r#type("uint").size(2))
        .field(Field::new("method").r#type("tstr").size(8))
        .field(Field::new("path").r#type("tstr"))
        .field(Field::new("message").r#type("tstr"))
        .field(Field::new("request_bytes").r#type("uint").size(3))
        .field(Field::new("response_bytes").r#type("uint").size(4))
        .sign()?;
    let (trie, registered) = Trunk::new("Native Performance Sample")?.registration(
        Registration {
            cache: "sample",
            service: "shop",
            resource: "requests",
            resource_type: "http",
            level: "",
            file: "http.rx",
        },
        &document,
    )?;
    fs::write(root.join("trunk.trie"), trie.bytes())?;
    fs::write(root.join("definitions/http.rx"), document.bytes()?)?;
    let mut trunk = data_seeds::registration::Trunk::open(trie.bytes())?;
    let definition = trunk.prescribe(registered.definition, document.bytes()?)?;
    let scope = trunk.trie().branches().select([registered.branch]).beds()?;
    let directory = root.join("sample/shop/requests");
    fs::create_dir_all(&directory)?;
    let mut beds = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("http.v0.beds"))?;
    let mut index = File::create(directory.join("http.v0.indx"))?;
    let mut preview = File::create(root.join("sample.jsonl"))?;
    let mut writer = scope.create(&[])?;
    let mut timeline = TimeTrie::create(FIRST, [3; 16], 0)?;
    let mut index_position = 0;
    let started = Instant::now();
    let mut rare = 0;
    for h in 0..HOURS {
        let hour = FIRST + h * TIME_HOUR_MS;
        let mut pack = scope.fill(registered.branch, PER_HOUR as usize)?;
        let mut hour_index = HourIndexBuilder::new(
            hour,
            PER_HOUR as usize,
            IndexFields::from_definition(&definition)?,
        )?;
        for row in 0..PER_HOUR {
            let sequence = h * PER_HOUR + row;
            let time = hour + row * TIME_HOUR_MS / PER_HOUR;
            let duration = sequence * 7919 % 20_000;
            rare += u64::from(duration == 19_999);
            let status =
                [200, 200, 200, 201, 204, 400, 401, 404, 429, 500][(sequence % 10) as usize];
            let level = if status >= 500 {
                17
            } else if status >= 400 {
                13
            } else {
                9
            };
            let method = if sequence % 5 == 0 { "POST" } else { "GET" };
            let path = format!(
                "/api/{}/{}",
                ["orders", "accounts", "payments", "inventory"][(sequence % 4) as usize],
                sequence % 1009
            );
            let message = if sequence % 97 == 0 {
                format!(
                    "upstream request {sequence} failed; retryable transport error: {}",
                    "detail ".repeat(40)
                )
            } else {
                format!(
                    "request {sequence} completed for account {}",
                    sequence % 6001
                )
            };
            let mut id = [0u8; 10];
            id[2..].copy_from_slice(&sequence.to_be_bytes());
            let pill = definition.ingest((), |_, p| {
                p.set_uint("time", time)?
                    .set_byte_string("id", &id)?
                    .set_uint("level", level as u8)?
                    .set_uint("duration", duration)?
                    .set_uint("status", status as u16)?
                    .set_text_string("method", method)?
                    .set_text_string("path", &path)?
                    .set_text_string("message", &message)?
                    .set_uint("request_bytes", sequence * 7 % 65536)?
                    .set_uint("response_bytes", sequence * 37 % 1_000_000)?;
                Ok(())
            })?;
            pack.load(pill)?;
            hour_index.append(time, 0)?;
            if sequence < 10 {
                writeln!(
                    preview,
                    "{}",
                    json!({"time":time,"id_entropy_hex":id.iter().map(|b|format!("{b:02x}")).collect::<String>(),"level":level,"duration":duration,"status":status,"method":method,"path":path,"message":message,"request_bytes":sequence*7%65536,"response_bytes":sequence*37%1_000_000})
                )?;
            }
        }
        pack.seal()?;
        let address = writer.next_beds_address();
        writer.pack(pack.into_bytes()?)?;
        let commit = writer.commit()?;
        beds.seek(SeekFrom::Start(commit.offset))?;
        beds.write_all(commit.bytes)?;
        beds.set_len(commit.length)?;
        let bytes = hour_index.finish();
        index.write_all(bytes.bytes())?;
        let end = index_position + PER_HOUR + 1;
        timeline = timeline.publish(
            hour + TIME_HOUR_MS,
            Some(TimeLeaf {
                hour,
                start: index_position,
                end,
                packs: address,
                beds: address,
                count: PER_HOUR,
            }),
        )?;
        index_position = end;
    }
    beds.sync_all()?;
    index.sync_all()?;
    fs::write(directory.join("http.v0.time"), timeline.bytes())?;
    let manifest = json!({"records":HOURS*PER_HOUR,"hours":HOURS,"fields":10,"branch":registered.branch,"from":FIRST,"to":FIRST+HOURS*TIME_HOUR_MS,
        "rare_matches":rare,"beds_bytes":beds.metadata()?.len(),"index_bytes":index.metadata()?.len(),"time_bytes":timeline.bytes().len(),"generation_ms":ms(started),
        "summary_packs":0,"identity":"deterministic test entropy, not production random generation","generation_in_query_timings":false});
    fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}
fn run(root: &Path) -> Result<serde_json::Value> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
    let started = Instant::now();
    let node = Node::open(root)?;
    let open_ms = ms(started);
    let mut cases = Vec::new();
    for (name, minimum, expected, drain) in [
        ("first_page", None, 100, false),
        ("rare", Some(19_999), 12, true),
        ("zero", Some(20_000), 0, true),
        ("all_rows", None, HOURS * PER_HOUR, true),
    ] {
        let mut runs = Vec::new();
        for iteration in 0..6 {
            let started = Instant::now();
            let mut query = Query::new();
            if let Some(min) = minimum {
                query = query.filter(Predicate::compare("duration", Compare::Ge, min));
            }
            let mut cursor = node.query(
                manifest["branch"].as_u64().unwrap() as u16,
                FIRST,
                FIRST + HOURS * TIME_HOUR_MS,
                query,
            )?;
            let selection_ms = ms(started);
            let mut fetch_ms = 0.0;
            let mut projection_ms = 0.0;
            let mut rows = 0;
            loop {
                let start = Instant::now();
                let batch = cursor.fetch(100)?;
                fetch_ms += ms(start);
                let count = batch.len();
                rows += count;
                let start = Instant::now();
                let objects = batch.objects()?;
                std::hint::black_box(&objects);
                drop(objects);
                projection_ms += ms(start);
                if !drain || count < 100 {
                    break;
                }
            }
            assert_eq!(rows as u64, expected, "{name}");
            runs.push(json!({"iteration":iteration,"rows":rows,"selection_ms":selection_ms,"fetch_ms":fetch_ms,"projection_ms":projection_ms,"total_ms":ms(started)}));
        }
        cases.push(json!({"name":name,"runs":runs}));
    }
    let library = data_seeds::summaries::summary_template()?;
    let stored = doc::read(include_bytes!("../../seeds/definitions/summary.rx"))?;
    let audit = json!({"definitions":stored.definitions().len(),"template_matches_bundled_rx":library.bytes()?==stored.bytes()?,
        "names":stored.definitions().iter().map(|d|d.header.rx_name.clone()).collect::<Vec<_>>()});
    Ok(
        json!({"dataset":manifest,"node_open_ms":open_ms,"cases":cases,"summary_audit":audit,
        "scope":"release native reader; all typed encoding and disk publication occur before these read timers; fetch includes lazy planning, positional reads, decoding and filtering; projection materializes Rust objects; excludes JSON/socket/UI",
        "cache":"new cursor each run, same node; OS file cache not evicted; generation warms file cache; iteration 0 is first cursor, not cold physical disk"}),
    )
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [operation, root] = args.as_slice() else {
        return Err("Expected generate|run <fresh sample directory>".into());
    };
    let result = match operation.as_str() {
        "generate" => generate(Path::new(root))?,
        "run" => run(Path::new(root))?,
        _ => return Err("Unknown operation".into()),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
