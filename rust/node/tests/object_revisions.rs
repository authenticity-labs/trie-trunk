use data_seeds::{Document, Field, TIME_HOUR_MS as HOUR, doc};
use std::{io::Read, path::Path, process::Command};
use trunk_node::{Node, RegistrationTarget, Result, Storage};

const KEY: [u8; 16] = [0x31; 16];
const UUID: &str = "31313131-3131-3131-3131-313131313131";
const SOURCE: &str = "demo/lab/meters/meter";
const CLOCK: u64 = 1800000000000;
fn meter(minor: u32) -> Result<Document> {
    Ok(doc::new("meter")
        .version(1, minor)
        .field(
            Field::new("serial")
                .r#type("bytes")
                .size(16)
                .annotation("@object"),
        )
        .field(
            Field::new("reading")
                .r#type("int")
                .size(if minor == 2 { 2 } else { 4 }),
        )
        .field(Field::new("label").r#type("tstr"))
        .sign()?)
}
fn register(node: &Node, d: &Document) -> Result<u16> {
    Ok(node
        .registration(
            &RegistrationTarget {
                cache: "demo".into(),
                service: "lab".into(),
                resource: "meters".into(),
                resource_type: "meter".into(),
                ..Default::default()
            },
            d.bytes()?,
        )?
        .branch)
}
// The Go transport test sends these exact Rust-authored bytes.
fn fixture(name: &str, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = std::env::var_os("TRUNK_REVISION_FIXTURES") {
        std::fs::write(Path::new(&dir).join(name), bytes)?;
    } else {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/object-revisions");
        assert_eq!(
            std::fs::read(dir.join(name))?,
            bytes,
            "fixture drift: {name}"
        );
    }
    Ok(())
}
fn change(node: &Node, b: u16, revision: u32, key: [u8; 16]) -> Result<Vec<u8>> {
    let scope = node.trie().branches().select([b]).pods()?;
    let def = scope.definition(b)?;
    let mut id = [0u8; 10];
    id[0] = 0x70;
    id[2] = 0x80;
    id[6..].copy_from_slice(&revision.to_be_bytes());
    let mut fields = vec![
        ("serial".into(), ciborium::Value::Bytes(key.to_vec())),
        (
            "reading".into(),
            (if def.version().1 == 3 { 90000 } else { -17 }).into(),
        ),
    ];
    if revision % 2 == 0 {
        fields.push(("label".into(), ciborium::Value::Null));
    }
    let mut p = def.prescription().dispense();
    p.set_uint("time", CLOCK + u64::from(revision.min(10000)))?
        .set_byte_string("event_id", &id)?
        .set_uint("revision_id", revision)?
        .set_object("object", fields)?
        .cap()?;
    let bytes = p.bytes()?;
    let minor = def.version().1;
    if key == KEY && matches!((minor, revision), (2, 1 | 3) | (3, 2)) {
        fixture(&format!("meter-1.{minor}-r{revision}.pil"), &bytes)?;
    }
    Ok(bytes.to_vec())
}

fn submit(node: &Node, b: u16, revision: u32, key: [u8; 16], batch: bool) -> Result<()> {
    let bytes = change(node, b, revision, key)?;
    if batch {
        let mut wire = (bytes.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(&bytes);
        node.ingest_batch(b, &wire, CLOCK + 10000)?;
    } else {
        node.ingest(b, &bytes, CLOCK + 10000)?;
    }
    Ok(())
}

#[test]
fn roots_writer_process() -> Result<()> {
    let Some(path) = std::env::var_os("TRUNK_REVISION_TEST_ROOT") else {
        return Ok(());
    };
    let node = Node::install_with(Path::new(&path), "Revision Test", Storage::Pack)?;
    let first = meter(2)?;
    let a = register(&node, &first)?;
    let route = node.trie().route(a)?;
    let sub = node.subscribe(a, &route.digest, SOURCE)?;
    submit(&node, a, 1, KEY, false)?;
    sub.readiness()?.read_exact(&mut [0])?;
    assert!(!sub.next()?.bytes.is_empty());
    let second = meter(3)?.compound(first.bytes()?).sign()?;
    let b = register(&node, &second)?;
    fixture("meter-1.2.rx", first.bytes()?)?;
    fixture("meter-1.3.rx", second.bytes()?)?;
    submit(&node, b, 2, KEY, true)?;
    submit(&node, b, 1, [0x52; 16], false)?;
    assert!(submit(&node, a, 2, KEY, false).is_err());
    assert!(submit(&node, a, 0, KEY, false).is_err());
    let limit = trunk_uuid_index::collection::MAX_REV;
    submit(&node, a, limit, [0x63; 16], false)?;
    let err = submit(&node, a, limit + 1, KEY, true).unwrap_err();
    assert!(
        err.to_string()
            .contains(&format!("Object revision exceeds {limit}")),
        "{err}"
    );
    // A duplicate at the end must also reject the new revision at the front.
    // The parent submits revision 3 successfully after replay.
    let mut wire = Vec::new();
    for revision in [3, 1] {
        let pill = change(&node, a, revision, KEY)?;
        wire.extend_from_slice(&(pill.len() as u32).to_be_bytes());
        wire.extend_from_slice(&pill);
    }
    assert!(node.ingest_batch(a, &wire, CLOCK + 10000).is_err());
    std::fs::write(Path::new(&path).join("branches"), format!("{a} {b}"))?;
    // Exit while the acknowledged revisions still live only in the recovery spool.
    std::process::exit(0)
}

#[test]
fn roots_replays_exact_object_type_and_publishes_indexed_history() -> Result<()> {
    let path = std::env::temp_dir().join(format!("object-binding-{}", std::process::id()));
    struct Clean(std::path::PathBuf);
    impl Drop for Clean {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _clean = Clean(path.clone());
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "roots_writer_process", "--nocapture"])
        .env("TRUNK_REVISION_TEST_ROOT", &path)
        .status()?;
    assert!(status.success());
    let branches = std::fs::read_to_string(path.join("branches"))?;
    let branches = branches
        .split_whitespace()
        .map(str::parse::<u16>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let node = Node::open(&path)?;
    submit(&node, branches[0], 3, KEY, true)?;
    assert!(submit(&node, branches[1], 1, KEY, false).is_err());
    // Keep a handle to the live spool. After commit it contains the exact durable
    // checkpoint that a process exit before spool unlink would leave behind.
    let pods = path.join(node.trie().route(branches[0])?.pods()?.unwrap());
    let hour = CLOCK / HOUR * HOUR;
    let staged = pods.with_extension(format!("{hour}.pack"));
    let active = pods.with_extension("seeds");
    let spool = std::fs::File::open(if active.exists() { &active } else { &staged })?;
    node.commit(CLOCK + 2 * HOUR)?;
    drop(node);
    {
        let mut recovered = std::fs::File::create_new(&staged)?;
        std::io::copy(&mut &spool, &mut recovered)?;
        recovered.sync_all()?;
    }
    let node = Node::open(&path)?;
    assert!(!staged.exists(), "replayed checkpoint must be retired");
    let h = node.history(SOURCE, UUID, 0, 1 << 48, false)?;
    let rows = h["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    for (i, (&b, value)) in [branches[0], branches[1], branches[0]]
        .iter()
        .zip([-17, 90000, -17])
        .enumerate()
    {
        assert_eq!(rows[i]["stream"], b);
        assert_eq!(rows[i]["definition"], node.trie().route(b)?.digest);
        assert_eq!(rows[i]["revision"], i + 1);
        let fields = h["definitions"][rows[i]["definition"].as_str().unwrap()].as_array().unwrap();
        let revision = fields.iter().find(|f| f["name"] == "revision_id").unwrap();
        assert_eq!(revision["annotations"], serde_json::json!(["@revision"]));
        let event = fields.iter().find(|f| f["name"] == "event_id").unwrap();
        assert_eq!(event["annotations"], serde_json::json!(["@event"]));
        assert_eq!(rows[i]["values"]["event_id"]["$bytes"].as_array().unwrap().len(), 16);
        assert_eq!(rows[i]["values"]["object"]["reading"], value);
        assert_eq!(
            rows[i]["values"]["object"].get("label"),
            (i == 1).then_some(&serde_json::Value::Null)
        );
    }
    assert_eq!(
        node.history(
            SOURCE,
            "52525252-5252-5252-5252-525252525252",
            0,
            1 << 48,
            false
        )?["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let high = node.history(
        SOURCE,
        "63636363-6363-6363-6363-636363636363",
        0,
        1 << 48,
        false,
    )?;
    assert_eq!(high["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        high["rows"][0]["revision"],
        trunk_uuid_index::collection::MAX_REV
    );
    Ok(())
}
