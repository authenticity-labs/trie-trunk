//! Paired local UUID lookups. Both stores contain identical revisions over four hours.
use data_seeds::{Field, TIME_HOUR_MS as HOUR, TimeLeaf, TimeTrie, doc};
use std::{fs, path::Path, time::Instant};
use trunk_node::{Node, RegistrationTarget, Result, Storage};
use trunk_uuid_index::collection::{Binding, Index, Posting};

const SOURCE: &str = "demo/lab/meters/meter";
const START: u64 = 1_800_000_000_000;
const LONG: &str = "31313131-3131-3131-3131-313131313131";
const SHORT: &str = "52525252-5252-5252-5252-525252525252";

fn prepare(root: &Path, bound: bool, mixed: bool) -> Result<Node> {
    let node = Node::install_with(root, "Lookup", Storage::Browse)?;
    let object = |minor, width| {
        doc::new("meter")
            .version(1, minor)
            .field(
                Field::new("serial")
                    .r#type("bytes")
                    .size(16)
                    .annotation("@object"),
            )
            .field(Field::new("reading").r#type("int").size(width))
    };
    let model = if bound {
        object(2, 4)
    } else {
        object(2, 4)
            .field(Field::new("event_id").r#type("bytes").size(10))
            .field(
                Field::new("time")
                    .r#type("uint")
                    .size(6)
                    .annotation("@time(ms)"),
            )
            .field(
                Field::new("revision_id")
                    .r#type("uint")
                    .size(4)
                    .annotation("@revision"),
            )
    }
    .sign()?;
    let target = RegistrationTarget {
        cache: "demo".into(),
        service: "lab".into(),
        resource: "meters".into(),
        resource_type: "meter".into(),
        ..Default::default()
    };
    let branch = node.registration(&target, model.bytes()?)?.branch;
    let other = if mixed {
        let revised = object(3, 8).sign()?.compound(model.bytes()?).sign()?;
        node.registration(&target, revised.bytes()?)?.branch
    } else {
        branch
    };
    let trie = node.trie();
    let scope = trie.branches().select([branch]).pods()?;
    let mut container = scope.create(&[])?;
    let generation = [47; 16];
    let mut time = TimeTrie::create(START, generation, 0)?;
    let mut posts = Vec::new();
    for h in 0..4 {
        let hour = START + h * HOUR;
        let mut pack = scope.fill(branch, 1025)?;
        pack.hour(hour)?;
        let mut wire = Vec::new();
        let mut batch_branch = branch;
        for i in 0..1025u32 {
            let b = if (342..684).contains(&i) {
                other
            } else {
                branch
            };
            if b != batch_branch {
                node.ingest_batch(batch_branch, &wire, hour + 2000)?;
                wire.clear();
                batch_branch = b;
            }
            let rx = scope.definition(b)?.prescription();
            let small = i == 1024;
            let n = if small {
                h as u32 + 1
            } else {
                h as u32 * 1024 + i + 1
            };
            let key = if small { [0x52; 16] } else { [0x31; 16] };
            let mut id = [0; 10];
            id[0] = 0x70;
            id[2] = 0x80;
            id[9] = key[0];
            id[3..7].copy_from_slice(&n.to_be_bytes());
            let mut p = rx.dispense();
            p.set_uint("time", hour + u64::from(i))?
                .set_uint("revision_id", n)?
                .set_byte_string("event_id", &id)?;
            if bound {
                p.set_object(
                    "object",
                    vec![
                        ("serial".into(), ciborium::Value::Bytes(key.to_vec())),
                        ("reading".into(), n.into()),
                    ],
                )?;
            } else {
                p.set_byte_string("serial", &key)?.set_int("reading", n)?;
            }
            p.cap()?;
            let bytes = p.bytes()?;
            if bound {
                wire.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                wire.extend_from_slice(&bytes);
                if i == 1023 || small {
                    node.ingest_batch(b, &wire, hour + 2000)?;
                    wire.clear();
                }
            } else {
                pack.push_bytes(&bytes)?;
                posts.push(Posting {
                    object: key,
                    revision: n,
                    hour: h as u32,
                    record: i + 1,
                });
            }
        }
        if bound {
            node.commit(hour + HOUR + 900_000)?;
        } else {
            let addr = container.next_address();
            pack.seal()?;
            container.pack(pack.into_bytes()?)?;
            let start = time.header().tail;
            time = time.publish(
                hour + HOUR,
                Some(TimeLeaf {
                    hour,
                    start,
                    end: start + 1026,
                    packs: addr,
                    beds: addr,
                    count: 1025,
                }),
            )?;
        }
    }
    if !bound {
        let path = root.join(trie.route(branch)?.pods()?.unwrap());
        fs::create_dir_all(path.parent().unwrap())?;
        container.commit()?;
        fs::write(&path, container.bytes()?)?;
        fs::write(path.with_extension("time"), time.bytes())?;
        let index = Index::open(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path.with_extension("indx"))?,
        )?;
        index.append(
            Binding {
                minor: 2,
                generation,
            },
            4,
            0,
            time.header().tail,
            &posts,
        )?;
    }
    drop(node);
    Node::open(root)
}
fn lookup(node: &Node, id: &str, n: usize, bound: bool) -> Result<f64> {
    let start = Instant::now();
    let result = node.history(SOURCE, id, 0, 1 << 48, false)?;
    let us = start.elapsed().as_secs_f64() * 1e6;
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), n);
    for (i, row) in rows.iter().enumerate() {
        let v = &row["values"];
        let obj = if bound { &v["object"] } else { v };
        assert_eq!(row["revision"], i + 1);
        assert_eq!(obj["reading"], i + 1);
    }
    Ok(us)
}
fn main() -> Result<()> {
    let root = std::env::temp_dir().join(format!("trunk-revision-perf-{}", std::process::id()));
    fs::create_dir(&root)?;
    let nodes = [
        prepare(&root.join("flat"), false, false)?,
        prepare(&root.join("bound"), true, false)?,
        prepare(&root.join("mixed"), true, true)?,
    ];
    for (id, n) in [(SHORT, 4), (LONG, 4096)] {
        let mut times: [Vec<f64>; 3] = Default::default();
        for i in 0..33 {
            for j in [i % 3, (i + 1) % 3, (i + 2) % 3] {
                let us = lookup(&nodes[j], id, n, j != 0)?;
                if i != 0 {
                    times[j].push(us);
                }
            }
        }
        for (layout, mut values) in ["flat", "bound", "bound-rollback"].into_iter().zip(times) {
            values.sort_by(f64::total_cmp);
            println!(
                "{}",
                serde_json::json!({"layout":layout,"rows":n,"lookups":values.len(),"median_us":(values[15]+values[16])/2.0,"p95_us":values[30]})
            );
        }
    }
    drop(nodes);
    fs::remove_dir_all(root)?;
    Ok(())
}
