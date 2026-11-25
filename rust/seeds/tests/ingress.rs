#![cfg(feature = "registration")]
use data_seeds::{
    Field, Query, SeedDefinition, doc,
    registration::{IngressLimits, Trunk},
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

const HOUR: u64 = 3_600_000;

#[test]
fn durable_admission_precedes_visibility_and_rejects_reads_after_write_failure() {
    use data_seeds::registration::ingress::{OwnedIngress, recovery};
    let document = event_document();
    let (trunk, branch) = registered(&document);
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let make = || {
        OwnedIngress::new(
            trunk.trie().branches().select([branch]).packs().unwrap(),
            branch,
            IngressLimits::default(),
            HOUR,
        )
        .unwrap()
    };
    let pill = encode(&definition, HOUR + 10, "events");
    let mut ingress = make();
    assert!(
        ingress
            .persist(branch, &[], HOUR + 20, |_, _| panic!(
                "invalid input reached persistence"
            ))
            .is_err()
    );
    assert!(ingress.healthy().is_ok());
    let mut durable = Vec::new();
    ingress
        .persist(branch, &pill, HOUR + 20, |prefix, bytes| {
            durable.extend_from_slice(prefix);
            durable.extend_from_slice(bytes);
            Ok(())
        })
        .unwrap();
    assert_eq!(ingress.count(HOUR), 1);
    assert_eq!(
        recovery::read(&durable, 1 << 20).unwrap(),
        (branch, pill.as_slice())
    );
    let mut failed = make();
    assert!(
        failed
            .persist(branch, &pill, HOUR + 20, |_, _| Err(
                data_seeds::Error::Incomplete
            ))
            .is_err()
    );
    assert_eq!(failed.count(HOUR), 0);
    assert!(failed.snapshot(HOUR).is_err());
    assert!(failed.commits(3 * HOUR).is_err());
    assert!(
        failed
            .persist(branch, &pill, HOUR + 20, |_, _| panic!(
                "failed owner attempted another write"
            ))
            .is_err()
    );
}

#[test]
fn commit_planning_uses_configured_deadlines_and_never_skips_unpublished_hours() {
    use data_seeds::registration::ingress::OwnedIngress;
    let document = event_document();
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut ingress = OwnedIngress::new(
        scope,
        branch,
        IngressLimits {
            straggler_ms: 60_000,
            ..Default::default()
        },
        HOUR,
    )
    .unwrap();
    ingress
        .load(branch, &encode(&definition, HOUR + 10, "events"), HOUR + 20)
        .unwrap();
    assert!(ingress.commits(2 * HOUR + 59_999).unwrap().hours.is_empty());
    let plan = ingress.commits(2 * HOUR + 60_000).unwrap();
    assert_eq!(plan.hours, [HOUR]);
    assert_eq!(plan.through, 2 * HOUR);
    assert_eq!(plan.head, 2 * HOUR);
    let time = data_seeds::TimeTrie::create(HOUR, [0; 16], 0).unwrap();
    assert!(ingress.completion(&time, 3 * HOUR).unwrap().is_none());
    ingress.seal(HOUR, 2 * HOUR + 60_000).unwrap();
    assert!(ingress.completion(&time, 3 * HOUR).unwrap().is_none());
    ingress.published(HOUR).unwrap();
    assert_eq!(
        ingress
            .completion(&time, 3 * HOUR)
            .unwrap()
            .unwrap()
            .header()
            .sealed,
        2 * HOUR
    );
}

#[test]
fn summary_checkpoint_replays_only_the_shared_publication_extent() {
    use data_seeds::registration::ingress::SummaryCommit;
    let commit = SummaryCommit {
        bytes: std::sync::Arc::new(vec![1, 2, 3]),
        offset: 100,
        length: 103,
    };
    let checkpoint = commit.checkpoint().unwrap();
    let restored = SummaryCommit::restore(&checkpoint, 1024).unwrap();
    assert_eq!(
        (restored.offset, restored.length, restored.bytes),
        (100, 103, std::sync::Arc::new(vec![1, 2, 3]))
    );
    assert!(
        SummaryCommit {
            bytes: std::sync::Arc::new(vec![1]),
            offset: u64::MAX,
            length: 0
        }
        .checkpoint()
        .is_err()
    );
    assert!(
        SummaryCommit {
            bytes: std::sync::Arc::new(vec![1]),
            offset: 1,
            length: 4
        }
        .checkpoint()
        .is_err()
    );
}
#[test]
fn full_pils_ingest_as_unsealed_packs_and_seal_in_time_order() {
    let document = doc::new("query")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("query").r#type("tstr"))
        .sign()
        .unwrap();
    let (tree, registered) = Taxonomy::new("Ingress")
        .unwrap()
        .registration(
            Registration {
                cache: "demo",
                service: "trie-trunk",
                resource: "queries",
                resource_type: "query",
                level: "",
                file: "query.rx",
            },
            &document,
        )
        .unwrap();
    let mut trunk = Trunk::open(tree.bytes()).unwrap();
    trunk
        .prescribe_file("query.rx", document.bytes().unwrap())
        .unwrap();
    let scope = trunk
        .trie()
        .branches()
        .select([registered.branch])
        .packs()
        .unwrap();
    let def = SeedDefinition::prescribe(&document).unwrap();
    let mut ingress = scope
        .ingress(registered.branch, IngressLimits::default(), HOUR)
        .unwrap();
    for time in [HOUR + 20, HOUR + 10, HOUR + 10] {
        let seed = def
            .ingest(time, |time, seed| {
                seed.set_uint("time", time)?
                    .set_text_string("query", "events")?;
                Ok(())
            })
            .unwrap();
        seed.cap().unwrap();
        ingress
            .load(registered.branch, &seed.bytes().unwrap(), HOUR + 100)
            .unwrap();
    }
    let raw = ingress.snapshot(HOUR).unwrap();
    let open = scope
        .open_pack(std::sync::Arc::new(raw.clone()), 0..raw.len(), 1 << 20)
        .unwrap();
    assert!(!open.is_sealed());
    assert_eq!(open.len(), 3);
    assert!(ingress.seal(HOUR, 2 * HOUR).is_err());
    let sealed = ingress.seal(HOUR, 2 * HOUR + 900_000).unwrap();
    assert_eq!(sealed.index.len(), 24);
    assert_eq!(
        ingress.count(HOUR),
        3,
        "prepare does not discard unpublished data"
    );
    let size = sealed.pack.len();
    let mut stored = scope.reader_scope();
    stored
        .load_registered(std::sync::Arc::new(sealed.pack), 0..size, 1 << 20)
        .unwrap();
    let batch = stored.query(Query::new()).unwrap().fetch(10).unwrap();
    let mut reader = batch.reader();
    let times = (0..batch.len())
        .map(|i| format!("{:?}", reader.view(i).unwrap().get("time").unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(times[0], times[1]);
    assert_ne!(times[1], times[2]);
    ingress.published(HOUR).unwrap();
    assert_eq!(ingress.retained_input_bytes(), 0);
}

fn registered(document: &data_seeds::Document) -> (Trunk, u16) {
    let (tree, binding) = Taxonomy::new("Ingress")
        .unwrap()
        .registration(
            Registration {
                cache: "demo",
                service: "trie-trunk",
                resource: "queries",
                resource_type: "query",
                level: "",
                file: "query.rx",
            },
            document,
        )
        .unwrap();
    let mut trunk = Trunk::open(tree.bytes()).unwrap();
    trunk
        .prescribe_file("query.rx", document.bytes().unwrap())
        .unwrap();
    (trunk, binding.branch)
}
fn event_document() -> data_seeds::Document {
    doc::new("query")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("leaf")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .field(Field::new("query").r#type("tstr"))
        .sign()
        .unwrap()
}
fn encode(def: &std::sync::Arc<SeedDefinition>, time: u64, text: &str) -> Vec<u8> {
    let seed = def
        .ingest((), |_, seed| {
            seed.set_uint("time", time)?
                .set_uint("leaf", 37u16)?
                .set_text_string("query", text)?;
            Ok(())
        })
        .unwrap();
    seed.cap().unwrap();
    seed.bytes().unwrap().into_owned()
}
#[test]
fn rejected_full_pils_do_not_change_the_pack_and_publication_requires_sealing() {
    let document = event_document();
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let def = SeedDefinition::prescribe(&document).unwrap();
    let bytes = encode(&def, HOUR + 20, "events");
    let mut ingress = scope
        .ingress(
            branch,
            IngressLimits {
                bytes: 2 * (bytes.len() + 16),
                pill_bytes: bytes.len(),
                ..Default::default()
            },
            HOUR,
        )
        .unwrap();
    assert!(ingress.load(branch, &[], HOUR + 100).is_err());
    assert!(
        ingress
            .load(branch, b"{\"time\":3600020}", HOUR + 100)
            .is_err()
    );
    assert!(
        ingress
            .load(branch, &bytes[..bytes.len() - 1], HOUR + 100)
            .is_err()
    );
    assert_eq!(ingress.hours().count(), 0);
    ingress.load(branch, &bytes, HOUR + 100).unwrap();
    assert!(ingress.published(HOUR).is_err());
    let before = ingress.snapshot(HOUR).unwrap();
    assert!(ingress.load(0, &bytes, HOUR + 100).is_err());
    assert!(ingress.load(branch, &bytes, HOUR + 10).is_err());
    assert!(ingress.load(branch, &bytes, 2 * HOUR + 900_000).is_err());
    assert_eq!(before, ingress.snapshot(HOUR).unwrap());
    ingress.load(branch, &bytes, HOUR + 100).unwrap();
    assert!(ingress.load(branch, &bytes, HOUR + 100).is_err());
    assert_eq!(ingress.count(HOUR), 2);
    let sealed = ingress.seal(HOUR, 2 * HOUR + 900_000).unwrap();
    // The retry retains data and reproduces the same serialized publication.
    let retry = ingress.seal(HOUR, 2 * HOUR + 900_000).unwrap();
    assert_eq!(sealed.pack, retry.pack);
    assert_eq!(sealed.index, retry.index);
    let index = data_seeds::HourIndex::open(
        std::sync::Arc::new(sealed.index.clone()),
        0..sealed.index.len(),
        HOUR,
        data_seeds::IndexFields::from_definition(&def).unwrap(),
    )
    .unwrap();
    assert_eq!(index.time_leaf(0).unwrap(), (HOUR + 20, 37));
    ingress.published(HOUR).unwrap();
    assert_eq!(ingress.count(HOUR), 0);
    assert!(ingress.load(branch, &bytes, HOUR + 100).is_err());
}

#[test]
fn full_pil_versions_and_rollbacks_survive_the_same_hour() {
    let old = event_document();
    let current = doc::new("query")
        .version(0, 1)
        .field(Field::new("query").r#type("tstr"))
        .field(
            Field::new("time")
                .r#type("uint")
                .size(8)
                .annotation("@time"),
        )
        .field(
            Field::new("leaf")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .compound(old.bytes().unwrap())
        .sign()
        .unwrap();
    let target = || Registration {
        cache: "demo",
        service: "trie-trunk",
        resource: "queries",
        resource_type: "query",
        level: "",
        file: "query.rx",
    };
    let (tree, a) = Taxonomy::new("Ingress")
        .unwrap()
        .registration(target(), &old)
        .unwrap();
    let (tree, b) = tree.registration(target(), &current).unwrap();
    let mut trunk = Trunk::open(tree.bytes()).unwrap();
    trunk
        .prescribe_file("query.rx", current.bytes().unwrap())
        .unwrap();
    let scope = trunk.trie().branches().select([a.branch]).packs().unwrap();
    let mut ingress = scope
        .ingress(a.branch, IngressLimits::default(), HOUR)
        .unwrap();
    for (id, time, text) in [
        (a.branch, HOUR + 30, "a"),
        (b.branch, HOUR + 10, "b"),
        (a.branch, HOUR + 20, "rollback"),
    ] {
        let def = trunk
            .definition(if id == a.branch {
                a.definition
            } else {
                b.definition
            })
            .unwrap();
        ingress
            .load(id, &encode(&def, time, text), HOUR + 100)
            .unwrap();
    }
    for bytes in [
        ingress.snapshot(HOUR).unwrap(),
        ingress.seal(HOUR, 2 * HOUR + 900_000).unwrap().pack,
    ] {
        let len = bytes.len();
        let mut reader = scope.reader_scope();
        reader
            .load_registered(std::sync::Arc::new(bytes), 0..len, 1 << 20)
            .unwrap();
        assert_eq!(
            reader.query(Query::new()).unwrap().fetch(10).unwrap().len(),
            3
        );
        assert_eq!(
            reader
                .query(Query::new().version(0, 0))
                .unwrap()
                .fetch(10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            reader
                .query(Query::new().version(0, 1))
                .unwrap()
                .fetch(10)
                .unwrap()
                .len(),
            1
        );
        let objects = reader
            .query(Query::new().version(0, 1))
            .unwrap()
            .fetch(10)
            .unwrap()
            .objects()
            .unwrap();
        assert!(
            objects[0]
                .iter()
                .any(|(key, value)| key == "query" && *value == ciborium::Value::Text("b".into()))
        );
    }
}

#[test]
fn recovery_replays_only_unpublished_full_pils_and_keeps_time_layout() {
    use data_seeds::registration::ingress::recovery;
    use data_seeds::{TimeLeaf, TimeTrie};
    let document = event_document();
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let bytes = encode(&definition, HOUR + 20, "durable");
    let prefix = recovery::prefix(branch, &bytes).unwrap();
    let record = [prefix.as_slice(), bytes.as_slice()].concat();
    for end in 0..record.len() {
        assert!(matches!(
            recovery::read(&record[..end], 1 << 20),
            Err(data_seeds::Error::Incomplete)
        ));
    }
    let (registered, decoded) = recovery::read(&record, 1 << 20).unwrap();
    assert_eq!(registered, branch);
    assert_eq!(decoded, bytes);
    let mut damaged = record.clone();
    damaged[8] ^= 1;
    assert!(recovery::read(&damaged, 1 << 20).is_err());
    let mut recovered = scope
        .ingress(branch, IngressLimits::default(), HOUR)
        .unwrap();
    recovered.recover(branch, decoded).unwrap();
    let pack = recovered.seal(HOUR, 9 * HOUR).unwrap();
    let before = TimeTrie::create(HOUR, [7; 16], 0).unwrap();
    let after = before
        .publish(
            4 * HOUR,
            Some(TimeLeaf {
                hour: HOUR,
                start: 0,
                end: 2,
                packs: 0,
                beds: 0,
                count: 1,
            }),
        )
        .unwrap();
    assert_eq!(after.header().tail, 2);
    assert_eq!(after.leaf(3 * HOUR).unwrap().unwrap().count, 0);
    assert_eq!(after.leaf(HOUR).unwrap().unwrap().count, 1);
    assert_eq!(before.header().sealed, HOUR);
    recovered.published(HOUR).unwrap();
    recovered.advance(4 * HOUR).unwrap();
    assert_eq!(recovered.recover(branch, decoded).unwrap(), None);
    assert_eq!(recovered.hours().count(), 0);
    assert!(!pack.pack.is_empty());
    // Address widening preserves previous hour coordinates and rows.
    let wide = after
        .publish(
            5 * HOUR,
            Some(TimeLeaf {
                hour: 4 * HOUR,
                start: 2,
                end: 70_003,
                packs: 1 << 40,
                beds: 1 << 40,
                count: 70_000,
            }),
        )
        .unwrap();
    assert_eq!(wide.header().widths[1], 6);
    assert_eq!(wide.header().widths[3], 3);
    assert_eq!(wide.leaf(HOUR).unwrap(), after.leaf(HOUR).unwrap());
    assert_eq!(wide.leaf(4 * HOUR).unwrap().unwrap().packs, 1 << 40);
}

#[test]
fn http_ingress_inputs_are_complete_pils_from_typed_ingestion() {
    let document = doc::new("query")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("query").r#type("tstr"))
        .field(Field::new("duration").r#type("uint").size(4))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let Some(path) = std::env::var_os("SEED_HTTP_FIXTURE") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("query.rx"), document.bytes().unwrap()).unwrap();
    let hour = 1_794_830_400_000u64;
    for (i, time) in [hour + 2000, hour + 1000, hour + HOUR + 1000]
        .into_iter()
        .enumerate()
    {
        let id = [i as u8; 10];
        let seed = definition
            .ingest((i, time), |(i, time), seed| {
                seed.set_uint("time", time)?
                    .set_byte_string("id", &id)?
                    .set_text_string("query", "events | where duration > 10000")?
                    .set_uint("duration", (i + 1) as u64 * 10)?;
                Ok(())
            })
            .unwrap();
        seed.cap().unwrap();
        std::fs::write(path.join(format!("{i}.pil")), seed.bytes().unwrap()).unwrap();
    }
}

#[test]
fn query_telemetry_encodes_millisecond_observations_as_microseconds() {
    use data_seeds::registration::query_events::{QueryEvent, definition};
    let document = definition().unwrap();
    assert_eq!(document.definitions()[0].header.namespace, "log");
    assert_eq!(document.definitions()[0].header.rx_name, "query");
    let def = SeedDefinition::prescribe(&document).unwrap();
    assert_eq!(def.query_ordinal("@time"), def.query_ordinal("time"));
    assert!(def.query_ordinal("@timestamp").is_none());
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let mut ingress = scope
        .ingress(branch, IngressLimits::default(), HOUR)
        .unwrap();
    let event: QueryEvent = serde_json::from_value(serde_json::json!({
        "time": HOUR + 1, "id": [1,2,3,4,5,6,7,8,9,10], "source": "demo/lab/test",
        "query": "events", "status": "complete", "from": 804556800000u64, "to": 804729600000u64,
        "rows": 0, "duration": 12.5, "worker": 10.25
    }))
    .unwrap();
    ingress
        .load(branch, &event.encode().unwrap(), HOUR + 2)
        .unwrap();
    let bytes = ingress.snapshot(HOUR).unwrap();
    let size = bytes.len();
    let mut reader = scope.reader_scope();
    reader
        .load_registered(std::sync::Arc::new(bytes), 0..size, 1 << 20)
        .unwrap();
    let rows = reader
        .query(Query::new().filter(data_seeds::Predicate::And(vec![
            data_seeds::Predicate::compare("from", data_seeds::Compare::Eq, "1995-07-01T00:00:00Z"),
            data_seeds::Predicate::compare("to", data_seeds::Compare::Eq, "1995-07-03T00:00:00Z"),
        ])))
        .unwrap()
        .fetch(1)
        .unwrap()
        .objects()
        .unwrap();
    assert!(
        rows[0]
            .iter()
            .any(|(k, v)| k == "duration" && *v == ciborium::Value::Float(12500.0))
    );
    assert!(rows[0].iter().any(|(k, v)| k == "checkpoints"
        && *v
            == ciborium::Value::Array(vec![ciborium::Value::Map(vec![
                ("name".into(), "worker".into()),
                ("duration".into(), ciborium::Value::Float(10250.0))
            ])])));
    assert!(
        rows[0]
            .iter()
            .any(|(k, v)| k == "level" && *v == ciborium::Value::Integer(3.into()))
    );
    assert!(
        rows[0]
            .iter()
            .any(|(k, v)| k == "operation" && *v == ciborium::Value::Text("execute".into()))
    );
    assert!(!rows[0].iter().any(|(k, _)| k == "presentation"));
}

#[test]
fn query_telemetry_levels_and_ordered_checkpoints_survive_ingress() {
    use data_seeds::registration::query_events::{QueryEvent, definition};
    let document = definition().unwrap();
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let mut ingress = scope
        .ingress(branch, IngressLimits::default(), HOUR)
        .unwrap();
    for (i, status) in ["complete", "cancelled", "error"].into_iter().enumerate() {
        let event: QueryEvent = serde_json::from_value(serde_json::json!({
            "time":HOUR+1+i as u64,"id":[1,2,3,4,5,6,7,8,9,10],"source":"demo/lab/test",
            "query":"events","status":status,"from":HOUR,"to":2*HOUR,"rows":0,
            "duration":12.5,"worker":10.25,"collection":0,"presentation":0.5
        }))
        .unwrap();
        ingress
            .load(branch, &event.encode().unwrap(), HOUR + 4)
            .unwrap();
    }
    let bytes = ingress.snapshot(HOUR).unwrap();
    if let Some(directory) = std::env::var_os("TRUNK_IDENTITY_FIXTURE") {
        let directory = std::path::Path::new(&directory);
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(directory.join("query.rx"), document.bytes().unwrap()).unwrap();
        std::fs::write(directory.join("query.pac"), &bytes).unwrap();
    }
    let def = SeedDefinition::prescribe(&document).unwrap();
    let ordinal = def.prescription().field_ordinal("level").unwrap();
    assert_eq!(def.metadata(ordinal).unwrap().name, "log.level");
    let mut reader = scope.reader_scope();
    let length = bytes.len();
    reader
        .load_registered(std::sync::Arc::new(bytes), 0..length, 1 << 20)
        .unwrap();
    let rows = reader
        .query(Query::new())
        .unwrap()
        .fetch(3)
        .unwrap()
        .objects()
        .unwrap();
    for (row, level) in rows.iter().zip([3, 4, 5]) {
        let get = |name: &str| &row.iter().find(|(k, _)| k == name).unwrap().1;
        assert_eq!(get("level"), &ciborium::Value::Integer(level.into()));
        assert_eq!(get("operation"), &ciborium::Value::Text("execute".into()));
        assert_eq!(
            get("checkpoints"),
            &ciborium::Value::Array(vec![
                ciborium::Value::Map(vec![
                    ("name".into(), "worker".into()),
                    ("duration".into(), 10250.0.into())
                ]),
                ciborium::Value::Map(vec![
                    ("name".into(), "collection".into()),
                    ("duration".into(), 0.0.into())
                ]),
                ciborium::Value::Map(vec![
                    ("name".into(), "presentation".into()),
                    ("start".into(), 12000.0.into()),
                    ("duration".into(), 500.0.into())
                ]),
            ])
        );
        assert!(!row.iter().any(|(k, _)| k == "worker" || k == "collection"));
    }
}

#[test]
fn query_checkpoints_keep_explicit_positions_and_parent_overlap() {
    use data_seeds::registration::query_events::{QueryEvent, definition};
    let document = definition().unwrap();
    let (trunk, branch) = registered(&document);
    let scope = trunk.trie().branches().select([branch]).packs().unwrap();
    let mut ingress = scope
        .ingress(branch, IngressLimits::default(), HOUR)
        .unwrap();
    let event: QueryEvent = serde_json::from_value(serde_json::json!({
        "time":HOUR+1,"id":[1,2,3,4,5,6,7,8,9,10],"source":"demo/lab/test",
        "query":"events","status":"complete","from":HOUR,"to":2*HOUR,"rows":0,
        "duration":12.5,"worker":10.25,"worker_start":1,"before_collection":2,
        "collection":3,"after_collection":5.25,"presentation":0.5
    }))
    .unwrap();
    ingress
        .load(branch, &event.encode().unwrap(), HOUR + 2)
        .unwrap();
    let bytes = ingress.snapshot(HOUR).unwrap();
    let length = bytes.len();
    let mut reader = scope.reader_scope();
    reader
        .load_registered(std::sync::Arc::new(bytes), 0..length, 1 << 20)
        .unwrap();
    let rows = reader
        .query(Query::new())
        .unwrap()
        .fetch(1)
        .unwrap()
        .objects()
        .unwrap();
    let value = &rows[0].iter().find(|(k, _)| k == "checkpoints").unwrap().1;
    let expected = [
        ("worker", 1000.0, 10250.0),
        ("before_collection", 1000.0, 2000.0),
        ("collection", 3000.0, 3000.0),
        ("after_collection", 6000.0, 5250.0),
        ("presentation", 12000.0, 500.0),
    ];
    assert_eq!(
        value,
        &ciborium::Value::Array(
            expected
                .into_iter()
                .map(|(name, start, duration)| ciborium::Value::Map(vec![
                    ("name".into(), name.into()),
                    ("start".into(), start.into()),
                    ("duration".into(), duration.into())
                ]))
                .collect()
        )
    );
}

#[test]
fn major_generations_ingest_the_same_hour_independently_and_query_together() {
    let first = doc::new("query")
        .version(1, 0)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("leaf")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .field(Field::new("query").r#type("tstr"))
        .sign()
        .unwrap();
    let minor = doc::new("query")
        .version(1, 1)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("leaf")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .field(Field::new("query").r#type("tstr"))
        .field(Field::new("extra").r#type("bool"))
        .compound(first.bytes().unwrap())
        .sign()
        .unwrap();
    let second = doc::new("query")
        .version(2, 0)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("leaf")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .field(Field::new("query").r#type("uint").size(4))
        .compound(minor.bytes().unwrap())
        .sign()
        .unwrap();
    let target = || Registration {
        cache: "demo",
        service: "trunk-client",
        resource: "queries",
        resource_type: "query",
        level: "",
        file: "query.rx",
    };
    let (tree, a) = Taxonomy::new("Ingress")
        .unwrap()
        .registration(target(), &first)
        .unwrap();
    let (tree, b) = tree.registration(target(), &minor).unwrap();
    let (tree, c) = tree.registration(target(), &second).unwrap();
    let mut node = Trunk::open(tree.bytes()).unwrap();
    node.prescribe_file("query.rx", second.bytes().unwrap())
        .unwrap();
    let scope = node
        .trie()
        .branches()
        .select([a.branch, c.branch])
        .packs()
        .unwrap();
    let mut old = scope
        .ingress(a.branch, IngressLimits::default(), HOUR)
        .unwrap();
    let mut new = scope
        .ingress(c.branch, IngressLimits::default(), HOUR)
        .unwrap();
    let a_bytes = encode(&node.definition(a.definition).unwrap(), HOUR + 10, "old");
    let b_bytes = encode(&node.definition(b.definition).unwrap(), HOUR + 20, "minor");
    let current = node.definition(c.definition).unwrap();
    let seed = current
        .ingest((), |_, s| {
            s.set_uint("time", HOUR + 30)?
                .set_uint("leaf", 37u16)?
                .set_uint("query", 42u32)?;
            Ok(())
        })
        .unwrap();
    seed.cap().unwrap();
    let c_bytes = seed.bytes().unwrap();
    if let Ok(path) = std::env::var("TRUNK_MAJOR_FIXTURE") {
        let path = std::path::Path::new(&path);
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(
            path.join("catalog.json"),
            serde_json::to_vec_pretty(&tree.catalog("demo").unwrap()).unwrap(),
        )
        .unwrap();
        for (name, bytes) in [
            ("trunk.trie", tree.bytes()),
            ("query.rx", second.bytes().unwrap()),
            ("v1.pil", a_bytes.as_slice()),
            ("v1-minor.pil", b_bytes.as_slice()),
            ("v2.pil", c_bytes.as_ref()),
        ] {
            std::fs::write(path.join(name), bytes).unwrap();
        }
    }

    old.load(a.branch, &a_bytes, HOUR + 100).unwrap();
    old.load(b.branch, &b_bytes, HOUR + 100).unwrap();
    // A rollback is routed into the retained generation, never a new resource.
    old.load(a.branch, &a_bytes, HOUR + 100).unwrap();
    new.load(c.branch, &c_bytes, HOUR + 100).unwrap();
    assert!(old.load(c.branch, &c_bytes, HOUR + 100).is_err());
    assert!(new.load(a.branch, &a_bytes, HOUR + 100).is_err());
    assert_eq!(old.count(HOUR), 3);
    assert_eq!(new.count(HOUR), 1);
    let mut query = scope.reader_scope();
    for (major, ingress) in [(1, &mut old), (2, &mut new)] {
        let sealed = ingress.seal(HOUR, 2 * HOUR + 900_000).unwrap();
        if let Ok(path) = std::env::var("TRUNK_MAJOR_FIXTURE") {
            std::fs::write(
                std::path::Path::new(&path).join(format!("v{major}.pac")),
                &sealed.pack,
            )
            .unwrap();
        }
        let length = sealed.pack.len();
        query
            .load_registered(std::sync::Arc::new(sealed.pack), 0..length, 1 << 20)
            .unwrap();
    }
    let batch = query.query(Query::new()).unwrap().fetch(10).unwrap();
    assert_eq!(
        batch.len(),
        4,
        "one logical query retains both typed generations"
    );
    let old_leaves = node.trie().branches().select([a.branch]).leaves().unwrap();
    let mut writer = old_leaves.create().unwrap();
    writer.register(a.branch).unwrap();
    writer.register(b.branch).unwrap();
    assert!(writer.register(c.branch).is_err());
}
