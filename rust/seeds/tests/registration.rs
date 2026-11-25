#![cfg(feature = "registration")]
use data_seeds::{doc, Field, Packs, Query, LeafTrie, registration::Trunk};
use std::sync::Arc;
use pil_codec::api::FieldBytes;
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

#[test]
fn shipped_event_lineages_bind_to_their_registered_branches() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata");
    for (directory, file) in [
        ("http-ingress", "query.rx"),
        ("major-generations", "query.rx"),
        ("registered-trunk", "sample.rx"),
        ("registered-indexed", "indexed.rx"),
    ] {
        let path = fixtures.join(directory);
        let raw = std::fs::read(path.join("trunk.trie")).unwrap();
        let taxonomy = Taxonomy::open(raw.as_slice()).unwrap();
        let registry = taxonomy.definitions().unwrap();
        let mut trunk = Trunk::open(raw.as_slice()).unwrap();
        let rx = std::fs::read(path.join(file)).unwrap();
        trunk.prescribe_file(file, &rx).unwrap();
        let mut bound = 0;
        for resource in registry.resources() {
            if registry.definitions()[resource.definition as usize].file == file {
                let packs = trunk.trie().branches().select([resource.branch]).packs().unwrap();
                packs.definition(resource.branch).unwrap();
                bound += 1;
            }
        }
        assert!(bound > 0, "no registered event branches in {directory}");
        let unrelated = doc::new("unrelated").field(Field::new("other").r#type("bool")).sign().unwrap();
        assert!(trunk.prescribe_file(file, unrelated.bytes().unwrap()).is_err());
        trunk.prescribe_file(file, &rx).unwrap();
    }
}

#[test]
fn historical_annotations_reuse_the_event_codec_after_restart_and_rollback() {
    let make = |selected: &str| {
        let mut document = doc::new("http").namespace("log");
        for name in ["duration", "size"] {
            let mut field = Field::new(name).r#type("uint").size(4);
            if name == selected { field = field.annotation("#summ(numeric)"); }
            document = document.field(field);
        }
        document.sign().unwrap()
    };
    let before = make("duration");
    let after = make("size");
    let target = || Registration { cache: "demo", service: "web", resource: "requests",
        resource_type: "http", level: "", file: "http.rx" };
    let (first, resource) = Taxonomy::new("Test").unwrap().registration(target(), &before).unwrap();
    let (second, same) = first.registration(target(), &after).unwrap();
    assert_eq!(resource.definition, same.definition);
    assert_eq!(resource.branch, same.branch);
    let (rollback, _) = second.registration(target(), &before).unwrap();
    let library = data_seeds::summaries::summary_template().unwrap();
    let rollback = rollback.register_summaries(resource.branch, "summary.rx", &library).unwrap();
    let mut node = Trunk::open(rollback.bytes()).unwrap();
    node.prescribe_file("http.rx", before.bytes().unwrap()).unwrap();
    node.prescribe_file("summary.rx", library.bytes().unwrap()).unwrap();
    let current = node.definition(resource.definition).unwrap();
    let stale = rollback.bytes().to_vec();
    let mut published = rollback;
    let mut storage = None;
    let mut daily = Vec::new();
    for (revision, field) in [(0, "duration"), (1, "size"), (2, "duration")] {
        let historical = node.definition_at(resource.definition, revision).unwrap();
        assert!(Arc::ptr_eq(current.prescription(), historical.prescription()));
        assert_eq!(historical.summary_fields().iter().map(|f| f.name()).collect::<Vec<_>>(), [field]);
        let plan = historical.summaries(288).unwrap();
        let first = revision as u64 * data_seeds::summaries::SUMMARY_DAY_MS;
        let mut accumulator = plan.accumulator(first).unwrap();
        let event = accumulator.ingest(first, (10u32, 200u32), |(duration, size), seed| {
            seed.set_uint("duration", duration)?.set_uint("size", size)?;
            Ok(())
        }).unwrap();
        event.cap().unwrap();
        assert!(!event.bytes().unwrap().is_empty());
        accumulator.complete(accumulator.end()).unwrap();
        published = published.register_summary_context(resource.branch,
            trie_trunk_taxonomy::registration::SummaryContext { first, end: first + data_seeds::summaries::SUMMARY_DAY_MS,
                sources: vec![[resource.definition, revision as u32]], sealed: true }).unwrap();
        let pack = accumulator.seal_registered(&published, resource.branch).unwrap();
        daily.push(pack.bytes().to_vec());
        let mut reader = data_seeds::summaries::SummaryReader::pack(plan.layout().clone(), pack.bytes().to_vec(), 0).unwrap();
        reader.rows(&data_seeds::summaries::SummaryWindow::new(0..300_000).unwrap(), |_, count, object| {
            assert_eq!(count, 1);
            assert_eq!(object[0].0, field);
            let sum = object[0].1.as_map().unwrap().iter().find(|(k, _)| k.as_text() == Some("sum")).unwrap();
            assert_eq!(sum.1, ciborium::Value::from(if field == "duration" { 10 } else { 200 }));
            Ok(())
        }).unwrap();
        if storage.is_none() {
            storage = Some(data_seeds::summaries::SummaryBox::new(plan.layout().clone()).unwrap());
        }
        storage.as_mut().unwrap().append(pack).unwrap();
        if revision == 1 {
            let file = storage.as_mut().unwrap();
            file.commit().unwrap();
            storage = Some(data_seeds::summaries::SummaryBox::open(file.bytes().unwrap().to_vec()).unwrap());
        }
    }
    assert!(node.definition_at(resource.definition, 3).is_err());
    assert!(node.definition_at(u32::MAX, 0).is_err());
    let mut storage = storage.unwrap();
    storage.commit().unwrap();
    let raw = pil_codec::r#box::Box::open(storage.bytes().unwrap()).unwrap();
    assert!(raw.headers().keys().all(|key| !key.starts_with("summary.bindings")));
    assert!(raw.headers().keys().all(|key| !key.starts_with("summary.rx")));
    assert_eq!(raw.headers()["summary.source"].len(), 8);
    let mut restored = Trunk::open(published.bytes()).unwrap();
    restored.prescribe_file("http.rx", before.bytes().unwrap()).unwrap();
    restored.prescribe_file("summary.rx", library.bytes().unwrap()).unwrap();
    for (index, bytes) in daily.iter().enumerate() {
        let first = index as u64 * data_seeds::summaries::SUMMARY_DAY_MS;
        let mut reader = data_seeds::summaries::SummaryReader::registered(&restored, resource.branch, bytes.clone(), first).unwrap();
        reader.rows(&data_seeds::summaries::SummaryWindow::new(first..first+300_000).unwrap(), |_, count, object| {
            assert_eq!(count, 1);
            assert_eq!(object[0].0, if index == 1 { "size" } else { "duration" });
            Ok(())
        }).unwrap();
    }
    assert!(data_seeds::summaries::SummaryReader::registered(&node, resource.branch, daily[0].clone(), 0).is_err());
    let mut reader = data_seeds::summaries::SummaryReader::registered_box(
        &restored, resource.branch, storage.bytes().unwrap().to_vec(), 0).unwrap();
    let mut names = Vec::new();
    reader.rows(&data_seeds::summaries::SummaryWindow::new(0..3 * data_seeds::summaries::SUMMARY_DAY_MS).unwrap(), |_, count, object| {
        if count != 0 { names.push(object[0].0.clone()); }
        Ok(())
    }).unwrap();
    assert_eq!(names, ["duration", "size", "duration"]);
    // Removing current annotations must not make the branch's old summaries
    // unreadable or require compiling a current summary plan first.
    let disabled = make("");
    let (disabled_tree, _) = published.registration(target(), &disabled).unwrap();
    let mut disabled_node = Trunk::open(disabled_tree.bytes()).unwrap();
    disabled_node.prescribe_file("http.rx", disabled.bytes().unwrap()).unwrap();
    disabled_node.prescribe_file("summary.rx", library.bytes().unwrap()).unwrap();
    assert!(disabled_node.definition(resource.definition).unwrap().summary_fields().is_empty());
    let mut old = data_seeds::summaries::SummaryReader::registered(&disabled_node, resource.branch, daily[0].clone(), 0).unwrap();
    old.rows(&data_seeds::summaries::SummaryWindow::new(0..300_000).unwrap(), |_, _, object| {
        assert_eq!(object[0].0, "duration"); Ok(())
    }).unwrap();
    if let Ok(path) = std::env::var("TRUNK_SUMMARY_HISTORY_FIXTURE") {
        let path = std::path::Path::new(&path);
        std::fs::create_dir_all(path).unwrap();
        for (file, bytes) in [("trunk.trie", published.bytes()), ("unpublished.trie", stale.as_slice()), ("http.rx", before.bytes().unwrap()),
            ("summary.rx", library.bytes().unwrap()), ("summaries.box", storage.bytes().unwrap())] {
            std::fs::write(path.join(file), bytes).unwrap();
        }
        for (index, bytes) in daily.iter().enumerate() { std::fs::write(path.join(format!("day-{index}.pac")), bytes).unwrap(); }
    }
}

#[test]
fn registered_definitions_drive_ingestion_and_queries_across_bundle_reordering() {
    let old = doc::new("sample").namespace("measurements").version(1,0)
        .field(Field::new("sequence").r#type("uint").size(6).annotation("#summ(numeric)"))
        .field(Field::new("count").r#type("uint").size(16))
        .field(Field::new("note").r#type("tstr").annotation("@display(message)"))
        .field(Field::new("extra").array("any").annotation("@fields"))
        .sign().unwrap();
    let bundle = doc::new("sample").namespace("measurements").version(1,1)
        .field(Field::new("label").r#type("tstr"))
        .compound(old.bytes().unwrap()).sign().unwrap();
    let target = || Registration {cache:"demo",service:"lab",resource:"measurements",
        resource_type:"sample",level:"",file:"sample.rx"};
    let (before, first) = Taxonomy::new("Test").unwrap().registration(target(), &old).unwrap();
    let (after, latest) = before.registration(target(), &bundle).unwrap();
    assert_eq!(after.definition(first.definition).unwrap().member,1);
    let mut prepared = Trunk::open(after.bytes()).unwrap();
    assert!(prepared.prescribe_file("missing.rx", bundle.bytes().unwrap()).is_err());
    assert!(prepared.prescribe_file("sample.rx", old.bytes().unwrap()).is_err());
    assert!(prepared.definition(first.definition).is_err(), "failed file preparation is atomic");
    prepared.prescribe_file("sample.rx", bundle.bytes().unwrap()).unwrap();
    let prepared_old = prepared.definition(first.definition).unwrap();
    assert!(prepared.definition(latest.definition).is_ok());
    assert!(prepared.prescribe_file("sample.rx", &[]).is_err());
    prepared.prescribe_file("sample.rx", bundle.bytes().unwrap()).unwrap();
    assert!(Arc::ptr_eq(&prepared_old, &prepared.definition(first.definition).unwrap()));
    let mut registered = Trunk::open(after.bytes()).unwrap();
    let definition = registered.prescribe(first.definition,bundle.bytes().unwrap()).unwrap();
    assert_eq!(definition.annotations(0).unwrap(), ["#summ(numeric)"]);
    assert_eq!(definition.annotations(2).unwrap(), ["@display(message)"]);
    let summaries = registered.trie().branches().select([first.branch]).summaries().unwrap();
    assert_eq!(summaries.layout().rows(), 288);
    assert_eq!(summaries.fields().map(|f| f.name()).collect::<Vec<_>>(), ["sequence"]);
    assert!(registered.trie().branches().summaries().is_err());
    assert!(registered.trie().branches().select([first.branch, latest.branch]).summaries().is_err());
    assert_eq!(registered.describe(first.definition).unwrap()["fields"][0]["annotations"],
        serde_json::json!(["#summ(numeric)"]));
    assert!(registered.prescribe(first.definition,&[]).is_err());
    assert!(Arc::ptr_eq(&definition,&registered.prescribe(first.definition,bundle.bytes().unwrap()).unwrap()));
    let current = registered.prescribe(latest.definition,bundle.bytes().unwrap()).unwrap();
    assert!(current.prescription().field_ordinal("label").is_some());
    assert!(definition.prescription().field_ordinal("label").is_none());
    assert!(registered.prescribe(u32::MAX,bundle.bytes().unwrap()).is_err());
    let wrong = doc::new("sample").namespace("measurements")
        .field(Field::new("sequence").r#type("uint").size(1)).sign().unwrap();
    assert!(Trunk::open(after.bytes()).unwrap().prescribe(first.definition,wrong.bytes().unwrap()).is_err());

    let mut leaves = LeafTrie::new(definition.clone());
    let mut pack = definition.fill(3).unwrap();
    pack.header(data_seeds::registration::DEFINITION_HEADER,first.branch.to_le_bytes().to_vec()).unwrap();
    pack.hour(1_793_520_000_000).unwrap();
    pack.header("crc32c",vec![0;4]).unwrap();
    for i in 0..3u64 {
        let seed = definition.ingest(i, |i, seed| {
            seed.set_uint("sequence",i)?.set_uint("count",(1u128<<100)+i as u128)?;
            match i { 0 => {seed.set_text_string("note","observed")?;
                    let FieldBytes::Bytes(raw) = seed.get(2)? else { panic!("missing ingress note") };
                    leaves.append(0,2,raw.to_vec())?;
                },
                1 => {seed.set_null("note")?;}, _ => {} }
            seed.set_array("extra",vec![ciborium::Value::Map(vec![
                (ciborium::Value::Text("unregistered".into()),ciborium::Value::Bool(i%2==0))
            ])])?;
            Ok(())
        }).unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    let mut bytes = pack.into_bytes().unwrap();
    data_seeds::integrity::stamp(&mut bytes).unwrap();
    let mut packs = registered.trie().branches().select([first.branch]).packs().unwrap();
    packs.load(first.branch, Arc::new(bytes.clone()), 0..bytes.len(), bytes.len()).unwrap();
    let rows = packs.query(Query::new()).unwrap().fetch(10).unwrap().objects().unwrap();
    assert_eq!(rows.len(),3);
    assert!(rows[1].iter().any(|(k,v)|k=="note"&&*v==ciborium::Value::Null));
    assert!(!rows[2].iter().any(|(k,_)|k=="note"));
    // One resource can retain different lineage members without an hour or
    // version in the filename. Each Pack selects its own registered handler.
    let all = registered.trie().branches().packs().unwrap();
    let mut current_leaves = LeafTrie::new(current.clone());
    let mut next = all.fill(latest.branch, 1).unwrap();
    next.header("crc32c", vec![0; 4]).unwrap();
    next.load(current.ingest("current", |value, seed| {
        seed.set_text_string("label", value)?;
        let FieldBytes::Bytes(raw)=seed.get(0)? else { panic!("missing label") };
        current_leaves.append(0,0,raw.to_vec())?; Ok(())
    }).unwrap()).unwrap();
    next.seal().unwrap();
    let mut next_bytes = next.into_bytes().unwrap();
    data_seeds::integrity::stamp(&mut next_bytes).unwrap();
    let opened = registered.open_pack(Arc::new(next_bytes.clone()), 0..next_bytes.len(), next_bytes.len()).unwrap();
    assert!(Arc::ptr_eq(opened.definition(), &current));
    assert_eq!(opened.hour().unwrap(), None);
    assert!(Arc::ptr_eq(packs.open_pack(Arc::new(next_bytes.clone()), 0..next_bytes.len(), next_bytes.len()).unwrap().definition(), &current));
    let mut mixed = registered.trie().branches().select([first.branch]).packs().unwrap();
    let mut stream = data_seeds::replication::PackStream::new(1 << 20);
    let joined = [bytes.as_slice(), next_bytes.as_slice()].concat();
    for chunk in joined.chunks(7) {
        stream.load(Arc::new(chunk.to_vec()), |pack| {
            let snapshot = pack.open_registered(&mixed, 1 << 20)?;
            let branch = data_seeds::registration::pack_registration(snapshot.stored_bytes())?;
            mixed.pack(branch, snapshot)?;
            Ok(())
        }).unwrap();
    }
    assert_eq!(stream.finish().unwrap(), 2);
    let mixed_rows = mixed.query(Query::new()).unwrap().fetch(10).unwrap().objects().unwrap();
    assert_eq!(mixed_rows.len(), 4);
    assert_eq!(mixed_rows[3], vec![("label".into(), "current".into())]);
    assert_eq!(mixed.query(Query::new().version(1, 0)).unwrap().fetch(10).unwrap().len(), 3);
    let current_rows = mixed.query(Query::new().version(1, 1)).unwrap().fetch(10).unwrap().objects().unwrap();
    assert_eq!(current_rows, vec![vec![("label".into(), "current".into())]]);
    assert_eq!(mixed.query(Query::new().version(1, 2)).unwrap().fetch(10).unwrap().len(), 0);
    assert_eq!(mixed.query(Query::new().limit(2)).unwrap().fetch(10).unwrap().len(), 2);
    // The shared physical Box spans versions. Addresses remain supplied by the
    // time trie; each child selects its own prescription before query planning.
    let mut storage = all.create(&[]).unwrap();
    storage.pack(bytes.clone()).unwrap();
    storage.commit().unwrap();
    let mut stored = storage.bytes().unwrap().to_vec();
    let next_address = storage.next_address();
    let footer = pil_codec::r#box::Box::footer_range(&stored, stored.len() as u64).unwrap();
    let mut resumed = all.resume(stored[footer.start as usize..].to_vec(), stored.len() as u64).unwrap();
    assert_eq!(resumed.next_address(), next_address);
    resumed.pack(next_bytes.clone()).unwrap();
    let tail = resumed.commit().unwrap();
    assert_eq!(tail.offset, next_address);
    stored.truncate(tail.offset as usize);
    stored.extend_from_slice(tail.bytes);
    assert_eq!(stored.len() as u64, tail.length);
    assert_eq!(&stored[..bytes.len()], bytes.as_slice(), "append must not move existing Packs");
    storage.pack(next_bytes.clone()).unwrap(); storage.commit().unwrap();
    assert_eq!(stored, storage.bytes().unwrap(), "footer-only publication matches resident Box publication");
    let closed = resumed.seal().unwrap();
    stored.truncate(closed.offset as usize); stored.extend_from_slice(closed.bytes);
    let footer = pil_codec::r#box::Box::footer_range(&stored, stored.len() as u64).unwrap();
    assert!(all.resume(stored[footer.start as usize..].to_vec(), stored.len() as u64).is_err());
    let mut boxed = registered.trie().branches().select([latest.branch]).beds().unwrap();
    boxed.load_box(first.branch, Arc::new(storage.bytes().unwrap().to_vec()), [0, next_address]).unwrap();
    assert_eq!(boxed.query(Query::new()).unwrap().fetch(10).unwrap().len(), 4);
    assert_eq!(boxed.query(Query::new().version(1, 0)).unwrap().fetch(10).unwrap().len(), 3);
    // Both roots are built from actual values passed through Seed ingestion.
    let scope = registered.trie().branches().select([first.branch]).leaves().unwrap();
    let mut forest = scope.create().unwrap();
    forest.register(first.branch).unwrap(); forest.append(&leaves).unwrap();
    let before = forest.publish().unwrap();
    let before_end = scope.directory(&before).unwrap().nodes_end;
    forest.freeze(first.branch).unwrap();
    forest.register(latest.branch).unwrap(); forest.append(&current_leaves).unwrap();
    forest.statistics(first.branch,ciborium::Value::Map(vec![("Nodes".into(),2u64.into())])).unwrap();
    let leaf_file = forest.publish().unwrap();
    assert_eq!(&before[..before_end], &leaf_file[..before_end], "publishing another version never moves or changes existing nodes");
    let nodes_end = scope.directory(&leaf_file).unwrap().nodes_end;
    let unlabeled = leaf_file.clone();
    let leaf_file = scope.label(&leaf_file, 1, Some("Canceled Order")).unwrap();
    let next_scope = registered.trie().branches().select([latest.branch]).leaves().unwrap();
    let leaf_file = next_scope.label(&leaf_file, 1, Some("New Account")).unwrap();
    assert_eq!(&leaf_file[..nodes_end], &unlabeled[..nodes_end], "labels never rewrite state nodes");
    assert_eq!(scope.labels(&leaf_file).unwrap()[&1], "Canceled Order");
    assert_eq!(next_scope.labels(&leaf_file).unwrap()[&1], "New Account");
    assert!(scope.label(&leaf_file, 65535, Some("Missing")).is_err());
    assert!(scope.label(&leaf_file, 1, Some(" ")).is_err());
    let removed = scope.label(&leaf_file, 1, None).unwrap();
    assert!(scope.labels(&removed).unwrap().is_empty());
    assert_eq!(next_scope.labels(&removed).unwrap()[&1], "New Account");
    let resumed = scope.resume(&leaf_file).unwrap().publish().unwrap();
    assert_eq!(scope.labels(&resumed).unwrap()[&1], "Canceled Order");
    let directory = scope.directory(&leaf_file).unwrap();
    assert_eq!(directory.roots.len(),2);
    assert!(directory.roots[0].frozen);
    assert!(!directory.roots[1].frozen);
    let loaded = scope.read(&leaf_file).unwrap();
    loaded.validate().unwrap();
    assert_eq!(loaded.encode_pil(0).unwrap(),leaves.encode_pil(0).unwrap());
    assert_eq!(registered.trie().branches().select([latest.branch]).leaves().unwrap().read(&leaf_file).unwrap().encode_pil(0).unwrap(),current_leaves.encode_pil(0).unwrap());
    let selected = registered.trie().branches().select([first.branch]).leaves().unwrap().read(&leaf_file).unwrap();
    assert_eq!(selected.encode_pil(0).unwrap(),leaves.encode_pil(0).unwrap());
    let selected = registered.trie().branches().select([latest.branch]).leaves().unwrap().read(&leaf_file).unwrap();
    assert_eq!(selected.encode_pil(0).unwrap(),current_leaves.encode_pil(0).unwrap());
    let layout = scope.inspect(&leaf_file).unwrap();
    assert_eq!(layout.nodes_end,directory.nodes_end);
    assert_eq!(layout.node_count,2);
    assert_eq!(layout.blocks.len(),1);
    assert!(layout.blocks[0].local > directory.roots[0].offset + 8);
    let mut damaged = leaf_file.clone(); damaged[layout.blocks[0].local-4]^=1;
    assert!(scope.inspect(&damaged).is_err());
    assert!(scope.read(&damaged).is_ok(),"reads do not repeat integrity checks");
    // A selected version does not decode another version's node payload.
    let other = directory.roots[1].blocks[0].local;
    damaged[other]=255;
    assert!(scope.read(&damaged).is_ok());
    for cut in 0..leaf_file.len() { assert!(scope.read(&leaf_file[..cut]).is_err()); }
    let mut resumed=scope.resume(&leaf_file).unwrap();
    let mut late=leaves.clone();
    let late_seed=definition.ingest("late",|value,seed|{seed.set_text_string("note",value)?;Ok(())}).unwrap();
    let FieldBytes::Bytes(raw)=late_seed.get(2).unwrap() else {panic!("missing late value")};
    late.append(0,2,raw.to_vec()).unwrap();
    assert!(resumed.append(&late).is_err(),"retired roots cannot acquire nodes");
    assert_eq!(resumed.publish().unwrap(),leaf_file,"failed append changes no published bytes");
    let more=current.ingest("second",|value,seed|{seed.set_text_string("label",value)?;Ok(())}).unwrap();
    let FieldBytes::Bytes(raw)=more.get(0).unwrap() else {panic!("missing current value")};
    current_leaves.append(0,0,raw.to_vec()).unwrap();resumed.append(&current_leaves).unwrap();
    let grown=resumed.publish().unwrap();
    assert_eq!(&grown[..directory.nodes_end],&leaf_file[..directory.nodes_end]);
    assert_eq!(registered.trie().branches().select([latest.branch]).leaves().unwrap().read(&grown).unwrap().len(),2);
    assert_eq!(scope.read(&grown).unwrap().len(),1);

    // Root declarations survive a missing footer. Recovery never renumbers or
    // rewrites intact nodes and never runs during an ordinary open.
    let recovered=scope.recover(&leaf_file[..directory.nodes_end]).unwrap().publish().unwrap();
    let recovered_file=scope.directory(&recovered).unwrap();
    assert_eq!(&recovered[..directory.nodes_end],&leaf_file[..directory.nodes_end]);
    assert_eq!(recovered_file.roots[0].definition,directory.roots[0].definition);
    assert!(recovered_file.roots[0].frozen);
    assert_eq!(scope.read(&recovered).unwrap().len(),1);
    assert!(scope.recover(&damaged).is_err(), "complete corrupted frames must not be silently discarded");
    for cut in directory.nodes_end..leaf_file.len() {
        assert!(scope.recover(&leaf_file[..cut]).is_ok(), "footer truncation must preserve node frames");
    }
    assert!(scope.create().unwrap().register(u16::MAX).is_err());

    // Existing published snapshots keep the exact root they opened.
    assert_eq!(loaded.len(),1);
    if let Ok(path)=std::env::var("SEED_REGISTERED_FIXTURE") {
        let path=std::path::Path::new(&path);std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("trunk.trie"),after.bytes()).unwrap();
        std::fs::write(path.join("sample.rx"),bundle.bytes().unwrap()).unwrap();
        let mut storage = data_seeds::packs::Writer::new(&[]).unwrap();
        storage.pack(bytes.as_slice()).unwrap();storage.commit().unwrap();
        std::fs::write(path.join("sample.packs"),storage.bytes().unwrap()).unwrap();
        let mut current_storage = data_seeds::packs::Writer::new(&[]).unwrap();
        current_storage.pack(next_bytes.as_slice()).unwrap(); current_storage.commit().unwrap();
        std::fs::write(path.join("current.packs"),current_storage.bytes().unwrap()).unwrap();
        for (name, raw) in [("sample", &bytes), ("current", &next_bytes)] {
            let mut beds = Packs::create_box(&[]).unwrap();
            beds.pack(raw.as_slice()).unwrap(); beds.commit().unwrap();
            std::fs::write(path.join(format!("{name}.beds")), beds.bytes().unwrap()).unwrap();
        }
        // Transport/writer fixture: an empty hour followed by two ingested
        // Packs using different registered Definition versions. Time alone
        // supplies placement; no per-Pack hour field is needed.
        let mut beds = Packs::create_box(&[]).unwrap();
        beds.pack(bytes.as_slice()).unwrap(); beds.pack(next_bytes.as_slice()).unwrap();
        beds.commit().unwrap();
        std::fs::write(path.join("collection.beds"), beds.bytes().unwrap()).unwrap();
        let first_hour = 1_767_225_600_000u64;
        let mut time = vec![0u8; 64 + 366 * 24 * 16];
        time[..8].copy_from_slice(b"TTTIME00");
        for (at, value) in [(8,first_hour),(16,first_hour),(24,6),(32,first_hour+3*data_seeds::TIME_HOUR_MS)] {
            time[at..at+8].copy_from_slice(&value.to_le_bytes());
        }
        time[40..46].copy_from_slice(&[4,5,5,2,2,2]);
        time[48..64].fill(7);
        for (hour, start, address, count) in [(1,0u64,0u64,3u64),(2,4,bytes.len() as u64,1)] {
            let mut at = 64 + hour * 16;
            for (value,width) in [(start,4),(address,5),(address,5),(count,2)] {
                time[at..at+width].copy_from_slice(&value.to_le_bytes()[..width]); at+=width;
            }
        }
        data_seeds::TimeTrie::open(std::sync::Arc::new(time.clone())).unwrap();
        std::fs::write(path.join("collection.time"),time).unwrap();
        #[cfg(feature = "compression")]
        {
            let mut compressed = Vec::new();
            pil_codec::compression::seal(&bytes, pil_codec::compression::Algorithm::Zstd, 1, false, &mut compressed).unwrap();
            std::fs::write(path.join("sample.zstd.pac"), compressed).unwrap();
            let mut compressed_storage = data_seeds::packs::Writer::new(&[]).unwrap();
            compressed_storage.compression(data_seeds::Compression::default()).unwrap();
            compressed_storage.pack(bytes.as_slice()).unwrap(); compressed_storage.commit().unwrap();
            std::fs::write(path.join("sample.zstd.packs"), compressed_storage.bytes().unwrap()).unwrap();
        }
        std::fs::write(path.join("sample.pac"),bytes).unwrap();
        std::fs::write(path.join("current.pac"),next_bytes).unwrap();
        std::fs::write(path.join("sample.leaf"),leaf_file).unwrap();
        let mut only=scope.create().unwrap(); only.register(first.branch).unwrap(); only.append(&leaves).unwrap();
        std::fs::write(path.join("old.leaf"),only.publish().unwrap()).unwrap();
        std::fs::write(path.join("old-nodes.pil"),leaves.encode_pil(1).unwrap()).unwrap();
        std::fs::write(path.join("current-nodes.pil"),current_leaves.encode_pil(1).unwrap()).unwrap();
        let mut only=scope.create().unwrap(); only.register(latest.branch).unwrap(); only.append(&current_leaves).unwrap();
        std::fs::write(path.join("current.leaf"),only.publish().unwrap()).unwrap();
        std::fs::write(path.join("identities.json"),serde_json::to_vec(&[first.definition,latest.definition]).unwrap()).unwrap();
    }
}

#[test]
fn branch_packs_share_readers_and_coordinate_one_global_query() {
    use data_seeds::{Direction, SeedDefinition};
    let document = doc::new("sample")
        .field(Field::new("sequence").r#type("uint").size(6))
        .sign().unwrap();
    let register = |resource| Registration { cache:"demo", service:"lab", resource,
        resource_type:"sample", level:"", file:"sample.rx" };
    let (trie, first) = Taxonomy::new("Test").unwrap().registration(register("first"), &document).unwrap();
    let (trie, second) = trie.registration(register("second"), &document).unwrap();
    let mut trunk = Trunk::open(trie.bytes()).unwrap();
    assert!(trunk.trie().branches().packs().is_err(), "missing prescriptions are explicit");
    let definition = trunk.prescribe(first.definition, document.bytes().unwrap()).unwrap();
    trunk.prescribe(second.definition, document.bytes().unwrap()).unwrap();
    assert!(trunk.trie().branches().select([u16::MAX]).packs().is_err());
    let mut packs = trunk.trie().branches().packs().unwrap();
    let mut scoped = trunk.trie().branches().select([first.branch]).packs().unwrap();
    let mut pack = packs.fill(first.branch,2).unwrap();
    for value in [3u64, 1] {
        pack.load(definition.ingest(value, |value, seed| {
            seed.set_uint("sequence", value)?; Ok(())
        }).unwrap()).unwrap();
    }
    pack.seal().unwrap();
    let bytes = pack.into_bytes().unwrap();
    let mut container = vec![0u8;17];container.extend_from_slice(&bytes);container.extend_from_slice(&[0;9]);
    let owner = Arc::new(container);
    let range = 17..17+bytes.len();
    let snapshot = definition.open_stored_range(owner.clone(), range.clone()).unwrap();
    assert_eq!(snapshot.stored_bytes().as_ptr(), owner[range.clone()].as_ptr());
    packs.pack(first.branch, snapshot.clone()).unwrap();
    assert!(packs.load(second.branch, owner.clone(), range.clone(), bytes.len()).is_err(), "registered Pack cannot be relabeled");
    packs.load_registered(owner.clone(), range.clone(), bytes.len()).unwrap();
    assert!(scoped.load(second.branch, owner.clone(), range.clone(), bytes.len()).is_err());
    assert!(packs.load(first.branch, owner.clone(), range.clone(), bytes.len()-1).is_err());
    // An unregistered reader cannot substitute itself for this registry's
    // prepared handler, even if its authored schema happens to match.
    let independent = SeedDefinition::prescribe(&document).unwrap().open(bytes).unwrap();
    assert!(packs.pack(first.branch, independent).is_err());
    scoped.pack(first.branch, snapshot).unwrap();
    let mut box_writer = Packs::create_box(&[]).unwrap();
    box_writer.pack(&owner[range.clone()]).unwrap();
    let second_address = box_writer.next_address();
    box_writer.pack(&owner[range]).unwrap();box_writer.commit().unwrap();
    let box_bytes = Arc::new(box_writer.bytes().unwrap().to_vec());
    let mut boxed = trunk.trie().branches().select([first.branch]).packs().unwrap();
    assert!(boxed.load_box(first.branch,box_bytes.clone(),[0,u64::MAX]).is_err());
    assert_eq!(boxed.query(Query::new()).unwrap().fetch(10).unwrap().len(),0,"failed selection is not published");
    boxed.load_box(first.branch,box_bytes.clone(),[second_address]).unwrap();
    assert!(Arc::strong_count(&box_bytes)>1,"Box memory is retained, not copied");
    assert_eq!(boxed.query(Query::new()).unwrap().fetch(10).unwrap().len(),2);
    drop(boxed);assert_eq!(Arc::strong_count(&box_bytes),1);
    let empty = trunk.trie().branches().select([]).packs().unwrap();
    assert_eq!(empty.query(Query::new()).unwrap().fetch(10).unwrap().len(),0);
    // Registry replacement/release does not invalidate a running source scope.
    drop(trunk);drop(scoped);
    let mut cursor = packs.query(Query::new().order_by("sequence", Direction::Ascending).limit(3)).unwrap();
    drop(packs);
    let rows = cursor.fetch(10).unwrap().objects().unwrap();
    assert_eq!(rows.len(),3);
    assert_eq!(rows.iter().map(|row|row[0].1.clone()).collect::<Vec<_>>(),
        vec![ciborium::Value::from(1u64),1u64.into(),3u64.into()]);
    drop(cursor);
    assert_eq!(Arc::strong_count(&owner),1,"no retained buffers after the cursor is released");
}

#[test]
fn registered_pack_requires_an_exact_trie_reference() {
    let document = doc::new("minimal").field(Field::new("value").r#type("uint").size(2)).sign().unwrap();
    let (trie, target) = Taxonomy::new("Test").unwrap().registration(Registration {
        cache:"demo", service:"service", resource:"resource", resource_type:"minimal", level:"", file:"minimal.rx"
    }, &document).unwrap();
    let mut trunk = Trunk::open(trie.bytes()).unwrap();
    let definition = trunk.prescribe(target.definition, document.bytes().unwrap()).unwrap();
    let scope = trunk.trie().branches().packs().unwrap();
    for reference in [None, Some(vec![]), Some(vec![1]), Some(vec![1,0,0]), Some(vec![0,0]), Some(vec![255,255])] {
        let mut pack = definition.fill(1).unwrap();
        if let Some(value) = reference { pack.header(data_seeds::registration::DEFINITION_HEADER, value).unwrap(); }
        pack.load(definition.ingest(42u16, |value, seed| { seed.set_uint("value", value)?; Ok(()) }).unwrap()).unwrap();
        pack.seal().unwrap();
        let bytes = pack.into_bytes().unwrap(); let length = bytes.len();
        assert!(scope.open_pack(Arc::new(bytes), 0..length, length).is_err());
    }
}

#[test]
fn leaves_scope_separates_types_and_preserves_contiguous_empty_roots() {
    let old = doc::new("event").field(Field::new("value").r#type("uint").size(1)).sign().unwrap();
    let new = doc::new("event").version(0,1).field(Field::new("value").r#type("uint").size(2))
        .compound(old.bytes().unwrap()).sign().unwrap();
    let other = doc::new("other").field(Field::new("label").r#type("tstr")).sign().unwrap();
    let target = |kind, file| Registration {cache:"demo",service:"lab",resource:"events",resource_type:kind,level:"",file};
    let (trie, a) = Taxonomy::new("Test").unwrap().registration(target("http","event.rx"),&old).unwrap();
    let (trie, b) = trie.registration(target("http","event.rx"),&new).unwrap();
    let (trie, c) = trie.registration(target("security","other.rx"),&other).unwrap();
    let mut trunk=Trunk::open(trie.bytes()).unwrap();
    trunk.prescribe_file("event.rx",new.bytes().unwrap()).unwrap();
    trunk.prescribe_file("other.rx",other.bytes().unwrap()).unwrap();
    let scope=trunk.trie().branches().select([a.branch]).leaves().unwrap();
    let foreign=trunk.trie().branches().select([c.branch]).leaves().unwrap();
    let mut writer=scope.create().unwrap();
    writer.register(a.branch).unwrap();
    let before=writer.publish().unwrap();
    let end=scope.directory(&before).unwrap().nodes_end;
    writer.register(b.branch).unwrap();
    let after=writer.publish().unwrap();
    assert_eq!(&before[..end],&after[..end]);
    let directory=scope.directory(&after).unwrap();
    assert_eq!(directory.roots[1].offset,end,"even an empty root has an explicit boundary");
    assert!(directory.roots[0].frozen);
    assert!(writer.register(c.branch).is_err());
    assert!(foreign.read(&after).is_err());
    assert!(foreign.recover(&after).is_err());
    let recovered=scope.recover(&after[..directory.nodes_end]).unwrap().publish().unwrap();
    assert_eq!(scope.directory(&recovered).unwrap().roots.len(),2);
    assert!(scope.read(&recovered).unwrap().is_empty());
}

#[test]
fn leaf_commit_handoff_publishes_only_the_tail_and_accepts_after_write() {
    let first_doc = doc::new("events").field(Field::new("value").r#type("tstr")).sign().unwrap();
    let next_doc = doc::new("events").version(0,1)
        .field(Field::new("count").r#type("uint").size(2))
        .field(Field::new("value").r#type("tstr"))
        .compound(first_doc.bytes().unwrap()).sign().unwrap();
    let target = || Registration {cache:"demo",service:"lab",resource:"items",resource_type:"http",level:"",file:"events.rx"};
    let (tree, first) = Taxonomy::new("Test").unwrap().registration(target(), &first_doc).unwrap();
    let (tree, next) = tree.registration(target(), &next_doc).unwrap();
    let mut registered = Trunk::open(tree.bytes()).unwrap();
    registered.prescribe_file("events.rx",next_doc.bytes().unwrap()).unwrap();
    let scope = registered.trie().branches().select([first.branch]).leaves().unwrap();
    let mut writer = scope.create().unwrap();
    let mut disk = Vec::new();
    let mut append = |commit: &data_seeds::LeafCommit| {
        let at = commit.offset();
        disk.truncate(at);
        disk.extend_from_slice(commit.bytes());
        assert_eq!(disk.len(),commit.file_length());
    };
    let old = registered.definition(first.definition).unwrap();
    let mut nodes = writer.nodes(first.branch).unwrap();
    let seed = old.ingest("first", |value, seed| {seed.set_text_string("value",value)?;Ok(())}).unwrap();
    let FieldBytes::Bytes(raw) = seed.get(0).unwrap() else {panic!()};
    nodes.append(0,0,raw.to_vec()).unwrap();
    let commit = writer.prepare(first.branch,nodes).unwrap();
    assert_eq!(commit.offset(),0);
    append(&commit); writer.accept(commit).unwrap();
    let before = writer.publish().unwrap();
    let end = scope.directory(&before).unwrap().nodes_end;
    let current = registered.definition(next.definition).unwrap();
    let mut nodes = writer.nodes(next.branch).unwrap();
    let seed = current.ingest("next", |value, seed| {seed.set_text_string("value",value)?;Ok(())}).unwrap();
    let FieldBytes::Bytes(raw) = seed.get(1).unwrap() else {panic!()};
    nodes.append(0,1,raw.to_vec()).unwrap();
    let failed = writer.prepare(next.branch,nodes.clone()).unwrap();
    assert_eq!(failed.offset(),end);
    let expected_tail=failed.bytes().to_vec();
    drop(failed); // failed I/O: no acknowledgement, no root handoff
    assert_eq!(writer.publish().unwrap(),before);
    assert!(!scope.directory(&before).unwrap().roots[0].frozen);
    let retry=writer.prepare(next.branch,nodes).unwrap();
    assert_eq!(retry.bytes(),expected_tail);
    append(&retry); writer.accept(retry).unwrap();
    assert_eq!(disk,writer.publish().unwrap());
    assert_eq!(&disk[..end],&before[..end]);
    let roots=scope.directory(&disk).unwrap();
    assert!(roots.roots[0].frozen);
    assert_eq!(roots.roots[1].offset,end);
    assert_eq!(scope.read(&disk).unwrap().len(),1);
    assert_eq!(registered.trie().branches().select([next.branch]).leaves().unwrap().read(&disk).unwrap().len(),1);
    assert!(writer.prepare(first.branch,writer.nodes(first.branch).unwrap()).is_err());
    // Independent prepared commits cannot overwrite another publication or
    // move into a different writer, even when their byte lengths match.
    let a=writer.prepare(next.branch,writer.nodes(next.branch).unwrap()).unwrap();
    let b=writer.prepare(next.branch,writer.nodes(next.branch).unwrap()).unwrap();
    writer.accept(a).unwrap();
    assert!(writer.accept(b).is_err());
    let c=writer.prepare(next.branch,writer.nodes(next.branch).unwrap()).unwrap();
    let mut other=scope.resume(&disk).unwrap();
    assert!(other.accept(c).is_err());
}
