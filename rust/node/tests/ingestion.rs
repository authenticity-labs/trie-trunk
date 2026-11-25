use data_seeds::{Field, Predicate, Query, TIME_HOUR_MS as HOUR, doc};
use trunk_node::{Destination, Node, Storage};

#[test]
fn durable_ingestion_live_reads_restart_hourly_commit_and_daily_analytics() {
    lifecycle(Storage::Browse, false);
}

#[test]
fn packed_storage_uses_the_same_ingestion_recovery_queries_and_analytics() {
    lifecycle(Storage::Pack, false);
}

#[test]
fn state_paths_survive_sealing_and_summary_recovery() {
    for storage in [Storage::Browse, Storage::Pack] { lifecycle(storage, true); }
}

fn lifecycle(storage_mode: Storage, factored: bool) {
    let root = std::env::temp_dir().join(format!("trunk-ingest-{storage_mode:?}-{factored}-{}", std::process::id()));
    struct Clean(std::path::PathBuf);
    impl Drop for Clean {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _clean = Clean(root.clone());
    let node = Node::install_with(&root, "Independent Trunk", storage_mode).unwrap();
    let doc = doc::new("sample")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(3)
                .annotation("#summ(numeric)"),
        )
        .field(Field::new("message").r#type("tstr"))
        .field(Field::new("status").r#type("uint").size(2))
        .sign()
        .unwrap();
    let branch = node
        .register_request(
            &data_seeds::registration::Request {
                cache: "local".into(),
                service: "lab".into(),
                resource: "sample".into(),
                resource_type: "sample".into(),
                file: "sample.rx".into(), level:String::new(),selection:None,
                binding:if factored {std::collections::BTreeMap::from([("path".into(),serde_json::json!(["message","duration"]))])} else {Default::default()},
            },
            doc.bytes().unwrap(),
        )
        .unwrap();
    let definition = data_seeds::SeedDefinition::prescribe(&doc).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let hour = now / HOUR * HOUR;
    for (i, value) in [10, 20, 30].into_iter().enumerate() {
        let id = [i as u8; 10];
        let seed = definition
            .ingest((), |_, s| {
                s.set_uint("time", hour)?
                    .set_byte_string("id", &id)?
                    .set_uint("duration", value)?
                    .set_text_string("message", "instrument reading completed")?;
                match i {
                    0 => { s.set_null("status")?; }
                    1 => { s.set_uint("status", 200)?; }
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
        seed.cap().unwrap();
        node.ingest(branch, &seed.bytes().unwrap(), now).unwrap();
    }
    fn rows(node: &Node, branch: u16, hour: u64) -> usize {
        let mut cursor = node
            .query(branch, hour, hour + HOUR, Query::default())
            .unwrap();
        let page = cursor.fetch(100).unwrap();
        let objects = page.objects().unwrap();
        assert!(objects.iter().all(|row| row.iter().any(|(name, value)|
            name == "message" && value.as_text() == Some("instrument reading completed"))));
        let mut values: Vec<_> = objects.iter().map(|row| {
            row.iter().find(|(name, _)| name == "duration").unwrap().1
                .as_integer().map(i128::from).unwrap()
        }).collect();
        values.sort();
        assert_eq!(values, [10, 20, 30]);
        page.len()
    }
    let check_filtered_pages = |node: &Node| {
        let fields = ["duration".to_owned()];
        let mut plan = data_seeds::language::parse(
            "SELECT COUNT(*) FROM events WHERE duration > 10", &fields,
        ).unwrap()["plan"].clone();
        for (text, expected) in [("reading", "2"), ("missing text", "0")] {
            plan["search"] = serde_json::json!({"mode":"contains","fields":["message"],"text":text});
            let result = node.summary_table(&[branch], hour, hour + HOUR, &plan, &serde_json::json!({})).unwrap();
            assert_eq!(result["rows"][0][0]["$integer"], expected);
        }
        // A partial page must resume the same ordered result across ties in event time.
        let page = Query::new()
            .filter(Predicate::Or(vec![
                Predicate::IsNull("status".into()),
                Predicate::IsAbsent("status".into()),
            ]))
            .select(["duration"])
            .order_by("duration", data_seeds::Direction::Descending)
            .limit(2);
        let mut cursor = node.query(branch, hour, hour + HOUR, page).unwrap();
        for duration in [30u64, 10] {
            assert_eq!(cursor.fetch(1).unwrap().objects().unwrap(),
                vec![vec![("duration".into(), duration.into())]]);
        }
        assert!(cursor.fetch(1).unwrap().is_empty());
        for (predicate, duration) in [(Predicate::IsNull("status".into()), 10u64),
                                      (Predicate::IsAbsent("status".into()), 30)] {
            let query = Query::new().filter(predicate).select(["duration"]);
            assert_eq!(node.query(branch, hour, hour + HOUR, query).unwrap().fetch(2).unwrap().objects().unwrap(),
                vec![vec![("duration".into(), duration.into())]]);
        }
    };
    let paths = node.trie().route(branch).unwrap().event_storage().unwrap();
    let base = root.join(paths.directory).join(paths.stem);
    let spool_len = std::fs::metadata(format!("{}.seeds", base.display())).unwrap().len();
    assert_eq!(node.storage_usage(branch).unwrap(), [(hour, 3, spool_len)]);
    check_filtered_pages(&node);
    assert_eq!(rows(&node, branch, hour), 3);
    assert_eq!(
        node.timeline(&[branch], hour, hour + HOUR).unwrap()["count"],
        3
    );
    let later = node.timeline(&[branch], hour + HOUR, hour + 2 * HOUR).unwrap();
    assert_eq!(later["count"], 0);
    assert!(later["first"].as_u64().unwrap() <= hour);
    let live = node
        .summaries(
            &[branch],
            hour,
            hour + HOUR,
            300_000,
            "duration",
            &Predicate::All,
        )
        .unwrap();
    assert_eq!(live["summary"]["count"], 3, "{live}");
    drop(node);
    let node = Node::open(&root).unwrap();
    assert_eq!(node.configuration().storage, storage_mode);
    assert_eq!(rows(&node, branch, hour), 3);
    let filtered = Query::new().filter(Predicate::And(vec![
        Predicate::compare("time", data_seeds::Compare::Eq, hour),
        Predicate::compare("duration", data_seeds::Compare::Eq, 20),
    ])).select(["duration"]);
    let live_rows = node.query(branch, hour, hour + HOUR, filtered.clone()).unwrap()
        .fetch(10).unwrap().objects().unwrap();
    assert_eq!(live_rows, vec![vec![("duration".to_owned(), 20u64.into())]]);
    node.commit(hour + HOUR + 900_000).unwrap();
    check_filtered_pages(&node);
    let file = std::fs::read(format!("{}{}", base.display(), storage_mode.suffix())).unwrap();
    let body = data_seeds::packs::footer_range(&file, file.len() as u64).unwrap().start;
    let index = std::fs::metadata(format!("{}.indx", base.display())).unwrap().len();
    assert_eq!(node.storage_usage(branch).unwrap(), [(hour, 3, body + index)]);
    let sealed_rows = node.query(branch, hour, hour + HOUR, filtered).unwrap()
        .fetch(10).unwrap().objects().unwrap();
    assert_eq!(sealed_rows, live_rows);
    let after_last = Query::new().filter(Predicate::compare("time", data_seeds::Compare::Gt, hour));
    let mut empty = node.query(branch, hour, hour + HOUR, after_last).unwrap();
    assert!(empty.fetch(10).unwrap().is_empty());
    assert_eq!(empty.scanned, 0);
    // All three IDs have the same millisecond. The index supplies candidates
    // and the remaining UUID bits select exactly one event.
    let prefix = format!("{hour:012x}");
    let lookup = |millis: &str| Query::new().filter(Predicate::compare("id", data_seeds::Compare::Eq,
        format!("{}-{}-7101-8101-010101010101", &millis[..8], &millis[8..])))
        .select(["duration"]);
    let mut hit = node.query(branch, hour, hour + HOUR, lookup(&prefix)).unwrap();
    assert_eq!(hit.fetch(4).unwrap().objects().unwrap(), live_rows);
    let mut miss = node.query(branch, hour, hour + HOUR, lookup(&format!("{:012x}", hour+1))).unwrap();
    assert!(miss.fetch(4).unwrap().is_empty());
    assert_eq!(miss.scanned, 0);
    assert_eq!(rows(&node, branch, hour), 3);
    let day = hour / 86_400_000 * 86_400_000;
    let node = if factored {
        let route=node.trie().route(branch).unwrap();
        let paths=route.event_storage().unwrap();
        let base=root.join(paths.directory).join(paths.stem);
        let raw=std::fs::read(format!("{}.indx",base.display())).unwrap();
        let len=raw.len();
        let index=data_seeds::HourIndex::open(std::sync::Arc::new(raw),0..len,hour,data_seeds::IndexFields::from_definition(&definition).unwrap()).unwrap();
        let ids=(0..3).map(|i|index.time_leaf(i).unwrap().1).collect::<Vec<_>>();
        assert_eq!(ids,[2,3,4],"one shared message prefix and three duration leaves");
        let bytes=std::fs::read(root.join(route.leaf_path().unwrap())).unwrap();
        let leaves=node.trie().branches().select([branch]).leaves().unwrap().read(&bytes).unwrap();
        assert_eq!(leaves.len(),4);
        let file=std::fs::read(format!("{}{}",base.display(),storage_mode.suffix())).unwrap();
        let time=std::fs::read(format!("{}.time",base.display())).unwrap();
        let slot=data_seeds::TimeTrie::open(std::sync::Arc::new(time)).unwrap().leaf(hour).unwrap().unwrap();
        let offset=if storage_mode==Storage::Pack {slot.packs} else {slot.beds} as usize;
        let end=data_seeds::packs::footer_range(&file,file.len() as u64).unwrap().start as usize;
        let scope=node.trie().branches().select([branch]).packs().unwrap();
        let raw_pack=scope.open_pack(std::sync::Arc::new(file),offset..end,16<<20).unwrap();
        let mut raw=data_seeds::Packs::new();
        raw.pack(raw_pack);
        let physical=raw.query(Query::new()).unwrap().fetch(10).unwrap().objects().unwrap();
        assert_eq!(physical.len(),3);
        assert!(physical.iter().flatten().all(|(key,_)|key!="duration" && key!="message"));
        let mut q=node.query(branch,hour,hour+HOUR,Query::new().filter(Predicate::compare("duration",data_seeds::Compare::Eq,99))).unwrap();
        assert!(q.fetch(10).unwrap().is_empty());
        assert_eq!(q.scanned,0);
        drop(q);
        drop(node);
        let node=Node::open(&root).unwrap();
        assert_eq!(rows(&node,branch,hour),3);
        // Starting the next open hour restores unfinished daily summaries from the sealed hour.
        let at=hour+HOUR;
        let seed=definition.ingest((),|_,s|{s.set_uint("time",at)?.set_byte_string("id",&[8;10])?.set_uint("duration",40)?;Ok(())}).unwrap();
        seed.cap().unwrap();
        node.ingest(branch,&seed.bytes().unwrap(),at).unwrap();
        let summary=node.summaries(&[branch],hour,hour+HOUR,300_000,"duration",&Predicate::All).unwrap();
        assert_eq!(summary["summary"]["count"],3,"{summary}");
        assert_eq!(summary["summary"]["statistics"]["duration"]["average"],20.0,"{summary}");
        node
    } else {node};
    node.commit(day + 86_400_000 + 900_000).unwrap();
    let sealed = node
        .summaries(
            &[branch],
            hour,
            hour + HOUR,
            300_000,
            "duration",
            &Predicate::All,
        )
        .unwrap();
    assert_eq!(sealed["summary"]["count"], 3, "{sealed}");
    drop(node);
    let node = Node::open(&root).unwrap();
    assert_eq!(rows(&node, branch, hour), 3);
    assert_eq!(
        node.summaries(
            &[branch],
            hour,
            hour + HOUR,
            300_000,
            "duration",
            &Predicate::All
        )
        .unwrap()["summary"]["count"],
        3
    );
    let storage = node.trie().route(branch).unwrap().storage().unwrap();
    assert!(
        root.join(format!("{}/{}{}", storage.directory, storage.stem, storage_mode.suffix()))
            .exists()
    );
    assert!(
        !root
            .join(format!("{}/{}{}", storage.directory, storage.stem,
                if storage_mode == Storage::Browse { ".packs" } else { ".beds" }))
            .exists()
    );
    drop(node);
    let time_path = root.join(format!("{}/{}.time", storage.directory, storage.stem));
    let before = std::fs::read(&time_path).unwrap();
    std::fs::write(root.join(".trunk/node.json"), serde_json::to_vec(&trunk_node::Configuration {
        name: "Independent Trunk".into(), storage: storage_mode.alternative(),
    }).unwrap()).unwrap();
    let error = match Node::open(&root) { Ok(_) => panic!("configuration silently converted storage"), Err(error) => error };
    assert!(error.to_string().contains("does not convert stored data"), "{error}");
    assert_eq!(std::fs::read(time_path).unwrap(), before);
    assert!(!root.join(format!("{}/{}{}", storage.directory, storage.stem, storage_mode.alternative().suffix())).exists());
}

#[test]
fn acknowledged_pil_survives_native_process_termination() {
    use std::io::{Read,Write};
    let root=std::path::PathBuf::from(format!("/tmp/trunk-crash-{}",std::process::id()));
    struct Clean(std::path::PathBuf);
    impl Drop for Clean{fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}}
    let _clean=Clean(root.clone());
    let node=Node::install(&root,"Recovery").unwrap();
    let doc=doc::new("sample")
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("duration").r#type("uint").size(3).annotation("#summ(numeric)"))
        .sign().unwrap();
    let branch=node.register(&Destination{cache:"local".into(),service:"lab".into(),resource:"recovery".into(),resource_type:"sample".into(),file:"sample.rx".into()},doc.bytes().unwrap()).unwrap();
    drop(node);
    let socket=root.join("node.sock");
    let mut child=std::process::Command::new(env!("CARGO_BIN_EXE_trunk-node"))
        .args(["serve",root.to_str().unwrap(),socket.to_str().unwrap()])
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::null()).spawn().unwrap();
    let start=std::time::Instant::now();
    while !socket.exists(){assert!(start.elapsed().as_secs()<5);std::thread::sleep(std::time::Duration::from_millis(5));}
    let time=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let definition=data_seeds::SeedDefinition::prescribe(&doc).unwrap();
    let pill=definition.ingest((),|_,s|{s.set_uint("time",time)?.set_byte_string("id",&[7;10])?.set_uint("duration",42)?;Ok(())}).unwrap();pill.cap().unwrap();
    let payload=serde_json::to_vec(&serde_json::json!({"op":"ingest","branch":branch,"bytes":pill.bytes().unwrap().as_ref()})).unwrap();
    let mut stream=std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    stream.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();stream.write_all(&payload).unwrap();
    let mut size=[0;4];stream.read_exact(&mut size).unwrap();let mut response=vec![0;u32::from_le_bytes(size) as usize];stream.read_exact(&mut response).unwrap();
    let response:serde_json::Value=serde_json::from_slice(&response).unwrap();assert_eq!(response["value"]["durable"],true,"{response}");
    child.kill().unwrap();child.wait().unwrap();
    let node=Node::open(&root).unwrap();
    let page=node.query(branch,time,time+1,Query::default()).unwrap().fetch(10).unwrap();assert_eq!(page.len(),1);
    assert!(page.objects().unwrap()[0].iter().any(|(name,value)|name=="duration"&&value.as_integer().map(i128::from)==Some(42)));
    let hour=time/HOUR*HOUR;
    assert_eq!(node.summaries(&[branch],hour,hour+HOUR,300_000,"duration",&Predicate::All).unwrap()["summary"]["count"],1);
}
