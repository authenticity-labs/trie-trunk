use data_seeds::{
    Query, TIME_HOUR_MS as HOUR,
    ports::import::{Column, Mapping},
};
use trunk_node::{Destination, Node};

#[test]
fn new_local_node_imports_unordered_history_and_reopens_without_an_origin() { imported_history("jsonl"); }
#[test]
fn csv_import_uses_the_same_durable_publication_summaries_and_queries() { imported_history("delimited"); }
#[test]
fn xml_import_uses_the_same_durable_publication_summaries_and_queries() { imported_history("xml"); }
fn imported_history(format:&str) {
    let root = std::env::temp_dir().join(format!("trunk-port-{format}-{}", std::process::id()));
    struct Clean(std::path::PathBuf);
    impl Drop for Clean {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _clean = Clean(root.clone());
    let node = std::sync::Arc::new(Node::install(&root, "Offline").unwrap());
    let destination = || Destination {
        cache: "local".into(),
        service: "lab".into(),
        resource: "import".into(),
        resource_type: "sample".into(),
        file: "sample.rx".into(),
    };
    let mapping = Mapping { log:false, format: data_seeds::ports::format::Profile{kind:format.into(),record:"/events/event".into(),..Default::default()}, name: "sample".into(),
        time_field: "timestamp".into(),
        time_format: "unix_ms".into(),
        fields: vec![
            Column {
                name: "timestamp".into(),
                kind: "uint".into(),
                size: 6,
                summarize: false, ..Default::default()
            },
            Column {
                name: "duration".into(),
                kind: "uint".into(),
                size: 2,
                summarize: true, ..Default::default()
            },
            Column {
                name: "message".into(),
                kind: "tstr".into(),
                size: 20,
                summarize: false, ..Default::default()
            },
        ],
    };
    let hour = 1_700_000_000_000 / HOUR * HOUR;
    let input = [hour + 2 * HOUR, hour, hour + HOUR]
        .iter()
        .map(|time| if format=="delimited"{format!("{time},42,\"hello\nworld\"\n")}else if format=="xml"{format!("<event><timestamp>{time}</timestamp><duration>42</duration><message>hello</message></event>")}else{format!("{{\"timestamp\":{time},\"duration\":42,\"message\":\"hello\"}}\n")})
        .collect::<String>();
    let input=if format=="delimited"{format!("timestamp,duration,message\n{input}")}else if format=="xml"{format!("<events>{input}</events>")}else{input};
    let token = node
        .upload(destination(), mapping.clone(), input.len() as u64)
        .unwrap();
    for (i, chunk) in input.as_bytes().chunks(13).enumerate() {
        node.append(&token, (i * 13) as u64, chunk).unwrap();
    }
    node.start_import(token.clone()).unwrap();
    let deadline=std::time::Instant::now();
    let result=loop {
        let progress=node.progress(&token).unwrap();
        if progress["done"]==true {assert!(progress["error"].is_null(),"{progress}");break progress["result"].clone();}
        assert!(deadline.elapsed()<std::time::Duration::from_secs(10));
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    assert_eq!(result["count"], 3);
    let branch = result["branch"].as_u64().unwrap() as u16;
    assert_eq!(
        node.query(branch, hour, hour + 3 * HOUR, Query::default())
            .unwrap()
            .fetch(10)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        node.summaries(
            &[branch],
            hour,
            hour + 3 * HOUR,
            300_000,
            "duration",
            &data_seeds::Predicate::All
        )
        .unwrap()["summary"]["count"],
        3
    );
    drop(node);
    let node = Node::open(&root).unwrap();
    assert_eq!(
        node.query(branch, hour, hour + 3 * HOUR, Query::default())
            .unwrap()
            .fetch(10)
            .unwrap()
            .len(),
        3
    );
    let before = std::fs::read(root.join("trunk.trie")).unwrap();
    let input:&[u8]=match format{
        "delimited"=>b"timestamp,duration,message\n1700000000000,999999999,no\n",
        "xml"=>b"<events><event><timestamp>1700000000000</timestamp><duration>999999999</duration><message>no</message></event></events>",
        _=>b"{\"timestamp\":1700000000000,\"duration\":999999999,\"message\":\"no\"}\n",
    };
    let token = node
        .upload(destination(), mapping, input.len() as u64)
        .unwrap();
    node.append(&token, 0, input).unwrap();
    assert!(node.import(&token).is_err());
    assert_eq!(std::fs::read(root.join("trunk.trie")).unwrap(), before);
    drop(node);
    let output=std::process::Command::new(env!("CARGO_BIN_EXE_trunk-node"))
        .args(["beds","query","--dir",root.to_str().unwrap(),"--scope","local/lab/import/sample","--from",&hour.to_string(),"--to",&(hour+3*HOUR).to_string(),"--sql","SELECT duration, COUNT(*) AS total FROM events GROUP BY duration","--json"])
        .output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let row:serde_json::Value=serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(row["total"]["$integer"],"3","{row}");
}

#[test]
fn full_import_validation_rejects_unseen_float_narrowing() {
    let mapping=Mapping{log:false,format:Default::default(),name:"measurement".into(),time_field:"time".into(),time_format:"unix_ms".into(),fields:vec![
        Column{name:"time".into(),kind:"uint".into(),size:6,summarize:false, ..Default::default()},
        Column{name:"value".into(),kind:"float".into(),size:4,summarize:false, ..Default::default()},
    ]};
    let plan=mapping.prepare().unwrap();
    assert!(plan.encode(br#"{"time":1700000000000,"value":1.5}"#,[0;10]).is_ok());
    assert!(plan.encode(br#"{"time":1700000000000,"value":1.0000000001}"#,[0;10]).is_err());
    assert!(plan.encode(br#"{"time":1700000000000,"value":1e300}"#,[0;10]).is_err());
}
