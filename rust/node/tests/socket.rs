use data_seeds::{Field, HourIndexBuilder, IndexFields, TIME_HOUR_MS, TimeLeaf, TimeTrie, doc};
use std::{
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    sync::Arc,
};
use trie_trunk_taxonomy::{Trunk, registration::Registration};
use trunk_node::{Node, service::Service};

fn call(path: &std::path::Path, request: serde_json::Value) -> serde_json::Value {
    let mut stream = UnixStream::connect(path).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let bytes = serde_json::to_vec(&request).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let mut response = vec![0; u32::from_le_bytes(size) as usize];
    stream.read_exact(&mut response).unwrap();
    serde_json::from_slice(&response).unwrap()
}

#[test]
fn standalone_socket_reads_registered_beds_with_native_time_and_predicates() {
    let root = std::path::PathBuf::from(format!("/tmp/trunk-socket-{}", std::process::id()));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    drop(Node::install(&root, "Personal").unwrap());
    let _cleanup = Cleanup(root.clone());
    let document = doc::new("sample")
        .version(1, 0)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("value").r#type("uint").size(3))
        .sign()
        .unwrap();
    let (trie, registration) = Trunk::new("Personal")
        .unwrap()
        .registration(
            Registration {
                cache: "local",
                service: "lab",
                resource: "sensor",
                resource_type: "sample",
                level: "",
                file: "sample.rx",
            },
            &document,
        )
        .unwrap();
    // A newly registered major has no Packs yet. Logical discovery and reads
    // must still include the retained older generation, without duplicate UI resources.
    let newer = doc::new("sample")
        .version(2, 0)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("value").r#type("uint").size(4))
        .sign()
        .unwrap();
    let (trie, new_registration) = trie
        .registration(
            Registration {
                cache: "local",
                service: "lab",
                resource: "sensor",
                resource_type: "sample",
                level: "",
                file: "sample-v2.rx",
            },
            &newer,
        )
        .unwrap();
    fs::write(root.join("trunk.trie"), trie.bytes()).unwrap();
    fs::write(
        root.join("definitions/sample.rx"),
        document.bytes().unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("definitions/sample-v2.rx"),
        newer.bytes().unwrap(),
    )
    .unwrap();
    let mut trunk = data_seeds::registration::Trunk::open(trie.bytes()).unwrap();
    let definition = trunk
        .prescribe(registration.definition, document.bytes().unwrap())
        .unwrap();
    let scope = trunk
        .trie()
        .branches()
        .select([registration.branch])
        .beds()
        .unwrap();
    let hour = 1_700_000_000_000 / TIME_HOUR_MS * TIME_HOUR_MS;
    let mut pack = scope.fill(registration.branch, 5).unwrap();
    let mut index =
        HourIndexBuilder::new(hour, 5, IndexFields::from_definition(&definition).unwrap()).unwrap();
    for n in 0..5u64 {
        let time = hour + n * 1000;
        let id = [n as u8; 10];
        let seed = definition
            .ingest((), |_, s| {
                s.set_uint("time", time)?
                    .set_byte_string("id", &id)?
                    .set_uint("value", n)?;
                Ok(())
            })
            .unwrap();
        pack.load(seed).unwrap();
        index.append(time, 0).unwrap();
    }
    pack.seal().unwrap();
    let mut writer = scope.create(&[]).unwrap();
    let address = writer.next_beds_address();
    writer.pack(pack.into_bytes().unwrap()).unwrap();
    let commit = writer.commit().unwrap();
    let storage = root.join("local/lab/sensor");
    fs::create_dir_all(&storage).unwrap();
    assert_eq!(commit.offset, 0);
    fs::write(storage.join("sample.v1.beds"), commit.bytes).unwrap();
    let index = index.finish();
    fs::write(storage.join("sample.v1.indx"), index.bytes()).unwrap();
    let time = TimeTrie::create(hour, [1; 16], 0)
        .unwrap()
        .publish(
            hour + TIME_HOUR_MS,
            Some(TimeLeaf {
                hour,
                start: 0,
                end: 6,
                packs: address,
                beds: address,
                count: 5,
            }),
        )
        .unwrap();
    fs::write(storage.join("sample.v1.time"), time.bytes()).unwrap();
    // Optional isolated fixture for the real WebKit/bridge/socket test. Never
    // read, replace or copy a developer's live datastore.
    if let Ok(output) = std::env::var("TRUNK_TEST_EXPORT") {
        let output = std::path::PathBuf::from(output);
        fs::create_dir(&output).unwrap();
        for name in [
            ".trunk/node.json",
            "trunk.trie",
            "definitions/sample.rx",
            "definitions/sample-v2.rx",
            "local/lab/sensor/sample.v1.beds",
            "local/lab/sensor/sample.v1.time",
            "local/lab/sensor/sample.v1.indx",
        ] {
            let target = output.join(name);
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::copy(root.join(name), target).unwrap();
        }
    }
    let path = root.join("node.sock");
    let node = Arc::new(Node::open(&root).unwrap());
    let service = Service::start(node, &path).unwrap();
    let status = call(&path, serde_json::json!({"op":"status"}));
    assert_eq!(status["value"]["runtime"], "native");
    assert_eq!(status["value"]["storage"], "beds");
    let bootstrap=call(&path,serde_json::json!({"op":"bootstrap"}));
    assert_eq!(bootstrap["ok"],true,"{bootstrap}");
    let timeline=call(&path,serde_json::json!({"op":"timeline","branches":[registration.branch,new_registration.branch],"from":hour,"to":hour+TIME_HOUR_MS}));
    assert_eq!(timeline["value"]["count"],5,"{timeline}");
    let language=call(&path,serde_json::json!({"op":"language","query":{"op":"parse","text":"SELECT * FROM events WHERE value >= 2","fields":["time","id","value"]}}));
    assert_eq!(language["ok"],true,"{language}");
    let presentation=call(&path,serde_json::json!({"op":"presentation"}));
    assert_eq!(presentation["ok"],true,"{presentation}");
    let catalog = call(&path, serde_json::json!({"op":"catalog"}));
    assert_eq!(catalog["value"].as_array().unwrap().len(), 1);
    assert_eq!(
        catalog["value"][0]["route"]["branch"],
        new_registration.branch
    );
    assert_eq!(catalog["value"][0]["source"], "local/lab/sensor/sample");
    let request = serde_json::json!({"op":"query","branch":registration.branch,"from":hour+1000,"to":hour+4000,
        "query":{"where":{"field":"value","op":">=","value":{"$literal":{"kind":"string","value":"2"}}}}});
    let response = call(&path, request);
    assert_eq!(response["ok"], true, "{response}");
    let rows = response["value"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["value"], 2);
    assert_eq!(rows[1]["value"], 3);
    let newer_scope = call(
        &path,
        serde_json::json!({"op":"query","branch":new_registration.branch,"from":hour,"to":hour+TIME_HOUR_MS}),
    );
    assert_eq!(
        newer_scope["value"]["rows"].as_array().unwrap().len(),
        5,
        "{newer_scope}"
    );
    let restricted = call(
        &path,
        serde_json::json!({"op":"query","branch":new_registration.branch,"from":hour,"to":hour+TIME_HOUR_MS,"query":{"version":[2,0]}}),
    );
    assert_eq!(restricted["value"]["rows"].as_array().unwrap().len(), 0);
    assert_eq!(rows[0]["id"]["$bytes"].as_array().unwrap().len(), 16);
    let first = call(
        &path,
        serde_json::json!({"op":"query","branch":registration.branch,"from":hour,"to":hour+TIME_HOUR_MS,"page_size":2}),
    );
    let cursor = first["value"]["cursor"].as_u64().unwrap();
    assert_eq!(first["value"]["rows"][0]["value"], 0);
    let next = call(&path, serde_json::json!({"op":"fetch","cursor":cursor}));
    assert_eq!(next["value"]["rows"][0]["value"], 2);
    let last = call(&path, serde_json::json!({"op":"fetch","cursor":cursor}));
    assert_eq!(last["value"]["rows"][0]["value"], 4);
    assert!(last["value"]["cursor"].is_null());
    assert_eq!(
        call(&path, serde_json::json!({"op":"fetch","cursor":cursor}))["ok"],
        false
    );
    let rejected = call(
        &path,
        serde_json::json!({"op":"query","branch":registration.branch,"from":hour,"to":hour+1,"page_size":1001}),
    );
    assert_eq!(rejected["ok"], false);
    assert_eq!(call(&path, serde_json::json!({"op":"status"}))["ok"], true);
    // Real one-shot process boundaries: stdin plan, streamed stdout, stderr
    // statistics, exit codes, paging and direct/socket execution must agree.
    let options = ["--scope", "local/lab/sensor/*", "--from", "2023-11-14T22:00:00Z", "--to", "2023-11-14T23:00:00Z"];
    let query = |resource: &str, more: &[&str]| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_trunk-node"));
        command.args([resource,"query","--socket",path.to_str().unwrap()]).args(options).args(more);
        command.output().unwrap()
    };
    let output=query("beds", &["--json","--where","value",">=","2","--select","value","--page-size","1","--stats"]);
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    assert_eq!(output.stdout,b"{\"value\":2}\n{\"value\":3}\n{\"value\":4}\n");
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stderr).unwrap()["rows"],3);
    let table=query("beds", &["--where","value",">=","2","--select","value","--page-size","1","--stats"]);
    assert!(table.status.success(),"{}",String::from_utf8_lossy(&table.stderr));
    assert_eq!(table.stdout,b"value\n2\n3\n4\n");
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&table.stderr).unwrap()["rows"],3);
    let sql=query("beds", &["--sql","SELECT value AS reading FROM events WHERE value >= 2","--page-size","1"]);
    assert!(sql.status.success(),"{}",String::from_utf8_lossy(&sql.stderr));
    assert_eq!(sql.stdout,b"reading\n2\n3\n4\n");
    let joined = query("beds", &["--sql",
        "SELECT a.value AS first, b.value AS second FROM events a INNER JOIN events b ON a.id=b.id WHERE a.value >= 2",
        "--limit", "2", "--page-size", "1"]);
    assert!(joined.status.success(), "{}", String::from_utf8_lossy(&joined.stderr));
    assert_eq!(joined.stdout, b"first\tsecond\n2\t2\n3\t3\n");
    let sql=query("beds", &["--json","--sql","SELECT CAST(value AS DOUBLE) AS reading FROM events WHERE value >= 3"]);
    assert!(sql.status.success(),"{}",String::from_utf8_lossy(&sql.stderr));
    let rows=String::from_utf8(sql.stdout).unwrap().lines().map(|line|serde_json::from_str::<serde_json::Value>(line).unwrap()).collect::<Vec<_>>();
    assert_eq!(rows.len(),2);assert_eq!(rows[0]["reading"].as_f64(),Some(3.0));
    assert!(!query("beds", &["--sql","SELECT * FROM summaries"]).status.success());
    let wide=query("beds", &["--select","value","--select","time","--limit","2","--page-size","1"]);
    assert!(wide.status.success(),"{}",String::from_utf8_lossy(&wide.stderr));
    assert_eq!(String::from_utf8(wide.stdout).unwrap(),format!("value\ttime\n0\t{hour}\n1\t{}\n",hour+1000));
    let whole=query("beds", &["--limit","1"]);
    assert!(whole.status.success(),"{}",String::from_utf8_lossy(&whole.stderr));
    assert!(String::from_utf8(whole.stdout).unwrap().starts_with("time\tid\tvalue\n"));
    let report=query("time", &[]);
    assert!(report.status.success());assert!(String::from_utf8(report.stdout).unwrap().starts_with("Field\tValue\n"));
    let empty=query("beds", &["--where","value",">","99"]);
    assert!(empty.status.success());assert!(empty.stdout.is_empty());assert!(empty.stderr.is_empty());
    let invalid=query("beds", &["--select","missing"]);
    assert!(!invalid.status.success());assert!(invalid.stdout.is_empty());assert!(!invalid.stderr.is_empty());
    let invalid=query("beds", &["--all","--order","time"]);
    assert!(!invalid.status.success());assert!(invalid.stdout.is_empty());
    let timeline=query("time", &["--json"]);
    assert!(timeline.status.success(),"{}",String::from_utf8_lossy(&timeline.stderr));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&timeline.stdout).unwrap()["count"],5);
    let rejected=query("summaries", &[]);
    assert!(!rejected.status.success());assert!(rejected.stdout.is_empty());
    let mut child=std::process::Command::new(env!("CARGO_BIN_EXE_trunk-node"))
        .args(["beds","query","--socket",path.to_str().unwrap()]).args(options).args(["--json","--query","-"])
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    child.stdin.take().unwrap().write_all(br#"{"select":["value"],"where":{"field":"value","op":">=","value":2},"orderBy":{"field":"value","direction":"desc"},"limit":2}"#).unwrap();
    let piped=child.wait_with_output().unwrap();
    assert!(piped.status.success(),"{}",String::from_utf8_lossy(&piped.stderr));
    assert_eq!(piped.stdout,b"{\"value\":4}\n{\"value\":3}\n");
    struct Closed;
    impl Write for Closed {
        fn write(&mut self,_:&[u8])->std::io::Result<usize>{Err(std::io::ErrorKind::BrokenPipe.into())}
        fn flush(&mut self)->std::io::Result<()>{Ok(())}
    }
    for _ in 0..10 {
        let mut args=vec!["beds".into(),"query".into(),"--socket".into(),path.to_string_lossy().into_owned()];
        args.extend(options.iter().map(|s|s.to_string()));args.extend(["--page-size".into(),"1".into()]);
        let error=trunk_node::cli::run(&args,std::io::empty(),&mut Closed,&mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("pipe"),"{error}");
    }
    assert!(query("beds",&["--limit","1"]).status.success());
    drop(service);
    assert!(!path.exists());
    assert!(Node::open(&root).is_ok());
    let local=std::process::Command::new(env!("CARGO_BIN_EXE_trunk-node"))
        .args(["beds","query","--dir",root.to_str().unwrap()]).args(options)
        .args(["--json","--where","value",">=","2","--select","value","--page-size","1"]).output().unwrap();
    assert!(local.status.success(),"{}",String::from_utf8_lossy(&local.stderr));
    assert_eq!(local.stdout,output.stdout);assert!(local.stderr.is_empty());
}
