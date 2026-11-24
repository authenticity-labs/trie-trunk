use std::sync::Arc;
use trunk_node::{Node, logs::Observation};

#[test]
fn observations_use_existing_http_definition_and_durable_ingress() {
    let root = std::env::temp_dir().join(format!("trunk-observations-{}", std::process::id()));
    let node = Arc::new(Node::install(&root, "Transport Test").unwrap());
    let logs = node.logs("test", &["packs".into()]).unwrap();
    let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    logs.write(Observation {
        resource: "packs".into(), time, duration: 250, method: "GET".into(),
        route: "/test/lab/events/http/packs".into(), status: 503, bytes: 42,
        protocol: "HTTP/2.0".into(), selection: "range=1%2C2".into(),
    }).unwrap();
    let branch = node.catalog().unwrap()[0]["route"]["branch"].as_u64().unwrap() as u16;
    let verify = |node: &Node| {
        let mut cursor = node.query(branch, time, time+1, data_seeds::Query::default()).unwrap();
        let rows = cursor.fetch(10).unwrap().objects().unwrap();
        assert_eq!(rows.len(), 1);
        let value = |field| &rows[0].iter().find(|(name,_)| name==field).unwrap().1;
        assert_eq!(value("status"), &ciborium::Value::Integer(503.into()));
        assert_eq!(value("duration"), &ciborium::Value::Integer(250.into()));
        assert_eq!(value("level"), &ciborium::Value::Integer(5.into()));
    };
    verify(&node);
    let written = std::fs::read(root.join("definitions/log.http.rx")).unwrap();
    assert_eq!(written, include_bytes!("../../../schemas/logs/log.http.rx"));
    drop(logs);
    drop(node);
    let node = Node::open(&root).unwrap();
    verify(&node);
    drop(node);
    std::fs::remove_dir_all(root).unwrap();
}
