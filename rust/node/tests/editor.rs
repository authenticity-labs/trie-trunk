use data_seeds::ports::import::{Column, Mapping};
fn mapping() -> Mapping {
    serde_json::from_value(serde_json::json!({"name":"reviewed","time_field":"at","time_format":"unix_ms","fields":[
        {"name":"at","kind":"uint","size":6},
        {"source":"duration","name":"elapsed","kind":"uint","size":16,"annotations":["@unit(us)","#summ(numeric)","#percentile(95)","@range(0,340282366920938463463374607431768211455)"]},
        {"name":"status","kind":"uint","size":2,"values":["200","404"],"annotations":["#summ(categorical)"]},
        {"name":"secret","kind":"any","size":0,"exclude":true}
    ]})).unwrap()
}
#[test]
fn reviewed_mapping_preserves_names_annotations_and_exact_integers() {
    let plan = mapping().prepare().unwrap();
    let fields = &plan.document.definitions()[0].fields;
    assert!(fields.iter().any(|f| f.name == "elapsed"
        && f.size == 16
        && f.annotations.contains(&"@unit(us)".into())));
    assert!(
        !fields
            .iter()
            .any(|f| f.name == "duration" || f.name == "secret")
    );
    plan.encode(br#"{"at":1700000000000,"duration":340282366920938463463374607431768211455,"status":200,"secret":{"anything":true}}"#,[0;10]).unwrap();
    assert!(
        plan.encode(
            br#"{"at":1700000000000,"duration":1,"status":500}"#,
            [0; 10]
        )
        .is_err()
    );
    assert!(
        plan.encode(
            br#"{"at":1700000000000,"duration":1,"status":200,"unmapped":1}"#,
            [0; 10]
        )
        .is_err()
    );
}
#[test]
fn bounds_reject_outside_values_without_truncation() {
    let mut m = mapping();
    m.fields[1].annotations = vec!["@min(10)".into(), "@max(20)".into()];
    let p = m.prepare().unwrap();
    for n in [10, 20] {
        p.encode(
            format!(r#"{{"at":1,"duration":{n},"status":200}}"#).as_bytes(),
            [0; 10],
        )
        .unwrap();
    }
    for n in [9, 21] {
        assert!(
            p.encode(
                format!(r#"{{"at":1,"duration":{n},"status":200}}"#).as_bytes(),
                [0; 10]
            )
            .is_err()
        );
    }
    p.encode(br#"{"at":1,"duration":null,"status":200}"#, [0; 10])
        .unwrap();
    for annotations in [
        vec!["@range(20,10)"],
        vec!["@range(-1,10)"],
        vec!["@min(1)", "@range(1,2)"],
        vec!["@min(nope)"],
        vec!["@typo(1)"],
        vec!["#summ(boolean)"],
        vec!["#percentile(101)", "#summ(numeric)"],
    ] {
        m.fields[1].annotations = annotations.into_iter().map(String::from).collect();
        assert!(m.prepare().is_err(), "{:?}", m.fields[1].annotations);
    }
}
#[test]
fn editor_rejects_collisions_and_event_identity_overrides() {
    let mut m = mapping();
    m.fields[1].name = "at".into();
    assert!(m.prepare().is_err());
    let mut m = mapping();
    m.fields[0].exclude = true;
    assert!(m.prepare().is_err());
    let mut m = mapping();
    m.fields[0].annotations = vec!["#summ(numeric)".into()];
    assert!(m.prepare().is_err());
    let mut m = mapping();
    m.fields[2].values = vec!["200".into(), "200".into()];
    assert!(m.prepare().is_err());
    let mut m = mapping();
    m.fields[1].kind = "float".into();
    m.fields[1].size = 4;
    m.fields[1].annotations = vec![];
    assert!(
        m.prepare()
            .unwrap()
            .encode(br#"{"at":1,"duration":0.1,"status":200}"#, [0; 10])
            .is_err()
    );
}
#[test]
fn changing_timestamp_name_does_not_change_the_source_mapping() {
    let mut m = mapping();
    m.fields[0].source = Some("at".into());
    m.fields[0].name = "timestamp".into();
    let plan = m.prepare().unwrap();
    assert_eq!(plan.document.definitions()[0].fields[0].name, "timestamp");
    plan.encode(br#"{"at":1,"duration":1,"status":200}"#, [0; 10])
        .unwrap();
}
#[test]
fn category_text_is_a_real_enum_and_summaries_use_generic_classes() {
    let mut m = mapping();
    m.fields[2] = Column {
        name: "status".into(),
        kind: "tstr".into(),
        size: 0,
        values: vec!["\"ok\"".into(), "\"failed\"".into()],
        annotations: vec!["#summ(ordinal)".into()],
        ..Default::default()
    };
    let p = m.prepare().unwrap();
    assert_eq!(p.document.definitions().len(), 1);
    p.encode(br#"{"at":1,"duration":1,"status":"ok"}"#, [0; 10])
        .unwrap();
    assert!(
        p.encode(br#"{"at":1,"duration":1,"status":"other"}"#, [0; 10])
            .is_err()
    );
}

#[test]
fn import_validates_unsampled_bounds_before_registering_anything() {
    let root=std::env::temp_dir().join(format!("trunk-editor-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let node=trunk_node::Node::install(&root,"Editor Test").unwrap();
    let before=std::fs::read(root.join("trunk.trie")).unwrap();
    let mut m=mapping();m.fields[1].annotations=vec!["@range(0,30)".into()];
    let input=b"{\"at\":1700000000000,\"duration\":10,\"status\":200}\n{\"at\":1700000000001,\"duration\":31,\"status\":200}\n";
    let token=node.upload(trunk_node::Destination{cache:"local".into(),service:"lab".into(),resource:"review".into(),resource_type:"event".into(),file:"event.rx".into()},m,input.len() as u64).unwrap();
    node.append(&token,0,input).unwrap();
    let error=node.import(&token).err().expect("out-of-range row accepted").to_string();
    assert!(error.contains("bounds"),"{error}");
    assert_eq!(std::fs::read(root.join("trunk.trie")).unwrap(),before);
    drop(node);std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provider_annotations_match_fixed_text_and_reject_enum_storage() {
    let mut m=mapping();
    m.fields[2]=Column{name:"method".into(),kind:"tstr".into(),size:8,annotations:vec!["@otel(http.request.method)".into()],..Default::default()};
    let p=m.prepare().unwrap();let definition=data_seeds::SeedDefinition::prescribe(&p.document).unwrap();
    let ordinal=definition.prescription().field_ordinal("method").unwrap();
    assert_eq!(definition.metadata(ordinal).unwrap().field,"http.request.method");
    m.fields[2].values=vec!["\"GET\"".into()];assert!(m.prepare().is_err());
}
