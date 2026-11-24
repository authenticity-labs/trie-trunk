#![cfg(feature = "registration")]
use data_seeds::{Field, SeedDefinition, doc, metadata::Value, registration::Trunk};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

#[test]
fn explicit_annotations_supply_shared_mappings_without_changing_definitions() {
    let bytes = include_bytes!("../../../schemas/logs/log.http.rx");
    let document = doc::read(bytes).unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let metadata = |name| {
        definition
            .metadata(definition.prescription().field_ordinal(name).unwrap())
            .unwrap()
    };
    let level = metadata("level");
    assert_eq!(level.value(&Value::Integer(17)).unwrap().label, "ERROR");
    assert_eq!(level.value(&Value::Integer(20)).unwrap().label, "ERROR4");
    assert_eq!(level.group(20).unwrap().label, "ERROR");
    assert!(level.value(&Value::Integer(25)).is_none());
    assert!(level.group(0).is_none());
    let status = metadata("status");
    assert_eq!(
        status.value(&Value::Integer(404)).unwrap().label,
        "Not Found"
    );
    assert_eq!(status.group(599).unwrap().label, "Server Error");
    assert!(status.value(&Value::Integer(599)).is_none());
    let method = metadata("method");
    let get = method.value(&Value::Text("GET".into())).unwrap();
    assert_eq!((get.safe, get.idempotent), (Some(true), Some(true)));
    assert!(
        method.value(&Value::Text("get".into())).is_none(),
        "methods are case sensitive"
    );
    assert!(method.open);

    let (registry, reference) = Taxonomy::new("Metadata")
        .unwrap()
        .registration(
            Registration {
                cache: "demo",
                service: "lab",
                resource: "requests",
                resource_type: "http",
                level: "",
                file: "http.rx",
            },
            &document,
        )
        .unwrap();
    let mut trunk = Trunk::open(registry.bytes()).unwrap();
    trunk.prescribe(reference.definition, bytes).unwrap();
    let fields = trunk.describe(reference.definition).unwrap()["fields"]
        .as_array()
        .unwrap();
    let described = fields.iter().find(|f| f["name"] == "level").unwrap();
    assert_eq!(described["metadata"]["name"], "log.level.group");
    assert_eq!(described["metadata"]["values"][5]["label"], "ERROR");
    assert_eq!(described["metadata"]["values"][5]["value"], 5);
    let severity = fields.iter().find(|f| f["name"] == "severity").unwrap();
    assert_eq!(severity["source"], "level");
    assert_eq!(severity["metadata"]["values"][4]["value"], 4);
    assert_eq!(severity["metadata"]["values"].as_array().unwrap().len(), 5);
    assert_eq!(described["type"], "U8");
    assert_eq!(
        described["values"],
        serde_json::json!([]),
        "labels are not codec enum values"
    );
    assert_eq!(document.bytes().unwrap(), bytes);
}

#[test]
fn mapping_follows_annotations_not_field_names_or_namespace() {
    let document = doc::new("http")
        .namespace("log")
        .field(Field::new("status").r#type("uint").size(2))
        .field(
            Field::new("response")
                .r#type("uint")
                .size(2)
                .annotation("@otel(http.response.status_code)"),
        )
        .field(
            Field::new("wrong_type")
                .r#type("tstr")
                .annotation("@otel(SeverityNumber)"),
        )
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    assert!(definition.metadata(0).is_none());
    assert_eq!(definition.metadata(1).unwrap().name, "log.http.status");
    assert!(definition.metadata(2).is_none());
    assert!(definition.metadata(100).is_none());
}

#[test]
fn open_mappings_preserve_unknown_values_through_ingestion() {
    let document = doc::read(include_bytes!("../../../schemas/logs/log.http.rx")).unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let seed = definition
        .ingest((), |_, seed| {
            seed.set_uint("time", 1_800_000_000_000u64)?
                .set_byte_string("id", &[7; 10])?
                .set_uint("level", 0u8)?
                .set_uint("status", 599u16)?
                .set_text_string("method", "CUSTOM")?;
            Ok(())
        })
        .unwrap();
    let mut pack = definition.fill(1).unwrap();
    pack.load(seed).unwrap();
    pack.seal().unwrap();
    let mut packs = data_seeds::Packs::new();
    packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    let rows = packs
        .query(data_seeds::Query::new())
        .unwrap()
        .fetch(1)
        .unwrap()
        .objects()
        .unwrap();
    for (name, value) in [
        ("level", ciborium::Value::Integer(0.into())),
        ("status", ciborium::Value::Integer(599.into())),
        ("method", ciborium::Value::Text("CUSTOM".into())),
    ] {
        assert_eq!(
            rows[0]
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, v)| v),
            Some(&value)
        );
    }
}
