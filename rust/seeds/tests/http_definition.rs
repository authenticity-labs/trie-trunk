use data_seeds::{Query, SeedDefinition, doc};
use pil_codec::{Scalar, Type};

#[test]
fn independent_http_definition_preserves_widths_and_roundtrips_ingested_values() {
    let document = doc::read(include_bytes!("../../../schemas/logs/log.http.rx")).unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let fields = definition.prescription().definition().fields();
    let duration = definition.prescription().field_ordinal("duration").unwrap();
    assert_eq!(fields[duration].kind, Type::Scalar(Scalar::U32));
    assert!(
        definition
            .annotations(duration)
            .unwrap()
            .iter()
            .any(|a| a == "@unit(us)")
    );
    assert!(
        fields
            .iter()
            .all(|f| !f.name.starts_with("http.") && !f.name.starts_with("log."))
    );
    let seed = definition
        .ingest((), |_, seed| {
            seed.set_uint("time", 1_800_000_000_000u64)?
                .set_byte_string("id", &[7; 10])?
                .set_uint("level", 24u8)?
                .set_uint("status", 503u16)?
                .set_uint("duration", u32::MAX)?
                .set_any(
                    "message",
                    ciborium::Value::Text("Service unavailable".into()),
                )?;
            seed.set_array(
                "attributes",
                vec![ciborium::Value::Map(vec![(
                    ciborium::Value::Text("request_size".into()),
                    ciborium::Value::Integer(42.into()),
                )])],
            )?;
            Ok(())
        })
        .unwrap();
    let mut pack = definition.fill(1).unwrap();
    pack.load(seed).unwrap();
    pack.seal().unwrap();
    let stored = definition.open(pack.into_bytes().unwrap()).unwrap();
    let mut query = data_seeds::Packs::new();
    query.pack(stored);
    let rows = query
        .query(Query::new())
        .unwrap()
        .fetch(1)
        .unwrap()
        .objects()
        .unwrap();
    assert_eq!(
        rows[0]
            .iter()
            .find(|(name, _)| name == "duration")
            .map(|(_, value)| value),
        Some(&ciborium::Value::Integer(u32::MAX.into()))
    );
    assert_eq!(
        rows[0]
            .iter()
            .find(|(name, _)| name == "level")
            .map(|(_, value)| value),
        Some(&ciborium::Value::Integer(6.into()))
    );
}

#[cfg(feature = "registration")]
#[test]
fn logger_styles_share_the_registered_layout_and_preserve_extensions() {
    use ciborium::Value;
    use data_seeds::seed::log::Style;
    let def = SeedDefinition::read(include_bytes!("../../../schemas/logs/log.http.rx")).unwrap();
    let logger = def.logger(Style::OpenTelemetry).unwrap();
    let mut event = logger.event(1_800_000_000_000_000_000, &[7; 10]).unwrap();
    assert!(
        event
            .set("Timestamp", 1_800_000_000_000_000_001u64)
            .is_err()
    );
    assert!(event.set("http.response.status_code", -1).is_err());
    let native = def.logger(Style::Trunk).unwrap();
    assert!(
        native
            .event(0, &[0; 10])
            .unwrap()
            .field("http.response.status_code", 400)
            .is_err()
    );
    event.set("http.response.status_code", 503).unwrap();
    event.field("order.id", "A17").unwrap();
    event
        .field(
            "extra",
            Value::Array(vec![Value::Null, Value::Bytes(vec![0, 255])]),
        )
        .unwrap();
    let raw = event.encode().unwrap();
    let mut pack = def.fill(1).unwrap();
    pack.load(
        def.prescription()
            .bind(pil_codec::api::Fill::Encoded(&raw))
            .unwrap(),
    )
    .unwrap();
    pack.seal().unwrap();
    let mut packs = data_seeds::Packs::new();
    packs.pack(def.open(pack.into_bytes().unwrap()).unwrap());
    let rows = packs
        .query(Query::new())
        .unwrap()
        .fetch(1)
        .unwrap()
        .objects()
        .unwrap();
    let get = |name| &rows[0].iter().find(|(n, _)| n == name).unwrap().1;
    assert_eq!(get("status"), &Value::Integer(503.into()));
    let attrs = get("attributes").as_array().unwrap()[0].as_map().unwrap();
    assert!(attrs.contains(&("order.id".into(), "A17".into())));
    assert!(attrs.contains(&(
        "extra".into(),
        Value::Array(vec![Value::Null, Value::Bytes(vec![0, 255])])
    )));
}

#[cfg(feature = "registration")]
#[test]
fn application_log_options_come_from_its_rx() {
    use data_seeds::{Field, seed::log::Style};
    let doc = doc::new("alarm")
        .field(
            Field::new("observed")
                .r#type("uint")
                .size(8)
                .annotation("@time(us)"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@log"))
        .field(
            Field::new("code")
                .r#type("uint")
                .size(1)
                .annotation("@otel(alarm.code)"),
        )
        .sign()
        .unwrap();
    let def = SeedDefinition::prescribe(&doc).unwrap();
    let plan = def.logger(Style::OpenTelemetry).unwrap();
    assert!(!plan.custom_fields());
    assert_eq!(
        plan.options()
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        ["observed", "alarm.code"]
    );
    let mut event = plan.event(123_456, &[0; 10]).unwrap();
    assert!(event.field("application.note", true).is_err());
    event.set("alarm.code", 7).unwrap();
    assert!(event.encode().is_ok());
}
