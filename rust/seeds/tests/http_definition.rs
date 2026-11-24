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
