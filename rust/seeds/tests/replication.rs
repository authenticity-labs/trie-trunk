use data_seeds::{replication::PackStream, *};
use std::sync::Arc;

fn ingress() -> (Arc<SeedDefinition>, Vec<u8>) {
    let document = doc::new("transport")
        .field(Field::new("sequence").r#type("uint").size(4))
        .field(Field::new("text").r#type("tstr"))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut pack = definition.fill(3).unwrap();
    for i in 0..3 {
        pack.load(
            definition
                .ingest(i, |i, seed| {
                    seed.set_uint("sequence", i)?
                        .set_text_string("text", if i == 0 { "a" } else { "different" })?;
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let bytes = pack.into_bytes().unwrap();
    (definition, bytes)
}

#[test]
fn stepping_stops_at_each_pack_and_preserves_fragment_boundaries() {
    let (_, raw) = ingress();
    let bytes = [raw.as_slice(), raw.as_slice()].concat();
    for split in 0..=bytes.len() {
        let mut stream = PackStream::new(raw.len());
        let mut received = vec![];
        for chunk in [&bytes[..split], &bytes[split..]] {
            let chunk = Arc::new(chunk.to_vec());
            let mut at = 0;
            while at < chunk.len() {
                let (next, pack) = stream.step(chunk.clone(), at).unwrap();
                assert!(next > at);
                if let Some(pack) = pack {assert_eq!(pack.bytes(), raw);received.push(pack);}
                at = next;
            }
        }
        assert_eq!(received.len(), 2);
        assert_eq!(stream.finish().unwrap(), 2);
    }
}

#[test]
fn adjacent_packs_accept_every_network_split_without_waiting_for_eof() {
    let (definition, raw) = ingress();
    let bytes = [raw.as_slice(), raw.as_slice()].concat();
    for split in 0..=bytes.len() {
        let mut stream = PackStream::new(raw.len());
        let mut received = vec![];
        stream
            .load(Arc::new(bytes[..split].to_vec()), |pack| {
                received.push(pack);
                Ok(())
            })
            .unwrap();
        assert_eq!(received.len(), split / raw.len());
        stream
            .load(Arc::new(bytes[split..].to_vec()), |pack| {
                received.push(pack);
                Ok(())
            })
            .unwrap();
        assert_eq!(stream.finish().unwrap(), 2);
        let mut packs = Packs::new();
        for pack in received {
            assert_eq!(pack.bytes(), raw);
            packs.pack(pack.open(&definition).unwrap());
        }
        assert_eq!(
            packs.query(Query::new()).unwrap().fetch(10).unwrap().len(),
            6
        );
    }
}

#[test]
fn complete_chunks_are_shared_and_fragmented_packs_are_owned() {
    let (_, raw) = ingress();
    let chunk = Arc::new([raw.as_slice(), raw.as_slice()].concat());
    let mut stream = PackStream::new(raw.len());
    let mut received = vec![];
    stream
        .load(chunk.clone(), |p| {
            received.push(p);
            Ok(())
        })
        .unwrap();
    assert_eq!(received[0].bytes().as_ptr(), chunk.as_ptr());
    assert_eq!(received[1].bytes().as_ptr(), chunk[raw.len()..].as_ptr());
    drop(chunk);
    assert_eq!(stream.finish().unwrap(), 2);
    assert_eq!(received[1].bytes(), raw);
    let mut stream = PackStream::new(raw.len());
    let mut result = None;
    for byte in &raw {
        stream
            .load(Arc::new(vec![*byte]), |p| {
                result = Some(p);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(stream.finish().unwrap(), 1);
    assert_eq!(result.unwrap().bytes(), raw);
}

#[test]
fn limits_truncation_callback_failure_and_trailing_junk_fail_closed() {
    let (_, raw) = ingress();
    let mut stream = PackStream::new(raw.len() - 1);
    assert!(
        stream
            .load(Arc::new(raw.clone()), |_| panic!("oversize accepted"))
            .is_err()
    );
    assert!(stream.finish().is_err());
    for n in 1..raw.len() {
        let mut stream = PackStream::new(raw.len());
        stream
            .load(Arc::new(raw[..n].to_vec()), |_| {
                panic!("truncated pack accepted")
            })
            .unwrap();
        assert!(stream.finish().is_err());
    }
    let mut stream = PackStream::new(raw.len());
    assert!(
        stream
            .load(Arc::new(raw.clone()), |_| Err(Error::Invalid(
                "storage rejected"
            )))
            .is_err()
    );
    assert_eq!(stream.count(), 0);
    assert!(stream.load(Arc::new(raw.clone()), |_| Ok(())).is_err());
    let mut stream = PackStream::new(raw.len());
    let mut bad = raw;
    bad.push(1);
    assert!(stream.load(Arc::new(bad), |_| Ok(())).is_err());
    assert!(stream.finish().is_err());
    assert_eq!(PackStream::new(0).finish().unwrap(), 0);
}

#[test]
fn hour_and_lifecycle_survive_shared_reads_and_sealing() {
    let (definition, _) = ingress();
    let mut pack = definition.fill(1).unwrap();
    pack.hour(7_200_000).unwrap();
    pack.header("source", b"example".to_vec()).unwrap();
    pack.load(
        definition
            .ingest(42, |value, seed| {
                seed.set_uint("sequence", value)?
                    .set_text_string("text", "live")?;
                Ok(())
            })
            .unwrap(),
    )
    .unwrap();
    let snapshot = pack.snapshot().unwrap();
    let mut bytes = vec![0; 17];
    bytes.extend_from_slice(&snapshot);
    let owner = Arc::new(bytes);
    let opened = definition
        .open_range(owner.clone(), 17..owner.len())
        .unwrap();
    assert_eq!(opened.stored_bytes().as_ptr(), owner[17..].as_ptr());
    assert_eq!(opened.hour().unwrap(), Some(7_200_000));
    assert_eq!(opened.headers()["source"], b"example");
    assert!(!opened.is_sealed());
    assert!(pack.hour(7_200_001).is_err());
    pack.seal().unwrap();
    assert!(pack.hour(10_800_000).is_err());
    assert!(pack.header("source", b"changed".to_vec()).is_err());
    let sealed = definition.open(pack.into_bytes().unwrap()).unwrap();
    assert_eq!(sealed.hour().unwrap(), opened.hour().unwrap());
    assert!(sealed.is_sealed());
    assert!(!opened.is_sealed());
    let mut malformed = definition.fill(0).unwrap();
    malformed.header("seed.hour", vec![0]).unwrap();
    malformed.seal().unwrap();
    assert!(
        definition
            .open(malformed.into_bytes().unwrap())
            .unwrap()
            .hour()
            .is_err()
    );
}

#[cfg(feature = "compression")]
#[test]
fn compressed_packs_are_self_framed_and_queryable() {
    let (definition, raw) = ingress();
    for algorithm in [
        pil_codec::compression::Algorithm::Gzip,
        pil_codec::compression::Algorithm::Zstd,
        pil_codec::compression::Algorithm::Brotli,
    ] {
        let mut compressed = vec![];
        pil_codec::compression::seal(&raw, algorithm, 1, false, &mut compressed).unwrap();
        let mut stream = PackStream::new(compressed.len());
        let mut packs = Packs::new();
        for chunk in compressed.chunks(3) {
            stream
                .load(Arc::new(chunk.to_vec()), |pack| {
                    packs.pack(pack.open(&definition)?);
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(stream.finish().unwrap(), 1);
        assert_eq!(
            packs.query(Query::new()).unwrap().fetch(10).unwrap().len(),
            3
        );
    }
}

#[test]
fn ingress_uses_explicit_version_with_original_object_pointer_ordinals() {
    use data_seeds::{SeedDefinition, doc};
    let document = doc::new("http")
        .namespace("log")
        .version(0, 1)
        .field(doc::Field::new("status").r#type("uint").size(2))
        .field(doc::Field::new("address").object(2))
        .definition("http")
        .namespace("log")
        .version(0, 0)
        .field(doc::Field::new("message").r#type("tstr").annotation("@old"))
        .definition("address")
        .field(doc::Field::new("city").r#type("tstr"))
        .select(0)
        .sign()
        .unwrap();
    let default = SeedDefinition::prescribe(&document).unwrap();
    let older =
        SeedDefinition::prescribe_member(&document, document.member("log", "http", 0, 0).unwrap())
            .unwrap();
    assert_eq!(older.annotations(0).unwrap(), ["@old"]);
    let mut old_seed = older
        .ingest("old event", |text, seed| {
            seed.set_text_string("message", text)?;
            Ok(())
        })
        .unwrap();
    old_seed.cap().unwrap();
    assert_eq!(old_seed.json().unwrap(), br#"{"message":"old event"}"#);
    let mut seed = default
        .ingest(201, |status, seed| {
            seed.set_uint("status", status)?;
            seed.set_native(
                1,
                Some(ciborium::Value::Map(vec![(
                    ciborium::Value::Text("city".into()),
                    ciborium::Value::Text("Boston".into()),
                )])),
            )?;
            Ok(())
        })
        .unwrap();
    seed.cap().unwrap();
    let rendered: serde_json::Value = serde_json::from_slice(&seed.json().unwrap()).unwrap();
    assert_eq!(
        rendered,
        serde_json::json!({"status":201,"address":{"city":"Boston"}})
    );
    assert!(SeedDefinition::prescribe_member(&document, 3).is_err());
}
