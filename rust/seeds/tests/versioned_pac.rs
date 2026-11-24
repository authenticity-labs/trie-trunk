use data_seeds::{doc, Field, SeedDefinition};
use pil_codec::pac::versions::{Directory, PackIndex};
use std::{collections::BTreeMap, sync::Arc};

fn source(variable: bool) -> Arc<SeedDefinition> {
    let mut document = doc::new("deployment").version(0, u32::from(variable));
    if variable {
        document = document.field(Field::new("message").r#type("tstr"));
    }
    let document = document
        .field(Field::new("sequence").r#type("uint").size(if variable { 8 } else { 3 }))
        .field(Field::new("accepted").r#type("bool"))
        .sign().unwrap();
    SeedDefinition::prescribe(&document).unwrap()
}

#[test]
fn ingestion_rollbacks_use_version_ranges_over_one_member_directory() {
    let a = source(false);
    let b = source(true);
    let references = [7u64, 9, 7, 9, 7, 9, 9, 9, 7];
    let mut directory = Directory::default();
    let mut members = Vec::new();
    for (ordinal, reference) in references.into_iter().enumerate() {
        let definition = if reference == 7 { &a } else { &b };
        let message = format!("event {ordinal}");
        let seed = definition.ingest(ordinal, |ordinal, seed| {
            if reference == 9 { seed.set_text_string("message", &message)?; }
            seed.set_uint("sequence", ordinal as u64)?;
            if ordinal != 2 { seed.set_bool("accepted", ordinal % 2 == 0)?; }
            if ordinal == 3 { seed.squeeze()?; }
            Ok(())
        }).unwrap();
        seed.cap().unwrap();
        members.push(seed.bytes().unwrap().into_owned());
        directory.append(reference, 1).unwrap();
    }
    let headers = BTreeMap::from([("owner".to_owned(), b"test".to_vec())]);
    let borrowed = members.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let snapshot = pil_codec::pac::versions::assemble(&borrowed, &directory, &headers).unwrap();
    let index = PackIndex::open(&snapshot).unwrap();
    assert!(!index.versions().is_sealed());
    assert_eq!(index.versions().ranges().len(), 7);
    assert_eq!(pil_codec::pac::application_header(&snapshot, "owner").unwrap(), Some(&b"test"[..]));
    directory.seal();
    let bytes = pil_codec::pac::versions::assemble(&borrowed, &directory, &headers).unwrap();
    let index = PackIndex::open(&bytes).unwrap();
    let mut reader = index.reader();
    let mut a_reader = a.prescription().decoder();
    let mut b_reader = b.prescription().decoder();
    for ordinal in [0,1,2,3,4,5,6,7,8,0,8,3] {
        let (reference, raw) = reader.member(ordinal).unwrap();
        assert_eq!(reference, references[ordinal as usize]);
        assert_eq!(raw, members[ordinal as usize]);
        let row = if reference == 7 { a_reader.read(raw).unwrap() } else { b_reader.read(raw).unwrap() };
        let object = row.crush().unwrap();
        assert_eq!(object["sequence"], Some(serde_json::json!(ordinal)));
        if ordinal == 2 {
            assert_eq!(row.mode(), data_seeds::EncodingMode::FixedSparse);
            assert!(!object.contains_key("accepted"));
        } else {
            assert_eq!(object["accepted"], Some(serde_json::json!(ordinal % 2 == 0)));
        }
        if ordinal == 3 { assert_eq!(row.mode(), data_seeds::EncodingMode::VariableConcise); }
        if ordinal == 0 { assert_eq!(row.mode(), data_seeds::EncodingMode::FixedFull); }
        if ordinal == 1 { assert_eq!(row.mode(), data_seeds::EncodingMode::VariableFast); }
        if reference == 9 { assert_eq!(object["message"], Some(serde_json::json!(format!("event {ordinal}")))); }
    }
    assert!(reader.member(9).is_err());
    // A single-prescription reader must not silently interpret mixed members.
    assert!(a.prescription().open(&bytes).is_err());
    assert!(PackIndex::open(&bytes[..bytes.len()-1]).is_err());
    #[cfg(feature="compression")]
    {
        let mut compressed = Vec::new();
        pil_codec::compression::seal(&bytes, pil_codec::compression::Algorithm::Zstd, 1, false, &mut compressed).unwrap();
        let mut expanded = Vec::new();
        assert!(pil_codec::compression::unseal_with_limit(&compressed, bytes.len(), &mut expanded).unwrap());
        assert_eq!(expanded, bytes);
        assert_eq!(PackIndex::open(&expanded).unwrap().member(8).unwrap().0, 7);
    }
}
