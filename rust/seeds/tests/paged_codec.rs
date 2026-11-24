use data_seeds::{Field, PackOptions, SeedDefinition, doc};
use pil_codec::api::{OwnedPack, PackMetadata};
use std::sync::Arc;

#[test]
fn metadata_member_reads_match_resident_reads_across_modes_and_commons() {
    for variable in [false, true] {
        for sparse in [false, true] {
            for concise in [false, true] {
                for common in [false, true] {
                    for identical in [false, true] {
                        let mut document = doc::new("paged")
                            .field(Field::new("n").r#type("uint").size(8))
                            .field(Field::new("flag").r#type("bool"));
                        if variable {
                            document = document.field(Field::new("text").r#type("tstr"));
                        }
                        let def = SeedDefinition::prescribe(&document.sign().unwrap()).unwrap();
                        let mut pack = def.fill(4).unwrap();
                        pack.configure(PackOptions {
                            common_values: common,
                            ..Default::default()
                        })
                        .unwrap();
                        for row in 0..4 {
                            let text = if identical {
                                "".to_owned()
                            } else {
                                "x".repeat(row * 25000)
                            };
                            let seed = def
                                .ingest(row, |row, seed| {
                                    seed.set_uint("n", if identical { 17 } else { row as u64 })?;
                                    if !sparse {
                                        seed.set_bool("flag", true)?;
                                    } else if !identical && row % 2 == 0 {
                                        seed.set_null("flag")?;
                                    }
                                    if variable {
                                        seed.set_text_string("text", &text)?;
                                    }
                                    if concise {
                                        seed.squeeze()?;
                                    }
                                    Ok(())
                                })
                                .unwrap();
                            pack.load(seed).unwrap();
                        }
                        pack.seal().unwrap();
                        let bytes = pack.into_bytes().unwrap();
                        let header = pil_codec::pac_envelope::Plain::read_prefix(&bytes)
                            .unwrap()
                            .header_length as usize;
                        let metadata = Arc::new(
                            PackMetadata::new(def.prescription().clone(), bytes[..header].to_vec())
                                .unwrap(),
                        );
                        assert_eq!(metadata.len(), 4);
                        assert_eq!(metadata.byte_length(), bytes.len());
                        assert!(metadata.is_sealed());
                        let resident = Arc::new(
                            OwnedPack::new(def.prescription().clone(), bytes.clone()).unwrap(),
                        );
                        let mut reference = resident.reader();
                        let mut reader = metadata.reader();
                        for row in [3, 0, 2, 1, 1] {
                            let range = metadata.member(row).unwrap();
                            assert_eq!(
                                reader.read(row, &bytes[range]).unwrap().crush().unwrap(),
                                reference.get(row).unwrap().crush().unwrap()
                            );
                        }
                        assert!(metadata.member(4).is_err());
                    }
                }
            }
        }
    }
}

#[test]
fn header_only_reader_checks_used_offsets_without_scanning_untouched_members() {
    let document = doc::new("directory")
        .field(Field::new("text").r#type("tstr"))
        .sign()
        .unwrap();
    let def = SeedDefinition::prescribe(&document).unwrap();
    let mut pack = def.fill(3).unwrap();
    for text in ["a", "bb", "ccc"] {
        pack.load(
            def.ingest(text, |text, seed| {
                seed.set_text_string("text", text)?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let bytes = pack.into_bytes().unwrap();
    let end = pil_codec::pac_envelope::Plain::read_prefix(&bytes)
        .unwrap()
        .header_length as usize;
    let mut header = bytes[..end].to_vec();
    let width = header[end - 1] as usize;
    assert!(width > 0);
    // Final member's end points before the preceding member. Earlier accesses
    // remain legal; accessing the bad member must fail before payload I/O.
    header[end - 1 - 3 * width..end - 1 - 2 * width].fill(255);
    let metadata = Arc::new(PackMetadata::new(def.prescription().clone(), header).unwrap());
    assert!(metadata.member(0).is_ok());
    assert!(metadata.member(2).is_err());
    let mut reader = metadata.reader();
    assert!(reader.read(0, &[]).is_err());
}
