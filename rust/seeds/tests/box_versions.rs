use data_seeds::{Field, SeedDefinition, doc};
use pil_codec::{
    r#box::{
        Box as Container, BoxOptions,
        versions::{Binding, Directory},
    },
    pac::versions::PackIndex,
};
use std::{collections::BTreeMap, sync::Arc};

fn source(variable: bool) -> Arc<SeedDefinition> {
    let mut d = doc::new("deployment")
        .version(0, u32::from(variable))
        .field(Field::new("number").r#type("uint").size(4));
    if variable {
        d = d.field(Field::new("message").r#type("tstr"));
    }
    SeedDefinition::prescribe(&d.sign().unwrap()).unwrap()
}
fn ordinary(source: &SeedDefinition, variable: bool, length: usize) -> Vec<u8> {
    ordinary_with_reference(source, variable, length, if variable { 9 } else { 7 })
}
fn ordinary_with_reference(
    source: &SeedDefinition,
    variable: bool,
    length: usize,
    reference: u8,
) -> Vec<u8> {
    let mut pack = source.fill(3).unwrap();
    pack.header("test.definition", vec![reference]).unwrap();
    let text = "v".repeat(length);
    for n in 0..3 {
        let seed = source
            .ingest(n, |n, seed| {
                seed.set_uint("number", n as u64)?;
                if variable {
                    seed.set_text_string("message", &text)?;
                }
                Ok(())
            })
            .unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    pack.into_bytes().unwrap()
}
fn mixed(a: &SeedDefinition, b: &SeedDefinition) -> Vec<u8> {
    let mut versions = pil_codec::pac::versions::Directory::default();
    let mut members = Vec::new();
    for (n, reference) in [7, 9, 7].into_iter().enumerate() {
        let source = if reference == 7 { a } else { b };
        let seed = source
            .ingest(n, |n, seed| {
                seed.set_uint("number", n as u64)?;
                if reference == 9 {
                    seed.set_text_string("message", "mixed")?;
                }
                Ok(())
            })
            .unwrap();
        seed.cap().unwrap();
        members.push(seed.bytes().unwrap().into_owned());
        versions.append(reference, 1).unwrap();
    }
    versions.seal();
    pil_codec::pac::versions::assemble(
        &members.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        &versions,
        &BTreeMap::new(),
    )
    .unwrap()
}
#[test]
fn box_offsets_select_fixed_variable_mixed_and_rollback_without_external_indexes() {
    let a = source(false);
    let b = source(true);
    let children = vec![
        ordinary(&a, false, 0),
        ordinary(&a, false, 0),
        ordinary(&b, true, 7),
        ordinary(&b, true, 400),
        mixed(&a, &b),
        mixed(&a, &b),
        ordinary(&b, true, 12),
        ordinary(&a, false, 0),
    ];
    let bindings = [
        Binding::Definition(7),
        Binding::Definition(7),
        Binding::MixedSpan,
        Binding::MixedSpan,
        Binding::MixedSpan,
        Binding::MixedSpan,
        Binding::Definition(9),
        Binding::Definition(7),
    ];
    let mut file = Container::new(
        &[],
        BoxOptions {
            fixed_length_objects: true,
            external_addresses: false,
        },
    )
    .unwrap();
    Directory::new(bindings[0]).attach(&mut file).unwrap();
    let mut disk = file.commit().unwrap().bytes.to_vec();
    let mut starts = Vec::new();
    let mut preserved = Vec::new();
    for (child, binding) in children.iter().zip(bindings) {
        let footer = Container::footer_range(&disk, disk.len() as u64).unwrap();
        let mut file =
            Container::open_footer_owned(disk[footer.start as usize..].to_vec(), disk.len() as u64)
                .unwrap();
        let mut versions = file.versions().unwrap().unwrap();
        let start = file.next_address();
        starts.push(start);
        if file.options().fixed_length_objects
            && !file.is_empty()
            && file.member_range(0).unwrap().end != child.len() as u64
        {
            file.indexed().unwrap();
        }
        file.pack(child.clone()).unwrap();
        versions.change(start, binding).unwrap();
        versions.attach(&mut file).unwrap();
        let commit = file.commit().unwrap();
        assert_eq!(commit.offset, footer.start);
        disk.truncate(commit.offset as usize);
        disk.extend_from_slice(commit.bytes);
        preserved.extend_from_slice(child);
        assert_eq!(&disk[..preserved.len()], preserved);
    }
    let file = Container::open(&disk).unwrap();
    let versions = file.versions().unwrap().unwrap();
    assert_eq!(
        versions
            .entries()
            .iter()
            .map(|e| (e.offset, e.binding))
            .collect::<Vec<_>>(),
        [
            (starts[0], bindings[0]),
            (starts[2], bindings[2]),
            (starts[6], bindings[6]),
            (starts[7], bindings[7])
        ]
    );
    assert_ne!(children[2].len(), children[3].len());
    for i in [7, 6, 5, 0, 3, 4, 2, 1] {
        let range = file.member_range(i).unwrap();
        assert_eq!(range.start, starts[i]);
        let raw = file.get(i).unwrap();
        assert_eq!(raw, children[i]);
        let binding = versions
            .binding_at(range.start, file.next_address())
            .unwrap();
        assert_eq!(binding, bindings[i]);
        // Only the exception span needs PAC-level version resolution. Its
        // first two PACs are ordinary; the next two are mixed.
        if binding == Binding::MixedSpan
            && pil_codec::pac::flags(raw).unwrap() & pil_codec::pac::MULTI_DEFINITION != 0
        {
            let index = PackIndex::open(raw).unwrap();
            for (n, reference) in [7, 9, 7].into_iter().enumerate() {
                let (found, raw) = index.member(n as u64).unwrap();
                assert_eq!(found, reference);
                let source = if reference == 7 { &a } else { &b };
                let mut reader = source.prescription().decoder();
                assert_eq!(
                    reader.read(raw).unwrap().crush().unwrap()["number"],
                    Some(serde_json::json!(n))
                );
            }
        } else {
            let reference = match binding {
                Binding::Definition(reference) => reference,
                Binding::MixedSpan => u64::from(
                    pil_codec::pac::application_header(raw, "test.definition")
                        .unwrap()
                        .unwrap()[0],
                ),
            };
            let source = if reference == 7 { &a } else { &b };
            let pack = source.prescription().open(raw).unwrap();
            assert_eq!(pack.len(), 3);
            let mut reader = pack.reader();
            let row = reader.get(2).unwrap();
            assert_eq!(row.crush().unwrap()["number"], Some(serde_json::json!(2)));
        }
    }

    assert_eq!(
        versions.binding_at(file.next_address(), file.next_address()),
        None
    );
    let mut file = Container::open_owned(disk).unwrap();
    file.seal().unwrap();
    assert!(versions.attach(&mut file).is_err());
    #[cfg(feature = "compression")]
    {
        let bytes = file
            .compress(pil_codec::api::Compression {
                algorithm: pil_codec::compression::Algorithm::Zstd,
                level: 1,
                only_if_smaller: false,
            })
            .unwrap();
        let reopened = Container::open_compressed(&bytes).unwrap();
        assert_eq!(
            reopened.versions().unwrap().unwrap().entries(),
            versions.entries()
        );
        assert_eq!(reopened.get(4).unwrap(), children[4]);
    }
}
#[test]
fn directory_rejects_transition_into_pack_interior_from_footer_only() {
    let source = source(false);
    let mut file = Container::new(&[], Default::default()).unwrap();
    file.pack(ordinary(&source, false, 0)).unwrap();
    let mut invalid = Directory::new(Binding::Definition(7));
    invalid.change(1, Binding::Definition(9)).unwrap();
    assert!(invalid.attach(&mut file).is_err());
    // Inject corrupt metadata to exercise the reader, not only the writer.
    let mut bytes = Vec::new();
    invalid.write(&mut bytes).unwrap();
    file.header(pil_codec::r#box::versions::HEADER, bytes)
        .unwrap();
    file.commit().unwrap();
    let disk = file.bytes().unwrap();
    let footer = Container::footer_range(disk, disk.len() as u64).unwrap();
    let reader = Container::open_footer(&disk[footer.start as usize..], disk.len() as u64).unwrap();
    assert!(reader.versions().is_err());
}

#[cfg(feature = "compression")]
#[test]
fn individual_compressed_pacs_do_not_change_version_bindings() {
    let source = source(true);
    let plain = ordinary(&source, true, 200);
    let mut compressed = Vec::new();
    pil_codec::compression::seal(
        &plain,
        pil_codec::compression::Algorithm::Zstd,
        1,
        false,
        &mut compressed,
    )
    .unwrap();
    assert_ne!(plain.len(), compressed.len());
    let mut file = Container::new(&[], Default::default()).unwrap();
    let versions = Directory::new(Binding::Definition(9));
    file.pack(plain.clone()).unwrap();
    let start = file.next_address();
    file.pack(compressed.clone()).unwrap();
    versions.attach(&mut file).unwrap();
    file.commit().unwrap();
    let reopened = Container::open(file.bytes().unwrap()).unwrap();
    let directory = reopened.versions().unwrap().unwrap();
    assert_eq!(directory.entries().len(), 1);
    assert_eq!(reopened.member_range(1).unwrap().start, start);
    assert_eq!(
        directory.binding_at(start, reopened.next_address()),
        Some(Binding::Definition(9))
    );
    let mut expanded = Vec::new();
    assert!(
        pil_codec::compression::unseal_with_limit(
            reopened.get(1).unwrap(),
            plain.len(),
            &mut expanded
        )
        .unwrap()
    );
    assert_eq!(expanded, plain);
}

#[test]
fn fixed_box_plans_clean_and_exception_spans_from_footer_alone() {
    let a = source(false);
    let document = doc::new("deployment")
        .version(0, 1)
        .field(Field::new("number").r#type("uint").size(4));
    let document = document.sign().unwrap();
    let b = SeedDefinition::prescribe(&document).unwrap();
    let children = [
        ordinary_with_reference(&a, false, 0, 7),
        ordinary_with_reference(&b, false, 0, 9),
        ordinary_with_reference(&a, false, 0, 7),
        ordinary_with_reference(&b, false, 0, 9),
        ordinary_with_reference(&a, false, 0, 7),
    ];
    let stride = children[0].len() as u64;
    assert!(children.iter().all(|p| p.len() as u64 == stride));
    let mut file = Container::new(
        &[],
        BoxOptions {
            fixed_length_objects: true,
            external_addresses: false,
        },
    )
    .unwrap();
    let mut directory = Directory::new(Binding::Definition(7));
    for child in children {
        file.pack(child).unwrap();
    }
    directory.change(stride, Binding::MixedSpan).unwrap();
    directory
        .change(3 * stride, Binding::Definition(9))
        .unwrap();
    directory
        .change(4 * stride, Binding::Definition(7))
        .unwrap();
    directory.attach(&mut file).unwrap();
    file.commit().unwrap();
    let bytes = file.bytes().unwrap();
    let range = Container::footer_range(bytes, bytes.len() as u64).unwrap();
    let footer =
        Container::open_footer(&bytes[range.start as usize..], bytes.len() as u64).unwrap();
    assert!(footer.options().fixed_length_objects);
    let plan = footer.versions().unwrap().unwrap();
    for (ordinal, expected) in [
        Binding::Definition(7),
        Binding::MixedSpan,
        Binding::MixedSpan,
        Binding::Definition(9),
        Binding::Definition(7),
    ]
    .into_iter()
    .enumerate()
    {
        let extent = footer.member_range(ordinal).unwrap();
        assert_eq!(
            extent,
            ordinal as u64 * stride..(ordinal as u64 + 1) * stride
        );
        assert_eq!(
            plan.span_at(extent.start, footer.next_address())
                .unwrap()
                .binding,
            expected
        );
        assert!(footer.get(ordinal).is_err()); // No PAC bytes available to inspect.
    }
    let exception = plan.span_at(stride, footer.next_address()).unwrap();
    assert_eq!(exception.bytes, stride..3 * stride);
    // M covers ordinary PACs of different Definitions, not necessarily a mixed PAC.
    let full = Container::open(bytes).unwrap();
    for (ordinal, expected) in [(1, 9), (2, 7)] {
        let raw = full.get(ordinal).unwrap();
        assert_eq!(
            pil_codec::pac::flags(raw).unwrap() & pil_codec::pac::MULTI_DEFINITION,
            0
        );
        assert_eq!(
            pil_codec::pac::application_header(raw, "test.definition")
                .unwrap()
                .unwrap(),
            &[expected]
        );
    }
}
