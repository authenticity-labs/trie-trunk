use trie_trunk_taxonomy::{Kind, Trunk};
const BEFORE: &[u8] = include_bytes!("fixtures/before.trie");
const AFTER: &[u8] = include_bytes!("fixtures/after.trie");
#[test]
fn production_writer_roundtrip_and_append_identity() {
    let before = Trunk::open(BEFORE).unwrap();
    let after = Trunk::open(AFTER).unwrap();
    assert!(after.extends(&before));
    assert_eq!(after.len(), 12);
    assert_eq!(after.children(1).unwrap(), &[2, 11]);
    assert_eq!(
        after.find(1, Kind::Cache, "研究", "", "").unwrap(),
        Some(11)
    );
    assert_eq!(after.find(1, Kind::Cache, "missing", "", "").unwrap(), None);
    assert_eq!(after.path(10).unwrap(), vec![1, 2, 3, 8, 9, 10]);
    for id in [1, 2] {
        assert_eq!(before.branch(id).unwrap().id, after.branch(id).unwrap().id);
    }
    assert_eq!(after.branch(3).unwrap().id, 10);
    assert_eq!(after.node(6).unwrap().resource_type, "http");
    assert_eq!(after.section(1).unwrap(), b"updated registration");
    assert_eq!(before.section(1).unwrap(), b"registered definitions");
    assert_eq!(after.section(9).unwrap(), &[1, 2, 3]);
    assert_eq!(after.section_ids(), vec![1, 9]);
    assert!(after.branch(0).is_err());
    assert!(after.node(0).is_err());
    assert!(after.children(13).is_err());
    // Labels are borrowed from the authoritative file, not copied into nodes.
    let label = after.node(11).unwrap().name;
    let start = after.bytes().as_ptr() as usize;
    assert!((start..start + after.bytes().len()).contains(&(label.as_ptr() as usize)));
    fn shareable<T: Send + Sync>() {}
    shareable::<Trunk>();
}
#[test]
fn breadcrumb_choices_come_from_trie_nodes() {
    let t = Trunk::open(AFTER).unwrap();
    assert_eq!(t.source_options(&[], 0).unwrap(), vec!["demo", "研究"]);
    let scope = vec!["demo".into(), "payment".into()];
    assert_eq!(t.source_options(&scope, 1).unwrap(), vec!["payment"]);
    assert_eq!(
        t.source_options(&scope, 2).unwrap(),
        vec!["refunds", "transactions"]
    );
    assert!(t.source_options(&[], 3).is_err());
}
#[test]
fn malformed_input_is_rejected_without_panics() {
    for length in 0..BEFORE.len() {
        assert!(Trunk::open(&BEFORE[..length]).is_err());
    }
    let mut b = BEFORE.to_vec();
    b[72..76].copy_from_slice(&99u32.to_le_bytes());
    assert!(Trunk::open(b).is_err());
    let mut b = BEFORE.to_vec();
    b[72 + 20 + 4..72 + 20 + 8].copy_from_slice(&2u32.to_le_bytes());
    assert!(Trunk::open(b).is_err());
    let mut b = BEFORE.to_vec();
    let start = b
        .windows(b"registered definitions".len())
        .position(|w| w == b"registered definitions")
        .unwrap();
    b[start] ^= 1;
    assert!(Trunk::open(b).is_err());
}
#[test]
fn resolution_uses_selected_ancestors_and_requires_explicit_type() {
    let trunk = Trunk::open(AFTER).unwrap();
    for path in [
        vec!["http"],
        vec!["transactions", "http"],
        vec!["payment", "transactions", "http"],
        vec!["demo", "payment", "transactions", "http"],
    ] {
        assert_eq!(
            trunk
                .resolve(4, &path.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap(),
            vec![1]
        );
    }
    assert_eq!(trunk.resolve(4, &["security".into()]).unwrap(), vec![2]);
    assert!(trunk.resolve(1, &["http".into()]).is_err());
    assert!(trunk.resolve(4, &["transactions".into()]).is_err());
    assert!(trunk.resolve(4, &["absent".into()]).is_err());
}
