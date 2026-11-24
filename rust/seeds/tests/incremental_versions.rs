use data_seeds::{Field, SeedDefinition, doc};
use pil_codec::{
    api::Pack,
    pac::{self, versions::PackIndex},
};
use std::sync::Arc;
fn definition(variable: bool) -> Arc<SeedDefinition> {
    let mut d = doc::new("ingest").version(0, u32::from(variable)).field(
        Field::new("sequence")
            .r#type("uint")
            .size(if variable { 8 } else { 3 }),
    );
    d = d.field(Field::new("accepted").r#type("bool"));
    if variable {
        d = d.field(Field::new("message").r#type("tstr"));
    }
    SeedDefinition::prescribe(&d.sign().unwrap()).unwrap()
}
fn seed(d: &Arc<SeedDefinition>, n: u64, variable: bool) -> data_seeds::Seed<'_> {
    d.ingest(n, |n, s| {
        s.set_uint("sequence", n)?;
        if n != 2 {
            s.set_bool("accepted", n % 2 == 0)?;
        }
        if n == 3 {
            s.squeeze()?;
        }
        if variable {
            s.set_text_string("message", "live")?;
        }
        Ok(())
    })
    .unwrap()
}
fn check(pack: &Pack<'_>, expected: &[u64]) {
    assert_eq!(pack.len(), expected.len());
    let mut reader = pack.reader();
    for (i, &n) in expected.iter().enumerate() {
        let view = reader.get(i).unwrap();
        assert_eq!(
            view.crush().unwrap()["sequence"],
            Some(serde_json::json!(n))
        );
        assert_eq!(view.crush().unwrap().contains_key("message"), n % 2 == 1);
        let pill = pack.get(i).unwrap();
        assert_eq!(
            pill.crush().unwrap()["sequence"],
            Some(serde_json::json!(n))
        );
        assert_eq!(pill.crush().unwrap().contains_key("message"), n % 2 == 1);
    }
}
#[test]
fn live_mixed_load_snapshot_resume_seal_and_compression() {
    let a = definition(false);
    let b = definition(true);
    let mut pack = a.fill(10).unwrap();
    pack.register(7, a.prescription())
        .unwrap()
        .register(9, b.prescription())
        .unwrap();
    pack.header("owner", b"test".to_vec()).unwrap();
    pack.load(seed(&a, 0, false)).unwrap();
    let ordinary = pack.snapshot().unwrap();
    assert_eq!(pac::flags(&ordinary).unwrap() & pac::MULTI_DEFINITION, 0);
    pack.load([seed(&b, 1, true), seed(&a, 2, false), seed(&b, 3, true)])
        .unwrap();
    check(&pack, &[0, 1, 2, 3]);
    let snapshot = pack.snapshot().unwrap();
    assert!(!PackIndex::open(&snapshot).unwrap().versions().is_sealed());
    assert!(a.prescription().open(&snapshot).is_err());
    pack.load(seed(&a, 4, false)).unwrap();
    assert_eq!(PackIndex::open(&snapshot).unwrap().count(), 4); // old publication stays stable
    let mut resumed = a.fill(0).unwrap();
    resumed
        .register(7, a.prescription())
        .unwrap()
        .register(9, b.prescription())
        .unwrap();
    resumed.restore(&snapshot).unwrap();
    assert_eq!(resumed.headers()["owner"], b"test");
    resumed.load(seed(&a, 4, false)).unwrap();
    let popped = resumed.pop().unwrap().unwrap();
    assert_eq!(
        popped.crush().unwrap()["sequence"],
        Some(serde_json::json!(4))
    );
    drop(popped);
    resumed.load(seed(&a, 6, false)).unwrap();
    resumed.seal().unwrap();
    resumed.seal().unwrap();
    check(&resumed, &[0, 1, 2, 3, 6]);
    resumed.validate().unwrap();
    assert!(resumed.load(seed(&a, 8, false)).is_err());
    let bytes = resumed.bytes().unwrap();
    let index = PackIndex::open(bytes).unwrap();
    assert!(index.versions().is_sealed());
    assert_eq!(index.versions().ranges().len(), 5);
    let (_, member) = index.member(3).unwrap();
    let offset = member.as_ptr() as usize - bytes.as_ptr() as usize;
    assert_eq!(
        resumed.offset(offset).unwrap().crush().unwrap()["sequence"],
        Some(serde_json::json!(3))
    );
    assert!(resumed.offset(offset + 1).is_err());
    let mut reopened = a.fill(0).unwrap();
    reopened
        .register(7, a.prescription())
        .unwrap()
        .register(9, b.prescription())
        .unwrap();
    reopened.restore(bytes).unwrap();
    check(&reopened, &[0, 1, 2, 3, 6]);
    assert!(reopened.pop().is_err());
    #[cfg(feature = "compression")]
    {
        let sealed = resumed.compress(Default::default()).unwrap();
        let reopened = sealed.unseal().unwrap();
        check(&reopened, &[0, 1, 2, 3, 6]);
        assert!(reopened.is_sealed());
    }
}
#[test]
fn batches_fail_without_publishing_and_pop_can_restore_uniform_framing() {
    let a = definition(false);
    let b = definition(true);
    let unknown = definition(true);
    let mut pack = a.fill(8).unwrap();
    pack.register(7, a.prescription())
        .unwrap()
        .register(9, b.prescription())
        .unwrap();
    assert!(pack.register(7, b.prescription()).is_err());
    pack.load(seed(&a, 0, false)).unwrap();
    let before = pack.snapshot().unwrap();
    assert!(
        pack.load([seed(&b, 1, true), seed(&unknown, 3, true)])
            .is_err()
    );
    assert_eq!(pack.snapshot().unwrap(), before);
    // Fail after one sleeve member encoded, using a legitimate explicit
    // record-width constraint rather than fabricating invalid record bytes.
    let width = {
        let pill = seed(&a, 0, false);
        pill.cap().unwrap();
        pill.bytes().unwrap().len() as u32
    };
    pack.configure(pil_codec::api::PackOptions {
        record_width: Some(width),
        common_values: false,
        ..Default::default()
    })
    .unwrap();
    let constrained = pack.snapshot().unwrap();
    assert!(pack.load([seed(&a, 4, false), seed(&b, 1, true)]).is_err());
    assert_eq!(pack.snapshot().unwrap(), constrained);
    pack.configure(Default::default()).unwrap();
    assert_eq!(pack.snapshot().unwrap(), before);
    pack.load(seed(&b, 1, true)).unwrap();
    let popped = pack.pop().unwrap().unwrap();
    assert_eq!(
        popped.crush().unwrap()["message"],
        Some(serde_json::json!("live"))
    );
    drop(popped);
    assert_eq!(pack.snapshot().unwrap(), before);
    pack.seal().unwrap();
    assert_eq!(
        pac::flags(pack.bytes().unwrap()).unwrap() & pac::MULTI_DEFINITION,
        0
    );
    check(&pack, &[0]);
}
#[test]
fn alternate_first_and_only_definition_retains_its_reference() {
    let a = definition(false);
    let b = definition(true);
    let mut pack = a.fill(0).unwrap();
    pack.register(7, a.prescription())
        .unwrap()
        .register(9, b.prescription())
        .unwrap();
    pack.load(seed(&b, 1, true)).unwrap();
    let snapshot = pack.snapshot().unwrap();
    let index = PackIndex::open(&snapshot).unwrap();
    assert_eq!(index.versions().ranges().len(), 1);
    assert_eq!(index.member(0).unwrap().0, 9);
    pack.seal().unwrap();
    check(&pack, &[1]);
}
