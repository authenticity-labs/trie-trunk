use data_seeds::replication::{ReadState, Readiness};
use data_seeds::{Field, MemoryBudget, SeedDefinition, SeedPackSnapshot, doc};
use std::sync::Arc;

fn pack(sealed: bool, value: u64) -> Arc<SeedPackSnapshot> {
    let doc = doc::new("ready")
        .field(Field::new("value").r#type("uint").size(4))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&doc).unwrap();
    let mut pack = definition.fill(1).unwrap();
    pack.load(
        definition
            .ingest(value, |v, p| {
                p.set_uint("value", v)?;
                Ok(())
            })
            .unwrap(),
    )
    .unwrap();
    if sealed {
        pack.seal().unwrap();
    }
    let bytes = if sealed {
        pack.into_bytes().unwrap()
    } else {
        pack.snapshot().unwrap()
    };
    definition.open(bytes).unwrap()
}

#[test]
fn queued_admission_is_bounded_and_owned_by_the_collector() {
    let pack = pack(true, 1);
    let bytes = pack.retained_bytes();
    let reads = Readiness::new(MemoryBudget::new(bytes * 2));
    assert_eq!(reads.queue("a", 0).unwrap(), ReadState::Queued);
    assert_eq!(reads.used().unwrap(), 0);
    assert!(reads.admit("a", 0, bytes * 2, 11).unwrap());
    assert!(!reads.admit("a", 0, bytes * 2, 12).unwrap());
    reads.cancel("a", 12).unwrap();
    assert_eq!(reads.used().unwrap(), bytes * 2);
    assert!(reads.load("a", 0, pack.clone(), 12).is_err());
    reads.load("a", 0, pack, 11).unwrap();
    assert_eq!(reads.used().unwrap(), bytes);
    assert_eq!(reads.state("a", 0).unwrap(), Some(ReadState::Ready));
    assert_eq!(reads.coverage("a", 0, 6).unwrap(), 16);
    assert_eq!(reads.coverage("a", 0, 2).unwrap(), 0);
    assert_eq!(reads.coverage("a", 0, 22).unwrap(), 0);
    reads.cancel("a", 11).unwrap();
    assert!(reads.get("a", 0).unwrap().is_some());
    assert!(!reads.admit("a", 100, bytes * 2, 12).unwrap());
    assert_eq!(reads.state("a", 100).unwrap(), Some(ReadState::Queued));
    assert!(reads.admit("b", 0, bytes, 12).unwrap());
    reads.cancel("b", 12).unwrap();
    assert_eq!(reads.used().unwrap(), bytes);
}

#[test]
fn eviction_requires_durability_and_no_reader_pin() {
    let pack = pack(true, 1);
    let bytes = pack.retained_bytes();
    let reads = Readiness::new(MemoryBudget::new(bytes));
    assert!(reads.admit("a", 0, bytes, 1).unwrap());
    reads.load("a", 0, pack, 1).unwrap();
    reads.submitted("a", 0).unwrap();
    reads.discard("a", 0, 1).unwrap();
    assert!(
        !reads.admit("a", 100, bytes, 2).unwrap(),
        "pending writes retain readable bytes"
    );
    let reader = reads.get("a", 0).unwrap().unwrap();
    reads.published("a", &[0]).unwrap();
    assert!(
        !reads.admit("a", 100, bytes, 2).unwrap(),
        "readers retain shared memory"
    );
    drop(reader);
    assert!(reads.admit("a", 100, bytes, 2).unwrap());
    assert_eq!(reads.state("a", 0).unwrap(), None);
    reads.cancel("a", 2).unwrap();
    assert_eq!(reads.used().unwrap(), 0);
}

#[test]
fn reset_releases_ownership_but_not_active_reader_memory() {
    let pack = pack(true, 1);
    let bytes = pack.retained_bytes();
    let reads = Readiness::new(MemoryBudget::new(bytes));
    reads.admit("a", 0, bytes, 1).unwrap();
    reads.load("a", 0, pack, 1).unwrap();
    let pin = reads.get("a", 0).unwrap().unwrap();
    reads.clear("a").unwrap();
    assert_eq!(reads.state("a", 0).unwrap(), None);
    assert_eq!(reads.used().unwrap(), bytes);
    drop(pin);
    assert_eq!(reads.used().unwrap(), 0);
}

#[test]
fn ready_bytes_are_immutable_and_publication_is_atomic() {
    let original = pack(true, 1);
    let reads = Readiness::new(MemoryBudget::new(65536));
    reads.admit("a", 0, 65536, 1).unwrap();
    assert!(reads.load("a", 0, pack(false, 1), 1).is_err());
    reads.load("a", 0, original.clone(), 1).unwrap();
    assert!(reads.load("a", 0, original.clone(), 2).is_err());
    assert!(reads.admit("a", 0, 1024, 2).unwrap());
    let charged = reads.used().unwrap();
    reads.cancel("a", 3).unwrap();
    assert_eq!(reads.used().unwrap(), charged);
    reads.load("a", 0, original, 2).unwrap();
    assert_eq!(reads.used().unwrap(), charged - 1024);
    assert!(reads.admit("a", 0, 1024, 2).unwrap());
    assert!(reads.load("a", 0, pack(true, 2), 2).is_err());
    assert!(reads.published("a", &[0, 100]).is_err());
    assert!(!reads.admit("a", 100, 65536, 2).unwrap());
    reads.discard("a", 0, 2).unwrap();
    assert!(reads.get("a", 0).unwrap().is_some());
    reads.discard("a", 0, 1).unwrap();
    assert_eq!(reads.used().unwrap(), 0);
}
