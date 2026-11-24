use data_seeds::{TimeTrie, TIME_HOUR_MS};
use std::sync::Arc;

fn timeline() -> TimeTrie {
    // Reuse the existing ingestion fixture; do not fabricate event bytes.
    TimeTrie::open(Arc::new(include_bytes!(
        "../../../testdata/time-trie/server.time"
    ).to_vec())).unwrap()
}

#[test]
fn range_matches_existing_individual_hour_reads() {
    let trie = timeline();
    let header = trie.header();
    let from = header.first;
    let to = header.sealed;
    assert!(from < to);
    let mut range = trie.range(from, to).unwrap();
    let expected = ((to - from) / TIME_HOUR_MS) as usize;
    assert_eq!(range.size_hint(), (expected, Some(expected)));
    for hour in (from..to).step_by(TIME_HOUR_MS as usize) {
        assert_eq!(range.next().unwrap().unwrap(), trie.leaf(hour).unwrap().unwrap());
    }
    assert!(range.next().is_none());
    assert_eq!(range.size_hint(), (0, Some(0)));
}

#[test]
fn range_clips_to_publication_and_preserves_half_open_boundaries() {
    let trie = timeline();
    let from = trie.header().first;
    let to = trie.header().sealed;
    assert!(trie.range(from, from).is_err());
    assert!(trie.range(to, from).is_err());
    assert_eq!(trie.range(to, to + TIME_HOUR_MS).unwrap().count(), 0);
    assert_eq!(trie.range(from + 1, from + TIME_HOUR_MS).unwrap().count(), 1);
    assert_eq!(trie.range(from, to + TIME_HOUR_MS).unwrap().count(),
               trie.range(from, to).unwrap().count());
    if from >= TIME_HOUR_MS {
        assert_eq!(trie.range(from - TIME_HOUR_MS, from).unwrap().count(), 0);
    }
}
