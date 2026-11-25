use super::*;

const K: Binding = Binding {
    minor: 0,
    generation: [0; 16],
};
fn post(revision: u32, hour: u32, record: u32) -> Posting {
    Posting {
        object: [1; 16],
        revision,
        hour,
        record,
    }
}
fn fixture() -> Page {
    Page(*include_bytes!(
        "../../../../go/server/cache/testdata/slotted.page"
    ))
}

#[test]
fn independent_scattered_rotated_fixture() {
    let p = fixture();
    p.validate(K).unwrap();
    for (key, base, span) in [(0u128, 65536, 14), (2, 0, 2), (4, MAX_REV - 1, 2)] {
        let n = p.find(&key.to_be_bytes()).unwrap();
        let expected = (0..span)
            .filter(|&j| j != 3)
            .map(|j| (base + j, (((key + 1) as u64) << 32) | u64::from(70000 + j)))
            .collect::<Vec<_>>();
        assert_eq!(p.entries(n).collect::<Vec<_>>(), expected);
        let mut streamed = Vec::new();
        p.visit_entries(n, |r, a| streamed.push((r, a)));
        assert_eq!(streamed, expected);
        for (r, v) in expected {
            assert_eq!(p.reference(n, r), Some(v))
        }
    }
}
#[test]
fn full_page_capacity_and_independent_widths() {
    let mut p = Page::new(K);
    assert_eq!(p.max_span(), 733);
    for n in 0..733 {
        assert!(p.put(&post(65536 + n, 9, n + 1)).unwrap())
    }
    assert_eq!(
        (p.used(), p.ref_at(0, 0), p.ref_at(0, 1)),
        (123, 5 * 32 + 27, 6 * 32)
    );
    assert!(!p.put(&post(65536 + 733, 9, 735)).unwrap());
    p.seal();
    p.validate(K).unwrap();
    let idx = Index::open(tempfile::tempfile().unwrap()).unwrap();
    for (n, (hour, record, w)) in [
        (0, 1, [2, 3]),
        (65536, 2, [3, 3]),
        (0, 1 << 24, [3, 4]),
        (1 << 24, 3, [4, 4]),
    ]
    .into_iter()
    .enumerate()
    {
        let p = post(n as u32, hour, record);
        idx.append(K, 4, n as u64, n as u64 + 1, &[p]).unwrap();
        let s = idx.snapshot();
        let id = s.view.sources[&K].fences[0].1;
        assert_eq!(read_page(&s.file, id, K).unwrap().0[30..32], w);
        assert_eq!(s.get(K, p.object, p.revision).unwrap(), Some(p.address()));
    }
    assert!(idx.append(K, 4, 4, 5, &[post(4, 0, 0)]).is_err());
}
#[test]
fn source_binding_watermark_conflicts_and_reopen() {
    let f = tempfile::tempfile().unwrap();
    let idx = Index::open(f.try_clone().unwrap()).unwrap();
    let other = Binding {
        minor: 1,
        generation: [7; 16],
    };
    idx.append(K, 4, 0, 10, &[post(0, 10, 1), post(MAX_REV, 20, 2)])
        .unwrap();
    idx.append(other, 1, 0, 1, &[post(0, 30, 3)]).unwrap();
    assert!(idx.append(other, 1, 1, 2, &[post(256, 1, 1)]).is_err());
    assert!(idx.append(K, 4, 0, 11, &[]).is_err());
    assert!(idx.append(K, 4, 10, 11, &[post(0, 40, 4)]).is_err());
    let reopened = Index::open(f).unwrap();
    let v = reopened.snapshot();
    assert_eq!(v.page_counts(K), (1, 1, 1));
    assert_eq!(v.get(K, [1; 16], 1).unwrap(), None);
    assert_eq!(v.latest(K, [1; 16]).unwrap(), Some((MAX_REV, 20 << 32 | 2)));
    assert_eq!(v.latest(other, [1; 16]).unwrap(), Some((0, 30 << 32 | 3)));
}
#[test]
fn corrupt_continuations_rejected() {
    for kind in ["cycle", "slot", "owner", "checksum"] {
        let f = tempfile::tempfile().unwrap();
        let idx = Index::open(f.try_clone().unwrap()).unwrap();
        idx.append(K, 4, 0, 2, &[post(0, 0, 1), post(MAX_REV, 0, 2)])
            .unwrap();
        let view = idx.snapshot();
        let mut id = view.view.sources[&K].fences[0].1;
        let mut p = read_page(&f, id, K).unwrap();
        let link = p.link(0).unwrap();
        match kind {
            "cycle" => p.set_link(
                0,
                Link {
                    page: id,
                    slot: p.slot(0),
                },
            ),
            "slot" => p.set_link(
                0,
                Link {
                    page: link.page,
                    slot: 127,
                },
            ),
            _ => {
                id = link.page;
                p = read_page(&f, id, K).unwrap();
                let at = p.at(0);
                p.0[at] ^= 1;
            }
        }
        if kind != "checksum" {
            p.seal()
        }
        f.write_all_at(&p.0, u64::from(id) * 4096).unwrap();
        assert!(Index::open(f).is_err(), "{kind}");
    }
}

#[test]
fn materialized_slot_is_reserved_independently_of_revision_zero() {
    for h in [2, 3, 4] {
        for r in [3, 4] {
            let mut p = Page::new(K);
            p.0[30..32].copy_from_slice(&[h, r]);
            for rev in [0, 1, 3, 7, 13] {
                assert!(p.put(&post(rev, 10, rev + 1)).unwrap());
            }
            p.seal();
            p.validate(K).unwrap();
            let mut actual = Vec::new();
            p.visit_entries(0, |r, a| actual.push((r, a)));
            assert_eq!(actual, p.entries(0).collect::<Vec<_>>());
            assert_eq!(actual[0], (0, 10u64 << 32 | 1));
            let slot = p.at(0) + 22;
            let width = usize::from(h + r);
            assert!(p.0[slot..slot + width].iter().all(|&b| b == 0));
            // The high byte of the record ordinal belongs to the reserved locator too.
            p.0[slot + width - 1] = 1;
            p.seal();
            assert!(p.validate(K).is_err());
        }
    }
}
