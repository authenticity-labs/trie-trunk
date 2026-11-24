use data_seeds::{Packs, TimeLeaf, TimeTrie, TIME_HOUR_MS};
use std::sync::Arc;

const FIRST: u64 = 1_767_225_600_000;
const GENERATION: [u8; 16] = [7; 16];

fn fixture(flags: u8, sparse: bool) -> (TimeTrie, Vec<TimeLeaf>) {
    let members = vec![
        TimeLeaf { hour: FIRST, start: 0, end: 4, packs: 0, beds: 0, count: 3 },
        TimeLeaf { hour: FIRST + TIME_HOUR_MS, start: 4, end: 6, packs: 64, beds: 64, count: 1 },
    ];
    let mut time = TimeTrie::create(FIRST, GENERATION, 0).unwrap();
    for &leaf in &members { time = time.publish(leaf.hour + TIME_HOUR_MS, Some(leaf)).unwrap(); }
    // The replica's existing representation adds one local availability byte.
    let stride: usize = time.header().widths[..4].iter().map(|&n| n as usize).sum();
    let mut bytes = time.bytes()[..64].to_vec();
    bytes[46] = 1;
    for row in time.bytes()[64..].chunks_exact(stride) { bytes.extend_from_slice(row); bytes.push(flags); }
    let received = if sparse { vec![FIRST..FIRST + TIME_HOUR_MS] }
        else { vec![FIRST..FIRST + 2 * TIME_HOUR_MS] };
    (TimeTrie::open_replica(Arc::new(bytes), received).unwrap(), members)
}

#[test]
fn publication_is_staged_and_preserves_other_completion_bits() {
    let (time, members) = fixture(15, false);
    let before = time.bytes().to_vec();
    let replica = Packs::reconstruct(1024, 2).unwrap();
    let patches = replica.publication(&time, GENERATION, &members).unwrap();
    assert_eq!(patches.len(), 2);
    assert_eq!(time.bytes(), before);
    for (offset, value) in patches { assert_eq!(before[offset], 15); assert_eq!(value, 31); }
}

#[test]
fn publication_rejects_changed_placement_and_duplicate_hours_atomically() {
    let (time, members) = fixture(6, false);
    let replica = Packs::reconstruct(1024, 2).unwrap();
    for field in 0..7 {
        let mut bad = members.clone();
        match field {
            0 => bad[1].start += 1, 1 => bad[1].end += 1,
            2 => bad[1].packs += 1, 3 => bad[1].beds += 1,
            4 => bad[1].count += 1, 5 => bad[1].hour += 1,
            _ => bad[1] = bad[0],
        }
        assert!(replica.publication(&time, GENERATION, &bad).is_err());
    }
    assert!(replica.publication(&time, [8; 16], &members).is_err());
    assert!(replica.publication(&time, GENERATION, &[members[1], members[0]]).is_err());
    assert_eq!(replica.publication(&time, GENERATION, &members).unwrap().len(), 2);
}

#[test]
fn publication_requires_received_time_and_both_dependencies() {
    let replica = Packs::reconstruct(1024, 2).unwrap();
    for flags in [0, 2, 4, 16, 6 | 128] {
        let (time, members) = fixture(flags, false);
        assert!(replica.publication(&time, GENERATION, &members).is_err());
    }
    let (time, members) = fixture(6, true);
    assert!(replica.publication(&time, GENERATION, &members).is_err());
    assert!(replica.publication(&time, GENERATION, &members[..1]).is_ok());
}

#[test]
fn publication_cannot_exceed_the_origin_count_or_member_extent() {
    let (time, members) = fixture(6, false);
    assert!(Packs::reconstruct(1024, 1).unwrap().publication(&time, GENERATION, &members).is_err());
    assert!(Packs::reconstruct(64, 2).unwrap().publication(&time, GENERATION, &members).is_err());
}

#[test]
fn planned_publication_reads_only_header_and_selected_leaves_and_uses_current_flags() {
    let (time, members) = fixture(6, false);
    let replica = Packs::reconstruct(1024, 2).unwrap();
    let expected = replica.publication(&time, GENERATION, &members).unwrap();
    let mut current = time.bytes().to_vec();
    current[expected[0].0] |= 1; // A summary completed since collection started.
    let mut read_bytes = 0;
    let patches = replica.publication_with(&time, GENERATION, &members, |at, out| {
        let at = at as usize; read_bytes += out.len();
        out.copy_from_slice(&current[at..at + out.len()]); Ok(())
    }).unwrap().unwrap();
    assert_eq!(read_bytes, 64 + 2 * 17);
    assert_eq!(patches[0].1, 23);
    assert_eq!(patches[1].1, 22);
    for (offset, _) in expected {
        let mut invalid = current.clone(); invalid[offset] = 0;
        assert!(replica.publication_with(&time, GENERATION, &members, |at, out| {
            let at = at as usize;out.copy_from_slice(&invalid[at..at + out.len()]);Ok(())
        }).is_err(), "lost dependencies must not reuse a stale plan");
    }
    current[47] = 1; // Changed publication header requests a new plan.
    assert!(replica.publication_with(&time, GENERATION, &members, |at, out| {
        let at = at as usize;out.copy_from_slice(&current[at..at + out.len()]);Ok(())
    }).unwrap().is_none());
}
