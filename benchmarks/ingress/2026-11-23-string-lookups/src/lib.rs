//! Isolated comparison of resident UTF-8 -> BOX ordinal lookups.
use hashbrown::HashTable;
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{BuildHasher, Hasher},
    hint::black_box,
    sync::Arc,
};

pub const LOOKUPS: usize = 4096 * 32;
const PROBES: usize = 4096;

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

type Vocab = Vec<(String, usize)>;
fn vocabulary(case: u32) -> (Vocab, Vocab) {
    let telemetry: Vocab = serde_json::from_str(include_str!("../telemetry.json")).unwrap();
    match case {
        // Exact BOX entry from the sealed /strings replication fixture.
        0 => {
            let key = "events | where duration > 10000";
            let misses = telemetry
                .into_iter()
                .filter(|(s, _)| s.starts_with("events | where") && s != key)
                .collect();
            (vec![(key.to_owned(), 1)], misses)
        }
        1 => telemetry
            .into_iter()
            .partition(|(_, frequency)| *frequency > 1),
        2 => {
            let group = |start: u64| {
                (start..start + 512)
                    .map(|n| {
                        (
                            format!(
                                "pressure outside calibration band on channel {:016x}",
                                mix(n)
                            ),
                            1,
                        )
                    })
                    .collect()
            };
            (group(0), group(512))
        }
        _ => unreachable!(),
    }
}

struct Plan {
    standard: HashMap<Arc<str>, usize>,
    candidates: [HashTable<(Arc<str>, usize)>; 4],
    ahash: ahash::RandomState,
    probes: [Vec<Box<str>>; 2],
    sums: [u32; 2],
}

// The current RustCrypto configurations use software SHA on aarch64.
// This optional control measures the system implementation on the same Mac.
#[cfg(all(target_os = "macos", feature = "apple-sha"))]
fn sha_bucket<const A: usize>(bytes: &[u8]) -> u64 {
    unsafe extern "C" {
        fn CC_SHA1(bytes: *const u8, len: u32, out: *mut u8) -> *mut u8;
        fn CC_SHA256(bytes: *const u8, len: u32, out: *mut u8) -> *mut u8;
    }
    let len = u32::try_from(bytes.len()).unwrap();
    let mut digest = [0; 32];
    // The input slice is valid for len bytes; both digests fit in out.
    unsafe {
        if A == 1 {
            CC_SHA1(bytes.as_ptr(), len, digest.as_mut_ptr());
        } else {
            CC_SHA256(bytes.as_ptr(), len, digest.as_mut_ptr());
        }
    }
    u64::from_le_bytes(digest[..8].try_into().unwrap())
}

#[cfg(not(all(target_os = "macos", feature = "apple-sha")))]
fn sha_bucket<const A: usize>(bytes: &[u8]) -> u64 {
    use sha2::Digest;
    if A == 1 {
        u64::from_le_bytes(sha1::Sha1::digest(bytes)[..8].try_into().unwrap())
    } else {
        u64::from_le_bytes(sha2::Sha256::digest(bytes)[..8].try_into().unwrap())
    }
}

fn hash<const A: usize>(text: &str, state: &ahash::RandomState) -> u64 {
    let bytes = text.as_bytes();
    match A {
        0 => xxhash_rust::xxh3::xxh3_64(bytes),
        1 => sha_bucket::<1>(bytes),
        2 => sha_bucket::<2>(bytes),
        3 => {
            let mut h = state.build_hasher();
            h.write(bytes);
            h.finish()
        }
        _ => unreachable!(),
    }
}

impl Plan {
    fn new(case: u32, seed: u32) -> Self {
        let (hits, misses) = vocabulary(case);
        let keys: Vec<Arc<str>> = hits
            .iter()
            .map(|(key, _)| Arc::from(key.as_str()))
            .collect();
        let mut standard = HashMap::new();
        for (id, key) in keys.iter().enumerate() {
            standard.insert(key.clone(), id);
        }
        let k = u64::from(seed);
        let ahash = ahash::RandomState::with_seeds(mix(k + 1), mix(k + 2), mix(k + 3), mix(k + 4));
        fn table<const A: usize>(
            keys: &[Arc<str>],
            state: &ahash::RandomState,
        ) -> HashTable<(Arc<str>, usize)> {
            let mut table = HashTable::with_capacity(keys.len());
            for (id, key) in keys.iter().enumerate() {
                table.insert_unique(hash::<A>(key, state), (key.clone(), id), |(key, _)| {
                    hash::<A>(key, state)
                });
            }
            table
        }
        let candidates = [
            table::<0>(&keys, &ahash),
            table::<1>(&keys, &ahash),
            table::<2>(&keys, &ahash),
            table::<3>(&keys, &ahash),
        ];
        let mut sums = [0u32; 2];
        let probes = [hits, misses].map(|group| {
            let weighted: Vec<_> = group
                .iter()
                .flat_map(|(s, n)| std::iter::repeat_n(s, *n))
                .collect();
            (0..PROBES)
                .map(|i| {
                    let index = mix((k << 32) + i as u64) % weighted.len() as u64;
                    Box::<str>::from(weighted[index as usize].as_str())
                })
                .collect::<Vec<_>>()
        });
        for (side, stream) in probes.iter().enumerate() {
            for key in stream {
                let expected = standard.get(key.as_ref()).copied();
                assert_eq!(expected.is_some(), side == 0);
                let hashes = [
                    hash::<0>(key, &ahash),
                    hash::<1>(key, &ahash),
                    hash::<2>(key, &ahash),
                    hash::<3>(key, &ahash),
                ];
                for (map, h) in candidates.iter().zip(hashes) {
                    assert_eq!(
                        map.find(h, |(s, _)| s.as_ref() == key.as_ref())
                            .map(|x| x.1),
                        expected
                    );
                }
                sums[side] += expected.map_or(0, |id| id as u32 + 1);
            }
        }
        Self {
            standard,
            candidates,
            ahash,
            probes,
            sums,
        }
    }
    fn run<const A: usize>(&self, side: usize) -> u32 {
        let mut sum = 0;
        for _ in 0..LOOKUPS / PROBES {
            for key in &self.probes[side] {
                let text = black_box(key.as_ref());
                let ordinal = if A == 4 {
                    self.standard.get(text).copied()
                } else {
                    self.candidates[A]
                        .find(hash::<A>(text, &self.ahash), |(key, _)| {
                            key.as_ref() == text
                        })
                        .map(|entry| entry.1)
                };
                sum += black_box(ordinal).map_or(0, |id| id as u32 + 1);
            }
        }
        assert_eq!(sum, self.sums[side] * (LOOKUPS / PROBES) as u32);
        sum
    }
}

thread_local! { static PLAN: RefCell<Option<Plan>> = const { RefCell::new(None) }; }

#[unsafe(no_mangle)]
pub extern "C" fn prepare(case: u32, seed: u32) {
    PLAN.with_borrow_mut(|plan| *plan = Some(Plan::new(case, seed)));
}

#[unsafe(no_mangle)]
pub extern "C" fn run(algorithm: u32, side: u32) -> u32 {
    PLAN.with_borrow(|plan| {
        let plan = plan.as_ref().unwrap();
        let side = side as usize;
        match algorithm {
            0 => plan.run::<0>(side),
            1 => plan.run::<1>(side),
            2 => plan.run::<2>(side),
            3 => plan.run::<3>(side),
            4 => plan.run::<4>(side),
            _ => unreachable!(),
        }
    })
}
