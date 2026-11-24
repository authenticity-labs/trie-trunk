//! Paired microbenchmark for the origin writer. Run optimized with --ignored --nocapture.
use super::Writer;
use crate::registration::{DEFINITION_HEADER, Storage};
use crate::{Error, Field, SeedDefinition, SeedPack, TIME_HOUR_MS, doc, integrity};
use std::time::Instant;

const HOUR: u64 = 480_000 * TIME_HOUR_MS;

fn hash(mut x: u64) -> u64 {
    // SplitMix64 finalizer. Identical reproducible entropy for both encoders.
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn rows(
    def: &SeedDefinition,
    hour: u64,
    n: usize,
    vocabulary: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let t = hour + i as u64 * TIME_HOUR_MS / n as u64;
        let bits = hash(t);
        let mut id = [0; 10];
        id[..8].copy_from_slice(&bits.to_le_bytes());
        id[8..].copy_from_slice(&(hash(bits) as u16).to_le_bytes());
        // For two events, show a true duplicate beside a singleton control.
        let key = match (vocabulary, n) {
            (0, 2) => t,
            (0, _) => hour + (i % (n / 2)) as u64,
            (_, 2) => 0,
            _ => bits % vocabulary as u64,
        };
        let message = format!(
            "pressure outside calibration band on channel {:016x}",
            hash(key)
        );
        let mut row = def.seed();
        row.set_uint(0, t)?
            .set_byte_string(1, &id)?
            .set_uint(2, bits % 70000)?
            .set_text_string(3, &message)?;
        row.cap()?;
        rows.push(row.bytes()?.into_owned());
    }
    Ok(rows)
}

fn fill<'a>(def: &'a SeedDefinition, hour: u64, rows: &[Vec<u8>]) -> Result<SeedPack<'a>, Error> {
    let mut pack = def.fill(rows.len())?;
    pack.register(1, def.prescription())?;
    pack.hour(hour)?;
    pack.header(DEFINITION_HEADER, 1u16.to_le_bytes())?;
    pack.header(integrity::HEADER, [0; 4])?;
    for row in rows {
        pack.load(row.as_slice())?;
    }
    Ok(pack)
}

struct Snapshot {
    footer: Vec<u8>,
    length: u64,
}

fn append(writer: &mut Writer, pack: SeedPack, enabled: bool) -> Result<(), Error> {
    if enabled {
        writer.filling(pack)
    } else {
        writer.append(pack)
    }
}

fn snapshot(def: &SeedDefinition, previous: &[Vec<u8>], enabled: bool) -> Result<Snapshot, Error> {
    let mut writer = Writer::new(&[])?;
    writer.compression(Storage::Pack.compression().unwrap())?;
    if !previous.is_empty() {
        append(
            &mut writer,
            fill(def, HOUR - TIME_HOUR_MS, previous)?,
            enabled,
        )?;
    }
    let start = writer.next_address() as usize;
    let commit = writer.commit()?;
    Ok(Snapshot {
        footer: commit.bytes[start..].to_vec(),
        length: commit.length,
    })
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct Size {
    stored: u64,
    pac: u64,
    plain: u64,
    entries: usize,
}

fn measure(
    def: &SeedDefinition,
    rows: &[Vec<u8>],
    previous: &Snapshot,
    enabled: bool,
) -> Result<(u128, Size), Error> {
    let mut writer = Writer::resume(previous.footer.clone(), previous.length)?;
    writer.compression(Storage::Pack.compression().unwrap())?;
    let pack = fill(def, HOUR, rows)?;
    let offset = writer.next_address();
    let beds = writer.next_beds_address();
    let start = Instant::now();
    append(&mut writer, pack, enabled)?;
    let end = writer.next_address();
    let length = writer.commit()?.length;
    let nanos = start.elapsed().as_nanos();
    let entries = (0..)
        .take_while(|&id| writer.container.string(id).is_some())
        .count();
    Ok((
        nanos,
        Size {
            stored: length - previous.length,
            pac: end - offset,
            plain: writer.next_beds_address() - beds,
            entries,
        },
    ))
}

#[test]
#[ignore = "release-mode throughput benchmark"]
fn hourly_common_strings() -> Result<(), Error> {
    let def = SeedDefinition::prescribe(
        &doc::new("pressure")
            .field(
                Field::new("time")
                    .r#type("uint")
                    .size(6)
                    .annotation("@time"),
            )
            .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
            .field(Field::new("duration").r#type("uint").size(4))
            .field(Field::new("message").r#type("tstr"))
            .sign()?,
    )?;
    let prior_eight = rows(&def, HOUR - TIME_HOUR_MS, 256, 8)?;
    let prior_wide = rows(&def, HOUR - TIME_HOUR_MS, 16_384, 512)?;
    println!(
        "rows,shape,box,pair,off_ns,on_ns,off_bytes,on_bytes,off_pac,on_pac,off_plain,on_plain,on_entries"
    );
    for count in [2, 16, 10_000, 100_000] {
        for vocabulary in [0, 8, 512] {
            if vocabulary == 512 && count < 10_000 {
                continue;
            }
            let previous = if vocabulary == 512 {
                &prior_wide
            } else {
                &prior_eight
            };
            let data = rows(&def, HOUR, count, vocabulary)?;
            for warm in [false, true] {
                let prior = if warm { previous.as_slice() } else { &[] };
                let snapshots = [snapshot(&def, prior, false)?, snapshot(&def, prior, true)?];
                let mut sizes = None;
                let pairs = if count < 100 { 60 } else { 12 };
                for pair in 0..pairs + 2 {
                    let mut result = [None, None];
                    for side in [pair % 2, 1 - pair % 2] {
                        result[side] = Some(measure(&def, &data, &snapshots[side], side == 1)?);
                    }
                    let [(off, a), (on, b)] = result.map(Option::unwrap);
                    if let Some(expected) = sizes {
                        assert_eq!([a, b], expected);
                    }
                    sizes = Some([a, b]);
                    if warm {
                        assert_eq!(b.entries, if vocabulary == 512 { 512 } else { 8 });
                    } else if vocabulary == 0 || count == 2 {
                        assert_eq!(b.entries, 0);
                    } else {
                        assert!((1..=vocabulary).contains(&b.entries));
                    }
                    if pair >= 2 {
                        println!(
                            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                            count,
                            match vocabulary {
                                0 => "rare",
                                8 => "repeat8",
                                _ => "repeat512",
                            },
                            if warm { "warm" } else { "new" },
                            pair - 2,
                            off,
                            on,
                            a.stored,
                            b.stored,
                            a.pac,
                            b.pac,
                            a.plain,
                            b.plain,
                            b.entries
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn monthly_candidates_pay_for_their_stored_bytes() -> Result<(), Error> {
    let def = SeedDefinition::prescribe(
        &doc::new("sample")
            .field(Field::new("time").r#type("uint").size(6))
            .field(Field::new("id").r#type("bytes").size(10))
            .field(Field::new("duration").r#type("uint").size(4))
            .field(Field::new("message").r#type("tstr"))
            .sign()?,
    )?;
    for (n, vocabulary, worthwhile) in [(10_000, 8, false), (100_000, 512, true)] {
        let data = rows(&def, HOUR, n, vocabulary)?;
        let candidates = (0..vocabulary)
            .map(|key| {
                format!(
                    "pressure outside calibration band on channel {:016x}",
                    hash(key as u64)
                )
                .into_boxed_str()
            })
            .collect();
        let mut plain = Writer::new(&[])?;
        let mut reviewed = Writer::new(&[])?;
        for writer in [&mut plain, &mut reviewed] {
            writer.compression(Storage::Pack.compression().unwrap())?;
        }
        plain.append(fill(&def, HOUR, &data)?)?;
        reviewed.selected(fill(&def, HOUR, &data)?, &candidates)?;
        assert_eq!(reviewed.container.has_common_strings(), worthwhile);
        let before = plain.commit()?.length;
        let after = reviewed.commit()?.length;
        if worthwhile {
            assert!(after < before);
        } else {
            assert_eq!(plain.bytes()?, reviewed.bytes()?);
        }
    }
    Ok(())
}
