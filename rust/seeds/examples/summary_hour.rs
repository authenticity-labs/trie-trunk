//! One hour projected from an actual fixed daily summary Pack. All example
//! events enter through Rust Seed ingestion and the annotated accumulator.
use data_seeds::*;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = doc::new("summary-hour-example")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time")
                .annotation("#summ(temporal)"),
        )
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("#summ(numeric)")
                .annotation("#percentile(50)")
                .annotation("#percentile(95)"),
        )
        .field(
            Field::new("status")
                .closed_enum("uint", [200.into(), 400.into(), 500.into()])
                .annotation("#summ(categorical)"),
        )
        .field(Field::new("message").r#type("tstr"))
        .sign()?;
    #[cfg(feature = "registration")]
    let (trunk, resource) = {
        use trie_trunk_taxonomy::{Trunk, registration::Registration};
        Trunk::new("Summary Example")?.registration(Registration {
            cache: "demo", service: "lab", resource: "measurements", resource_type: "sample",
            level: "", file: "summary-hour.rx",
        }, &document)?
    };
    #[cfg(feature = "registration")]
    let source = {
        let mut node = registration::Trunk::open(trunk.bytes())?;
        node.prescribe(resource.definition, document.bytes()?)?
    };
    #[cfg(not(feature = "registration"))]
    let source = SeedDefinition::prescribe(&document)?;
    let plan = source.summaries(288)?;
    let start = 1_767_225_600_000u64;
    let mut live = plan.accumulator(start)?;
    let mut events = source.fill(27)?;
    for (period, count) in [3, 2, 0, 4, 1, 2, 3, 2, 4, 1, 2, 3].into_iter().enumerate() {
        for row in 0..count {
            let duration = if period == 5 {
                None
            } else {
                Some((period as u64 + 1) * 100 + row * 50)
            };
            let event = live.ingest(
                start + period as u64 * 300_000 + row * 1000,
                (duration, row),
                |(duration, row), seed| {
                    seed.set_uint("time", start + period as u64 * 300_000 + row * 1000)?;
                    if let Some(value) = duration {
                        seed.set_uint("duration", value)?;
                    } else if row == 0 {
                        seed.set_null("duration")?;
                    }
                    seed.set_enum(
                        "status",
                        if row == 0 {
                            200.into()
                        } else if row == 1 {
                            400.into()
                        } else {
                            500.into()
                        },
                    )?
                    .set_text_string("message", "sample event")?;
                    Ok(())
                },
            )?;
            events.load(event)?;
        }
    }
    events.seal()?;
    // This fixture declares the rest of the day silent. In the runtime, the
    // commit scheduler advances this watermark after the straggler window.
    live.complete(live.end())?;
    let count = live
        .rollup(&summaries::SummaryWindow::new(start..live.end())?)?
        .count();
    // A transport Time window derived from those same ingested events. It
    // permits publication/cache tests to verify summary counts against Time.
    let mut time = vec![0u8; 80 + 24 * 16];
    time[..8].copy_from_slice(b"TTTIME00");
    for (at, value) in [(8,start),(16,start),(24,count+1),(32,live.end()),(64,start),(72,live.end())] {
        time[at..at+8].copy_from_slice(&value.to_le_bytes());
    }
    time[40..46].copy_from_slice(&[4,5,5,2,2,2]);
    time[48..64].fill(0xaa);
    let mut ordinal=0u32;
    for hour in 0..24 {
        let from=start+hour as u64*3_600_000;
        let n=live.rollup(&summaries::SummaryWindow::new(from..from+3_600_000)?)?.count();
        let at=80+hour*16;
        time[at..at+4].copy_from_slice(&ordinal.to_le_bytes());
        time[at+14..at+16].copy_from_slice(&u16::try_from(n)?.to_le_bytes());
        if n>0 {ordinal+=u32::try_from(n)?+1;}
    }
    #[cfg(feature = "registration")]
    let (trunk, branch) = {
        let trunk = trunk.register_summaries(resource.branch, "summary-hour-summaries.rx", plan.document())?
            .register_summary_context(resource.branch, trie_trunk_taxonomy::registration::SummaryContext {
                first: start, end: live.end(), sources: vec![[resource.definition, 0]], sealed: true,
            })?;
        (trunk, resource.branch)
    };
    #[cfg(feature = "registration")]
    let pack = live.seal_registered(&trunk, branch)?;
    #[cfg(not(feature = "registration"))]
    let pack = live.seal()?;
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())?;
    let mut reader = opened.reader();
    let mut periods = Vec::new();
    let mut raw = Vec::new();
    for i in 0..12 {
        let row = reader.get(i)?;
        let fields = plan
            .object(&row)?
            .into_iter()
            .map(|(k, v)| Ok((k, pil_codec::json_view::data_json(v)?)))
            .collect::<Result<serde_json::Map<_, _>, std::io::Error>>()?;
        periods.push(
            json!({"start":format!("00:{:02}",i*5),"count":plan.restore(&row)?.count(),"fields":fields}),
        );
        raw.push(row.crush()?);
    }
    let view = json!({"hour":"2026-01-01T00:00:00Z","periodMinutes":5,"durationUnit":"microseconds","periods":periods});
    let destination = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/summary-hour".into());
    std::fs::write(
        format!("{destination}.json"),
        serde_json::to_string_pretty(&view)?,
    )?;
    std::fs::write(
        format!("{destination}.raw.json"),
        serde_json::to_string_pretty(&raw)?,
    )?;
    std::fs::write(
        format!("{destination}.definition.json"),
        serde_json::to_string_pretty(plan.document().definitions())?,
    )?;
    std::fs::write(format!("{destination}.source.rx"), document.bytes()?)?;
    std::fs::write(format!("{destination}.time"), time)?;
    #[cfg(feature = "registration")]
    {
        std::fs::write(format!("{destination}.trunk"), trunk.bytes())?;
        std::fs::write(format!("{destination}.summary.rx"), plan.document().bytes()?)?;
    }
    std::fs::write(format!("{destination}.events.pac"), events.into_bytes()?)?;
    std::fs::write(format!("{destination}.pac"), pack.bytes())?;
    #[cfg(feature = "compression")]
    {
        let mut compressed = Vec::new();
        pil_codec::compression::seal(pack.bytes(), pil_codec::compression::Algorithm::Zstd, 1, false, &mut compressed)?;
        std::fs::write(format!("{destination}.pac.zstd"), compressed)?;
    }
    let pack_len = pack.bytes().len();
    let mut file = summaries::SummaryBox::new(plan.layout().clone())?;
    file.append(pack)?;
    file.commit()?;
    std::fs::write(format!("{destination}.summ"), file.bytes()?)?;
    eprintln!(
        "{} source events; 288 fixed summary Pills; {} bytes in the uncompressed daily Pack",
        count, pack_len
    );
    println!("{}", serde_json::to_string(&view)?);
    Ok(())
}
