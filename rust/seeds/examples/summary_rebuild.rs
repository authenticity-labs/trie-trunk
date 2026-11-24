//! Rebuild disposable summary fixtures from existing ingested event Packs.
//! A supplied registration manifest is authored outside this tool; no old wire
//! format is decoded here. Source event files are read-only and never replaced.
use data_seeds::{
    Cell, HourIndex, IndexFields, LeafIndex, Query, Sources, doc, registration::Trunk, summaries::*,
};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use trie_trunk_taxonomy::{
    Trunk as Taxonomy,
    registration::{Registration, Selection},
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len()==2 && args[0]=="manifest" {
        let root=PathBuf::from(&args[1]);
        let trie=Taxonomy::open(std::fs::read(root.join("trunk.trie"))?)?;
        let registry=trie.definitions()?;
        let resources=registry.resources().iter().map(|resource|Ok(serde_json::json!({
            "route":trie.route(resource.branch)?,
            "source":registry.definitions()[resource.definition as usize],
            "binding":resource.binding,
        }))).collect::<Result<Vec<_>,String>>()?;
        println!("{}",serde_json::to_string_pretty(&serde_json::json!({"name":trie.node(trie.root())?.name,"resources":resources}))?);
        return Ok(());
    }
    if args.len() != 3 {
        return Err(
            "expected source directory, registration manifest, new output directory".into(),
        );
    }
    let root = PathBuf::from(&args[0]);
    let out = PathBuf::from(&args[2]);
    if out.exists() {
        return Err("output directory must not exist".into());
    }
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let mut taxonomy = Taxonomy::new(manifest["name"].as_str().ok_or("missing trunk name")?)?;
    let resources = manifest["resources"]
        .as_array()
        .ok_or("missing registrations")?;
    std::fs::create_dir_all(out.join("definitions"))?;
    let mut files = BTreeSet::new();
    for resource in resources {
        let r = &resource["route"];
        let s = &resource["source"];
        let text = |key| r[key].as_str().ok_or("missing route property");
        let file = s["file"].as_str().ok_or("missing Definition file")?;
        let raw = std::fs::read(root.join("definitions").join(file))?;
        let document = doc::read(raw.as_slice())?;
        let selection = Selection {
            namespace: s["namespace"].as_str().unwrap_or("").into(),
            name: s["name"].as_str().ok_or("missing Definition name")?.into(),
            major: s["major"].as_u64().ok_or("missing major")? as u32,
            minor: s["minor"].as_u64().ok_or("missing minor")? as u32,
        };
        let (next, registered) = taxonomy.registration_selected(
            Registration {
                cache: text("cache")?,
                service: text("service")?,
                resource: text("resource")?,
                resource_type: text("resource_type")?,
                level: text("level")?,
                file,
            },
            &document,
            Some(serde_json::from_value(resource["binding"].clone())?),
            Some(&selection),
        )?;
        if registered.branch as u64 != r["branch"].as_u64().ok_or("missing branch")? {
            return Err("manifest reordered stable Branch IDs".into());
        }
        if registered.definition as u64 != r["definition"].as_u64().ok_or("missing Definition ID")? {
            return Err("manifest reordered stable Definition IDs".into());
        }
        taxonomy = next;
        files.insert(file.to_owned());
        std::fs::write(out.join("definitions").join(file), raw)?;
    }
    let library = summary_template()?;
    for resource in resources {
        taxonomy = taxonomy.register_summaries(
            resource["route"]["branch"].as_u64().unwrap() as u16,
            "summary.rx",
            &library,
        )?;
    }
    std::fs::write(out.join("definitions/summary.rx"), library.bytes()?)?;
    let mut trunk = Trunk::open(taxonomy.bytes())?;
    for file in files
        .iter()
        .chain(std::iter::once(&"summary.rx".to_owned()))
    {
        trunk.prescribe_file(file, &std::fs::read(out.join("definitions").join(file))?)?;
    }
    let mut visited = BTreeSet::new();
    let mut reports = Vec::new();
    for resource in resources {
        let branch = resource["route"]["branch"].as_u64().unwrap() as u16;
        let route = taxonomy.route(branch)?;
        let binding = route.storage()?;
        let prefix = PathBuf::from(&binding.directory).join(&binding.stem);
        if !visited.insert(prefix.clone()) {
            continue;
        }
        let time = data_seeds::TimeTrie::open(Arc::new(std::fs::read(
            root.join(PathBuf::from(format!("{}.time",prefix.display()))),
        )?))?;
        let calendar = time.summary_calendar()?;
        let first = calendar.day(time.header().first)?.start;
        let end = calendar.day(time.header().sealed)?.start;
        let scope = trunk.trie().branches().select([branch]).summaries()?;
        let mut ingress = data_seeds::registration::SummaryIngress::new(scope.clone(), calendar);
        let packs = Arc::new(std::fs::read(root.join(PathBuf::from(format!("{}.packs",prefix.display()))))?);
        let index = Arc::new(std::fs::read(root.join(PathBuf::from(format!("{}.indx",prefix.display()))))?);
        let leaf_bytes = std::fs::read(root.join(PathBuf::from(format!("{}.leaf",prefix.display()))))?;
        let mut count = 0usize;
        if first < end {
            for hour in time.range(first, end)? {
                let hour = hour?;
                if hour.count == 0 {
                    continue;
                }
                let at = hour.packs as usize;
                let length =
                    usize::try_from(pil_codec::pac_envelope::stored_length(&packs[at..])?)?;
                let pack = trunk.open_pack(packs.clone(), at..at + length, 256 << 20)?;
                let definition = pack.definition().clone();
                let selected = taxonomy
                    .definitions()?
                    .resources()
                    .iter()
                    .find(|r| {
                        trunk
                            .definition(r.definition)
                            .is_ok_and(|d| Arc::ptr_eq(&d, &definition))
                    })
                    .ok_or("unregistered Pack Definition")?
                    .branch;
                let fields = IndexFields::from_definition(&definition)?;
                let hour_index = HourIndex::open(
                    index.clone(),
                    hour.start as usize * 6..hour.end as usize * 6,
                    hour.hour,
                    fields,
                )?;
                let leaves = trunk
                    .trie()
                    .branches()
                    .select([selected])
                    .leaves()?
                    .read(&leaf_bytes)?;
                let timestamps = (0..hour.count as usize)
                    .map(|row| hour_index.time_leaf(row).map(|(time, _)| time))
                    .collect::<Result<Vec<_>, _>>()?;
                let pack = pack.with_storage(Arc::new(LeafIndex::new(hour_index, leaves)))?;
                let names = definition
                    .summary_fields()
                    .iter()
                    .map(|f| (f.ordinal(), f.name().to_owned()))
                    .collect::<Vec<_>>();
                let mut source = Sources::new();
                source.pack(pack);
                let mut cursor = source
                    .query(Query::new().select(names.iter().map(|(_, name)| name.clone())))?;
                loop {
                    let batch = cursor.fetch(256)?;
                    if batch.is_empty() {
                        break;
                    }
                    let mut reader = batch.reader();
                    for i in 0..batch.len() {
                        let row = reader.view(i)?;
                        // The source hourly index is authoritative for logical time.
                        let original = batch.origin(i)?.1;
                        let timestamp = timestamps[original];
                        ingress.day(calendar.day(timestamp)?.start)?.ingest(
                            selected,
                            timestamp,
                            (),
                            |_, seed| {
                                for (ordinal, name) in &names {
                                    match row.stored(name)? {
                                        Cell::Absent => {}
                                        Cell::Value(value) => {
                                            seed.set_native(*ordinal, Some(value))?;
                                        }
                                    }
                                }
                                Ok(())
                            },
                        )?;
                        count += 1;
                    }
                }
            }
        }
        let mut storage = SummaryBox::new(scope.plan().layout().clone())?;
        for day in (first..end).step_by(SUMMARY_DAY_MS as usize) {
            let accumulator = ingress.day(day)?;
            accumulator.complete(accumulator.end())?;
            let (next, _, _, pack) = accumulator.seal_registered(&taxonomy, "summary.rx")?;
            taxonomy = next;
            storage.append(pack)?;
        }
        storage.commit()?;
        let path = out.join(PathBuf::from(format!("{}.summ",prefix.display())));
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, storage.bytes()?)?;
        reports.push(serde_json::json!({"branch":branch,"source":route.source()?,"first":first,"end":end,"events":count,"days":(end-first)/SUMMARY_DAY_MS,"bytes":storage.bytes()?.len()}));
    }
    std::fs::write(out.join("trunk.trie"), taxonomy.bytes())?;
    std::fs::write(
        out.join("summary-rebuild.json"),
        serde_json::to_vec_pretty(&reports)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&reports)?);
    Ok(())
}
