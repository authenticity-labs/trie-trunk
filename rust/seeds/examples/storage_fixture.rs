//! Storage-binding fixture built only through typed Seed ingress.
use data_seeds::{doc, Field, HourIndexBuilder, IndexFields};
use pil_codec::api::FieldBytes;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(std::env::args().nth(1).ok_or("output directory required")?);
    std::fs::create_dir_all(&output)?;
    let document = doc::new("indexed-state")
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("leaf_id").r#type("uint").size(2).annotation("@leaf"))
        .field(Field::new("region").r#type("tstr"))
        .field(Field::new("count").r#type("uint").size(16))
        .field(Field::new("message").r#type("tstr"))
        .sign()?;
    document.write(output.join("state.rx"))?;
    let (taxonomy, binding) = trie_trunk_taxonomy::Trunk::new("Fixture")?.registration(
        trie_trunk_taxonomy::registration::Registration { cache:"demo",service:"lab",resource:"states",resource_type:"state",level:"",file:"state.rx" }, &document)?;
    std::fs::write(output.join("trunk.trie"),taxonomy.bytes())?;
    std::fs::write(output.join("registration.json"),serde_json::to_vec(&[binding.branch as u32,binding.definition])?)?;
    let mut trunk = data_seeds::registration::Trunk::open(taxonomy.bytes())?;
    let definition = trunk.prescribe(binding.definition,document.bytes()?)?;
    let leaves = trunk.trie().branches().select([binding.branch]).leaves()?;
    let mut trie = leaves.nodes()?;
    let mut file = leaves.create()?; file.register(binding.branch)?;
    let mut storage = data_seeds::Beds::create_box(&[])?;
    let mut addresses = Vec::new();
    for hour in 0..2u64 {
        let start = hour * 3_600_000;
        let first_node = trie.len() + 1;
        let mut index = HourIndexBuilder::new(start, 2, IndexFields::from_definition(&definition)?)?;
        let mut pack = definition.fill(2)?;
        pack.hour(start)?;
        pack.header(data_seeds::integrity::HEADER, vec![0; 4])?;
        for row in 0..2 {
            let seed = definition.ingest(row, |row, seed| {
                seed.set_text_string("region", if row == 0 {"west"} else {"east"})?
                    .set_uint("count", (1u128 << 100) + hour as u128 * 2 + row as u128)?
                    .set_text_string("message", "received")?;
                let mut leaf = 0;
                for field in 2..=3 {
                    let FieldBytes::Bytes(raw) = seed.get(field)? else { return Err(data_seeds::Error::Invalid("missing source field")); };
                    leaf = trie.append(leaf, field as u16, raw.to_vec())?;
                    seed.set(field, FieldBytes::Absent)?;
                }
                index.append(start + row as u64, leaf)?;
                Ok(())
            })?;
            pack.load(seed)?;
        }
        pack.seal()?;
        let mut raw = pack.into_bytes()?;
        data_seeds::integrity::stamp(&mut raw)?;
        std::fs::write(output.join(format!("{hour}.pac")), &raw)?;
        addresses.push(storage.next_address());
        if hour == 1 {
            let mut compressed = Vec::new();
            pil_codec::compression::seal(&raw, pil_codec::compression::Algorithm::Zstd, 1, false, &mut compressed)?;
            storage.pack(compressed)?;
        } else { storage.pack(raw)?; }
        std::fs::write(output.join(format!("{hour}.indx")), index.finish().bytes())?;
        file.append(&trie)?;
        std::fs::write(output.join(format!("{hour}.leaf")),file.publish()?)?;
        std::fs::write(output.join(format!("{hour}.leaves")), trie.encode_pil(if hour == 0 {0} else {first_node})?)?;
    }
    storage.commit()?;
    std::fs::write(output.join("state.beds"), storage.bytes()?)?;
    std::fs::write(output.join("addresses.json"), serde_json::to_vec(&addresses)?)?;
    Ok(())
}
