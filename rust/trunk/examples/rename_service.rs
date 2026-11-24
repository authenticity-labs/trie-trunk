//! Prepare a service rename for an offline registration publication.
//! Does not move storage or replace the running registry.
use trie_trunk_taxonomy::{Kind, Trunk};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 { return Err("usage: rename_service input.trie output.trie cache old new".into()); }
    let trunk = Trunk::open(std::fs::read(&args[0])?)?;
    let cache = trunk.find(trunk.root(), Kind::Cache, &args[2], "", "")?.ok_or("Unknown cache")?;
    let service = trunk.find(cache, Kind::Service, &args[3], "", "")?.ok_or("Unknown service")?;
    let next = trunk.rename_service(service, &args[4])?;
    use std::io::Write;
    let mut out = std::fs::OpenOptions::new().write(true).create_new(true).open(&args[1])?;
    out.write_all(next.bytes())?;
    out.sync_all()?;
    println!("Prepared rename; {} node identities retained", next.len());
    for id in 1..=next.len() as u32 {
        let node = next.node(id)?;
        if node.kind == Kind::Branch && next.path(id)?.contains(&service) {
            println!("Retained Branch ID: {:?}", node.branch_id);
        }
    }
    Ok(())
}
