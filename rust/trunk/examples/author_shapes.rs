//! Author complete prescriptions and discover their shared field shapes.
use pil_codec::doc::{self, Document, Field};
use std::path::PathBuf;
use trie_trunk_taxonomy::{
    Trunk,
    registration::{Registration, Registry},
};
fn event(name: &str, base: &str) -> Document {
    doc::new(name)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation(base))
        .field(
            Field::new("leaf_id")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
}
fn log(name: &str) -> Document {
    event(name, "@log")
        .field(Field::new("message").r#type("tstr"))
        .field(Field::new("level").r#type("uint").size(1))
}
fn http(name: &str) -> Document {
    log(name)
        .field(Field::new("method").r#type("tstr"))
        .field(Field::new("status").r#type("uint").size(2))
        .field(Field::new("uri").r#type("tstr"))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(std::env::args().nth(1).ok_or("Expected output directory")?);
    let definitions = output.join("definitions");
    std::fs::create_dir_all(&definitions)?;
    let mut trunk = Trunk::new("Prototype")?;
    // Register concrete shapes before bases to prove discovery is not dependent
    // on authoring order, inheritance syntax, or a list of recognized names.
    for document in [
        http("http-user")
            .namespace("log")
            .field(Field::new("user_id").r#type("tstr")),
        log("security")
            .namespace("log")
            .field(Field::new("actor").r#type("tstr"))
            .field(Field::new("action").r#type("tstr"))
            .field(Field::new("outcome").r#type("tstr")),
        http("http").namespace("log"),
        doc::new("attributed-to-user").field(Field::new("user_id").r#type("tstr")),
        log("log"),
        event("event", "@event"),
        doc::new("transaction")
            .field(Field::new("id").r#type("bytes").size(16).annotation("@object"))
            .field(Field::new("amount").r#type("uint").size(8)),
    ] {
        let document = document.sign()?;
        let definition = &document.definitions()[0];
        let name = &definition.header.rx_name;
        let qualified = if definition.header.namespace.is_empty() {
            name.clone()
        } else {
            format!("{}.{name}", definition.header.namespace)
        };
        let file = format!("{qualified}.v0.0.rx");
        let path = definitions.join(&file);
        document.write(&path)?;
        let restored = doc::read(path.as_path())?;
        restored.validate()?;
        if restored.definitions() != document.definitions() {
            return Err("Definition round trip changed fields".into());
        }
        let (service, resource) = if name == "transaction" {
            ("payment", "transactions")
        } else {
            ("application", "events")
        };
        let old = trunk;
        let (next, registration) = old.registration(
            Registration {
                cache: "demo",
                service,
                resource,
                resource_type: name,
                level: "",
                file: &file,
            },
            &restored,
        )?;
        assert!(next.extends(&old));
        trunk = next;
        if name == "http" {
            // Storage partitions are explicit configuration; presentation can
            // still distinguish WARN, ERROR and FATAL within the error partition.
            let groups = serde_json::json!({
                "trace":"1,2,3,4", "debug":"5,6,7,8", "info":"9,10,11,12",
                "error":"13,14,15,16,17,18,19,20,21,22,23,24"
            });
            let binding: std::collections::BTreeMap<String,serde_json::Value> = std::iter::once(("field.split".into(),"level".into())).chain(
                groups.as_object().unwrap().iter().map(|(group,values)|(format!("split.{group}"),values.clone()))).collect();
            for group in groups.as_object().unwrap().keys() {
                let (next, _) = trunk.registration_with_binding(Registration {
                    cache:"demo", service:"application", resource:"events",
                    resource_type:name, level:group, file:&file,
                }, &restored, binding.clone())?;
                trunk = next;
            }
        }
        println!(
            "{file}: {} fields, {} bytes, branch {}",
            definition.fields.len(),
            document.bytes()?.len(),
            registration.branch
        );
    }
    std::fs::write(output.join("trunk.trie"), trunk.bytes())?;
    let reopened = Trunk::open(std::fs::read(output.join("trunk.trie"))?)?;
    let registry = Registry::read(&reopened)?;
    for (id, definition) in registry.definitions().iter().enumerate() {
        let bases: Vec<_> = registry.shapes()[definition.shape as usize]
            .bases
            .iter()
            .flat_map(|base| {
                registry
                    .definitions()
                    .iter()
                    .enumerate()
                    .filter(move |(_, d)| d.shape == *base)
            })
            .collect();
        println!(
            "{} <- {}",
            definition.name,
            if bases.is_empty() {
                "(root)".into()
            } else {
                bases
                    .iter()
                    .map(|(_, d)| d.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
        for (base, d) in bases {
            println!(
                "  {} ordinals -> {:?}",
                d.name,
                registry.projection(base as u32, id as u32)?
            );
        }
    }
    println!(
        "{} shapes, {} shared field identities; {} bytes in trunk.trie",
        registry.shapes().len(),
        registry.field_count(),
        trunk.bytes().len()
    );
    Ok(())
}
