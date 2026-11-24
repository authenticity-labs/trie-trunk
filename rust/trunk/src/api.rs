//! Shared native/WASM projection of the authoritative trie.
use super::*;
use serde_json::{Value, json};
fn node(n: Node<'_>) -> Value {
    json!({"id":n.id,"kind":n.kind as u32,"parent":n.parent,"name":n.name,"type":n.resource_type,"level":n.level,"branchId":n.branch_id})
}
impl Trunk {
    pub fn query(&self, v: &Value) -> Result<Value, Error> {
        let t = self;
        let integer = |key: &str| -> Result<u32, Error> {
            u32::try_from(v[key].as_u64().ok_or_else(|| format!("Missing {key}"))?)
                .map_err(|_| format!("Invalid {key}"))
        };
        let out = match v["op"].as_str() {
            Some("info") => {
                json!({"root":t.root(),"nodes":t.len(),"generation":t.generation().to_string(),"bytes":t.bytes().len()})
            }
            Some("node") => node(t.node(integer("id")?)?),
            Some("branch") => {
                node(t.branch(u16::try_from(integer("id")?).map_err(|_| "Invalid branch ID")?)?)
            }
            Some("children") => Value::Array(
                t.children(integer("id")?)?
                    .iter()
                    .map(|&i| t.node(i).map(node))
                    .collect::<Result<_, _>>()?,
            ),
            Some("path") => Value::Array(
                t.path(integer("id")?)?
                    .into_iter()
                    .map(|i| t.node(i).map(node))
                    .collect::<Result<_, _>>()?,
            ),
            Some("find") => json!(t.find(
                integer("parent")?,
                Kind::try_from(integer("kind")?)?,
                v["name"].as_str().unwrap_or(""),
                v["type"].as_str().unwrap_or(""),
                v["level"].as_str().unwrap_or("")
            )?),
            Some("resolve") => json!(
                t.resolve(
                    integer("scope")?,
                    &serde_json::from_value::<Vec<String>>(v["qualifiers"].clone())
                        .map_err(|e| e.to_string())?
                )?
            ),
            Some("sections") => json!(t.section_ids()),
            #[cfg(feature = "registration")]
            Some("registrations") => {
                let registry = t.definitions()?;
                json!(registry.resources_at(t, integer("node")?)?)
            }
            #[cfg(feature = "registration")]
            Some("lineage") => {
                let registry = t.definitions()?;
                json!(registry.lineage(v["namespace"].as_str().unwrap_or(""),
                    v["name"].as_str().ok_or("Missing Definition name")?))
            }
            #[cfg(feature = "registration")]
            Some("definition-graph") => {
                let registry = t.definitions()?;
                json!({
                    "definitions": registry.definitions(),
                    "shapes": registry.shapes(),
                    "resources": registry.resources(),
                    "drafts":registry.drafts,
                    "fieldCount": registry.field_count(),
                    "attributes": registry.attributes(),
                })
            }
            #[cfg(feature = "registration")]
            Some("definition-files") => {
                let registry = t.definitions()?;
                json!(registry.artifacts())
            }
            #[cfg(feature = "registration")]
            Some("definition") => json!(t.definition(integer("id")?)?),
            #[cfg(feature = "registration")]
            Some("route") => {
                let route = t.route(u16::try_from(integer("id")?).map_err(|_| "Invalid branch ID")?)?;
                let mut value = json!(&route);
                value["source"] = json!(route.source()?);
                value["storage"] = json!(route.storage()?);
                if let Some(path)=route.pods()? {value["storage"]["pods"]=json!(path);}
                value
            },
            #[cfg(feature = "registration")]
            Some("storage") => json!(t.route(u16::try_from(integer("id")?).map_err(|_| "Invalid branch ID")?)?.storage()?),
            Some("source-options") => json!(
                t.source_options(
                    &serde_json::from_value::<Vec<String>>(v["scope"].clone())
                        .map_err(|e| e.to_string())?,
                    integer("index")? as usize
                )?
            ),
            _ => return Err("Unknown trunk operation".into()),
        };
        Ok(out)
    }
}
