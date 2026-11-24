//! Read-only viewer projections. Registration and traversal remain in Trunk;
//! clients render these values without constructing a second registry.
use crate::{Node, Result};
use serde_json::{Value, json};

impl Node {
    pub fn resolve(&self, scope:&str) -> Result<Value> {
        let wanted=scope.split('/').collect::<Vec<_>>();
        if !scope.is_empty() && wanted.len()>4 {return Err("Invalid source scope".into());}
        let trie=self.trie();
        let mut selected=Vec::new();
        for resource in trie.definitions()?.resources() {
            let route=trie.route(resource.branch)?;
            if route.pods()?.is_some(){continue;}
            let source=route.source()?;
            if scope.is_empty() || wanted.iter().zip(source.split('/')).all(|(a,b)| *a=="*" || *a==b) {
                selected.push(resource.branch);
            }
        }
        Ok(json!(selected))
    }
    pub fn bootstrap(&self) -> Result<Value> {
        let trie = self.trie();
        let registry = trie.definitions()?;
        let mut definitions = Vec::new();
        for (id, definition) in registry.definitions().iter().enumerate() {
            if let Ok(description) = trie.registered().describe(id as u32) {
                definitions.push(json!({"digest":hex(&definition.digest),"store":[],"pil":[],
                    "registered":{"id":id,"file":definition.file,"fields":description["fields"]}}));
            }
        }
        let mut bindings = Vec::new();
        let mut sources = std::collections::BTreeSet::new();
        let mut files = Vec::new();
        let mut collections=std::collections::BTreeSet::new();
        for resource in registry.resources() {
            let route = trie.route(resource.branch)?;
            let source = route.source()?;
            let storage = route.storage()?;
            let digest = hex(&registry.definitions()[resource.definition as usize].digest);
            sources.insert(source.clone());
            bindings.push(json!({"stream":resource.branch,"branchId":resource.branch,"digest":digest,"tap":source}));
            if route.pods()?.is_some(){collections.insert(source);continue;}
            let relative = format!("{}/{}.time", storage.directory, storage.stem);
            if let Ok(metadata) = std::fs::metadata(self.root.join(&relative)) {
                files.push(json!({"path":relative,"branchId":resource.branch,"definition":digest,
                    "kind":"time","size":metadata.len(),"bootstrap":false}));
            }
        }
        Ok(json!({"native":true,"version":trie.generation().to_string(),"definitions":definitions,"bindings":bindings,
            "catalog":{"scope":"","directories":[],"files":files,"sources":sources,"collections":collections}}))
    }

    pub fn presentation(&self) -> Result<Value> {
        fn visit(node: &Node, id: u32, path: Vec<String>) -> Result<Value> {
            let trie = node.trie();
            let current = trie.query(&json!({"op":"node","id":id}))?;
            let kind = current["kind"].as_u64().ok_or("Missing node kind")?;
            let mut children = Vec::new();
            if kind < 4 {
                for &child in trie.children(id)? {
                    let value = trie.query(&json!({"op":"node","id":child}))?;
                    if matches!(value["kind"].as_u64(), Some(2..=4)) {
                        let mut next = path.clone();
                        next.push(value["name"].as_str().ok_or("Missing node name")?.into());
                        children.push(visit(node, child, next)?);
                    }
                }
            }
            let name = if path.is_empty() { "Trunk" } else { current["name"].as_str().unwrap_or("") };
            Ok(json!({"id":serde_json::to_string(&path)?,"name":name,
                "kind":(["","trunk","cache","service","resource"][kind as usize]),
                "path":path,"scope":path,"children":children}))
        }
        let trie = self.trie();
        let mut paths = serde_json::Map::new();
        for resource in trie.definitions()?.resources() {
            let branch = trie.query(&json!({"op":"branch","id":resource.branch}))?;
            let path = trie.query(&json!({"op":"path","id":branch["id"]}))?;
            paths.insert(resource.branch.to_string(), path);
        }
        Ok(json!({"tree":visit(self,trie.root(),Vec::new())?,
            "graph":trie.query(&json!({"op":"definition-graph"}))?,"paths":paths}))
    }
}
pub(crate) fn hex(bytes: &[u8]) -> String { bytes.iter().map(|v| format!("{v:02x}")).collect() }
