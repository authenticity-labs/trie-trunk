//! Query resolution borrows the authoritative trunk trie. No generated catalog.
use serde::Serialize;
use trie_trunk_taxonomy::{NodeId, Trunk};

pub struct Scope<'a> {
    pub trunk: &'a Trunk,
    pub node: NodeId,
}
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Source {
    pub cache: String,
    pub service: String,
    pub resource: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub branches: Vec<u16>,
}
impl Scope<'_> {
    pub fn resolve(&self, name: &[String]) -> Result<Source, String> {
        let branches = self.trunk.resolve(self.node, name)?;
        let leaf = self.trunk.branch(branches[0])?;
        let path = self.trunk.path(leaf.id)?;
        Ok(Source {
            cache: self.trunk.node(path[1])?.name.into(),
            service: self.trunk.node(path[2])?.name.into(),
            resource: self.trunk.node(path[3])?.name.into(),
            kind: leaf.resource_type.into(),
            branches,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_resolves_the_published_trie_without_a_catalog() {
        let trunk =
            Trunk::open(&include_bytes!("../../../trunk/tests/fixtures/after.trie")[..]).unwrap();
        let scope = Scope {
            trunk: &trunk,
            node: 4,
        };
        let fields = vec!["duration".into()];
        for source in [
            "http",
            "transactions.http",
            "payment.transactions.http",
            "demo.payment.transactions.http",
        ] {
            let result = super::super::parse_scoped(
                &format!("SELECT duration FROM {source} WHERE duration > 10000"),
                &fields,
                Some(&scope),
            )
            .unwrap();
            assert_eq!(result["plan"]["table"]["branches"], serde_json::json!([1]));
            assert_eq!(result["plan"]["table"]["type"], "http");
        }
        assert!(
            super::super::parse_scoped("SELECT * FROM transactions", &fields, Some(&scope))
                .is_err()
        );
    }
}
