#![cfg(feature = "registration")]
use std::collections::BTreeMap;
use pil_codec::doc::{self, Document, Field};
use trie_trunk_taxonomy::{
    Trunk,
    registration::{Registration, Registry},
};
fn uint(name: &str, width: u16) -> Field {
    Field::new(name).r#type("uint").size(width)
}

#[test]
fn service_rename_preserves_registered_identity() {
    use trie_trunk_taxonomy::Kind;
    let definition = doc("query", vec![uint("duration", 4)]);
    let (old, registered) = Trunk::new("Trunk").unwrap().registration(Registration {
        cache: "demo", service: "trie-trunk", resource: "queries",
        resource_type: "query", level: "", file: "log-query.rx",
    }, &definition).unwrap();
    let cache = old.find(old.root(), Kind::Cache, "demo", "", "").unwrap().unwrap();
    let service = old.find(cache, Kind::Service, "trie-trunk", "", "").unwrap().unwrap();
    let next = old.rename_service(service, "trunk-client").unwrap();
    assert_eq!(next.len(), old.len());
    assert_eq!(next.route(registered.branch).unwrap().definition, registered.definition);
    assert_eq!(next.route(registered.branch).unwrap().source().unwrap(), "demo/trunk-client/queries/query");
    assert_eq!(old.route(registered.branch).unwrap().source().unwrap(), "demo/trie-trunk/queries/query");
    assert_eq!(next.branch(registered.branch).unwrap().id, old.branch(registered.branch).unwrap().id);
    for id in old.section_ids() { assert_eq!(old.section(id), next.section(id)); }
    assert_eq!(next.bytes(), next.rename_service(service, "trunk-client").unwrap().bytes());
    assert!(old.rename_service(service, "../escape").is_err());
    assert!(old.rename_service(cache, "new").is_err());
}
fn text(name: &str) -> Field {
    Field::new(name).r#type("tstr")
}
fn doc(name: &str, fields: Vec<Field>) -> Document {
    fields
        .into_iter()
        .fold(doc::new(name), |d, f| d.field(f))
        .sign()
        .unwrap()
}
fn add(t: &Trunk, d: &Document, kind: &str) -> (Trunk, u32) {
    let file = format!("{kind}.v0.0.rx");
    let (t, r) = t
        .registration(
            Registration {
                cache: "demo",
                service: "application",
                resource: "events",
                resource_type: kind,
                level: "",
                file: &file,
            },
            d,
        )
        .unwrap();
    (t, r.definition)
}
fn bases(r: &Registry, d: u32) -> Vec<String> {
    let node = &r.definitions()[d as usize];
    let mut out: Vec<_> = r.shapes()[node.shape as usize]
        .bases
        .iter()
        .flat_map(|&base| {
            r.definitions()
                .iter()
                .filter(move |d| d.shape == base)
                .map(|d| d.qualified_name())
        })
        .collect();
    out.sort();
    out
}
fn split_binding(field: &str, groups: serde_json::Value) -> std::collections::BTreeMap<String,serde_json::Value> {
    std::iter::once(("field.split".into(),field.into())).chain(
        groups.as_object().unwrap().iter().map(|(group,values)|(format!("split.{group}"),values.clone()))).collect()
}

#[test]
fn arbitrary_shapes_discover_multiple_roots_and_late_bases() {
    let start = Trunk::new("Example").unwrap();
    let child = doc(
        "combined",
        vec![text("other"), uint("reading", 4), text("operator")],
    );
    let (t, child) = add(&start, &child, "combined");
    let (t, operator) = add(&t, &doc("operator", vec![text("operator")]), "operator");
    let (t, reading) = add(&t, &doc("reading", vec![uint("reading", 4)]), "reading");
    let r = Registry::read(&t).unwrap();
    assert_eq!(r.roots().len(), 2);
    assert_eq!(bases(&r, child), vec!["operator", "reading"]);
    assert_eq!(r.projection(operator, child).unwrap(), vec![2]);
    assert_eq!(r.projection(reading, child).unwrap(), vec![1]);
    let (t, middle) = add(
        &t,
        &doc("middle", vec![uint("reading", 4), text("operator")]),
        "middle",
    );
    let r = Registry::read(&t).unwrap();
    assert_eq!(bases(&r, child), vec!["middle"]);
    assert_eq!(bases(&r, middle), vec!["operator", "reading"]);
    assert_eq!(
        r.bases(r.definitions()[child as usize].shape)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(r.projection(middle, child).unwrap(), vec![1, 2]);
    assert!(t.extends(&start));
}
#[test]
fn reorder_shares_shape_but_keeps_separate_definition_ordinal_maps() {
    let t = Trunk::new("Example").unwrap();
    let a = doc("a", vec![uint("n", 4), text("s")]);
    let b = doc("b", vec![text("s"), uint("n", 4)]);
    let original = a.bytes().unwrap().to_vec();
    let (t, a_id) = add(&t, &a, "a");
    let (t, b_id) = add(&t, &b, "b");
    let r = Registry::read(&t).unwrap();
    assert_ne!(
        a.definitions()[0].header.rx_hash,
        b.definitions()[0].header.rx_hash
    );
    assert_eq!(
        r.definitions()[a_id as usize].shape,
        r.definitions()[b_id as usize].shape
    );
    assert_eq!(r.projection(a_id, b_id).unwrap(), vec![1, 0]);
    assert_eq!(a.bytes().unwrap(), original);
    assert_eq!(r.roots().len(), 1);
}
#[test]
fn rebalancing_field_ordinals_preserves_dag_edges_and_refreshes_access_mapping() {
    let t = Trunk::new("Example").unwrap();
    let (t, base) = add(&t, &doc("base", vec![text("s")]), "base");
    let (t, first) = add(&t, &doc("layout", vec![uint("n", 4), text("s")]), "first");
    let before = Registry::read(&t).unwrap();
    let (t, rebalanced) = add(
        &t,
        &doc("layout", vec![text("s"), uint("n", 4)]),
        "rebalanced",
    );
    let after = Registry::read(&t).unwrap();
    assert_eq!(before.shapes(), after.shapes());
    assert_eq!(after.projection(base, first).unwrap(), vec![1]);
    assert_eq!(after.projection(base, rebalanced).unwrap(), vec![0]);
    assert_ne!(
        after.definitions()[first as usize].digest,
        after.definitions()[rebalanced as usize].digest
    );
}
#[test]
fn nested_field_order_does_not_change_inline_or_referenced_shape_relationships() {
    let mut t = Trunk::new("Example").unwrap();
    for form in ["inline", "object", "array", "map"] {
        let make = |reverse| {
            let fields = if reverse {
                vec![text("city"), uint("code", 4)]
            } else {
                vec![uint("code", 4), text("city")]
            };
            if form == "inline" {
                return doc("nested", vec![Field::new("payload").record(fields)]);
            }
            let field = match form {
                "object" => Field::new("payload").object("address"),
                "array" => Field::new("payload").array_object("address"),
                "map" => Field::new("payload").map_object("tstr", "address"),
                _ => unreachable!(),
            };
            fields
                .into_iter()
                .fold(
                    doc::new("nested").field(field).definition("address"),
                    |d, f| d.field(f),
                )
                .select(0)
                .sign()
                .unwrap()
        };
        let (next, a) = add(&t, &make(false), &format!("{form}-a"));
        let before = Registry::read(&next).unwrap();
        let (next, b) = add(&next, &make(true), &format!("{form}-b"));
        let after = Registry::read(&next).unwrap();
        assert_eq!(before.shapes(), after.shapes(), "{form}");
        assert_eq!(after.projection(a, b).unwrap(), vec![0]);
        assert_ne!(
            after.definitions()[a as usize].digest,
            after.definitions()[b as usize].digest
        );
        t = next;
    }
}
#[test]
fn annotations_do_not_change_structural_shapes_or_registration_identity() {
    let t = Trunk::new("Example").unwrap();
    let plain = doc("sample", vec![uint("n", 4)]);
    let changed = doc(
        "sample",
        vec![uint("n", 4).annotation("#application-owned(value)")],
    );
    assert_eq!(
        plain.definitions()[0].header.rx_hash,
        changed.definitions()[0].header.rx_hash
    );
    assert_ne!(plain.bytes().unwrap(), changed.bytes().unwrap());
    let (t, id) = add(&t, &plain, "sample");
    let (next, id2) = add(&t, &changed, "sample");
    assert_eq!(id, id2);
    assert_ne!(t.bytes(), next.bytes());
    assert!(next.extends(&t));
    let before = Registry::read(&t).unwrap();
    let after = Registry::read(&next).unwrap();
    assert_eq!(before.definitions(), after.definitions());
    assert_eq!(before.annotations(id).unwrap(), &[vec![Vec::<String>::new()]]);
    assert_eq!(after.annotations(id).unwrap(), &[
        vec![Vec::<String>::new()],
        vec![vec!["#application-owned(value)".to_owned()]],
    ]);
    assert_eq!(before.shapes(), after.shapes());
    let file = "sample.v0.0.rx";
    assert_ne!(before.artifact(file).unwrap(), after.artifact(file).unwrap());
    assert!(before.artifact(file).unwrap().matches(plain.bytes().unwrap()));
    assert!(after.artifact(file).unwrap().matches(changed.bytes().unwrap()));
    assert!(!before.artifact(file).unwrap().matches(changed.bytes().unwrap()));
    let (same, _) = add(&next, &changed, "sample");
    assert_eq!(same.bytes(), next.bytes());
    let (rollback, rollback_id) = add(&next, &plain, "sample");
    assert_eq!(rollback_id, id);
    let restored = Registry::read(&rollback).unwrap();
    assert_eq!(&restored.annotations(id).unwrap()[..2], after.annotations(id).unwrap());
    assert_eq!(restored.annotations(id).unwrap().last().unwrap(), &vec![Vec::<String>::new()]);
    assert!(std::ptr::eq(next.definitions().unwrap(), next.definitions().unwrap()));
    assert_eq!(next.definition(id).unwrap(), &after.definitions()[id as usize]);
    if let Ok(path) = std::env::var("TRUNK_REPLICATION_FIXTURE") {
        let path = std::path::Path::new(&path);
        std::fs::create_dir_all(path).unwrap();
        for (name, bytes) in [("before.trie", t.bytes()), ("after.trie", next.bytes()),
            ("before.rx", plain.bytes().unwrap()), ("after.rx", changed.bytes().unwrap())] {
            std::fs::write(path.join(name), bytes).unwrap();
        }
    }
}
#[test]
fn types_widths_header_roles_and_enum_dictionaries_are_not_conflated() {
    let mut t = Trunk::new("Example").unwrap();
    let mut ids = vec![];
    for (i, field) in [
        uint("value", 4),
        uint("value", 8),
        text("value"),
        uint("value", 4).header(true),
        Field::new("value")
            .enumeration("tstr", ["a".into(), "b".into()])
            .size(1),
        Field::new("value")
            .enumeration("tstr", ["b".into(), "a".into()])
            .size(1),
        Field::new("value").array("uint").element_size(4),
        Field::new("value").array("uint").element_size(8),
        Field::new("value").map("tstr", "uint").value_size(4),
        Field::new("value").map("tstr", "uint").value_size(8),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("shape{i}");
        let (next, id) = add(&t, &doc(&name, vec![field]), &name);
        t = next;
        ids.push(id);
    }
    let r = Registry::read(&t).unwrap();
    assert_eq!(r.roots().len(), ids.len());
    for &a in &ids {
        for &b in &ids {
            if a != b {
                assert!(r.projection(a, b).is_err());
            }
        }
    }
}
#[test]
fn object_reference_ordinals_are_resolved_through_the_rx_bundle() {
    let a = doc::new("a")
        .field(Field::new("payload").object("address"))
        .definition("address")
        .field(text("city"))
        .select(0)
        .sign()
        .unwrap();
    let b = doc::new("b")
        .field(Field::new("payload").object("address"))
        .definition("unrelated")
        .field(uint("extra", 4))
        .definition("address")
        .field(text("city").annotation("!display(foo)"))
        .select(0)
        .sign()
        .unwrap();
    let t = Trunk::new("Example").unwrap();
    let (t, a_id) = add(&t, &a, "a");
    let (t, b_id) = add(&t, &b, "b");
    let r = Registry::read(&t).unwrap();
    assert_eq!(r.projection(a_id, b_id).unwrap(), vec![0]);
}
#[test]
fn inline_records_and_any_are_supported_without_interpreting_application_fields() {
    let t = Trunk::new("Example").unwrap();
    let a = doc(
        "a",
        vec![
            Field::new("body").record([text("x")]),
            Field::new("extras").array("any"),
        ],
    );
    let b = doc(
        "b",
        vec![
            Field::new("body").record([text("x").annotation("@anything")]),
            Field::new("extras").array("any"),
            Field::new("open").map("tstr", "any"),
        ],
    );
    let (t, a_id) = add(&t, &a, "a");
    let (t, b_id) = add(&t, &b, "b");
    let r = Registry::read(&t).unwrap();
    assert_eq!(r.projection(a_id, b_id).unwrap(), vec![0, 1]);
}
#[test]
fn old_taxonomy_nodes_labels_sections_survive_and_failed_registration_is_atomic() {
    let t = Trunk::open(&include_bytes!("fixtures/after.trie")[..]).unwrap();
    let d = doc("sample", vec![uint("n", 4)]);
    let (next, _) = add(&t, &d, "sample");
    assert!(next.extends(&t));
    assert_eq!(t.section(1), next.section(1));
    assert_eq!(t.section(9), next.section(9));
    for id in 1..=3 {
        assert_eq!(t.branch(id).unwrap().id, next.branch(id).unwrap().id);
    }
    let snapshot = next.bytes().to_vec();
    let err = next.registration(
        Registration {
            cache: "demo",
            service: "application",
            resource: "events",
            resource_type: "sample",
            level: "",
            file: "conflict.v0.0.rx",
        },
        &doc("conflict", vec![text("n")]),
    );
    assert!(err.is_err());
    assert_eq!(next.bytes(), snapshot);
    let (same, _) = add(&next, &d, "sample");
    assert_eq!(same.bytes(), snapshot);
    let unsigned = doc::new("unsigned").field(text("x"));
    assert!(
        next.registration(
            Registration {
                cache: "demo",
                service: "application",
                resource: "events",
                resource_type: "unsigned",
                level: "",
                file: "unsigned.rx"
            },
            &unsigned
        )
        .is_err()
    );
}
#[test]
fn authored_examples_reopen_with_the_inferred_dag() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/implicit-shapes");
    let t = Trunk::open(std::fs::read(path.join("trunk.trie")).unwrap()).unwrap();
    let r = Registry::read(&t).unwrap();
    let find = |name: &str| {
        r.definitions()
            .iter()
            .position(|d| d.qualified_name() == name)
            .unwrap() as u32
    };
    assert_eq!(r.roots().len(), 3);
    assert_eq!(
        bases(&r, find("log.http-user")),
        vec!["attributed-to-user", "log.http"]
    );
    assert_eq!(bases(&r, find("log.http")), vec!["log"]);
    assert_eq!(bases(&r, find("log.security")), vec!["log"]);
    assert_eq!(bases(&r, find("log")), vec!["event"]);
    assert_eq!(
        r.projection(find("event"), find("log.http-user")).unwrap(),
        vec![0, 1, 2]
    );
    for d in r.definitions() {
        let source = doc::read(path.join("definitions").join(&d.file).as_path()).unwrap();
        source.validate().unwrap();
        assert_eq!(
            source.definitions()[d.member as usize].header.rx_hash,
            d.digest
        );
    }
}

#[test]
fn registration_rejects_generic_identity_annotations() {
    for tag in ["@id", "@id(uuidv7)"] {
        let document = doc("sample", vec![Field::new("key").r#type("bytes").size(10).annotation(tag)]);
        let result = trie_trunk_taxonomy::base::Base::read(&document.definitions()[0].fields);
        assert!(result.unwrap_err().contains("Choose @event, @log, or @object"));
    }
}

#[test]
fn resource_bindings_live_in_trie_without_changing_definition_or_addresses() {
    use serde_json::json;
    let document = doc(
        "event",
        vec![
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
            Field::new("id").r#type("bytes").size(10).annotation("@event"),
            text("region"),
        ],
    );
    let original_rx = document.bytes().unwrap().to_vec();
    let original_hash = document.definitions()[0].header.rx_hash.clone();
    let target = || Registration {
        cache: "demo",
        service: "payment",
        resource: "transactions",
        resource_type: "http",
        level: "",
        file: "event.v0.0.rx",
    };
    let binding: BTreeMap<String, serde_json::Value> = serde_json::from_value(json!({
        "strategy":"log", "timezone":"UTC", "path":["region"],
        "roles":{"event_id":"id"}, "references":[]
    }))
    .unwrap();
    let (first, registered) = Trunk::new("Example")
        .unwrap()
        .registration_with_binding(target(), &document, binding.clone())
        .unwrap();
    let node = first.branch(registered.branch).unwrap().id;
    let mut changed = binding.clone();
    changed.insert("timezone".into(), json!("America/New_York"));
    let (second, updated) = first
        .registration_with_binding(target(), &document, changed.clone())
        .unwrap();
    assert_eq!(registered.branch, updated.branch);
    assert_eq!(registered.definition, updated.definition);
    assert_eq!(second.branch(updated.branch).unwrap().id, node);
    assert!(second.extends(&first));
    let reopened = Trunk::open(second.bytes().to_vec()).unwrap();
    let registry = Registry::read(&reopened).unwrap();
    assert_eq!(registry.resources()[0].binding, changed);
    assert_eq!(registry.definitions()[0].digest, original_hash);
    assert_eq!(document.bytes().unwrap(), original_rx);
    assert_eq!(
        Registry::read(&first).unwrap().resources()[0].binding,
        binding
    );
    // Registering the same .rx without a metadata update must not erase policy.
    let (third, _) = reopened.registration(target(), &document).unwrap();
    assert_eq!(
        Registry::read(&third).unwrap().resources()[0].binding,
        changed
    );
    // The same Definition can be used by another resource with independent policy.
    let (fourth, other) = third
        .registration_with_binding(
            Registration {
                resource: "invoices",
                ..target()
            },
            &document,
            BTreeMap::new(),
        )
        .unwrap();
    assert_ne!(other.branch, updated.branch);
    assert_eq!(other.definition, updated.definition);
    let registry = Registry::read(&fourth).unwrap();
    assert_eq!(registry.definitions().len(), 1);
    assert_eq!(registry.resources()[0].binding, changed);
    assert!(registry.resources()[1].binding.is_empty());
}

#[test]
fn optional_namespaces_and_version_selection_bind_exact_taxonomy_branches() {
    use trie_trunk_taxonomy::registration::Selection;
    let old = doc::new("http")
        .namespace("log")
        .version(0, 0)
        .field(uint("status", 2))
        .sign()
        .unwrap();
    let current = doc::new("http")
        .namespace("log")
        .version(0, 1)
        .field(uint("status", 2))
        .field(text("method"))
        .compound(old.bytes().unwrap())
        .sign()
        .unwrap();
    assert_eq!(current.member("log", "http", 0, 0).unwrap(), 1);
    assert!(current.member("", "http", 0, 0).is_err());
    let target = || Registration {
        cache: "demo",
        service: "payment",
        resource: "transactions",
        resource_type: "http",
        level: "",
        file: "log.http.rx",
    };
    let (t, first) = Trunk::new("Example")
        .unwrap()
        .registration(target(), &old)
        .unwrap();
    let old_registry = Registry::read(&t).unwrap();
    let (t, latest) = t.registration(target(), &current).unwrap();
    assert_ne!(first.branch, latest.branch);
    let registry = Registry::read(&t).unwrap();
    assert_eq!(registry.definitions()[first.definition as usize].member, 1);
    let rx = old_registry.prescribe(first.definition, &current).unwrap();
    assert!(rx.field_ordinal("status").is_some());
    assert!(rx.field_ordinal("method").is_none());
    assert!(
        current
            .prescribe()
            .unwrap()
            .field_ordinal("method")
            .is_some()
    );
    assert!(
        current
            .prescribe_version("log", "http", 0, 0)
            .unwrap()
            .field_ordinal("method")
            .is_none()
    );
    assert!(current.prescribe_version("log", "http", 0, 9).is_err());
    let (same, selected) = t
        .registration_selected(
            target(),
            &current,
            None,
            Some(&Selection {
                namespace: "log".into(),
                name: "http".into(),
                major: 0,
                minor: 0,
            }),
        )
        .unwrap();
    assert_eq!(first, selected);
    assert_eq!(same.bytes(), t.bytes());
    assert_eq!(
        registry
            .lineage("log", "http")
            .iter()
            .map(|(_, d)| d.minor)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        registry.definitions()[first.definition as usize].qualified_name(),
        "log.http"
    );
    assert_eq!(registry.resources_at(&t, t.root()).unwrap().len(), 2);
    assert_eq!(
        registry
            .resources_at(&t, t.branch(first.branch).unwrap().id)
            .unwrap(),
        vec![&first]
    );
    assert!(
        t.registration(target(), &old)
            .err()
            .unwrap()
            .contains("would remove registered schema")
    );
    let plain = doc("transaction", vec![uint("amount", 8)]);
    let (t, plain_resource) = t
        .registration(
            Registration {
                resource_type: "transaction",
                file: "transaction.rx",
                service: "payment",
                resource: "transactions",
                ..target()
            },
            &plain,
        )
        .unwrap();
    let registry = Registry::read(&t).unwrap();
    let definition = &registry.definitions()[plain_resource.definition as usize];
    assert!(definition.namespace.is_empty());
    assert_eq!(definition.qualified_name(), "transaction");
    assert_eq!(registry.lineage("", "transaction").len(), 1);
    assert!(
        registry
            .lineage("collection", "transaction")
            .is_empty()
    );
}

#[test]
fn storage_paths_share_versions_but_reject_ambiguous_type_branch_pairs() {
    let document = doc("event", vec![uint("id", 4), Field::new("category").enumeration("tstr",["c".into(),"b.c".into()])]);
    let register = |kind, level| Registration { cache: "cache", service: "service", resource: "resource",
        resource_type: kind, level, file: "event.rx" };
    let (trie, first) = Trunk::new("Test").unwrap().registration_with_binding(register("a.b", "c"), &document, split_binding("category",serde_json::json!({"c":["c"],"b_c":["b.c"]}))).unwrap();
    let path = trie.route(first.branch).unwrap().storage().unwrap();
    assert_eq!(path.directory, "cache/service/resource");
    assert_eq!(path.stem, "a.b.c.0");
    assert!(trie.registration(register("a", "b.c"), &document).err().unwrap().contains("alias"));
    assert!(trie.registration(register("../a", "c"), &document).is_err());
    let later = doc::new("event").version(0, 1).field(uint("id", 4))
        .field(Field::new("category").enumeration("tstr",["c".into(),"b.c".into()]))
        .compound(document.bytes().unwrap()).sign().unwrap();
    let (updated, next) = trie.registration(register("a.b", "c"), &later).unwrap();
    assert_ne!(first.branch, next.branch);
    assert_eq!(updated.route(next.branch).unwrap().storage().unwrap(), path);
}

#[test]
fn discovery_and_version_scopes_come_from_registered_resource_identity() {
    let first = doc::new("event").version(1,0).field(uint("time",6)).sign().unwrap();
    let next = doc::new("event").version(1,1).field(text("message"))
        .compound(first.bytes().unwrap()).sign().unwrap();
    let target = |resource, kind, level| Registration {cache:"demo",service:"service",resource,
        resource_type:kind,level,file:"event.rx"};
    let (trie, old) = Trunk::new("Test").unwrap().registration(target("events","http",""), &first).unwrap();
    let (trie, current) = trie.registration(target("events","http",""), &next).unwrap();
    let (trie, security) = trie.registration(target("events","security",""), &next).unwrap();
    let object = doc::new("account").version(1,0).field(Field::new("id").r#type("bytes").size(16).annotation("@object")).sign().unwrap();
    let (trie, revision) = trie.registration(Registration {file:"account.rx",..target("events","account","")}, &object).unwrap();
    let (trie, other) = trie.registration(target("other","http",""), &next).unwrap();
    for branch in [old.branch, current.branch] {
        assert_eq!(trie.branch_versions(branch).unwrap(), vec![old.branch,current.branch]);
    }
    for branch in [security.branch, revision.branch, other.branch] {
        assert_eq!(trie.branch_versions(branch).unwrap(), vec![branch]);
    }
    let mut catalog = trie.catalog("demo").unwrap();
    assert_eq!(catalog.bindings.len(),5);
    assert_eq!(catalog.sources,vec!["demo/service/events/account","demo/service/events/http","demo/service/events/security","demo/service/other/http"]);
    assert_eq!(catalog.collections,vec!["demo/service/events/account"]);
    assert_eq!(trie.route(revision.branch).unwrap().pods().unwrap().as_deref(),Some("demo/service/events/account.pods"));
    assert!(trie.route(revision.branch).unwrap().event_storage().is_err());
    assert!(catalog.files.iter().any(|f|f.kind=="pods" && f.path=="demo/service/events/account.pods"));
    assert_eq!(catalog.files.iter().filter(|f|f.path=="demo/service/events/http.v1.packs").count(),1);
    assert!(catalog.files.iter().any(|f|f.path=="demo/service/events/security.v1.packs"));
    assert!(catalog.files.iter().any(|f|f.path.contains(".v1")));
    let states:Vec<_>=catalog.files.iter().filter(|f|f.kind=="state").collect();
    assert_eq!(states.len(),3,"each event type has its own state file");
    assert_eq!(trie.state_path("demo/service/events/http",None).unwrap(),"demo/service/events/http.v1.leaf");
    assert_eq!(trie.state_path("demo/service/events/security",None).unwrap(),"demo/service/events/security.v1.leaf");
    assert!(trie.state_path("demo/service/events",None).is_err(),"type selection cannot be guessed");
    assert!(trie.state_path("demo/service/events",Some(&trie.route(current.branch).unwrap().digest)).is_err(),"one Definition registered to multiple types is still ambiguous");
    let state=states.into_iter().find(|f|f.path=="demo/service/events/http.v1.leaf").unwrap();
    assert_eq!(state.definitions.len(),2);
    let mut bad=state.clone();bad.definitions=vec!["00".repeat(32)];bad.digest=Some("11".repeat(32));
    assert!(catalog.files_present(&[bad]).is_err(),"unknown roots cannot enter a type's state file");
    let file = catalog.files.iter().find(|f|f.path=="definitions/event.rx").unwrap().clone();
    let mut bad = file.clone(); bad.digest = Some("00".repeat(32));
    assert!(catalog.files_present(&[bad]).is_err());
    catalog.files_present(&[file]).unwrap();
    assert_eq!(catalog.files.len(),1);
    assert_eq!(catalog.bindings.len(),5);
    assert!(trie.catalog("absent").unwrap().bindings.is_empty());
}

#[test]
fn splits_require_explicit_disjoint_partition_configuration() {
    let document = doc("http", vec![Field::new("severity").enumeration("tstr", ["info".into(),"error".into()]), text("message")]);
    let target = |value| Registration {cache:"demo",service:"service",resource:"events",resource_type:"http",level:value,file:"http.rx"};
    let original = Trunk::new("Test").unwrap();
    assert!(original.registration(target("info"), &document).is_err());
    let configure = |field: &str| split_binding(field,serde_json::json!({"info":["info"],"error":["error"]}));
    assert!(original.registration_with_binding(target("info"), &document, configure("message")).is_ok());
    assert!(original.registration_with_binding(target("info"), &document, configure("missing")).is_err());
    assert!(original.registration_with_binding(target("warning"), &document, configure("severity")).is_err());
    let (trie, info) = original.registration_with_binding(target("info"), &document, configure("severity")).unwrap();
    let (trie, error) = trie.registration_with_binding(target("error"), &document, configure("severity")).unwrap();
    assert_eq!(trie.route(info.branch).unwrap().source().unwrap(), "demo/service/events/http.info");
    assert_eq!(trie.route(error.branch).unwrap().storage().unwrap().stem,"http.error.0");
    assert_eq!(trie.route(info.branch).unwrap().leaf_path().unwrap(),trie.route(error.branch).unwrap().leaf_path().unwrap());
    assert_eq!(trie.catalog("demo").unwrap().files.iter().filter(|f| f.kind=="state").count(),1);
    let (same, _) = trie.registration(target("info"), &document).unwrap();
    assert_eq!(same.bytes(),trie.bytes(),"registration preserves configured split field");
    assert_eq!(trie.branch_versions(info.branch).unwrap(), vec![info.branch]);
    assert_eq!(trie.branch_versions(error.branch).unwrap(), vec![error.branch]);
    let updated = doc::new("http").version(0, 1)
        .field(Field::new("severity").enumeration("tstr", ["info".into(), "error".into()]))
        .field(text("message")).field(text("detail")).compound(document.bytes().unwrap()).sign().unwrap();
    let (later, next) = trie.registration(target("info"), &updated).unwrap();
    assert_eq!(next.binding, info.binding, "new versions retain the configured split field");
    let (same, repeated) = later.registration(target("info"), &updated).unwrap();
    assert_eq!(repeated.binding, info.binding);
    assert_eq!(same.bytes(), later.bytes());
    let (other, security) = trie.registration(Registration {resource_type:"security",level:"",file:"security.rx",..target("")},&doc("security",vec![text("message")])).unwrap();
    assert_eq!(other.route(security.branch).unwrap().source().unwrap(), "demo/service/events/security");
}

#[test]
fn numeric_severity_uses_metadata_groups_without_changing_its_definition() {
    use serde_json::{Value, json};
    use trie_trunk_taxonomy::splits::Splits;
    let metadata: Value = serde_json::from_str(include_str!("../../../schemas/logs/presentation.json")).unwrap();
    let level = metadata["base"].as_array().unwrap().iter().find(|f| f["name"] == "level").unwrap();
    let binding = split_binding("level",level["groups"].clone());
    let document = doc("http", vec![uint("level",1),text("message")]);
    let original = document.bytes().unwrap();
    let plan = Splits::compile(&binding,&document.definitions()[0].fields).unwrap().unwrap();
    let mut trie = Trunk::new("Test").unwrap();
    for (i, group) in ["trace","debug","info","warn","error","fatal"].into_iter().enumerate() {
        for value in 1+i*4..=4+i*4 { assert_eq!(plan.group(&json!(value)),Some(group)); }
        let (next, registered) = trie.registration_with_binding(Registration {
            cache:"demo",service:"service",resource:"events",resource_type:"http",level:group,file:"http.rx",
        },&document,binding.clone()).unwrap();
        assert_eq!(next.route(registered.branch).unwrap().storage().unwrap().stem,format!("http.{group}.0"));
        trie = next;
    }
    for value in [Value::Null,json!(0),json!(25),json!("ERROR")] { assert_eq!(plan.group(&value),None); }
    assert_eq!(document.bytes().unwrap(),original,"grouping does not alter RX or its digest");
    let registry = Registry::read(&trie).unwrap();
    let persisted = &registry.resources()[0].binding;
    assert_eq!(persisted, &binding, "numeric grouping metadata survives trie publication");
    let restored = Splits::compile(persisted, &document.definitions()[0].fields).unwrap().unwrap();
    assert_eq!(restored.group(&json!(18)),Some("error"));
    for groups in [json!({"error":[17],"fatal":[17]}),json!({"error":[256]}),
        json!({"error":["17"]}),json!({"../error":[17]})] {
        assert!(Splits::compile(&split_binding("level",groups),&document.definitions()[0].fields).is_err());
    }
}

#[test]
fn explicit_partition_lists_and_wildcard_have_deterministic_precedence() {
    use serde_json::{Value,json};
    use trie_trunk_taxonomy::splits::Splits;
    let document=doc("http",vec![uint("level",1)]);
    let fields=&document.definitions()[0].fields;
    let config=|groups| split_binding("level",groups);
    let binding=config(json!({
        "trace":"1, 2,3,4", "debug":"5,6,7,8", "info":"9,10,11,12",
        "error":"13,14,15,16,17,18,19,20,21,22,23,24"
    }));
    let plan=Splits::compile(&binding,fields).unwrap().unwrap();
    for level in 13..=24 {assert_eq!(plan.group(&json!(level)),Some("error"));}
    assert_eq!(plan.select(None),None);
    for value in [Value::Null,json!(0),json!(25),json!("ERROR")] {assert_eq!(plan.group(&value),None);}
    let binding=config(json!({"other":"*","error":"17,18,19,20"}));
    let plan=Splits::compile(&binding,fields).unwrap().unwrap();
    assert_eq!(plan.group(&json!(18)),Some("error"));
    assert_eq!(plan.group(&json!(0)),Some("other"));
    assert_eq!(plan.group(&Value::Null),Some("other"));
    assert_eq!(plan.select(None),Some("other"));
    assert!(plan.contains("other"));
    for groups in [json!({"a":"*","b":"*"}),json!({"a":"1,2","b":"2,3"}),
        json!({"a":"1,,2"}),json!({"a":"256"}),json!({"a":"-1"}),json!({"a":"1.5"})] {
        assert!(Splits::compile(&config(groups),fields).is_err());
    }
    let t=Trunk::new("Example").unwrap();
    let (t,_)=t.registration_with_binding(Registration {cache:"demo",service:"service",resource:"events",resource_type:"http",level:"other",file:"http.rx"},&document,binding.clone()).unwrap();
    assert_eq!(Registry::read(&t).unwrap().resources()[0].binding,binding);
}

#[test]
fn summary_contexts_belong_to_the_branch_and_preserve_sealed_history() {
    use trie_trunk_taxonomy::registration::SummaryContext;
    let (tree, definition) = add(&Trunk::new("Test").unwrap(), &doc("http", vec![uint("duration", 4)]), "http");
    let branch = tree.definitions().unwrap().resources()[0].branch;
    let span = |first, end, sealed| SummaryContext { first, end, sources: vec![[definition, 0]], sealed };
    let tree = tree.register_summary_context(branch, span(0, 100, false)).unwrap();
    let same = tree.register_summary_context(branch, span(0, 100, false)).unwrap();
    assert_eq!(tree.bytes(), same.bytes());
    let tree = tree.register_summary_context(branch, span(0, 100, true)).unwrap()
        .register_summary_context(branch, span(100, 200, true)).unwrap();
    let tree = Trunk::open(tree.bytes()).unwrap();
    let context = tree.summary_context(branch, 150).unwrap();
    assert_eq!((context.first, context.end), (0, 200));
    assert!(tree.summary_context(branch, 200).is_err());
    assert!(tree.register_summary_context(branch, span(0, 100, false)).is_err());
    assert!(tree.register_summary_context(branch, SummaryContext { sources: vec![[definition, 1]], ..span(0, 100, true) }).is_err());
    let (tree, unrelated) = add(&tree, &doc("security", vec![uint("code", 2)]), "security");
    assert!(tree.register_summary_context(branch, SummaryContext { sources: vec![[unrelated, 0]], ..span(200, 300, false) }).is_err());
    assert!(tree.summary_context(tree.definitions().unwrap().resources()[1].branch, 0).is_err());
}

#[test]
fn major_generations_share_a_source_but_isolate_files_and_summary_contexts() {
    use trie_trunk_taxonomy::registration::SummaryContext;
    let first = doc::new("http").version(1,0).field(uint("duration",2)).sign().unwrap();
    let minor = doc::new("http").version(1,1).field(uint("duration",4))
        .compound(first.bytes().unwrap()).sign().unwrap();
    let major = doc::new("http").version(2,0).field(text("duration"))
        .compound(minor.bytes().unwrap()).sign().unwrap();
    let target = || Registration { cache:"demo", service:"web", resource:"requests",
        resource_type:"http", level:"", file:"http.rx" };
    let (tree, a) = Trunk::new("Test").unwrap().registration(target(), &first).unwrap();
    let (tree, b) = tree.registration(target(), &minor).unwrap();
    let (tree, c) = tree.registration(target(), &major).unwrap();
    assert_eq!(tree.branch_versions(a.branch).unwrap(), [a.branch,b.branch,c.branch]);
    assert_eq!(tree.storage_versions(a.branch).unwrap(), [a.branch,b.branch]);
    assert_eq!(tree.storage_versions(c.branch).unwrap(), [c.branch]);
    let catalog = tree.catalog("demo").unwrap();
    assert_eq!(catalog.sources, ["demo/web/requests/http"]);
    for (version, branch) in [(1,b.branch),(2,c.branch)] {
        for extension in ["packs","time","summ","indx","leaf"] {
            let file = catalog.files.iter().find(|f| f.path == format!("demo/web/requests/http.v{version}.{extension}")).unwrap();
            assert_eq!(file.branch_id, Some(branch));
        }
    }
    // Concurrent versions may publish the same day without replacing each
    // other's immutable interpretation or adding a user-facing resource.
    let context = |definition| SummaryContext {first:0,end:86_400_000,sources:vec![[definition,0]],sealed:true};
    let tree = tree.register_summary_context(a.branch, context(a.definition)).unwrap();
    let tree = tree.register_summary_context(c.branch, context(c.definition)).unwrap();
    assert_eq!(tree.summary_context(b.branch,0).unwrap().sources, [[a.definition,0]]);
    assert_eq!(tree.summary_context(c.branch,0).unwrap().sources, [[c.definition,0]]);
    assert!(tree.register_summary_context(a.branch, context(c.definition)).is_err());
    assert!(tree.state_path("demo/web/requests/http",None).is_err());
    for branch in [a.branch,c.branch] {
        let route = tree.route(branch).unwrap();
        assert_eq!(tree.state_path(&route.source().unwrap(),Some(&route.digest)).unwrap(),route.leaf_path().unwrap());
    }
}
