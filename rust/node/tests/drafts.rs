use data_seeds::{Document, Field, Query, SeedDefinition, TIME_HOUR_MS as HOUR, doc};
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use trunk_node::{Node, RegistrationTarget, Result, Storage};

struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn target(kind: &str, action: &str, generation: Option<&str>) -> RegistrationTarget {
    serde_json::from_value(json!({"cache":"prod","service":"customer","resource":"accounts","resource_type":kind,"action":action,"generation":generation})).unwrap()
}
fn event(name: &str) -> Document {
    doc::new(name)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("id")
                .r#type("bytes")
                .size(10)
                .annotation("@event"),
        )
        .field(
            Field::new("value")
                .r#type("uint")
                .size(4)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap()
}
#[test]
fn promotion_reclaims_only_the_selected_generation_after_publication() -> Result<()> {
    for storage in [Storage::Browse, Storage::Pack] {
        let root = std::env::temp_dir().join(format!(
            "draft-promotion-{storage:?}-{}",
            std::process::id()
        ));
        let _cleanup = Cleanup(root.clone());
        let node = Node::install_with(&root, "Test", storage)?;
        let model = event("measurement");
        let working = node
            .registration(&target("measurement", "save", None), model.bytes()?)?
            .generation
            .unwrap();
        let other = node
            .registration(&target("measurement.extra", "save", None), model.bytes()?)?
            .generation
            .unwrap();
        let workspace = root.join(".trunk/drafts").join(&working);
        let other_root = root.join(".trunk/drafts").join(&other);
        assert!(node.trie().definitions()?.resources().is_empty());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let hour = now / HOUR * HOUR;
        let definition = SeedDefinition::prescribe(&model)?;
        let seed = definition.ingest((), |_, seed| {
            seed.set_uint("time", now)?
                .set_byte_string("id", &[1; 10])?
                .set_uint("value", 42)?;
            Ok(())
        })?;
        seed.cap()?;
        node.registration(
            &target("measurement", "ingest", Some(&working)),
            &seed.bytes()?,
        )?;
        drop(node);
        let args = [
            "beds",
            "query",
            "--dir",
            root.to_str().unwrap(),
            "--scope",
            "prod/customer/accounts/measurement",
            "--draft",
            &working,
            "--from",
            &hour.to_string(),
            "--to",
            &(hour + HOUR).to_string(),
            "--select",
            "value",
            "--json",
        ]
        .map(str::to_owned);
        let mut output = Vec::new();
        trunk_node::cli::run(&args, &[][..], &mut output, &mut Vec::new())?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output)?,
            json!({"value":42})
        );
        let mut join_args = args[..args.len() - 3].to_vec();
        join_args.extend(["--sql", "SELECT a.value AS reading FROM events AS a JOIN events AS b ON a.id=b.id", "--json"].map(str::to_owned));
        let check_join = || -> Result<()> {
            let mut output = Vec::new();
            trunk_node::cli::run(&join_args, &[][..], &mut output, &mut Vec::new())?;
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&output)?, json!({"reading":42}));
            Ok(())
        };
        check_join()?;
        let node = Node::open(&root)?;
        let preview = Node::open_with(&workspace, node.configuration().clone(), 8 << 20, now)?;
        let branch = preview.trie().definitions()?.resources()[0].branch;
        assert_eq!(
            preview
                .query(branch, hour, hour + HOUR, Query::default())?
                .fetch(2)?
                .len(),
            1
        );
        preview.commit(hour + 2 * HOUR)?;
        let packed = workspace.join(format!(
            "prod/customer/accounts/measurement.0{}",
            storage.suffix()
        ));
        assert!(packed.is_file(), "{packed:?}");
        drop(preview);
        drop(node);
        check_join()?;
        let node = Node::open(&root)?;
        assert_eq!(
            node.registration(
                &target("measurement", "save", Some(&working)),
                model.bytes()?
            )?
            .generation
            .as_ref(),
            Some(&working)
        );
        let changed = model
            .clone()
            .field(Field::new("location").r#type("tstr"))
            .sign()?;
        assert!(
            node.registration(
                &target("measurement", "save", Some(&working)),
                changed.bytes()?
            )
            .is_err()
        );
        let promote = target("measurement", "promote", Some(&working));
        let pending = root.join("trunk.trie.pending");
        fs::create_dir(&pending)?;
        assert!(node.registration(&promote, &[]).is_err());
        assert!(packed.is_file());
        assert!(node.trie().definitions()?.resources().is_empty());
        fs::remove_dir(pending)?;
        drop(node);
        let node = Node::open(&root)?;
        let registered = node.registration(&promote, &[])?;
        assert!(!workspace.exists());
        assert!(
            other_root
                .join("definitions/measurement.extra.0.rx")
                .is_file()
        );
        assert_eq!(node.trie().route(registered.branch)?.major, 1);
        assert_eq!(
            node.query(registered.branch, hour, hour + HOUR, Query::default())?
                .fetch(2)?
                .len(),
            0
        );
        let next = node
            .registration(&target("measurement", "save", None), changed.bytes()?)?
            .generation
            .unwrap();
        assert!(node.registration(&promote, &[]).is_err());
        let second = node.registration(&target("measurement", "promote", Some(&next)), &[])?;
        let route = node.trie().route(second.branch)?;
        assert_eq!((route.major, route.minor), (1, 1));
        let orphan = root.join(".trunk/drafts").join(&working);
        fs::create_dir(&orphan)?;
        fs::write(orphan.join("measurement.0.time"), b"retired")?;
        drop(node);
        let node = Node::open(&root)?;
        assert!(!orphan.exists());
        assert!(other_root.exists());
        assert_eq!(node.trie().route(registered.branch)?.minor, 0);
    }
    Ok(())
}

#[test]
fn object_draft_reuses_zero_and_names_its_disposable_pods() -> Result<()> {
    let root = std::env::temp_dir().join(format!("draft-object-{}", std::process::id()));
    let _cleanup = Cleanup(root.clone());
    let node = Node::install(&root, "Test")?;
    let model = doc::new("account")
        .field(
            Field::new("id")
                .r#type("bytes")
                .size(16)
                .annotation("@object"),
        )
        .sign()?;
    let first = node
        .registration(&target("account", "save", None), model.bytes()?)?
        .generation
        .unwrap();
    let revised = model.field(Field::new("name").r#type("tstr")).sign()?;
    let next = node
        .registration(&target("account", "save", Some(&first)), revised.bytes()?)?
        .generation
        .unwrap();
    let workspace = root.join(".trunk/drafts").join(&next);
    let working = Node::open_with(&workspace, node.configuration().clone(), 8 << 20, 0)?;
    let route = working
        .trie()
        .route(working.trie().definitions()?.resources()[0].branch)?;
    assert_eq!((route.major, route.minor), (0, 0));
    let pods = route.pods()?.unwrap();
    assert_eq!(pods, "prod/customer/accounts/account.0.pods");
    // The object writer is independent of promotion. Use its resolved filename.
    let file = workspace.join(pods);
    fs::create_dir_all(file.parent().unwrap())?;
    fs::write(&file, b"disposable")?;
    drop(working);
    let id = node
        .registration(&target("account", "promote", Some(&next)), &[])?
        .branch;
    assert!(!file.exists());
    assert_eq!(
        node.trie().route(id)?.pods()?.as_deref(),
        Some("prod/customer/accounts/account.pods")
    );
    Ok(())
}

#[test]
fn reopen_edit_and_promote_preserves_nested_members_and_rejects_stale_editors() -> Result<()> {
    let root = std::env::temp_dir().join(format!("draft-edit-{}", std::process::id()));
    let _cleanup = Cleanup(root.clone());
    let node = Node::install(&root, "Test")?;
    let model = event("measurement")
        .field(Field::new("probe").object(1))
        .definition("probe")
        .namespace("instruments")
        .field(Field::new("reading").r#type("int").size(2).annotation("@unit(mm)"))
        .sign()?;
    let save = node.registration(&target("measurement", "save", None), model.bytes()?)?;
    let token = save.generation.unwrap();
    let read = target("measurement", "read", Some(&token));
    let bytes = node.registration(&read, &[])?.bytes.unwrap();
    let revise: data_seeds::ports::design::Design = serde_json::from_value(json!({
        "action":"revise","bytes":bytes,"changes":[{"member":1,"field":0,"name":"distance","size":3}]
    }))?;
    let revised: Vec<u8> = serde_json::from_value(revise.run()?)?;
    let saved = node.registration(&target("measurement", "save", Some(&token)), &revised)?;
    assert!(node.registration(&read, &[]).is_err());
    assert!(node.registration(&target("measurement", "save", Some(&token)), &bytes).is_err());
    let next = saved.generation.unwrap();
    drop(node);
    let node = Node::open(&root)?;
    let saved = node.registration(&target("measurement", "read", Some(&next)), &[])?.bytes.unwrap();
    let document = doc::read(saved.as_slice())?;
    assert_eq!(document.definitions()[0].fields, model.definitions()[0].fields);
    let member = &document.definitions()[1];
    assert_eq!(member.header.namespace, "instruments");
    let mut expected = model.definitions()[1].fields[0].clone();
    expected.name = "distance".into();
    expected.size = 3;
    assert_eq!(member.fields, [expected]);
    let release = node.registration(&target("measurement", "promote", Some(&next)), &[])?;
    assert_eq!(node.trie().route(release.branch)?.major, 1);
    assert!(node.registration(&target("measurement", "read", Some(&next)), &[]).is_err());
    Ok(())
}
