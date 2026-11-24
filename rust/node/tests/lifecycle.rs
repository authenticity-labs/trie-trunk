use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};
use trunk_node::{Node, Storage};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "trunk-node-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn install_reopen_and_single_owner() {
    let tmp = Directory::new();
    let path = tmp.0.join("local");
    let node = Node::install(&path, "Personal").unwrap();
    assert_eq!(node.configuration().storage, Storage::Browse);
    let original = fs::read(path.join("trunk.trie")).unwrap();
    assert!(Node::open(&path).is_err());
    assert!(Node::install(&path, "Replacement").is_err());
    assert_eq!(fs::read(path.join("trunk.trie")).unwrap(), original);
    let c = node
        .ports()
        .inspect(b"{\"message\":\"test\"}".as_slice(), Default::default())
        .unwrap();
    assert_eq!(c.records, 1);
    drop(node);
    let reopened = Node::open(&path).unwrap();
    assert_eq!(reopened.configuration().name, "Personal");
    assert!(!path.join("local.packs").exists());
}

#[test]
fn failed_install_does_not_touch_existing_directory() {
    let tmp = Directory::new();
    fs::write(tmp.0.join("precious"), b"preserve").unwrap();
    assert!(Node::install(&tmp.0, "Local").is_err());
    assert_eq!(fs::read(tmp.0.join("precious")).unwrap(), b"preserve");
    let path = tmp.0.join("bad");
    assert!(Node::install(&path, "\n").is_err());
    assert!(!path.exists());
}

#[test]
fn process_exit_releases_ownership() {
    use std::{
        io::{BufRead, BufReader, Write},
        process::{Command, Stdio},
    };
    let tmp = Directory::new();
    let path = tmp.0.join("local");
    drop(Node::install(&path, "Personal").unwrap());
    let mut process = Command::new(env!("CARGO_BIN_EXE_trunk-node"))
        .arg("serve")
        .arg(&path)
        .arg(tmp.0.join("node.sock"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    process
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"status\n")
        .unwrap();
    let mut line = String::new();
    BufReader::new(process.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["ok"],
        true
    );
    assert!(Node::open(&path).is_err());
    process.kill().unwrap();
    process.wait().unwrap();
    assert!(Node::open(&path).is_ok());
}
