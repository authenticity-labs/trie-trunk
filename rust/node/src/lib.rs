//! Trunk node lifecycle over the shared Seed libraries. Platform transports
//! invoke this owner; UI state and alternate storage formats do not belong here.
pub mod cli;
pub mod command;
mod ingress;
mod collections;
mod drafts;
pub mod logs;
pub mod roots;
mod ports;
mod presentation;
pub mod publication;
mod read;
mod sql;
pub mod postgres;
#[cfg(all(test, feature = "planner-lab"))]
#[path = "../benchmarks/public_events.rs"]
mod public_events;
mod registration;
pub mod service;
mod summaries;
pub use data_seeds::registration::Storage;
use data_seeds::registration::Trunk;
pub use registration::Destination;
pub use registration::{Target as RegistrationTarget, Registered};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Invalid producer input is distinct from unavailable storage or capacity.
#[derive(Debug)]
pub struct InputError(pub data_seeds::Error);
impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for InputError {}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub name: String,
    pub storage: Storage,
}

/// One writable owner per native directory. Queries share its registered plans;
/// the lock is held until all of this owner's work has been joined and dropped.
pub struct Node {
    root: PathBuf,
    configuration: Configuration,
    trunk: std::sync::RwLock<std::sync::Arc<Trunk>>,
    roots: roots::Roots,
    runtime: std::sync::Mutex<ingress::Runtime>,
    uploads: std::sync::Mutex<ports::Uploads>,
    jobs: std::sync::Mutex<ports::Jobs>,
    memory: data_seeds::MemoryBudget,
    object_publication: std::sync::RwLock<()>,
    publication: std::sync::atomic::AtomicU64,
    registry_generation: std::sync::atomic::AtomicU64,
    _lock: Ownership,
}
impl Node {
    pub fn install(root: &Path, name: &str) -> Result<Self> {
        Self::install_with(root, name, Storage::Browse)
    }

    pub fn install_with(root: &Path, name: &str, storage: Storage) -> Result<Self> {
        if name.trim().is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
            return Err("Trunk name must contain 1–128 bytes without control characters".into());
        }
        // Exclusive directory creation never replaces an existing installation.
        // The parent is chosen/provisioned by the environment adapter.
        fs::create_dir(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        let mut lease = None;
        let result = (|| -> Result<Self> {
            metadata_directory(root)?;
            lease = Some(ownership(root)?);
            fs::create_dir(root.join("definitions"))?;
            let configuration = Configuration {
                name: name.trim().into(),
                storage,
            };
            let taxonomy = trie_trunk_taxonomy::Trunk::new(&configuration.name)?;
            write_new(
                &root.join(".trunk/node.json"),
                &serde_json::to_vec(&configuration)?,
            )?;
            write_new(&root.join("trunk.trie"), taxonomy.bytes())?;
            sync_directory(&root.join(".trunk"))?;
            sync_directory(root)?;
            if let Some(parent) = root.parent() {
                sync_directory(parent)?;
            }
            let trunk = Trunk::open(taxonomy.bytes())?;
            let memory = data_seeds::MemoryBudget::new(64 << 20);
            Ok(Self {
                root: root.canonicalize()?,
                configuration,
                trunk: std::sync::RwLock::new(std::sync::Arc::new(trunk)),
                roots: roots::Roots::new(memory.clone()),
                runtime: std::sync::Mutex::new(ingress::Runtime::new(memory.clone())?),
                memory,
                object_publication: Default::default(),
                publication: Default::default(),
                registry_generation: Default::default(),
                uploads: Default::default(),
                jobs: Default::default(),
                _lock: lease.take().expect("installation owns the directory"),
            })
        })();
        if result.is_err() {
            // Only known files in the directory created by this call; never a
            // recursive delete and never another installation's data.
            for name in ["trunk.trie", ".trunk/node.json", ".trunk/owner.lock"] {
                let _ = fs::remove_file(root.join(name));
            }
            for name in ["definitions", ".trunk"] {
                let _ = fs::remove_dir(root.join(name));
            }
            let _ = fs::remove_dir(root);
        }
        result
    }
    pub fn open(root: &Path) -> Result<Self> {
        let configuration: Configuration =
            serde_json::from_reader(File::open(root.join(".trunk/node.json"))?)?;
        Self::open_with(root, configuration, 64 << 20, ingress::now())
    }
    /// Explicit configuration for embedders and command transports. This is the
    /// same owner and recovery path used by persisted CLI configuration.
    pub fn open_with(
        root: &Path,
        configuration: Configuration,
        bytes: usize,
        clock: u64,
    ) -> Result<Self> {
        Self::open_scoped(root,configuration,data_seeds::MemoryBudget::new(bytes),clock)
    }
    fn open_scoped(root:&Path,configuration:Configuration,memory:data_seeds::MemoryBudget,clock:u64)->Result<Self>{
        let root = root.canonicalize()?;
        metadata_directory(&root)?;
        let lock = ownership(&root)?;
        fs::create_dir_all(root.join("definitions"))?;
        let path = root.join("trunk.trie");
        if !path.try_exists()? {
            let trie = trie_trunk_taxonomy::Trunk::new(&configuration.name)?;
            ingress::atomic(&path, trie.bytes())?;
        }
        let mut trunk = Trunk::open(fs::read(path)?)?;
        let files: Vec<_> = trunk
            .trie()
            .definitions()?
            .artifacts()
            .keys()
            .cloned()
            .collect();
        for file in files {
            let path = read::within(&root.join("definitions"), &file)?;
            trunk.prescribe_file(&file, &read::bounded(&path, 16 << 20)?)?;
        }
        let node = Self {
            root,
            configuration,
            trunk: std::sync::RwLock::new(std::sync::Arc::new(trunk)),
            roots: roots::Roots::new(memory.clone()),
            runtime: std::sync::Mutex::new(ingress::Runtime::new(memory.clone())?),
            memory,
            object_publication: Default::default(),
            publication: Default::default(),
            registry_generation: Default::default(),
            uploads: Default::default(),
            jobs: Default::default(),
            _lock: lock,
        };
        node.cleanup_drafts()?;
        node.recover(clock)?;
        Ok(node)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }
    pub fn memory(&self) -> data_seeds::MemoryBudget {
        self.memory.clone()
    }
    pub fn trie(&self) -> data_seeds::registration::Snapshot {
        self.trunk
            .read()
            .expect("registry publication poisoned")
            .snapshot()
    }
    pub fn ports(&self) -> data_seeds::ports::Ports {
        data_seeds::ports::Ports
    }
    pub(crate) fn changed(&self) -> u64 {
        self.publication
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1
    }
}

struct Ownership(File);
impl Drop for Ownership {
    fn drop(&mut self) {
        // Explicitly release before close: a concurrent fork/exec can briefly
        // inherit the descriptor even though it is close-on-exec.
        let _ = self.0.unlock();
    }
}
fn metadata_directory(root: &Path) -> io::Result<()> {
    let mut dir = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        dir.mode(0o700);
    }
    dir.recursive(true).create(root.join(".trunk"))
}
fn ownership(root: &Path) -> io::Result<Ownership> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options.open(root.join(".trunk/owner.lock"))?;
    lock.try_lock().map_err(|e| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("Could not acquire Trunk ownership: {e}"),
        )
    })?;
    Ok(Ownership(lock))
}

fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}
