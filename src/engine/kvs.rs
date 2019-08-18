use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{KvStoreError, KvsEngine, Result};

const COMPACTION_POINT: u64 = 1_000_000;

type KvStoreEntryPoints = SkipMap<String, (u64, u64)>;

#[derive(Serialize, Deserialize, Debug)]
enum Commands {
    Set(SetCommand),
    Remove(RemoveCommand),
}

#[derive(Serialize, Deserialize, Debug)]
struct SetCommand {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RemoveCommand {
    key: String,
}

/// The struct KvStore stores k-v string pairs
/// implemented with std::collections::HashMap, totally in memory
///
/// Example:
/// ```rust
/// # use kvs::KvStore;
/// # use kvs::KvsEngine;
/// let mut store = KvStore::open(std::path::Path::new(".")).unwrap();
/// store.set("k".to_owned(), "v".to_owned()).expect("set error");
/// let v = store.get("k".to_owned()).expect("get error");
/// assert_eq!(Some("v".to_owned()), v);
/// store.remove("k".to_owned()).expect("remove error");
/// assert_eq!(None, store.get("k".to_owned()).unwrap());
/// ```
#[derive(Clone)]
pub struct KvStore {
    writer: Arc<Mutex<KvStoreWriter>>,
    reader: KvStoreReader,
    compactor: Arc<Mutex<KvStoreCompactor>>,
    entrypoints: Arc<KvStoreEntryPoints>,
    meta: Arc<KvStoreMeta>,
}

struct KvStoreWriter {
    writer: BufWriter<File>,
    entrypoints: Arc<KvStoreEntryPoints>,
    meta: Arc<KvStoreMeta>,
    scoped_version: u64,
}

impl KvStoreWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        meta: Arc<KvStoreMeta>,
        entrypoints: Arc<KvStoreEntryPoints>,
    ) -> Result<Self> {
        let db_file = OpenOptions::new().create(true).append(true).open(&path)?;
        let writer = BufWriter::new(db_file);
        let scoped_version = meta.version.load(Ordering::Relaxed);
        Ok(KvStoreWriter {
            writer,
            entrypoints,
            meta,
            scoped_version,
        })
    }

    pub fn write_cmd(&mut self, cmd: &Commands) -> Result<()> {
        // always using the latest version db file
        self.check_writer_version()?;

        // write to tail
        let pos = self.writer.seek(SeekFrom::End(0))?;
        self.writer
            .write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
        self.writer.flush()?;

        // update index
        let next_pos = self.writer.stream_position()?;
        let pos_len_pair = (pos, next_pos - pos);

        match cmd {
            Commands::Set(s_cmd) => {
                self.entrypoints.insert(s_cmd.key.clone(), pos_len_pair);
            }
            Commands::Remove(r_cmd) => {
                self.entrypoints.remove(&r_cmd.key);
            }
        };

        // update uncompact_size
        self.meta
            .uncompact_size
            .fetch_add(next_pos - pos, Ordering::SeqCst);
        Ok(())
    }
    pub fn check_writer_version(&mut self) -> Result<()> {
        let version = self.meta.version.load(Ordering::Relaxed);
        if self.scoped_version < version {
            let db_path = get_db_path(&self.meta.db_dir, version);
            let db_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&db_path)?;
            self.writer = BufWriter::new(db_file);
            self.scoped_version = version;
        }
        Ok(())
    }
}

struct KvStoreReader {
    reader: RefCell<Option<BufReader<File>>>,
    meta: Arc<KvStoreMeta>,
    scoped_version: AtomicU64,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> KvStoreReader {
        let scoped_version = self.meta.version.load(Ordering::Relaxed);
        KvStoreReader {
            reader: RefCell::new(None),
            meta: self.meta.clone(),
            scoped_version: AtomicU64::new(scoped_version),
        }
    }
}

impl KvStoreReader {
    pub fn new<P: AsRef<Path>>(path: P, meta: Arc<KvStoreMeta>) -> Result<Self> {
        let reader = BufReader::new(File::open(path.as_ref())?);
        let scoped_version = meta.version.load(Ordering::Relaxed);
        Ok(KvStoreReader {
            reader: RefCell::new(Some(reader)),
            meta,
            scoped_version: AtomicU64::new(scoped_version),
        })
    }
    pub fn read_cmd(&self, pos: u64, len: u64) -> Result<Commands> {
        self.check_reader_versioin()?;

        if let Some(reader) = &mut *self.reader.borrow_mut() {
            reader.seek(SeekFrom::Start(pos))?;
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            let cmd = serde_json::from_slice(&buf)?;
            return Ok(cmd);
        }
        unreachable!()
    }
    pub fn check_reader_versioin(&self) -> Result<()> {
        let version = self.meta.version.load(Ordering::Relaxed);
        let scoped_version = self.scoped_version.load(Ordering::Relaxed);
        if scoped_version < version || self.reader.borrow().is_none() {
            let db_path = get_db_path(&self.meta.db_dir, version);
            *self.reader.borrow_mut() = Some(BufReader::new(File::open(&db_path)?));
            self.scoped_version.store(version, Ordering::Relaxed);
        }
        Ok(())
    }
}

struct KvStoreCompactor {
    entrypoints: Arc<KvStoreEntryPoints>,
    reader: KvStoreReader,
    meta_writer: BufWriter<File>,
    meta: Arc<KvStoreMeta>,
}

impl KvStoreCompactor {
    pub fn new<P: AsRef<Path>>(
        meta_file: P,
        entrypoints: Arc<KvStoreEntryPoints>,
        reader: KvStoreReader,
        meta: Arc<KvStoreMeta>,
    ) -> Result<Self> {
        let meta_writer = BufWriter::new(OpenOptions::new().write(true).open(&meta_file)?);
        Ok(KvStoreCompactor {
            entrypoints,
            reader,
            meta_writer,
            meta,
        })
    }
    pub fn compact(&mut self) -> Result<()> {
        if self.meta.uncompact_size.load(Ordering::Relaxed) < COMPACTION_POINT {
            return Ok(());
        }

        let cur_version = self.meta.version.load(Ordering::SeqCst);
        let new_version = cur_version + 1;
        let old_log_path = get_db_path(&self.meta.db_dir, cur_version);
        let new_log_path = get_db_path(&self.meta.db_dir, new_version);
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_log_path)?;
        let mut new_writer = BufWriter::new(new_log);

        for entry in self.entrypoints.iter() {
            let (pos, len) = *entry.value();
            let cmd = self.reader.read_cmd(pos, len)?;
            match cmd {
                Commands::Set(set_cmd) => {
                    let pos = new_writer.seek(SeekFrom::End(0))?;
                    let key = set_cmd.key.clone();
                    new_writer
                        .write_all(&serde_json::to_string(&Commands::Set(set_cmd))?.into_bytes())?;
                    new_writer.flush()?;
                    let next_pos = new_writer.stream_position()?;
                    let pos_len_pair = (pos, next_pos - pos);
                    self.entrypoints.insert(key, pos_len_pair);
                }
                _ => {}
            }
        }

        // update meta
        self.meta.version.fetch_add(1, Ordering::SeqCst);
        self.meta.uncompact_size.store(0, Ordering::SeqCst);

        self.meta_writer.seek(SeekFrom::Start(0))?;
        self.meta_writer
            .write_all(&serde_json::to_string(&self.meta.clone_to_plain_meta())?.into_bytes())?;
        self.meta_writer.flush()?;

        // delete old log
        fs::remove_file(&old_log_path)?;

        Ok(())
    }
}

#[derive(Debug)]
struct KvStoreMeta {
    uncompact_size: AtomicU64,
    db_dir: String,
    version: AtomicU64,
}

#[derive(Serialize, Deserialize, Debug)]
struct KvMeta {
    uncompact_size: u64,
    db_dir: String,
    version: u64,
}

impl From<KvMeta> for KvStoreMeta {
    fn from(meta: KvMeta) -> Self {
        KvStoreMeta {
            uncompact_size: AtomicU64::new(meta.uncompact_size),
            db_dir: meta.db_dir,
            version: AtomicU64::new(meta.version),
        }
    }
}

impl KvStoreMeta {
    pub fn clone_to_plain_meta(&self) -> KvMeta {
        KvMeta {
            uncompact_size: self.uncompact_size.load(Ordering::Relaxed),
            db_dir: self.db_dir.clone(),
            version: self.version.load(Ordering::Relaxed),
        }
    }
}

fn build_entrypoints<P: AsRef<Path>>(path: P) -> Result<KvStoreEntryPoints> {
    let entrypoints: KvStoreEntryPoints = SkipMap::new();
    let mut cur_pos = 0;
    let mut reader = BufReader::new(File::open(&path)?);
    reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(&mut reader).into_iter::<Commands>();
    while let Some(cmd) = stream.next() {
        let next_pos = stream.byte_offset() as u64;
        let len = next_pos - cur_pos;
        let k = match cmd? {
            Commands::Set(set_cmd) => set_cmd.key,
            Commands::Remove(rm_cmd) => rm_cmd.key,
        };
        entrypoints.insert(k, (cur_pos, len));
        cur_pos = next_pos;
    }
    Ok(entrypoints)
}

fn get_db_path<P: AsRef<Path>>(path: P, version: u64) -> String {
    let path = path.as_ref().clone();
    path.join(format!("kv.{}.log", version))
        .to_str()
        .expect("invalid db path")
        .to_owned()
}
fn get_meta_path<P: AsRef<Path>>(path: P) -> String {
    let path = path.as_ref().clone();
    path.join("kv.meta")
        .to_str()
        .expect("invalid db path")
        .to_owned()
}
fn read_meta(path: &Path) -> Result<KvMeta> {
    let dir = Path::new(path);
    let meta_path = get_meta_path(path);
    // check if file exists
    match fs::metadata(&meta_path) {
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => {
                let version = 0;
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&meta_path)?;
                let mut writer = BufWriter::new(file);
                let meta = KvMeta {
                    uncompact_size: 0,
                    db_dir: dir.to_str().expect("read meta error").to_owned(),
                    version,
                };
                writer.write_all(&serde_json::to_string(&meta)?.into_bytes())?;
                writer.flush()?;
                Ok(meta)
            }
            _ => Err(e.into()),
        },
        Ok(_) => {
            let mut reader = BufReader::new(File::open(&meta_path)?);
            let mut s = String::new();
            let len = reader.read_to_string(&mut s);
            let reader = BufReader::new(File::open(&meta_path)?);
            let meta: KvMeta = serde_json::from_reader(reader)?;
            Ok(meta)
        }
    }
}
impl KvStore {
    /// constructor
    pub fn open(path: &Path) -> Result<Self> {
        let dir = Path::new(path);
        if dir.is_dir() {
            let meta = read_meta(&dir)?;
            let meta_path = get_meta_path(path);
            let db_path = get_db_path(path, meta.version);
            let meta = KvStoreMeta::from(meta);

            // create here
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&db_path)?;
            let kv_store_meta = Arc::new(meta);
            let kv_store_reader = KvStoreReader::new(&db_path, kv_store_meta.clone())?;
            let kv_store_entrypoints = Arc::new(build_entrypoints(&db_path)?);
            let kv_store_writer = KvStoreWriter::new(
                &db_path,
                kv_store_meta.clone(),
                kv_store_entrypoints.clone(),
            )?;
            let kv_store_compactor = KvStoreCompactor::new(
                &meta_path,
                kv_store_entrypoints.clone(),
                kv_store_reader.clone(),
                kv_store_meta.clone(),
            )?;

            let store = KvStore {
                writer: Arc::new(Mutex::new(kv_store_writer)),
                reader: kv_store_reader,
                compactor: Arc::new(Mutex::new(kv_store_compactor)),
                entrypoints: kv_store_entrypoints,
                meta: kv_store_meta,
            };
            return Ok(store);
        }
        Err(KvStoreError::PathInvalid)
    }
}

impl KvsEngine for KvStore {
    /// Set a k-v pair
    fn set(&self, key: String, value: String) -> Result<()> {
        if self.meta.uncompact_size.load(Ordering::Relaxed) >= COMPACTION_POINT {
            self.compactor.lock().unwrap().compact()?;
        }

        let cmd = Commands::Set(SetCommand {
            key: key.clone(),
            value,
        });

        self.writer.lock().unwrap().write_cmd(&cmd)
    }

    /// Get value of key
    /// Returns None if key is not exists
    fn get(&self, key: String) -> Result<Option<String>> {
        let entry = self.entrypoints.get(&key);
        match entry {
            Some(pos_len_pair) => {
                let (pos, len) = *pos_len_pair.value();
                match self.reader.read_cmd(pos, len)? {
                    Commands::Set(cmd) => Ok(Some(cmd.value)),
                    Commands::Remove(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key
    fn remove(&self, key: String) -> Result<()> {
        if self.meta.uncompact_size.load(Ordering::Relaxed) >= COMPACTION_POINT {
            self.compactor.lock().unwrap().compact()?;
        }
        match self.entrypoints.get(&key) {
            Some(_) => {
                let cmd = Commands::Remove(RemoveCommand { key: key.clone() });
                self.writer.lock().unwrap().write_cmd(&cmd)
            }
            None => Err(KvStoreError::KeyNotFound),
        }
    }
}
