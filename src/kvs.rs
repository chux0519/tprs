use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use crate::{KvStoreError, KvsEngine, Result};

const COMPACTION_POINT: u64 = 1_000_000;

#[derive(Serialize, Deserialize)]
enum Commands {
    Set(SetCommand),
    Remove(RemoveCommand),
}

#[derive(Serialize, Deserialize)]
struct SetCommand {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize)]
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
    writer: Arc<Mutex<BufWriter<File>>>,
    meta_writer: Arc<Mutex<BufWriter<File>>>,
    reader: Arc<RwLock<BufReader<File>>>,
    entrypoints: Arc<RwLock<HashMap<String, (u64, u64)>>>,
    meta: Arc<RwLock<KvMeta>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct KvMeta {
    uncompact_size: u64,
    db_dir: String,
    version: u64,
}

fn build_entrypoints<P: AsRef<Path>>(path: P) -> Result<HashMap<String, (u64, u64)>> {
    let mut entrypoints: HashMap<String, (u64, u64)> = HashMap::new();
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
            // create here
            let db_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&db_path)?;
            let store = KvStore {
                writer: Arc::new(Mutex::new(BufWriter::new(db_file))),
                meta_writer: Arc::new(Mutex::new(BufWriter::new(
                    OpenOptions::new().write(true).open(&meta_path)?,
                ))),
                reader: Arc::new(RwLock::new(BufReader::new(File::open(&db_path)?))),
                entrypoints: Arc::new(RwLock::new(build_entrypoints(&db_path)?)),
                meta: Arc::new(RwLock::new(meta)),
            };
            return Ok(store);
        }
        Err(KvStoreError::PathInvalid)
    }

    fn check_compact(&self) -> Result<()> {
        let mut meta = self.meta.write().unwrap();

        if meta.uncompact_size < COMPACTION_POINT {
            return Ok(());
        }

        let mut vals = vec![];
        let mut entrypoints = self.entrypoints.write().unwrap();
        for pos_len_pair in entrypoints.values() {
            vals.push(pos_len_pair.clone());
        }

        let new_version = meta.version + 1;
        let old_log_path = get_db_path(&meta.db_dir, meta.version);
        let new_log_path = get_db_path(&meta.db_dir, new_version);
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_log_path)?;
        let mut new_writer = BufWriter::new(new_log);
        let mut new_entrypoints = HashMap::new();

        for pos_len_pair in vals {
            let (pos, len) = pos_len_pair;
            let cmd = self.read_cmd(pos, len)?;
            match cmd {
                Commands::Set(set_cmd) => {
                    let pos = new_writer.seek(SeekFrom::End(0))?;
                    let key = set_cmd.key.clone();
                    new_writer
                        .write_all(&serde_json::to_string(&Commands::Set(set_cmd))?.into_bytes())?;
                    new_writer.flush()?;
                    let next_pos = new_writer.stream_position()?;
                    let pos_len_pair = (pos, next_pos - pos);
                    new_entrypoints.insert(key, pos_len_pair);
                }
                _ => {}
            }
        }

        // update meta
        meta.version = new_version;
        meta.uncompact_size = 0;

        let mut meta_writer = self.meta_writer.lock().unwrap();
        meta_writer.seek(SeekFrom::Start(0))?;
        meta_writer.write_all(&serde_json::to_string(&*meta)?.into_bytes())?;
        meta_writer.flush()?;

        // update cur instance
        let mut writer = self.writer.lock().unwrap();
        *writer = new_writer;

        let mut reader = self.reader.write().unwrap();
        *reader = BufReader::new(File::open(&new_log_path)?);
        *entrypoints = new_entrypoints;

        // delete old log
        fs::remove_file(&old_log_path)?;

        Ok(())
    }

    fn read_cmd(&self, pos: u64, len: u64) -> Result<Commands> {
        let mut reader = self.reader.write().unwrap();
        reader.seek(SeekFrom::Start(pos))?;
        let mut buf = vec![0; len as usize];
        reader.read_exact(&mut buf)?;
        let cmd = serde_json::from_slice(&buf)?;
        Ok(cmd)
    }
}

impl KvsEngine for KvStore {
    /// Set a k-v pair
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Commands::Set(SetCommand {
            key: key.clone(),
            value,
        });

        {
            let mut writer = self.writer.lock().unwrap();
            let pos = writer.seek(SeekFrom::End(0))?;
            writer.write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
            writer.flush()?;
            let next_pos = writer.stream_position()?;

            let pos_len_pair = (pos, next_pos - pos);

            let mut entrypoints = self.entrypoints.write().unwrap();
            entrypoints.insert(key.clone(), pos_len_pair);

            let mut meta = self.meta.write().unwrap();
            meta.uncompact_size += next_pos - pos;
        }
        self.check_compact()?;
        Ok(())
    }

    /// Get value of key
    /// Returns None if key is not exists
    fn get(&self, key: String) -> Result<Option<String>> {
        let entrypoints = self.entrypoints.read().unwrap();
        let entry = entrypoints.get(&key);
        match entry {
            Some(pos_len_pair) => {
                let (pos, len) = *pos_len_pair;
                match self.read_cmd(pos, len)? {
                    Commands::Set(cmd) => Ok(Some(cmd.value)),
                    Commands::Remove(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key
    fn remove(&self, key: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let mut entrypoints = self.entrypoints.write().unwrap();
        match entrypoints.get(&key) {
            Some(_) => {
                let cmd = Commands::Remove(RemoveCommand { key: key.clone() });
                writer.write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
                writer.flush()?;
                entrypoints.remove(&key);
                Ok(())
            }
            None => Err(KvStoreError::KeyNotFound),
        }
    }
}
