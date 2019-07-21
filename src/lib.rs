#![deny(missing_docs)]
#![feature(seek_convenience)]
//! kvs implement an in memory k-v store

#[macro_use]
extern crate failure;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

mod error;
use error::KvStoreError;

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
/// let mut store = KvStore::open(std::path::Path::new("."));
/// store.set("k".to_owned(), "v".to_owned());
/// let v = store.get("k".to_owned());
/// assert_eq!(Some("v".to_owned()), v);
/// store.remove("k".to_owned());
/// assert_eq!(None, store.get("k".to_owned()));
/// ```
pub struct KvStore {
    writer: BufWriter<File>,
    reader: BufReader<File>,
    entrypoints: HashMap<String, Vec<(u64, u64)>>,
}

/// Short for Result<T, Box<std::error::Error>>
pub type Result<T> = std::result::Result<T, KvStoreError>;

fn build_entrypoints(path: &Path) -> Result<HashMap<String, Vec<(u64, u64)>>> {
    let mut entrypoints: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
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
        let entries = entrypoints.get_mut(&k);
        match entries {
            Some(entries_vec) => {
                entries_vec.push((cur_pos, len));
            }
            None => {
                entrypoints.insert(k, vec![(cur_pos, len)]);
            }
        }
        cur_pos = next_pos;
    }
    Ok(entrypoints)
}

impl KvStore {
    /// constructor
    pub fn open(path: &Path) -> Result<Self> {
        let dir = Path::new(path);
        if dir.is_dir() {
            let db_path = dir.join("db.kv");
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&db_path)?;
            let store = KvStore {
                writer: BufWriter::new(file),
                reader: BufReader::new(File::open(&db_path)?),
                entrypoints: build_entrypoints(&db_path)?,
            };
            return Ok(store);
        }
        Err(KvStoreError::PathInvalid)
    }

    /// Set a k-v pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Commands::Set(SetCommand {
            key: key.clone(),
            value,
        });
        let pos = self.writer.seek(SeekFrom::End(0))?;
        self.writer
            .write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
        self.writer.flush()?;
        let next_pos = self.writer.stream_position()?;
        let pos_len_pair = (pos, next_pos - pos);
        match self.entrypoints.get_mut(&key) {
            Some(entries_vec) => {
                entries_vec.push(pos_len_pair);
            }
            None => {
                self.entrypoints.insert(key.clone(), vec![pos_len_pair]);
            }
        }
        Ok(())
    }

    /// Get value of key
    /// Returns None if key is not exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.entrypoints.get(&key) {
            Some(v) => {
                let (pos, len) = v[v.len() - 1];
                let cmd = self.read_cmd(pos, len)?;
                match cmd {
                    Commands::Remove(_) => Ok(None),
                    Commands::Set(set_cmd) => Ok(Some(set_cmd.value)),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.entrypoints.get(&key) {
            Some(_) => {
                let cmd = Commands::Remove(RemoveCommand { key: key.clone() });
                self.writer
                    .write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
                self.writer.flush()?;
                self.entrypoints.remove(&key).expect("KeyNotFound");
                Ok(())
            }
            None => Err(KvStoreError::KeyNotFound),
        }
    }

    fn read_cmd(&mut self, pos: u64, len: u64) -> Result<Commands> {
        self.reader.seek(SeekFrom::Start(pos))?;
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;
        let cmd = serde_json::from_slice(&buf)?;
        Ok(cmd)
    }
}
