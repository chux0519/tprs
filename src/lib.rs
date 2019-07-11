#![deny(missing_docs)]
//! kvs implement an in memory k-v store

use failure::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

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
/// let mut store = KvStore::new();
/// store.set("k".to_owned(), "v".to_owned());
/// let v = store.get("k".to_owned());
/// assert_eq!(Some("v".to_owned()), v);
/// store.remove("k".to_owned());
/// assert_eq!(None, store.get("k".to_owned()));
/// ```
pub struct KvStore {
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

/// Short for Result<T, Box<std::error::Error>>
pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    /// constructor
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let store = KvStore {
            writer: BufWriter::new(file),
            reader: BufReader::new(File::open(&path)?),
        };
        Ok(store)
    }

    /// Set a k-v pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Commands::Set(SetCommand { key, value });
        self.writer
            .write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    /// Get value of key
    /// Returns None if key is not exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let entrypoints = self.get_entrypoints()?;
        match entrypoints.get(&key) {
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
        let entrypoints = self.get_entrypoints()?;
        match entrypoints.get(&key) {
            Some(_) => {
                let cmd = Commands::Remove(RemoveCommand { key });
                self.writer
                    .write_all(&serde_json::to_string(&cmd)?.into_bytes())?;
                self.writer.flush()?;
                Ok(())
            }
            // TODO: throw key not found
            None => Ok(()),
        }
    }

    fn get_entrypoints(&mut self) -> Result<HashMap<String, Vec<(usize, usize)>>> {
        let mut entrypoints: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        let mut cur_pos = 0;
        let mut next_pos = 0;
        self.reader.seek(SeekFrom::Start(0))?;
        let mut stream =
            serde_json::Deserializer::from_reader(&mut self.reader).into_iter::<Commands>();
        while let Some(cmd) = stream.next() {
            next_pos = stream.byte_offset();
            let len = next_pos - cur_pos;
            let k = match cmd? {
                Commands::Set(set_cmd) => set_cmd.key,
                Commands::Remove(rm_cmd) => rm_cmd.key,
            };
            let entries = entrypoints.get_mut(&k);
            match entries {
                Some(entries_vec) => {
                    entries_vec.push((next_pos, len));
                }
                None => {
                    entrypoints.insert(k, vec![(next_pos, len)]);
                }
            }
            cur_pos = next_pos + 1;
        }
        Ok(entrypoints)
    }

    fn read_cmd(&mut self, pos: usize, len: usize) -> Result<Commands> {
        self.reader.seek(SeekFrom::Start(pos as u64))?;
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?;
        let cmd = serde_json::from_slice(&buf)?;
        Ok(cmd)
    }
}
