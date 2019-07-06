#![deny(missing_docs)]
//! kvs implement an in memory k-v store

use std::collections::HashMap;
use std::path::Path;
use failure::Error;

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
    store: HashMap<String, String>,
}

/// Short for Result<T, Box<std::error::Error>>
pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    /// constructor
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore {
            store: HashMap::new(),
        };
        Ok(store)
    }

    /// Set a k-v pair
    /// Behavior is exactly the same as hashmap.insert
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// Get value of key
    /// Returns None if key is not exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let res = self.store.get(&key).cloned();
        Ok(res)
    }

    /// Remove a key
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
