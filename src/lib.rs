#![deny(missing_docs)]
//! kvs implement an in memory k-v store

use std::collections::HashMap;

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

impl KvStore {
    /// constructor
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Set a k-v pair
    /// Behavior is exactly the same as hashmap.insert
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Get value of key
    /// Returns None if key is not exists
    pub fn get(&mut self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Remove a key
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
