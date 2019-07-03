use std::collections::HashMap;

pub struct KvStore {
    store: HashMap<String, String>
}


impl KvStore {
    pub fn new() -> Self{
        KvStore {
            store: HashMap::new()
        }
    }

    pub fn set(&mut self, key: String, value: String) {}

    pub fn get(&mut self, key: String) -> Option<String> {
        None
    }

    pub fn remove(&mut self, key: String) {}
}
