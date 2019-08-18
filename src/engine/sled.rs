use crate::error::{KvStoreError, Result};
use crate::KvsEngine;
use sled::Db;
use std::path::Path;

pub struct SledKvsEngine {
    tree: Db,
}

impl SledKvsEngine {
    pub fn open(p: &Path) -> Result<Self> {
        let tree = Db::start_default(p)?;
        let sledkv = SledKvsEngine { tree };
        Ok(sledkv)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.tree.set(key, value.as_bytes())?;
        self.tree.flush()?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let res = self.tree.get(key)?;
        match res {
            Some(iv) => {
                let res = String::from_utf8(Vec::from(iv.as_ref())).expect("utf8 error");
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }
    fn remove(&mut self, key: String) -> Result<()> {
        let res = self.tree.del(key)?;
        self.tree.flush()?;
        match res {
            Some(_) => Ok(()),
            None => Err(KvStoreError::KeyNotFound),
        }
    }
}
