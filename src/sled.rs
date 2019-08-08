use crate::error::Result;
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
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let res = self.tree.get(key)?;
        let v = match res {
            Some(iv) => String::from_utf8(Vec::from(iv.as_ref())).expect("utf8 error"),
            None => "".to_owned(),
        };
        Ok(Some(v))
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.tree.del(key)?;
        Ok(())
    }
}
