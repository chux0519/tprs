// #![deny(missing_docs)]
//! kvs implement an in memory k-v store
#![feature(seek_convenience)]

#[macro_use]
extern crate failure;

mod error;
mod kvs;
pub mod network;
mod sled;

pub use crate::sled::SledKvsEngine;
pub use error::{KvStoreError, Result};
pub use kvs::KvStore;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
