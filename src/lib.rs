// #![deny(missing_docs)]
//! kvs implement an in memory k-v store
#![feature(seek_convenience)]

#[macro_use]
extern crate failure;

mod error;
mod kvs;
pub mod network;
mod sled;
mod threadpool;

pub use crate::error::{KvStoreError, Result};
pub use crate::kvs::KvStore;
pub use crate::sled::SledKvsEngine;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}
