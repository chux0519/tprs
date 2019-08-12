// #![deny(missing_docs)]
//! kvs implement an in memory k-v store
#![feature(seek_convenience)]

#[macro_use]
extern crate failure;

pub mod engine;
pub mod error;
pub mod network;
pub mod thread_pool;

pub use crate::error::{KvStoreError, Result};
pub use engine::{KvStore, SledKvsEngine};

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}
