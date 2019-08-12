// #![deny(missing_docs)]
//! kvs implement an in memory k-v store
#![feature(seek_convenience)]

#[macro_use]
extern crate failure;

mod error;
pub mod network;
pub mod engine;


pub use crate::error::{KvStoreError, Result};
pub use crate::engine::{KvStore, SledKvsEngine};

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
