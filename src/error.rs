use sled;
use std::io;

#[derive(Fail, Debug)]
pub enum KvStoreError {
    #[fail(display = "Path is not a directory")]
    PathInvalid,
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "Engine not match")]
    EngineNotMatch,
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
    #[fail(display = "{}", _0)]
    Rpc(String)
}

impl From<io::Error> for KvStoreError {
    fn from(error: io::Error) -> Self {
        KvStoreError::Io(error)
    }
}

impl From<serde_json::Error> for KvStoreError {
    fn from(error: serde_json::Error) -> Self {
        KvStoreError::Serde(error)
    }
}

impl From<sled::Error> for KvStoreError {
    fn from(error: sled::Error) -> Self {
        KvStoreError::Sled(error)
    }
}

pub type Result<T> = std::result::Result<T, KvStoreError>;
