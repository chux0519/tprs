use std::io;

#[derive(Fail, Debug)]
pub enum KvStoreError {
    #[fail(display = "path is not a directory")]
    PathInvalid,
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
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

pub type Result<T> = std::result::Result<T, KvStoreError>;
