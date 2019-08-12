use crate::network::{SessionClientCommand, SessionServerResp};
use crate::{KvStoreError, Result};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

pub struct KvsClient {
    buf: Vec<u8>,
    stream: TcpStream,
    ready: bool,
}

impl KvsClient {
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvsClient {
            buf: vec![0u8; 1024],
            stream,
            ready: false,
        })
    }

    pub fn handshake(&mut self) -> Result<()> {
        if self.ready {
            return Ok(());
        }

        let handshake = SessionClientCommand::Handshake;
        self.cmd(&handshake)?;
        self.ready = true;

        Ok(())
    }

    pub fn cmd(&mut self, cmd: &SessionClientCommand) -> Result<SessionServerResp> {
        self.stream
            .write_all(&serde_json::to_string(cmd)?.as_bytes())?;
        let len = self.stream.read(&mut self.buf)?;
        let resp: SessionServerResp = serde_json::from_slice(&self.buf[..len])?;
        Ok(resp)
    }

    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        let cmd = SessionClientCommand::Set(k, v);
        match self.cmd(&cmd)? {
            SessionServerResp::OK => Ok(()),
            SessionServerResp::ERR(e) => Err(KvStoreError::Rpc(e)),
            _ => Err(KvStoreError::Rpc("unnkown error".to_owned())),
        }
    }

    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        let cmd = SessionClientCommand::Get(k);
        let resp = self.cmd(&cmd)?;
        match resp {
            SessionServerResp::Value(v) => Ok(Some(v)),
            SessionServerResp::NotFound => Ok(None),
            SessionServerResp::ERR(e) => Err(KvStoreError::Rpc(e)),
            _ => Err(KvStoreError::Rpc("unnkown error".to_owned())),
        }
    }
    pub fn remove(&mut self, k: String) -> Result<()> {
        let cmd = SessionClientCommand::Remove(k);
        let resp = self.cmd(&cmd)?;
        match resp {
            SessionServerResp::OK => Ok(()),
            SessionServerResp::ERR(e) => Err(KvStoreError::Rpc(e)),
            _ => Err(KvStoreError::Rpc("unnkown error".to_owned())),
        }
    }
    pub fn quit(&mut self) -> Result<()> {
        let cmd = SessionClientCommand::Quit;
        self.cmd(&cmd)?;
        Ok(())
    }
}
