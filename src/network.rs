use crate::error::{KvStoreError, Result};
use crate::KvsEngine;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};

pub struct Session<'a, E: KvsEngine> {
    store: &'a mut E,
    sock: TcpStream,
    state: SessionState,
}

#[derive(PartialEq)]
pub enum SessionState {
    // wait for handshake
    Wait,
    // ready to handle cmd
    Connect,
    // done, should quit
    Done,
}

// For client
#[derive(Serialize, Deserialize, Debug)]
pub enum SessionClientCommand {
    Handshake,
    Quit,
    Get(String),
    Set(String, String),
    Remove(String),
    Invalid,
}

// For server
#[derive(Serialize, Deserialize, Debug)]
pub enum SessionServerResp {
    OK,
    ERR(String),
    Value(String),
    NotFound,
    InvalidCmd,
}

impl<'a, E: KvsEngine> Session<'a, E> {
    pub fn new(stream: TcpStream, store: &'a mut E) -> Self {
        Session {
            store,
            sock: stream,
            state: SessionState::Wait,
        }
    }

    pub fn poll(&mut self) -> Result<()> {
        let mut buf = vec![0u8; 1024];
        let cmd = match self.sock.read(&mut buf) {
            Ok(len) => {
                if len == 0 {
                    self.state = SessionState::Done;
                    SessionClientCommand::Invalid
                } else {
                    let msg = &buf[..len];
                    let cmd: SessionClientCommand = serde_json::from_slice(&msg)?;
                    cmd
                }
            }
            Err(_e) => SessionClientCommand::Invalid,
        };
        self.handle(cmd)
    }

    pub fn close(&mut self) -> Result<()> {
        self.sock.shutdown(Shutdown::Both)?;
        Ok(())
    }

    pub fn handle(&mut self, cmd: SessionClientCommand) -> Result<()> {
        match cmd {
            SessionClientCommand::Handshake => {
                self.state = SessionState::Connect;
                let resp = SessionServerResp::OK;
                self.sock
                    .write_all(serde_json::to_string(&resp)?.as_bytes())?
            }
            SessionClientCommand::Quit => {
                self.state = SessionState::Done;
                let resp = SessionServerResp::OK;
                self.sock
                    .write_all(serde_json::to_string(&resp)?.as_bytes())?
            }
            SessionClientCommand::Get(k) => {
                let resp = match self.store.get(k) {
                    Ok(some_v) => match some_v {
                        Some(v) => SessionServerResp::Value(v),
                        None => SessionServerResp::NotFound,
                    },
                    Err(e) => SessionServerResp::ERR(format!("{}", e)),
                };
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Set(k, v) => {
                let resp = match self.store.set(k, v) {
                    Ok(_) => SessionServerResp::OK,
                    Err(e) => SessionServerResp::ERR(format!("{}", e)),
                };
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Remove(k) => {
                let resp = match self.store.remove(k) {
                    Ok(_) => SessionServerResp::OK,
                    Err(e) => SessionServerResp::ERR(format!("{}", e)),
                };
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Invalid => {
                let resp = SessionServerResp::InvalidCmd;
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
        };
        Ok(())
    }

    pub fn should_quit(&self) -> bool {
        self.state == SessionState::Done
    }
}

pub struct KvsServer<E: KvsEngine> {
    store: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(store: E) -> Self {
        KvsServer { store }
    }
    pub fn listen(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    handle(stream, &mut self.store)?;
                }
                Err(e) => {
                    return Err(KvStoreError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("{}", e),
                    )));
                }
            }
        }
        Ok(())
    }
}

pub fn handle<E: KvsEngine>(stream: TcpStream, store: &mut E) -> Result<()> {
    let mut session = Session::new(stream, store);
    while !session.should_quit() {
        session.poll()?;
    }
    Ok(())
}

// Client

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
