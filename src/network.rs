use crate::error::{KvStoreError, Result};
use crate::KvsEngine;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

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
                // TODO: corner case: 0
                let msg = &buf[..len];
                let cmd: SessionClientCommand = serde_json::from_slice(&msg)?;
                cmd
            }
            Err(e) => SessionClientCommand::Invalid,
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
