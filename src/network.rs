use crate::error::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

pub struct Session {
    // TODO: add store ref
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
}

impl Session {
    pub fn new(stream: TcpStream) -> Self {
        Session {
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
        dbg!(&cmd);
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
                let resp = SessionServerResp::Value(k);
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Set(k, v) => {
                let resp = SessionServerResp::OK;
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Remove(k) => {
                let resp = SessionServerResp::OK;
                self.sock
                    .write_all(&serde_json::to_string(&resp)?.into_bytes())?
            }
            SessionClientCommand::Invalid => {}
        };
        Ok(())
    }

    pub fn should_quit(&self) -> bool {
        self.state == SessionState::Done
    }
}
