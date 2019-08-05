use crate::error::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpStream;

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
#[derive(Serialize, Deserialize)]
pub enum SessionClientCommand {
    Handshake,
    Quit,
    Get(String),
    Set(String, String),
    Remove(String),
}

// For server
#[derive(Serialize, Deserialize)]
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

    pub fn poll(&mut self) -> Result<SessionClientCommand> {
        // TODO: read from sock
        Ok(SessionClientCommand::Quit)
    }

    pub fn handle(&mut self, cmd: SessionClientCommand) -> Result<()> {
        match cmd {
            SessionClientCommand::Handshake => {
                if self.state == SessionState::Wait {
                    self.state = SessionState::Connect;
                }
            }
            SessionClientCommand::Quit => {
                self.state = SessionState::Done;
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
        };
        Ok(())
    }

    pub fn should_quit(&self) -> bool {
        self.state == SessionState::Done
    }
}
