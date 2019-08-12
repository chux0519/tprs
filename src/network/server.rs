use crate::network::Session;
use crate::{KvStoreError, KvsEngine, Result};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

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
