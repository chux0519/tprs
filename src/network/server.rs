use crate::network::Session;
use crate::thread_pool::ThreadPool;
use crate::{KvStoreError, KvsEngine, Result};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    store: E,
    pool: T,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    pub fn new(store: E, pool: T) -> Self {
        KvsServer { store, pool }
    }
    pub fn listen(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let store = self.store.clone();
            match stream {
                Ok(stream) => self.pool.spawn(|| {
                    handle(stream, store).expect("error session");
                }),
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

pub fn handle<E: KvsEngine>(stream: TcpStream, store: E) -> Result<()> {
    let mut store = store;
    let mut session = Session::new(stream, &mut store);
    while !session.should_quit() {
        session.poll()?;
    }
    Ok(())
}
