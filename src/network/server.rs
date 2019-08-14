use crate::network::Session;
use crate::thread_pool::ThreadPool;
use crate::{KvStoreError, KvsEngine, Result};
use crossbeam::channel::{Receiver, Sender};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    store: E,
    pool: T,
    rx: Option<Receiver<()>>,
    tx: Option<Sender<()>>,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    pub fn new(store: E, pool: T) -> Self {
        KvsServer {
            store,
            pool,
            rx: None,
            tx: None,
        }
    }
    pub fn rx(mut self, rx: Receiver<()>) -> Self {
        self.rx = Some(rx);
        self
    }
    pub fn tx(mut self, tx: Sender<()>) -> Self {
        self.tx = Some(tx);
        self
    }
    pub fn listen(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let store = self.store.clone();
                    s.set_nonblocking(false)?;
                    self.pool.spawn(|| {
                        handle(s, store).expect("error session");
                    })
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // wait until network socket is ready, typically implemented
                    // via platform-specific APIs such as epoll or IOCP
                    if let Some(r) = &self.rx {
                        if let Ok(_) = r.try_recv() {
                            dbg!("now quit");
                            break;
                        }
                    }
                    continue;
                }
                Err(e) => {
                    return Err(KvStoreError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("{}", e),
                    )));
                }
            }
        }

        if let Some(t) = &self.tx {
            dbg!("now send shutdown back");
            t.send(()).expect("failed to send shutdown back");
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
