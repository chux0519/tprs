use crate::network::Session;
use crate::thread_pool::ThreadPool;
use crate::{KvStoreError, KvsEngine, Result};
use crossbeam::channel::{Receiver, Sender};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

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
        if let Some(t) = &self.tx {
            // start ack, avoid sock not connected
            t.send(()).expect("failed to send start ack");
        }

        for stream in listener.incoming() {
            // no receiver costs nothing
            if let Some(r) = &self.rx {
                // non block recv to reduce overhead
                if let Ok(_) = r.try_recv() {
                    break;
                }
            }
            match stream {
                Ok(s) => {
                    let store = self.store.clone();
                    self.pool.spawn(|| {
                        handle(s, store).expect("error session");
                    })
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
            // shutdown ack, avoid address in use
            t.send(()).expect("failed to send shutdown ack");
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
