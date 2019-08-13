use crate::thread_pool::{ThreadPool, ThreadPoolMessage};
use crate::Result;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::thread;

pub struct SharedQueueThreadPool {
    threads: u32,
    sender: Sender<ThreadPoolMessage>,
}

struct ThreadedReceiver {
    receiver: Receiver<ThreadPoolMessage>,
}

impl Drop for ThreadedReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            // recreate a new thread here
            spawn_thread(self.receiver.clone());
        }
    }
}

fn spawn_thread(r: Receiver<ThreadPoolMessage>) {
    thread::spawn(move || {
        let thread_rx = ThreadedReceiver { receiver: r };
        loop {
            // block here
            match thread_rx.receiver.recv() {
                Ok(msg) => match msg {
                    ThreadPoolMessage::RunJob(job) => {
                        job();
                    }
                    ThreadPoolMessage::Shutdown => {
                        break;
                    }
                },
                _ => {}
            }
        }
        // thread receiver will drop here
    });
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (s, r) = unbounded();
        for _ in 0..threads as usize {
            let recv: Receiver<ThreadPoolMessage> = r.clone();
            spawn_thread(recv);
        }
        Ok(SharedQueueThreadPool { threads, sender: s })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .expect("failed to spawn job");
    }
}

impl SharedQueueThreadPool {
    fn quit(&self) {
        for _ in 0..self.threads as usize {
            self.sender
                .send(ThreadPoolMessage::Shutdown)
                .expect("failed to send shutdown message");
        }
    }
}
