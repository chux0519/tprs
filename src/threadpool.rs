use crate::error::Result;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::spawn(job);
    }
}
