use anyhow::Result;
use bytes::Bytes;

pub trait RecordStorage: Send + Sync {
    fn add(&self, topic: &str, records: Bytes) -> Result<()>;
    fn fetch(&self, topic: &str) -> Result<Bytes>;
}

pub mod in_memory_log;
pub mod in_memory_queue;
