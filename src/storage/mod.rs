use anyhow::Result;
use arrow_array::RecordBatch;
use bytes::Bytes;

#[derive(Debug, PartialEq, Clone)]
pub enum StoredRecord {
    Raw(Bytes),
    Batch(RecordBatch),
}

pub trait RecordStorage: Send + Sync {
    fn add(&self, topic: &str, records: StoredRecord) -> Result<()>;
    fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes>;
}

pub mod in_memory_log;
pub mod in_memory_queue;
