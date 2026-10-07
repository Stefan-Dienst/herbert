use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::error::HerbertError;

#[derive(Debug, PartialEq, Clone)]
pub enum StoredRecord {
    Raw(Bytes),
    Batch(RecordBatch),
}

pub trait RecordStorage: Send + Sync {
    fn add(&self, topic: &str, records: StoredRecord) -> Result<(), HerbertError>;
    fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes, HerbertError>;
}

pub mod in_memory_log;
pub mod in_memory_queue;
pub mod offsets;
pub mod partition;
pub mod segment;
pub mod stream;
pub mod stream_config;
