use anyhow::Result;
use arrow_ipc::writer::StreamWriter;
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use crate::error::HerbertError;

use super::{RecordStorage, StoredRecord};

pub struct InMemoryQueue {
    topics: RwLock<HashMap<String, VecDeque<StoredRecord>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }
}

impl RecordStorage for InMemoryQueue {
    fn add(&self, topic: &str, record: StoredRecord) -> Result<(), HerbertError> {
        let mut write = self.topics.write().map_err(|e| HerbertError::PoisonError)?;

        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        queue.push_front(record);
        info!("Currently topics have {:?}", write);
        Ok(())
    }

    fn fetch(&self, topic: &str, _fetch_offset: i64) -> Result<Bytes, HerbertError> {
        let mut write = self.topics.write().map_err(|e| HerbertError::PoisonError)?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();
        while !queue.is_empty() {
            let stored_record = queue.pop_back().ok_or_else(|| HerbertError::EmptyQueue)?;

            // TODO: adjust for sending arrow batches as a single batch.
            let record_bytes = match stored_record {
                StoredRecord::Raw(bytes) => bytes.clone(),
                StoredRecord::Batch(batch) => {
                    let mut buffer = Vec::new();
                    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
                    writer.write(&batch)?;
                    writer.finish()?;
                    Bytes::from(buffer)
                }
            };
            records.put(record_bytes);
            if !queue.is_empty() {
                records.put_u8(0);
            }
        }
        Ok(records.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let in_memory_queue = InMemoryQueue::new();
        assert!(in_memory_queue.topics.read().unwrap().is_empty())
    }

    #[test]
    fn test_add() {
        let in_memory_queue = InMemoryQueue::new();
        let record = StoredRecord::Raw(Bytes::from("test"));
        let _ = in_memory_queue.add("foobar", record.clone());
        assert!(
            !in_memory_queue
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            *in_memory_queue
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .get(0)
                .unwrap(),
            record
        );
    }

    #[test]
    fn test_remove() {
        let in_memory_queue = InMemoryQueue::new();
        let record = StoredRecord::Raw(Bytes::from("test"));
        let _ = in_memory_queue.add("foobar", record.clone());
        assert!(
            !in_memory_queue
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            *in_memory_queue
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .get(0)
                .unwrap(),
            record
        );

        let _ = in_memory_queue.fetch("foobar", 0);
        assert!(
            in_memory_queue
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .is_empty()
        );
    }
}
