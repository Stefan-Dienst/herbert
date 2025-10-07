use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use super::RecordStorage;

pub struct InMemoryQueue {
    topics: RwLock<HashMap<String, VecDeque<Bytes>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }
}

impl RecordStorage for InMemoryQueue {
    fn add(&self, topic: &str, record: Bytes) -> Result<()> {
        let mut write = self
            .topics
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        queue.push_front(record);
        info!("Currently topics have {:?}", write);
        Ok(())
    }

    fn fetch(&self, topic: &str, _fetch_offset: i64) -> Result<Bytes> {
        let mut write = self
            .topics
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();
        while !queue.is_empty() {
            let message = queue
                .pop_back()
                .ok_or_else(|| anyhow::anyhow!("Queue is empyt? Why?"))?;
            records.put(message.clone());
            if !queue.is_empty() {
                records.put_u8(0);
            }
            info!("Found message {:?}", message);
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
        let record = Bytes::from("test");
        in_memory_queue.add("foobar", record.clone());
        assert!(!in_memory_queue
            .topics
            .read()
            .unwrap()
            .get("foobar")
            .unwrap()
            .is_empty());
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
        let record = Bytes::from("test");
        in_memory_queue.add("foobar", record.clone());
        assert!(!in_memory_queue
            .topics
            .read()
            .unwrap()
            .get("foobar")
            .unwrap()
            .is_empty());
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

        in_memory_queue.fetch("foobar", 0);
        assert!(in_memory_queue
            .topics
            .read()
            .unwrap()
            .get("foobar")
            .unwrap()
            .is_empty());
    }
}
