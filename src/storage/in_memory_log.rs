use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use super::RecordStorage;

pub struct InMemoryLog {
    topics: RwLock<HashMap<String, VecDeque<Bytes>>>,
}

impl InMemoryLog {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }
}

impl RecordStorage for InMemoryLog {
    fn add(&self, topic: &str, record: Bytes) -> Result<()> {
        let mut write = self
            .topics
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        queue.push_back(record);
        info!("Currently topics have {:?}", write);
        Ok(())
    }

    fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes> {
        let mut write = self
            .topics
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();

        let mut iter = queue.iter().skip(fetch_offset as usize).peekable();

        while let Some(record) = iter.next() {
            records.put(record.clone());
            if iter.peek().is_some() {
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
        let in_memory_log = InMemoryLog::new();
        assert!(in_memory_log.topics.read().unwrap().is_empty())
    }

    #[test]
    fn test_add() {
        let in_memory_log = InMemoryLog::new();
        let record = Bytes::from("test");
        let _ = in_memory_log.add("foobar", record.clone());
        assert!(!in_memory_log
            .topics
            .read()
            .unwrap()
            .get("foobar")
            .unwrap()
            .is_empty());
        assert_eq!(
            *in_memory_log
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
    fn test_fetch() {
        let in_memory_log = InMemoryLog::new();
        let topic_name = "foobar";
        let record = Bytes::from("test");
        let _ = in_memory_log.add(&topic_name, record.clone());
        let fetched_record = in_memory_log.fetch(&topic_name, 0);
        assert_eq!(fetched_record.unwrap(), record)
    }

    #[test]
    fn test_fetch_multiple() {
        let in_memory_log = InMemoryLog::new();
        let topic_name = "foobar";
        let record = Bytes::from("test");
        let _ = in_memory_log.add(&topic_name, record.clone());
        let _ = in_memory_log.add(&topic_name, record.clone());
        let fetched_records = in_memory_log.fetch(&topic_name, 0).unwrap();
        let parts: Vec<Bytes> = fetched_records
            .split(|b| *b == 0)
            .map(Bytes::copy_from_slice)
            .collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.get(0).unwrap(), &record);
        assert_eq!(parts.get(1).unwrap(), &record);
    }

    #[test]
    fn test_fetch_with_offset() {
        let in_memory_log = InMemoryLog::new();
        let topic_name = "foobar";
        for idx in 0..5 {
            let _ = in_memory_log.add(&topic_name, Bytes::from(idx.to_string()));
        }
        let fetched_records = in_memory_log.fetch(&topic_name, 2).unwrap();
        let parts: Vec<Bytes> = fetched_records
            .split(|b| *b == 0)
            .map(Bytes::copy_from_slice)
            .collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.get(0).unwrap(), &Bytes::from("2"));
        assert_eq!(parts.get(1).unwrap(), &Bytes::from("3"));
        assert_eq!(parts.get(2).unwrap(), &Bytes::from("4"));
    }
}
