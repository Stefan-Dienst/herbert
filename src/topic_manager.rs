use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use anyhow::Result;

pub struct TopicManager {
    pub topics: RwLock<HashMap<String,VecDeque<Bytes>>>,
}

impl TopicManager {
    pub fn new() -> Self {
        TopicManager {
            topics: RwLock::new(HashMap::new()),
        }
    }

    pub fn add(&self, topic: &str, record: Bytes) -> Result<()> {
        let mut write = self.topics.write().map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        queue.push_front(record);
        info!("Currently topics have {:?}", write);
        Ok(())
    }

    pub fn remove(&self, topic: &str) -> Result<Bytes> {
        let mut write = self.topics.write().map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();
        while !queue.is_empty() {
            let message = queue.pop_back().ok_or_else(|| anyhow::anyhow!("Queue is empyt? Why?"))?;
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
        let topic_manager = TopicManager::new();
        assert!(topic_manager.topics.read().unwrap().is_empty())
    }

    #[test]
    fn test_add() {
        let topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());
        assert!(!topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
        assert_eq!(*topic_manager.topics.read().unwrap().get("foobar").unwrap().get(0).unwrap(), record);
    }

    #[test]
    fn test_remove() {
        let topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());
        assert!(!topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
        assert_eq!(*topic_manager.topics.read().unwrap().get("foobar").unwrap().get(0).unwrap(), record);

        topic_manager.remove("foobar");
        assert!(topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
    }
}
