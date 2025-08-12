use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::storage::{in_memory_queue::InMemoryQueue, RecordStorage};

pub struct TopicMetadata {
    name: String,
}

impl TopicMetadata {
    fn new(name: &str) -> Self {
        TopicMetadata { name: name.into() }
    }
}

pub struct TopicManager {
    backend: Arc<dyn RecordStorage>,
    topic_metadatas: RwLock<HashMap<String, TopicMetadata>>,
}

impl TopicManager {
    pub fn new(
        backend: Arc<dyn RecordStorage>,
        topics: RwLock<HashMap<String, TopicMetadata>>,
    ) -> Self {
        TopicManager {
            backend,
            topic_metadatas: topics,
        }
    }

    pub fn add(&self, topic: &str, record: Bytes) -> Result<()> {
        self.backend.add(topic, record)
    }

    pub fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes> {
        self.backend.fetch(topic, fetch_offset)
    }

    pub fn create(&self, topic: &str) -> Result<()> {
        let mut write = self
            .topic_metadatas
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        if write.contains_key(topic) {
            bail!("Topic {} already exists", topic)
        } else {
            write.insert(topic.into(), TopicMetadata::new(topic));
            info!("Created metadata for topic {}", topic);
        }
        Ok(())
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        let backend = Arc::new(InMemoryQueue::new());
        let topics = RwLock::new(HashMap::new());
        TopicManager {
            backend,
            topic_metadatas: topics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let topic_manager = TopicManager::default();
        topic_manager.create("foobar");
        assert!(topic_manager
            .topic_metadatas
            .read()
            .unwrap()
            .contains_key("foobar"));
        let result = topic_manager.create("foobar");
        assert!(result.is_err());
    }
}
