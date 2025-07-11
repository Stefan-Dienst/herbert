use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;

use crate::storage::{in_memory_queue::InMemoryQueue, RecordStorage};

pub struct TopicManager {
    backend: Arc<dyn RecordStorage>,
}

impl TopicManager {
    pub fn new(backend: Arc<dyn RecordStorage>) -> Self {
        TopicManager { backend }
    }

    pub fn add(&self, topic: &str, record: Bytes) -> Result<()> {
        self.backend.add(topic, record)
    }

    pub fn fetch(&self, topic: &str) -> Result<Bytes> {
        self.backend.fetch(topic)
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        let backend = Arc::new(InMemoryQueue::new());
        TopicManager { backend }
    }
}
