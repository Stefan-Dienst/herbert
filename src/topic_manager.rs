use anyhow::{bail, Result};
use arrow_schema::Schema;
use bytes::Bytes;
use log::info;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::storage::{in_memory_queue::InMemoryQueue, RecordStorage};

#[derive(Debug, PartialEq)]
pub struct TopicMetadata {
    name: String,
    schema: Option<Schema>,
}

impl TopicMetadata {
    fn new(name: &str, schema: Option<Schema>) -> Self {
        TopicMetadata {
            name: name.into(),
            schema: schema,
        }
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

    pub fn create(&self, topic: &str, schema: Option<Schema>) -> Result<()> {
        let mut write = self
            .topic_metadatas
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        if write.contains_key(topic) {
            bail!("Topic {} already exists", topic)
        } else {
            write.insert(topic.into(), TopicMetadata::new(topic, schema.clone()));
            info!(
                "Created metadata for topic {} with schema:\n{:#?}",
                topic, schema
            );
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
        topic_manager.create("foobar", None);
        assert!(topic_manager
            .topic_metadatas
            .read()
            .unwrap()
            .contains_key("foobar"));
        let result = topic_manager.create("foobar", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_topic_with_schema() {
        let topic_manager = TopicManager::default();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        topic_manager.create("foobar", Some(schema.clone()));
        assert!(topic_manager
            .topic_metadatas
            .read()
            .unwrap()
            .contains_key("foobar"));
        assert_eq!(
            topic_manager
                .topic_metadatas
                .read()
                .unwrap()
                .get("foobar")
                .unwrap(),
            &TopicMetadata::new("foobar", Some(schema.clone()))
        )
    }
}
