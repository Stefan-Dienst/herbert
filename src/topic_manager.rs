use anyhow::{bail, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::storage::{in_memory_queue::InMemoryQueue, RecordStorage};

use arrow_ipc::reader::StreamReader;
use std::io::Cursor;

fn parse_record_batch_from_bytes(record: &Bytes, expected_schema: &Schema) -> Result<RecordBatch> {
    if record.len() < 8 {
        bail!("Record too small to be valid Arrow IPC stream");
    }

    // Arrow IPC streams start with 0xFFFFFFFF (continuation marker) or the "ARROW1" magic
    let magic = &record[..6.min(record.len())];
    if magic != b"ARROW1" && &record[..4] != &[0xFF, 0xFF, 0xFF, 0xFF] {
        bail!("Invalid Arrow IPC format: missing magic bytes");
    }

    let cursor = Cursor::new(record);
    let mut reader = StreamReader::try_new(cursor, None)?;

    if let Some(batch) = reader.next() {
        let batch = batch?;
        if batch.schema().as_ref() != expected_schema {
            bail!("Schema mismatch");
        }
        Ok(batch)
    } else {
        bail!("No record batch found")
    }
}

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
        let read = self
            .topic_metadatas
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;

        let stored_record = if let Some(metadata) = read.get(topic) {
            if let Some(ref schema) = metadata.schema {
                // Topic has schema: parse and store as RecordBatch
                match parse_record_batch_from_bytes(&record, schema) {
                    Ok(batch) => crate::storage::StoredRecord::Batch(batch),
                    Err(e) => {
                        error!("{}", e);
                        error!("Record was not added to topic.");
                        bail!("{}", e)
                    }
                }
            } else {
                // Topic without schema: store raw bytes
                crate::storage::StoredRecord::Raw(record)
            }
        } else {
            // Topic doesn't exist yet: store raw bytes
            crate::storage::StoredRecord::Raw(record)
        };

        self.backend.add(topic, stored_record)
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
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_create_topic() {
        let topic_manager = TopicManager::default();
        let _ = topic_manager.create("foobar", None);
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

        let _ = topic_manager.create("foobar", Some(schema.clone()));
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
