use anyhow::{bail, Result};
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

fn validate_record_batch_against_schema(record: &Bytes, expected_schema: &Schema) -> Result<()> {
    if record.len() < 8 {
        bail!("Record too small to be valid Arrow IPC stream");
    }

    // Arrow IPC streams start with 0xFFFFFFFF (continuation marker) or the "ARROW1" magic
    let magic = &record[..6.min(record.len())];
    if magic != b"ARROW1" && &record[..4] != &[0xFF, 0xFF, 0xFF, 0xFF] {
        bail!("Invalid Arrow IPC format: missing magic bytes");
    }

    let cursor = Cursor::new(record);

    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| anyhow::anyhow!("Failed to parse Arrow IPC stream: {}", e))?;

    // TODO: Handle if multiple record batches are found
    if let Some(Ok(batch)) = reader.next() {
        let actual_schema = batch.schema();

        if actual_schema.as_ref() != expected_schema {
            bail!(
                "Record does not match expected schema.\nExpected: {:?}\nActual: {:?}",
                expected_schema,
                actual_schema
            );
        }

        Ok(())
    } else {
        bail!("No record batch found in the record")
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

        // TODO: key idea is that record is just bytest that could hold multiple records, e.g.
        // arrow recordBatch. I should implement that if there is a schema we are expecting
        // sa recordBtach and handle it this way.
        if let Some(metadata) = read.get(topic) {
            if let Some(ref schema) = metadata.schema {
                let validation = validate_record_batch_against_schema(&record, schema);
                match validation {
                    Err(e) => {
                        error!("{}", e);
                        error!("Record was not added to topic.");
                        bail!("{}", e)
                    }
                    _ => {}
                }
            };
        }
        self.backend
            .add(topic, crate::storage::StoredRecord::Raw(record))
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
