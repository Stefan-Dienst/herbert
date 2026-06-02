use arrow_array::RecordBatch;
use arrow_schema::Schema;
use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use bytes::Bytes;
use log::{error, info};
use std::fs::File;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Cursor, Read},
    sync::{Arc, RwLock},
};

use crate::config::Config;
use crate::error::HerbertError;
use crate::storage::{
    RecordStorage, StoredRecord, in_memory_log::InMemoryLog, in_memory_queue::InMemoryQueue,
};
use crate::wal::WriteAheadLog;

use arrow_ipc::reader::StreamReader;

fn parse_record_batch_from_bytes(
    record: &Bytes,
    expected_schema: &Schema,
) -> Result<RecordBatch, HerbertError> {
    if record.len() < 8 {
        return Err(HerbertError::InvalidArrowIpc(format!(
            "record len {} smaller than 8",
            record.len()
        )));
    }

    // Arrow IPC streams start with 0xFFFFFFFF (continuation marker) or the "ARROW1" magic
    let magic = &record[..6.min(record.len())];
    if magic != b"ARROW1" && &record[..4] != &[0xFF, 0xFF, 0xFF, 0xFF] {
        return Err(HerbertError::InvalidArrowIpc(
            "missing magic bytes".to_string(),
        ));
    }

    let cursor = Cursor::new(record);
    let mut reader = StreamReader::try_new(cursor, None)?;

    if let Some(batch) = reader.next() {
        let batch = batch?;
        if batch.schema().as_ref() != expected_schema {
            return Err(HerbertError::SchemaError {
                expected: format!("{:?}", batch.schema().as_ref()),
                found: format!("{:?}", expected_schema),
            });
        }
        Ok(batch)
    } else {
        return Err(HerbertError::InvalidArrowIpc(
            "no record batch found".to_string(),
        ));
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
    wal: WriteAheadLog,
    config: Arc<Config>,
}

impl TopicManager {
    // FIXME: In tests this auto creates an empty herbert.wal.
    pub fn new(
        backend: Arc<dyn RecordStorage>,
        topics: RwLock<HashMap<String, TopicMetadata>>,
        config: Arc<Config>,
    ) -> Self {
        let wal = WriteAheadLog::new(config.clone())
            .expect("It should be possible to build a WAL from the config.");
        TopicManager {
            backend,
            topic_metadatas: topics,
            wal: wal,
            config: config,
        }
    }

    pub fn default_log() -> Self {
        let backend = Arc::new(InMemoryLog::new());
        let topics = RwLock::new(HashMap::new());
        let config = Arc::new(Config::default());
        let wal = WriteAheadLog::new(config.clone())
            .expect("It should be possible to build a WAL from the config.");
        TopicManager {
            backend,
            topic_metadatas: topics,
            wal: wal,
            config: config,
        }
    }

    pub fn with_config(self, config: Arc<Config>) -> Self {
        let wal = WriteAheadLog::new(config.clone())
            .expect("It should be possible to build a WAL from the config.");
        Self {
            config: config,
            wal: wal,
            ..self
        }
    }

    pub fn exists(&self, topic: &str) -> Result<bool, HerbertError> {
        let read = self
            .topic_metadatas
            .read()
            .map_err(|_e| HerbertError::PoisonError)?;
        Ok(read.contains_key(topic))
    }

    pub fn add(&self, topic: &str, record: Bytes) -> Result<(), HerbertError> {
        let read = self
            .topic_metadatas
            .read()
            .map_err(|_e| HerbertError::PoisonError)?;

        let stored_record = if let Some(metadata) = read.get(topic) {
            if let Some(ref schema) = metadata.schema {
                // Topic has schema: parse and store as RecordBatch
                match parse_record_batch_from_bytes(&record, schema) {
                    Ok(batch) => StoredRecord::Batch(batch),
                    Err(e) => {
                        error!("{}", e);
                        error!("Record was not added to topic.");
                        return Err(e);
                    }
                }
            } else {
                // Topic without schema: store raw bytes
                // NOTE: I could here also automatically create topic with schema for record batch.
                StoredRecord::Raw(record)
            }
        } else {
            // Topic doesn't exist yet: store raw bytes
            StoredRecord::Raw(record)
        };

        // WAL commit happens here
        self.wal.add(topic, stored_record)?;
        if let Ok(true) = self.wal.need_to_flush() {
            self.wal.flush(&self.backend)?;
        };
        Ok(())
    }

    pub fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes, HerbertError> {
        self.backend.fetch(topic, fetch_offset)
    }

    pub fn create(&self, topic: &str, schema: Option<Schema>) -> Result<(), HerbertError> {
        let mut write = self
            .topic_metadatas
            .write()
            .map_err(|_e| HerbertError::PoisonError)?;
        if write.contains_key(topic) {
            return Err(HerbertError::TopicAlreadyExists(topic.to_string()));
        } else {
            write.insert(topic.into(), TopicMetadata::new(topic, schema.clone()));
            info!(
                "Created metadata for topic {} with schema:\n{:#?}",
                topic, schema
            );
        }
        Ok(())
    }

    pub fn recover(&self) -> Result<(), HerbertError> {
        let _read = self
            .wal
            .file
            .lock()
            .map_err(|_e| HerbertError::PoisonError)?;

        let file = File::open(&self.config.wal_path).map_err(|_e| HerbertError::NoWalFileFound)?;
        let mut buf_reader = BufReader::new(file);

        loop {
            let mut contents = Vec::new();
            buf_reader.read_until(b'\n', &mut contents)?;

            // Read everything
            if contents.len() == 0 {
                break;
            }

            contents.pop();
            let topic = String::from_utf8(contents)?;

            let indicator = buf_reader.read_u8()?;
            let size = buf_reader.read_u32::<LittleEndian>()?;

            let mut buf = vec![0u8; size as usize];

            match indicator {
                // Raw bytes
                0 => {
                    buf_reader.read_exact(&mut buf)?;
                    let record = StoredRecord::Raw(Bytes::from(buf));
                    self.backend.add(&topic, record)?;
                }
                // RecordBatch
                1 => {
                    buf_reader.read_exact(&mut buf)?;
                    let mut reader = StreamReader::try_new(Cursor::new(&buf), None)?;
                    if let Some(record_batch) = reader.next() {
                        let record_batch = record_batch?;

                        // If topic does not yet exist create it with schema.
                        if !self.exists(&topic)? {
                            self.create(&topic, Some(record_batch.schema().as_ref().to_owned()))?;
                        };
                        let record = StoredRecord::Batch(record_batch);
                        self.backend.add(&topic, record)?;
                    };
                }
                _ => {
                    return Err(HerbertError::IndicatorError(indicator));
                }
            };
        }

        Ok(())
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        let backend = Arc::new(InMemoryQueue::new());
        let topics = RwLock::new(HashMap::new());
        let config = Arc::new(Config::default());
        let wal = WriteAheadLog::new(config.clone())
            .expect("It should be possible to build a WAL from the config.");

        TopicManager {
            backend,
            topic_metadatas: topics,
            wal: wal,
            config: config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::record_batch;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_create_topic() {
        let temp_dir = TempDir::new().expect("Should be able to create temp dir for testing.");
        let wal_path = temp_dir.path().join("test.wal");
        let topic_manager = Arc::new(
            TopicManager::default()
                .with_config(Arc::new(Config::default().with_wal_path(&wal_path))),
        );

        let _ = topic_manager.create("foobar", None);
        assert!(
            topic_manager
                .topic_metadatas
                .read()
                .unwrap()
                .contains_key("foobar")
        );
        let result = topic_manager.create("foobar", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_exists() {
        let temp_dir = TempDir::new().expect("Should be able to create temp dir for testing.");
        let wal_path = temp_dir.path().join("test.wal");
        let topic_manager = Arc::new(
            TopicManager::default()
                .with_config(Arc::new(Config::default().with_wal_path(&wal_path))),
        );

        let _ = topic_manager.create("foobar", None);
        assert!(topic_manager.exists("foobar").unwrap());

        assert!(!topic_manager.exists("does_not_exist").unwrap());
    }

    #[test]
    fn test_create_topic_with_schema() {
        let temp_dir = TempDir::new().expect("Should be able to create temp dir for testing.");
        let wal_path = temp_dir.path().join("test.wal");
        let topic_manager = Arc::new(
            TopicManager::default()
                .with_config(Arc::new(Config::default().with_wal_path(&wal_path))),
        );

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        let _ = topic_manager.create("foobar", Some(schema.clone()));
        assert!(
            topic_manager
                .topic_metadatas
                .read()
                .unwrap()
                .contains_key("foobar")
        );
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

    #[test]
    fn test_recover() -> Result<(), HerbertError> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let config = Arc::new(Config::test_default().with_wal_path(&wal_path));

        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )?;

        let wal = WriteAheadLog::new(config.clone())?;
        wal.add("foobar", StoredRecord::Batch(batch.clone()))?;
        wal.add("foobar", StoredRecord::Batch(batch.clone()))?;

        wal.add("test", StoredRecord::Batch(batch.clone()))?;

        wal.file.lock().unwrap().flush()?;

        let topic_manager = TopicManager::default_log().with_config(config.clone());

        let _result = topic_manager.recover()?;

        dbg!(&topic_manager.exists("foobar")?);
        assert!(&topic_manager.exists("foobar")?);
        assert!(&topic_manager.exists("test")?);

        // Check record batches on foobar
        let record_bytes = topic_manager.fetch("foobar", 0).unwrap();
        dbg!(&record_bytes);
        let cursor = Cursor::new(record_bytes);
        let mut reader = StreamReader::try_new(cursor, None)?;
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());

        // Check record batches on test
        let record_bytes = topic_manager.fetch("test", 0).unwrap();
        let cursor = Cursor::new(record_bytes);
        let mut reader = StreamReader::try_new(cursor, None)?;
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());

        Ok(())
    }
}
