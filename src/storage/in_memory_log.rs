use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use bytes::{BufMut, Bytes, BytesMut};
use log::{info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use crate::error::HerbertError;

use super::{RecordStorage, StoredRecord};

#[derive(Debug, Clone, Copy)]
struct RecordOffset {
    batch_index: usize,
    row_offset: usize,
}

pub struct InMemoryLog {
    topics: RwLock<HashMap<String, VecDeque<StoredRecord>>>,
    offsets: RwLock<HashMap<String, VecDeque<RecordOffset>>>,
}

impl InMemoryLog {
    pub fn new() -> Self {
        Self {
            // FIXME: don't use a RW lock on all topics. Maybe look into DashMap.
            // Later maybe also look into cross beam.
            topics: RwLock::new(HashMap::new()),
            offsets: RwLock::new(HashMap::new()),
        }
    }
}

impl RecordStorage for InMemoryLog {
    fn add(&self, topic: &str, record: StoredRecord) -> Result<(), HerbertError> {
        let mut write = self
            .topics
            .write()
            .map_err(|_e| HerbertError::PoisonError)?;

        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());

        let mut offset_write = self
            .offsets
            .write()
            .map_err(|_e| HerbertError::PoisonError)?;
        let offset_queue = offset_write
            .entry(topic.to_string())
            .or_insert(VecDeque::new());
        let batch_index = queue.len();

        match record {
            StoredRecord::Raw(_) => {
                offset_queue.push_back(RecordOffset {
                    batch_index: batch_index,
                    row_offset: 0,
                });
            }
            StoredRecord::Batch(ref record_batch) => {
                let num_rows = record_batch.num_rows();

                for row_offset in 0..num_rows {
                    offset_queue.push_back(RecordOffset {
                        batch_index: batch_index,
                        row_offset: row_offset,
                    });
                }
            }
        }

        // Only push after offsets have been set
        // FIXME: How do we handle if system crashes after pushing offsets, but not the record?
        queue.push_back(record);
        info!("Currently topics have {:?}", write);

        Ok(())
    }

    fn fetch(&self, topic: &str, fetch_offset: i64) -> Result<Bytes, HerbertError> {
        let mut offset_write = self
            .offsets
            // FIXME don't hold a write lock on a fetch.
            .write()
            .map_err(|_e| HerbertError::PoisonError)?;

        let offset_queue = offset_write
            .entry(topic.to_string())
            .or_insert(VecDeque::new());
        let record_offset = match offset_queue.get(fetch_offset as usize) {
            Some(record_offset) => record_offset,
            None => {
                warn!(
                    "Could not find the RecordOffset for offset: {:?}",
                    fetch_offset
                );
                return Ok(Bytes::new());
            }
        };

        let mut write = self
            .topics
            .write()
            .map_err(|_e| HerbertError::PoisonError)?;

        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();
        let mut batches: Vec<RecordBatch> = Vec::new();

        let mut iter = queue.iter().skip(record_offset.batch_index).peekable();
        while let Some(stored_record) = iter.next() {
            match stored_record {
                StoredRecord::Raw(bytes) => {
                    records.put(bytes.clone());
                    if iter.peek().is_some() {
                        records.put_u8(0);
                    }
                }
                StoredRecord::Batch(batch) => {
                    // If this is the first batch we also have to consider the offset inside the
                    // RecordBatch
                    if batches.is_empty() {
                        let batch_slice = batch.slice(
                            record_offset.row_offset,
                            batch.num_rows() - record_offset.row_offset,
                        );
                        batches.push(batch_slice)
                    } else {
                        batches.push(batch.clone());
                    }
                }
            };
        }

        if batches.len() > 0 {
            let mut buffer = Vec::new();
            let mut writer = StreamWriter::try_new(&mut buffer, &batches[0].schema())?;
            for batch in batches {
                writer.write(&batch)?;
            }
            writer.finish()?;
            records.put(Bytes::from(buffer));
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
        let record = StoredRecord::Raw(Bytes::from("test"));
        let _ = in_memory_log.add("foobar", record.clone());
        assert!(
            !in_memory_log
                .topics
                .read()
                .unwrap()
                .get("foobar")
                .unwrap()
                .is_empty()
        );
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
        let record = StoredRecord::Raw(Bytes::from("test"));
        let _ = in_memory_log.add(&topic_name, record.clone());
        let fetched_record = in_memory_log.fetch(&topic_name, 0);
        if let StoredRecord::Raw(bytes) = record {
            assert_eq!(fetched_record.unwrap(), bytes)
        }
    }

    #[test]
    fn test_fetch_multiple() {
        let in_memory_log = InMemoryLog::new();
        let topic_name = "foobar";
        let record = StoredRecord::Raw(Bytes::from("test"));
        let _ = in_memory_log.add(&topic_name, record.clone());
        let _ = in_memory_log.add(&topic_name, record.clone());
        let fetched_records = in_memory_log.fetch(&topic_name, 0).unwrap();
        let parts: Vec<Bytes> = fetched_records
            .split(|b| *b == 0)
            .map(Bytes::copy_from_slice)
            .collect();
        assert_eq!(parts.len(), 2);
        if let StoredRecord::Raw(bytes) = record {
            assert_eq!(parts.get(0).unwrap(), &bytes);
            assert_eq!(parts.get(1).unwrap(), &bytes);
        }
    }

    #[test]
    fn test_fetch_with_offset() {
        let in_memory_log = InMemoryLog::new();
        let topic_name = "foobar";
        for idx in 0..5 {
            let _ = in_memory_log.add(&topic_name, StoredRecord::Raw(Bytes::from(idx.to_string())));
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
