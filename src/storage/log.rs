use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use super::RecordStorage;

pub struct Log {
    topics: RwLock<HashMap<String, VecDeque<Bytes>>>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }
}

impl RecordStorage for Log {
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

    fn fetch(&self, topic: &str) -> Result<Bytes> {
        let mut write = self
            .topics
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        let queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        let mut records = BytesMut::new();
        for record in queue {
            records.put(record.clone());
            records.put_u8(0);
        }
        Ok(records.into())
    }
}
