use arrow_array::RecordBatch;

use crate::storage::{partition::Partition, stream_config::StreamConfig};
use std::collections::HashMap;

struct Stream {
    partitions: HashMap<String, Partition>,
    config: StreamConfig,
}

// TODO: What are the responsibilites of a Stream?
// TODO: Where should schema validation happen?
impl Stream {
    pub fn new() -> Self {
        Stream {
            partitions: HashMap::default(),
            config: StreamConfig::default(),
        }
    }

    pub fn append() {
        todo!("define signature: one or listo f recordbatches? What is return type.")
    }

    pub fn get(offset: usize) -> RecordBatch {
        todo!("don't knwo yet")
    }
}
