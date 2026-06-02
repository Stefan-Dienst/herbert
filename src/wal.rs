use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use log::info;

use crate::{
    config::Config,
    error::HerbertError,
    storage::{RecordStorage, StoredRecord},
    topic_manager::TopicManager,
};

pub struct WriteAheadLog {
    pub file: Mutex<BufWriter<File>>,
    buffer: Mutex<Vec<(String, StoredRecord)>>,
    config: Arc<Config>,
}

impl WriteAheadLog {
    pub fn new(config: Arc<Config>) -> Result<Self, HerbertError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.wal_path)?;

        Ok(Self {
            file: Mutex::new(BufWriter::new(file)),
            buffer: Mutex::new(Vec::new()),
            config: config,
        })
    }

    pub fn add(&self, topic: &str, records: StoredRecord) -> Result<(), HerbertError> {
        let mut write = self.file.lock().map_err(|_e| HerbertError::PoisonError)?;

        let buffer = self.serialize(topic, &records)?;
        write.write(&buffer)?;

        self.buffer
            .lock()
            .map_err(|_e| HerbertError::PoisonError)?
            .push((topic.to_string(), records));

        Ok(())
    }

    pub fn serialize(&self, topic: &str, records: &StoredRecord) -> Result<Vec<u8>, HerbertError> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend(Bytes::from(topic.to_owned()));
        buffer.push(b'\n');

        match records {
            StoredRecord::Raw(bytes) => {
                // zero indicates raw bytes
                buffer.push(0);
                // Write length
                buffer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                buffer.extend(bytes)
            }
            StoredRecord::Batch(record_batch) => {
                // one indicates record batch
                buffer.push(1);

                let mut tmp_buffer: Vec<u8> = Vec::new();
                let mut writer = StreamWriter::try_new(&mut tmp_buffer, &record_batch.schema())?;
                writer.write(&record_batch)?;
                writer.finish()?;

                buffer.write_u32::<LittleEndian>(tmp_buffer.len() as u32)?;
                buffer.extend(tmp_buffer)
            }
        };

        Ok(buffer)
    }

    pub fn flush(&self, backend: &Arc<dyn RecordStorage>) -> Result<(), HerbertError> {
        info!("Flushing the WAL");
        self.file
            .lock()
            .map_err(|_e| HerbertError::PoisonError)?
            .flush()?;

        for (topic, record) in self
            .buffer
            .lock()
            .map_err(|_e| HerbertError::PoisonError)?
            .drain(..)
        {
            backend.add(&topic, record)?;
        }

        Ok(())
    }

    pub fn need_to_flush(&self) -> Result<bool, HerbertError> {
        Ok(self
            .buffer
            .lock()
            .map_err(|_e| HerbertError::PoisonError)?
            .len()
            >= self.config.num_uncommitted_messages)
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use crate::topic_manager;

    use super::*;
    use arrow_array::record_batch;
    use arrow_schema;
    use bytes::Bytes;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn test_new() -> Result<(), HerbertError> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let wal = WriteAheadLog::new(Arc::new(Config::default().with_wal_path(&wal_path)))?;

        assert!(wal.file.lock().unwrap().buffer().is_empty());

        Ok(())
    }

    #[test]
    fn test_add_raw_bytes() -> Result<(), HerbertError> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");
        let config = Arc::new(Config::test_default().with_wal_path(&wal_path));

        let mut wal = WriteAheadLog::new(config)?;
        wal.add("foobar", StoredRecord::Raw(Bytes::from("test")));

        wal.file.lock().unwrap().flush();
        let file = File::open(wal_path).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut contents = Vec::new();
        buf_reader.read_to_end(&mut contents)?;

        let mut expected: Vec<u8> = Vec::new();
        expected.extend(b"foobar\n");
        expected.push(0);
        expected.extend(vec![4, 0, 0, 0]);
        expected.extend(b"test");
        assert_eq!(contents, expected);
        Ok(())
    }

    #[test]
    fn test_add_record_batch() -> Result<(), HerbertError> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");
        let config = Arc::new(Config::test_default().with_wal_path(&wal_path));

        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )?;

        let mut wal = WriteAheadLog::new(config)?;
        wal.add("foobar", StoredRecord::Batch(batch.clone()));

        wal.file.lock().unwrap().flush();
        let file = File::open(wal_path).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut contents = Vec::new();
        buf_reader.read_to_end(&mut contents)?;

        let mut expected: Vec<u8> = Vec::new();
        expected.extend(b"foobar\n");
        expected.push(1);

        let mut tmp_buffer: Vec<u8> = Vec::new();
        let mut writer = StreamWriter::try_new(&mut tmp_buffer, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        expected.write_u32::<LittleEndian>(tmp_buffer.len() as u32)?;
        expected.extend(tmp_buffer);

        assert_eq!(contents, expected);

        Ok(())
    }
}
