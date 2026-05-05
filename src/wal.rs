use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Cursor, Read, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use log::info;

use crate::{
    storage::{RecordStorage, StoredRecord},
    topic_manager::TopicManager,
};

// TODO: make this somehow configurable.
const NUM_UNCOMMITTED_MESSAGES: usize = 1;

pub struct WriteAheadLog {
    file: Mutex<BufWriter<File>>,
    buffer: Mutex<Vec<(String, StoredRecord)>>,
}

impl WriteAheadLog {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            file: Mutex::new(BufWriter::new(file)),
            buffer: Mutex::new(Vec::new()),
        })
    }

    pub fn add(&self, topic: &str, records: StoredRecord) -> Result<()> {
        let mut write = self
            .file
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?;

        let buffer = self.serialize(topic, &records)?;
        write.write(&buffer);

        self.buffer
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
            .push((topic.to_string(), records));

        Ok(())
    }

    pub fn serialize(&self, topic: &str, records: &StoredRecord) -> Result<Vec<u8>> {
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

    pub fn flush(&self, backend: &Arc<dyn RecordStorage>) -> Result<()> {
        self.file
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
            .flush()?;

        for (topic, record) in self
            .buffer
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
            .drain(..)
        {
            backend.add(&topic, record)?;
        }

        Ok(())
    }

    pub fn need_to_flush(&self) -> Result<bool> {
        Ok(self
            .buffer
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
            .len()
            >= NUM_UNCOMMITTED_MESSAGES)
    }

    pub fn read(path: impl AsRef<Path>, topic_manager: &mut TopicManager) -> Result<()> {
        let file = File::open(path)?;
        println!("Opened WAL file at");
        let mut buf_reader = BufReader::new(file);

        loop {
            let mut contents = Vec::new();
            let result = buf_reader.read_until(b'\n', &mut contents)?;

            // Read everything
            if contents.len() == 0 {
                break;
            }

            contents.pop();
            let topic = String::from_utf8(contents)?;

            println!("Found entry for topic {}", topic);

            let indicator = buf_reader.read_u8()?;
            let size = buf_reader.read_u32::<LittleEndian>()?;

            let mut buf = vec![0u8; size as usize];

            match indicator {
                // Raw bytes
                0 => {
                    buf_reader.read_exact(&mut buf)?;
                    topic_manager.add(&topic, Bytes::from(buf));
                }
                // RecordBatch
                1 => {
                    println!("found record batch");
                    buf_reader.read_exact(&mut buf)?;

                    println!("Read the record batch");

                    // If topic does not yet exit create it with schema.
                    if !topic_manager.exists(&topic)? {
                        let mut reader = StreamReader::try_new(Cursor::new(&buf), None)?;
                        if let Some(record_batch) = reader.next() {
                            let record_batch = record_batch?;

                            topic_manager
                                .create(&topic, Some(record_batch.schema().as_ref().to_owned()));
                        };
                    };
                    topic_manager.add(&topic, Bytes::from(buf));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown indicator encountered: {}",
                        indicator
                    ))
                }
            };
        }

        Ok(())
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
    fn test_new() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&wal_path)?;

        assert!(wal.file.lock().unwrap().buffer().is_empty());

        Ok(())
    }

    #[test]
    fn test_add_raw_bytes() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let mut wal = WriteAheadLog::new(&wal_path)?;
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
    fn test_add_record_batch() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )?;

        let mut wal = WriteAheadLog::new(&wal_path)?;
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

    #[test]
    fn test_read() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_path = temp_dir.path().join("test.wal");

        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )?;

        let mut wal = WriteAheadLog::new(&wal_path)?;
        wal.add("foobar", StoredRecord::Batch(batch.clone()));
        wal.add("foobar", StoredRecord::Batch(batch.clone()));

        wal.add("test", StoredRecord::Batch(batch.clone()));

        wal.file.lock().unwrap().flush();

        let mut topic_manager = TopicManager::default_log();

        println!("hi");
        let result = WriteAheadLog::read(&wal_path, &mut topic_manager);
        dbg!(result);

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
