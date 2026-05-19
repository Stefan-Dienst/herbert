use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use crate::config::Config;

pub struct OffsetManager {
    pub offsets: RwLock<HashMap<(String, String), i64>>,
    config: Arc<Config>,
}

impl OffsetManager {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            offsets: RwLock::new(HashMap::new()),
            config: config,
        }
    }

    pub fn get_offset(&self, consumer_group: &str, topic: &str) -> Result<i64> {
        let offsets = self
            .offsets
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;

        let offset = offsets
            .get(&(consumer_group.to_string(), topic.to_string()))
            .copied()
            .unwrap_or(0);
        Ok(offset)
    }

    pub fn set_offset(&self, consumer_group: &str, topic: &str, offset: i64) -> Result<()> {
        self.offsets
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .insert((consumer_group.to_string(), topic.to_string()), offset);
        self.flush()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let offsets = self
            .offsets
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;

        let json = serde_json::to_string(
            &offsets
                .iter()
                .map(|((consumer_group, topic), offset)| {
                    (format!("{}:{}", consumer_group, topic), *offset)
                })
                .collect::<HashMap<String, i64>>(),
        )?;
        fs::write(&self.config.offset_path, json)?;

        Ok(())
    }

    pub fn recover(&mut self) -> Result<()> {
        let json = fs::read(&self.config.offset_path)?;
        let stored_offsets: HashMap<String, i64> = serde_json::from_slice(&json)?;

        let offsets = stored_offsets
            .iter()
            .map(|(key, offset)| {
                (
                    key.split_once(':')
                        .map(|(consumer_group, topic)| {
                            (consumer_group.to_string(), topic.to_string())
                        })
                        .expect("Flushed json should be parse-able."),
                    *offset,
                )
            })
            .collect::<HashMap<(String, String), i64>>();

        self.offsets = RwLock::new(offsets);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_new() {
        let config = Arc::new(Config::test_default());
        let offset_manager = OffsetManager::new(config);
        assert!(offset_manager.offsets.read().unwrap().is_empty())
    }

    #[test]
    fn test_set_offset() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let offset_path = temp_dir.path().join("offset.json");
        let config = Arc::new(Config::test_default().with_offset_path(&offset_path));

        let offset_manager = OffsetManager::new(config);
        let _ = offset_manager.set_offset("test", "foobar", 10);
        assert_eq!(
            offset_manager
                .offsets
                .read()
                .unwrap()
                .get(&("test".to_string(), "foobar".to_string()))
                .unwrap(),
            &10
        );

        Ok(())
    }

    #[test]
    fn test_get_offset() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let offset_path = temp_dir.path().join("offset.json");
        let config = Arc::new(Config::test_default().with_offset_path(&offset_path));

        let offset_manager = OffsetManager::new(config);
        let offset = 10;
        let _ = offset_manager.set_offset("test", "foobar", offset);
        let got_offset = offset_manager.get_offset("test", "foobar");
        assert_eq!(offset, got_offset.unwrap());
        Ok(())
    }

    #[test]
    fn test_get_offset_for_new_consumer_group() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let offset_path = temp_dir.path().join("offset.json");
        let config = Arc::new(Config::test_default().with_offset_path(&offset_path));

        let offset_manager = OffsetManager::new(config);
        let offset = 0;
        let got_offset = offset_manager.get_offset("test", "foobar");
        assert_eq!(offset, got_offset.unwrap());
        Ok(())
    }

    #[test]
    fn test_flush_and_recover() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let offset_path = temp_dir.path().join("offset.json");
        let config = Arc::new(Config::test_default().with_offset_path(&offset_path));

        let offset_manager = OffsetManager::new(config.clone());
        let _ = offset_manager.set_offset("test", "foobar", 10);
        let _ = offset_manager.set_offset("walk", "foobar", 11);
        let _ = offset_manager.set_offset("test", "asap", 54);

        let mut new_om = OffsetManager::new(config);
        new_om.recover().unwrap();

        assert_eq!(
            new_om
                .offsets
                .read()
                .unwrap()
                .get(&("test".to_string(), "foobar".to_string()))
                .unwrap(),
            &10
        );
        assert_eq!(
            new_om
                .offsets
                .read()
                .unwrap()
                .get(&("walk".to_string(), "foobar".to_string()))
                .unwrap(),
            &11
        );
        assert_eq!(
            new_om
                .offsets
                .read()
                .unwrap()
                .get(&("test".to_string(), "asap".to_string()))
                .unwrap(),
            &54
        );
        Ok(())
    }
}
