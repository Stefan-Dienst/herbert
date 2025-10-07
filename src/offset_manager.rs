use anyhow::Result;
use std::{collections::HashMap, sync::RwLock};

pub struct OffsetManager {
    pub offsets: RwLock<HashMap<(String, String), i64>>,
}

impl OffsetManager {
    pub fn new() -> Self {
        Self {
            offsets: RwLock::new(HashMap::new()),
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let offset_manager = OffsetManager::new();
        assert!(offset_manager.offsets.read().unwrap().is_empty())
    }

    #[test]
    fn test_set_offset() {
        let offset_manager = OffsetManager::new();
        let _ = offset_manager.set_offset("test", "foobar", 10);
        assert_eq!(
            offset_manager
                .offsets
                .read()
                .unwrap()
                .get(&("test".to_string(), "foobar".to_string()))
                .unwrap(),
            &10
        )
    }

    #[test]
    fn test_get_offset() {
        let offset_manager = OffsetManager::new();
        let offset = 10;
        let _ = offset_manager.set_offset("test", "foobar", offset);
        let got_offset = offset_manager.get_offset("test", "foobar");
        assert_eq!(offset, got_offset.unwrap())
    }

    #[test]
    fn test_get_offset_for_new_consumer_group() {
        let offset_manager = OffsetManager::new();
        let offset = 0;
        let got_offset = offset_manager.get_offset("test", "foobar");
        assert_eq!(offset, got_offset.unwrap())
    }
}
