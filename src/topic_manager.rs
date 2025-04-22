use bytes::Bytes;
use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

pub struct TopicManager {
    pub topics: RwLock<HashMap<String,VecDeque<Bytes>>>,
}

impl TopicManager {
    pub fn new() -> Self {
        TopicManager {
            topics: RwLock::new(HashMap::new()),
        }
    }

    pub fn add(&mut self, topic: &str, record: Bytes) {
        let mut write = self.topics.write().unwrap();
        let mut queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        queue.push_front(record);
        info!("Currently topics have {:?}", write);
    }

    pub fn remove(&mut self, topic: &str) {
        let mut write = self.topics.write().unwrap();
        let mut queue = write.entry(topic.to_string()).or_insert(VecDeque::new());
        while !queue.is_empty() {
            let message = queue.pop_back().unwrap();
            info!("Found message {:?}", message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let topic_manager = TopicManager::new();
        assert!(topic_manager.topics.read().unwrap().is_empty())
    }

    #[test]
    fn test_add() {
        let mut topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());
        assert!(!topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
        assert_eq!(*topic_manager.topics.read().unwrap().get("foobar").unwrap().get(0).unwrap(), record);
    }

    #[test]
    fn test_remove() {
        let mut topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());
        assert!(!topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
        assert_eq!(*topic_manager.topics.read().unwrap().get("foobar").unwrap().get(0).unwrap(), record);

        topic_manager.remove("foobar");
        assert!(topic_manager.topics.read().unwrap().get("foobar").unwrap().is_empty());
    }
}
