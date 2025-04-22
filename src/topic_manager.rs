use bytes::Bytes;
use log::info;
use std::collections::VecDeque;
use std::sync::RwLock;

pub struct TopicManager {
    pub topic: RwLock<VecDeque<Bytes>>,
}

impl TopicManager {
    pub fn new() -> Self {
        TopicManager {
            topic: RwLock::new(VecDeque::new()),
        }
    }

    pub fn add(&mut self, record: Bytes) {
        let mut write = self.topic.write().unwrap();
        write.push_front(record);
        info!("Currently topic has {:?}", write);
    }

    pub fn remove(&mut self) {
        let mut write = self.topic.write().unwrap();
        while !write.is_empty() {
            let message = write.pop_back().unwrap();
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
        assert!(topic_manager.topic.read().unwrap().is_empty())
    }

    #[test]
    fn test_add() {
        let mut topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add(record.clone());
        assert!(!topic_manager.topic.read().unwrap().is_empty());
        assert_eq!(*topic_manager.topic.read().unwrap().get(0).unwrap(), record);
    }

    #[test]
    fn test_remove() {
        let mut topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add(record.clone());
        assert!(!topic_manager.topic.read().unwrap().is_empty());
        assert_eq!(*topic_manager.topic.read().unwrap().get(0).unwrap(), record);

        topic_manager.remove();
        assert!(topic_manager.topic.read().unwrap().is_empty());
    }
}
