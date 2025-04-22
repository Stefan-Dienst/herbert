use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;

use crate::topic_manager::TopicManager;

pub fn create_produce_request(topic: &str, record: Bytes) -> ProduceRequest {
    let mut produce_request = ProduceRequest::default();
    let topic_to_produce_to = create_topic_produce_data(topic, record);
    produce_request.topic_data.push(topic_to_produce_to);
    produce_request
}

fn create_topic_produce_data(topic: &str, record: Bytes) -> TopicProduceData {
    let mut topic_to_produce_to = TopicProduceData::default();
    topic_to_produce_to.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    let mut things_to_produce = PartitionProduceData::default();
    things_to_produce.records = Some(record);

    topic_to_produce_to.partition_data.push(things_to_produce);
    topic_to_produce_to
}

pub fn handle_produce_request(buf: Bytes, api_version: i16, topic_manager: &mut TopicManager) {
    let produce_request = ProduceRequest::decode(&mut Bytes::from(buf), api_version);
    match produce_request {
        Ok(ProduceRequest { topic_data, .. }) => {
            handle_topic_data(topic_data, topic_manager);
        }
        _ => {
            error!("Something wrong with the produce request.")
        }
    }
}

fn handle_topic_data(topic_data: Vec<TopicProduceData>, topic_manager: &mut TopicManager) {
    let record = topic_data
        .first()
        .unwrap()
        .partition_data
        .first()
        .unwrap()
        .records
        .clone()
        .unwrap();
    topic_manager.add(record)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::protocol::Encodable;

    use super::*;

    #[test]
    fn test_handle_topic_data() {
        let topic_name = "test";
        let record = Bytes::from("yeah");

        let mut topic_manager = TopicManager::new();
        let produce_request = create_produce_request(&topic_name, record.clone());

        handle_topic_data(produce_request.topic_data, &mut topic_manager);

        let read = topic_manager.topic.read().unwrap();
        let message = read.get(0).unwrap();

        assert_eq!(*message, record)
    }

    #[test]
    fn test_handle_produce_request() {
        let topic_name = "test";
        let record = Bytes::from("yeah");

        let mut topic_manager = TopicManager::new();
        let produce_request = create_produce_request(&topic_name, record.clone());

        let mut request_buffer = BytesMut::new();
        let produce_request_api_version = 9;
        produce_request.encode(&mut request_buffer, produce_request_api_version);
        handle_produce_request(request_buffer.into(), produce_request_api_version, &mut topic_manager);

        let read = topic_manager.topic.read().unwrap();
        let message = read.get(0).unwrap();

        assert_eq!(*message, record)
    }
}
