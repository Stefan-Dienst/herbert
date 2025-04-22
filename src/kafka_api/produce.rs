use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::protocol::{Decodable, StrBytes};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;

pub fn create_produce_request(topic: &str, record: Bytes) -> ProduceRequest {
    let mut produce_request = ProduceRequest::default();

    let mut topic_to_produce_to = TopicProduceData::default();
    topic_to_produce_to.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    let mut things_to_produce = PartitionProduceData::default();
    things_to_produce.records = Some(record);

    topic_to_produce_to.partition_data.push(things_to_produce);

    produce_request.topic_data.push(topic_to_produce_to);
    produce_request
}


pub fn handle_produce_request(buf: Bytes, api_version: i16, topic: &Lazy<RwLock<VecDeque<Bytes>>>) {
    let produce_request = ProduceRequest::decode(&mut Bytes::from(buf), api_version);
    match produce_request {
        Ok(ProduceRequest { topic_data, .. }) => {
            handle_topic_data(topic_data, topic);
        }
        _ => {
            error!("Something wrong with the produce request.")
        }
    }
}


fn handle_topic_data(topic_data: Vec<TopicProduceData>, topic: &Lazy<RwLock<VecDeque<Bytes>>>) {
    let record = topic_data
        .first()
        .unwrap()
        .partition_data
        .first()
        .unwrap()
        .records
        .clone()
        .unwrap();
    let mut write = topic.write().unwrap();
    write.push_front(record);
    info!("Currently topic has {:?}", write);
}




#[cfg(test)]
mod tests {
    use kafka_protocol::{messages::{produce_request::PartitionProduceData, TopicName}, protocol::StrBytes};

    use super::*;

    #[test]
    fn test_handle_topic_data() {

        let topic: Lazy<RwLock<VecDeque<Bytes>>> = Lazy::new(|| RwLock::new(VecDeque::new()));
        let mut topic_data = Vec::new();


        let mut topic_to_produce_to = TopicProduceData::default();
        topic_to_produce_to.name = TopicName::from(StrBytes::from_string("test".to_string()));

        let mut things_to_produce = PartitionProduceData::default();
        things_to_produce.records = Some(Bytes::from("test"));

        topic_to_produce_to.partition_data.push(things_to_produce);


        topic_data.push(topic_to_produce_to);

        handle_topic_data(topic_data, &topic);

        let read = topic.read().unwrap();
        let message = read.get(0).unwrap();

        assert_eq!(*message, Bytes::from("test"))

    }
}
