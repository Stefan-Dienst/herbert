use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::protocol::Decodable;
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;

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
