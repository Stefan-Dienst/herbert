use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;

use crate::topic_manager::{self, TopicManager};

pub fn create_fetch_request(topic: &str, max_messages: i32) -> FetchRequest {
    let mut fetch_request = FetchRequest::default();

    // FIXME: Max bytes does not work. Herbet can't decode it somehow.
    fetch_request.max_bytes = max_messages;

    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName::from(StrBytes::from_string(topic.to_string()));

    fetch_request.topics.push(fetch_topic);
    fetch_request
}

pub fn handle_fetch_request(buf: Bytes, api_version: i16, topic_manager: &mut TopicManager) -> FetchResponse {
    let fetch_request = FetchRequest::decode(&mut Bytes::from(buf), api_version);
    match fetch_request {
        Ok(FetchRequest { max_bytes, topics, .. }) => {
            let topic_name = topics.first().unwrap().topic.to_string();
            let records = topic_manager.remove(&topic_name);

    let mut response = FetchResponse::default();
    let mut topic_response = FetchableTopicResponse::default();
    let mut partition_data = PartitionData::default();
    partition_data.records = Some(records);
    topic_response.partitions.push(partition_data);
    response.responses.push(topic_response);
    response
        }
        _ => {
            error!("Something wrong with the fetch request.");
                panic!();

        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_fetch_request() {
        let mut topic_manager = TopicManager::new();
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());

        let fetch_request = create_fetch_request("test", 3);
        let mut request_buffer = BytesMut::new();
        let fetch_request_api_version = 9;
        fetch_request.encode(&mut request_buffer, fetch_request_api_version);
        handle_fetch_request(
            request_buffer.into(),
            fetch_request_api_version,
            &mut topic_manager,
        );
        // TODO: write sensicel test so that something is actually retured.
    }
}
