use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;


pub fn create_fetch_request(topic: &str, max_messages: i32) -> FetchRequest {
    let mut fetch_request = FetchRequest::default();

    // FIXME: Max bytes does not work. Herbet can't decode it somehow.
    fetch_request.max_bytes = max_messages;

    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName::from(StrBytes::from_string(topic.to_string()));

    fetch_request.topics.push(fetch_topic);
    fetch_request
}

pub fn handle_fetch_request(buf: Bytes, api_version: i16, topic: &Lazy<RwLock<VecDeque<Bytes>>>) {
    let fetch_request = FetchRequest::decode(&mut Bytes::from(buf), api_version);
    match fetch_request {
        Ok(FetchRequest { max_bytes, .. }) => {
            let mut write = topic.write().unwrap();
            dbg!(max_bytes);
            while !write.is_empty() {
                let message = write.pop_back().unwrap();
                info!("Found message {:?}", message);
            }
        }
        _ => {
            error!("Something wrong with the fetch request.")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_fetch_request() {
        let topic: Lazy<RwLock<VecDeque<Bytes>>> = Lazy::new(|| RwLock::new(VecDeque::new()));
        let mut write = topic.write().unwrap();
        write.push_back(Bytes::from("test"));
        drop(write);

        let fetch_request = create_fetch_request("test", 3);
        let mut request_buffer = BytesMut::new();
        let fetch_request_api_version = 9;
        fetch_request.encode(&mut request_buffer, fetch_request_api_version);
        handle_fetch_request(request_buffer.into(), fetch_request_api_version, &topic);
        // TODO: write sensicel test so that something is actually retured.
    }
}
