use kafka_protocol::messages::FetchRequest;
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::protocol::{Decodable, Encodable};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;

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
        },
        _ => {
            error!("Something wrong with the fetch request.")
        }
    }
}
