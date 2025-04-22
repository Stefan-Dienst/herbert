use bytes::{Buf, Bytes, BytesMut};
use herbert::kafka_api::handle_fetch_request;
use herbert::kafka_api::handle_produce_request;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

static TOPIC: Lazy<RwLock<VecDeque<Bytes>>> = Lazy::new(|| RwLock::new(VecDeque::new()));

fn handle_connection(mut stream: TcpStream) {
    info!("I have received a connection!");
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                info!("Client disconnected.");
                break;
            }
            Ok(_n) => {
                let mut new_buf = Bytes::from(Vec::from(&buffer[4..]));

                let api_key = new_buf.peek_bytes(0..2).get_i16();
                let api_version = new_buf.peek_bytes(2..4).get_i16();
                let header_version = ApiKey::try_from(api_key)
                    .unwrap()
                    .request_header_version(api_version);
                info!("The header version: {:?}", header_version);

                let header = RequestHeader::decode(&mut new_buf, header_version).unwrap();
                let api_key = ApiKey::try_from(header.request_api_key);

                info!("The api key: {:?}", api_key);
                info!("The api version: {:?}", api_version);

                let mut response_buffer = BytesMut::new();
                let mut response_header = ResponseHeader::default();
                // Set the response correlation_id to the one of the request match them.
                response_header.correlation_id = header.correlation_id;
                let mut size = response_header.compute_size(header_version).unwrap();

                match api_key {
                    Ok(ApiKey::Produce) => {
                        handle_produce_request(new_buf, header.request_api_version, &TOPIC);
                    }
                    Ok(ApiKey::Fetch) => {
                        handle_fetch_request(new_buf, header.request_api_version, &TOPIC);
                    }
                    _ => {
                        info!("This request of the kafka protocol is not yet covered. :(");
                    }
                }

                stream.write(&response_buffer[..]).unwrap();
                stream.flush().unwrap();
            }
            Err(..) => {
                error!("Error");
                break;
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    add(1, 2);
    let adress = "127.0.0.1:9001";

    info!("Create the topic");

    info!("Starting the TCP server. Listening on {:?}", adress);
    let listener = TcpListener::bind(adress)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream);
            }
            Err(..) => {
                error!("Oh oh!");
            }
        }
    }
    Ok(())
}

fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 1 + 2)
    }
}
