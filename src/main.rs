use kafka_protocol::messages::ApiKey as OtherApiKey;
use kafka_protocol::messages::ApiKey as RequestKind;
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::protocol::buf::ByteBuf;
use bytes::{BytesMut, Buf, Bytes};
use kafka_protocol::messages::RequestHeader as OtherRequestHeader;
use kafka_protocol::protocol::{Encodable, Decodable, StrBytes, HeaderVersion};
use log::{error, info};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

#[derive(Debug)]
struct RequestHeader {
    // TODO: Maybe add a total size thing here so that this can be used as an offset to parsing the rest
    // of the message.
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
}

impl RequestHeader {
    fn parse(buffer: &[u8; 512]) -> Self {
        let mut cursor = Cursor::new(&buffer);

        let size = cursor.read_i32::<BigEndian>().unwrap();

        let request_api_key = cursor.read_i16::<BigEndian>().unwrap();
        let request_api_version = cursor.read_i16::<BigEndian>().unwrap();
        let correlation_id = cursor.read_i32::<BigEndian>().unwrap();
        let size_of_client_id = cursor.read_i16::<BigEndian>().unwrap();

        let mut unparsed_client_id = vec![0; size_of_client_id.try_into().unwrap()];
        cursor.read_exact(&mut unparsed_client_id).unwrap();

        let client_id = String::from_utf8(unparsed_client_id).expect("Invalid utf8 sequence");

        RequestHeader {
            request_api_key: request_api_key,
            request_api_version: request_api_version,
            correlation_id: correlation_id,
            client_id: client_id,
        }
    }
}

#[derive(Debug)]
struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

#[derive(Debug)]
struct ApiVersionsResponse {
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    // TODO: Put this into traits for Responses. Do the same with requests.
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.write_i16::<BigEndian>(self.error_code).unwrap();

        buffer
            .write_i32::<BigEndian>(self.api_keys.len() as i32)
            .unwrap();
        for api_key in &self.api_keys {
            buffer.write_i16::<BigEndian>(api_key.api_key).unwrap();
            buffer.write_i16::<BigEndian>(api_key.min_version).unwrap();
            buffer.write_i16::<BigEndian>(api_key.max_version).unwrap();
        }

        buffer
            .write_i32::<BigEndian>(self.throttle_time_ms)
            .unwrap();

        // Prefix the message length
        let mut full_response = Vec::new();
        let _ = full_response.write_i32::<BigEndian>(buffer.len() as i32);
        full_response.extend_from_slice(&buffer);
        full_response
    }
}

fn handle_connection(mut stream: TcpStream) {
    info!("I have received a connection!");
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                info!("Client disconnected.");
                break;
            }
            Ok(n) => {
                // info!("{:?}", buffer);
                // Old stuff. TODO: Remove and go with new.
                let header = RequestHeader::parse(&buffer);
                dbg!(&header);


                let mut new_buf = Bytes::from(Vec::from(&buffer[4..]));

                let api_key = new_buf.peek_bytes(0..2).get_i16();
                let api_version = new_buf.peek_bytes(2..4).get_i16();
                let header_version = OtherApiKey::try_from(api_key).unwrap().request_header_version(api_version);

                let header = OtherRequestHeader::decode(&mut new_buf, header_version).unwrap();
                dbg!(&header);
                let api_key = OtherApiKey::try_from(header.request_api_key);
                dbg!(api_key);
                let a = ApiVersionsRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                dbg!(a);
                // RequestKind::ApiVersions(ApiVersionsRequest::decode(&mut Bytes::from(), header.request_api_version));
                


                // TODO: Add a function that handles the request by using the correct method and
                // creating the correct response.
                // Do fake response
                let response = ApiVersionsResponse {
                    error_code: 0,
                    api_keys: vec![ApiKey {
                        api_key: 1,
                        min_version: 0,
                        max_version: 10,
                    }],
                    throttle_time_ms: 0,
                };

                // stream.write(&response.to_bytes()).unwrap();
                // stream.flush().unwrap();

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
