use bytes::BufMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::metadata_response::MetadataResponsePartition;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::request_header;
use kafka_protocol::messages::ApiKey as ApiKey;
use kafka_protocol::messages::ApiKey as RequestKind;
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;
use kafka_protocol::messages::BrokerId;
use kafka_protocol::messages::FindCoordinatorRequest;
use kafka_protocol::messages::FindCoordinatorResponse;
use kafka_protocol::messages::ListGroupsRequest;
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::messages::MetadataResponse;
use kafka_protocol::messages::OffsetFetchRequest;
use kafka_protocol::messages::OffsetFetchResponse;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::buf::ByteBuf;
use bytes::{BytesMut, Buf, Bytes};
use kafka_protocol::messages::RequestHeader as RequestHeader;
use kafka_protocol::protocol::buf::ByteBufMut;
use kafka_protocol::protocol::{Encodable, Decodable, StrBytes, HeaderVersion};
use log::{error, info};
use std::collections::BTreeMap;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
struct ResponseApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

// #[derive(Debug)]
// struct ApiVersionsResponse {
//     error_code: i16,
//     api_keys: Vec<ResponseApiKey>,
//     throttle_time_ms: i32,
// }

// impl ApiVersionsResponse {
//     // TODO: Put this into traits for Responses. Do the same with requests.
//     fn to_bytes(&self) -> Vec<u8> {
//         let mut buffer = Vec::new();
//         buffer.write_i16::<BigEndian>(self.error_code).unwrap();

//         buffer
//             .write_i32::<BigEndian>(self.api_keys.len() as i32)
//             .unwrap();
//         for api_key in &self.api_keys {
//             buffer.write_i16::<BigEndian>(api_key.api_key).unwrap();
//             buffer.write_i16::<BigEndian>(api_key.min_version).unwrap();
//             buffer.write_i16::<BigEndian>(api_key.max_version).unwrap();
//         }

//         buffer
//             .write_i32::<BigEndian>(self.throttle_time_ms)
//             .unwrap();

//         // Prefix the message length
//         let mut full_response = Vec::new();
//         let _ = full_response.write_i32::<BigEndian>(buffer.len() as i32);
//         full_response.extend_from_slice(&buffer);
//         full_response
//     }
// }

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

                let mut new_buf = Bytes::from(Vec::from(&buffer[4..]));

                let api_key = new_buf.peek_bytes(0..2).get_i16();
                let api_version = new_buf.peek_bytes(2..4).get_i16();
                let header_version = ApiKey::try_from(api_key).unwrap().request_header_version(api_version);
                info!("This is the header version: {:?}", header_version);


                let header = RequestHeader::decode(&mut new_buf, header_version).unwrap();
                let api_key = ApiKey::try_from(header.request_api_key);


                info!("This api key mannn: {:?}", api_key);
                info!("This api version mannn: {:?}", api_version);

                let mut response_buffer = BytesMut::new();
                let mut response_header = ResponseHeader::default();
                // Set the response correlation_id to the one of the request match them.
                response_header.correlation_id = header.correlation_id;
                let mut size = response_header.compute_size(header_version).unwrap();

                match api_key {
                    Ok(ApiKey::ApiVersions) => {
                        let a = ApiVersionsRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                        let mut response = ApiVersionsResponse::default();

                        for key in 0..82 {
                            let mut api_version_struct = ApiVersion::default();
                            api_version_struct.api_key = key;
                            api_version_struct.max_version = 10;

                            response.api_keys.push(api_version_struct);
                        }
                        dbg!(&response);
                        size += response.compute_size(header.request_api_version).unwrap();

                        response_buffer.put_u32(size as u32); 
                        let _ = response_header.encode(&mut response_buffer, header_version);
                        let _ = response.encode(&mut response_buffer, api_version);
                    },
                    Ok(ApiKey::ListGroups) => {
                        let a = ListGroupsRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                        let response = ApiVersionsResponse::default();
                        size += response.compute_size(header.request_api_version).unwrap();

                        dbg!(&response);
                        response_buffer.put_u32(size as u32); 
                        let _ = response_header.encode(&mut response_buffer, header_version);
                        let _ = response.encode(&mut response_buffer, api_version);
                    },
                    Ok(ApiKey::OffsetFetch) => {
                        let a = OffsetFetchRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                        let mut response = OffsetFetchResponse::default();

                        size += response.compute_size(header.request_api_version).unwrap();

                        dbg!(&response);
                        response_buffer.put_u32(size as u32); 
                        let _ = response_header.encode(&mut response_buffer, header_version);
                        let _ = response.encode(&mut response_buffer, api_version);
                    },
                    Ok(ApiKey::FindCoordinator) => {
                        let a = FindCoordinatorRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                        let mut response = FindCoordinatorResponse::default();
                        response.node_id = (1).into();
                        response.port = 9001;
                        response.host = StrBytes::from_string("localhost".to_string());

                        size += response.compute_size(header.request_api_version).unwrap();

                        dbg!(&response);
                        response_buffer.put_u32(size as u32); 
                        let _ = response_header.encode(&mut response_buffer, header_version);
                        let _ = response.encode(&mut response_buffer, api_version);
                    },
                    Ok(ApiKey::Metadata) => {
                        let a = MetadataRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                        let mut response = MetadataResponse::default();

                        response.throttle_time_ms = 1000;

                        let mut topic = MetadataResponseTopic::default();
                        topic.name = Some(TopicName::from(StrBytes::from_string("foobar".to_string())));
                        let mut partition = MetadataResponsePartition::default();
                        partition.leader_id = (1).into();
                        topic.partitions.push(partition);
                        // response.topics.push(topic);

                        let mut broker = MetadataResponseBroker::default();
                        broker.node_id = (1).into();
                        broker.host = "localhost".into();
                        broker.port = 9001;
                        response.brokers.push(broker);

                        dbg!(&response);

                        size += response.compute_size(header.request_api_version).unwrap();

                        response_buffer.put_u32(size as u32); 
                        let _ = response_header.encode(&mut response_buffer, header_version);
                        let _ = response.encode(&mut response_buffer, 1);
                    },
                    Ok(ApiKey::Produce) => {
                        let a = ProduceRequest::decode(&mut Bytes::from(new_buf), header.request_api_version);
                        dbg!(a);
                    }
                    _ => {info!("Something unexpected happend.");}
                }
                info!("The size is {:}", size);

                dbg!(&response_buffer);

                // // Prepend the size to the buffer.
                // response_buffer.reserve(4);
                // response_buffer.advance(4);
                // response_buffer.put_u32((size as u32).to_be()); 
                // dbg!(&response_buffer);

                stream.write(&response_buffer[..]).unwrap();
                stream.flush().unwrap();
                


                // TODO: Add a function that handles the request by using the correct method and
                // creating the correct response.
                // Do fake response
                // let response = ApiVersionsResponse {
                //     error_code: 0,
                //     api_keys: vec![ResponseApiKey {
                //         api_key: 1,
                //         min_version: 0,
                //         max_version: 10,
                //     }],
                //     throttle_time_ms: 0,
                // };


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
