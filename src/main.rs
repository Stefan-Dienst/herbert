use anyhow::Context;
use anyhow::Result;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use herbert::kafka_api::handle_fetch_request;
use herbert::kafka_api::handle_produce_request;
use herbert::topic_manager::TopicManager;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable};
use log::{error, info};

use std::sync::Arc;
use std::thread;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn read_message_len(stream: &mut TcpStream) -> Result<usize> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .context("Failed to read message length")?;
    let msg_len = (&len_buf[..])
        .read_u32::<BigEndian>()
        .context("Failed to parse message length")? as usize;
    Ok(msg_len)
}

fn handle_connection(mut stream: TcpStream, topic_manager: Arc<TopicManager>) -> Result<()> {
    info!("I have received a connection!");

    loop {
        let msg_len = read_message_len(&mut stream)?;
        let mut buf = vec![0u8; msg_len];

        match stream.read_exact(&mut buf) {
            Ok(()) => {
                let mut buffer = Bytes::from(buf);

                let api_key = buffer.peek_bytes(0..2).get_i16();
                let api_version = buffer.peek_bytes(2..4).get_i16();
                let header_version = ApiKey::try_from(api_key)
                    .unwrap()
                    .request_header_version(api_version);
                // info!("The header version: {:?}", header_version);

                let header = RequestHeader::decode(&mut buffer, header_version).unwrap();
                let api_key = ApiKey::try_from(header.request_api_key);

                info!("The api key: {:?}", api_key);
                info!("The api version: {:?}", api_version);

                let mut response_buffer = BytesMut::new();
                let mut response_header = ResponseHeader::default();
                // Set the response correlation_id to the one of the request match them.
                response_header.correlation_id = header.correlation_id;
                let response_header_api_version = 1;
                let mut size = response_header.compute_size(header_version).unwrap();

                //TODO: Wrap response in an enum, and implement ecode trait for it so that I can
                //treat all response the same. This is needed as encode is not object safe to g
                //can't use Box(dyn)...
                match api_key {
                    Ok(ApiKey::Produce) => {
                        let response = handle_produce_request(
                            buffer,
                            header.request_api_version,
                            &topic_manager,
                        )?;
                    }
                    Ok(ApiKey::Fetch) => {
                        let response = handle_fetch_request(
                            buffer,
                            header.request_api_version,
                            &topic_manager,
                        )?;

                        size += response_header
                            .compute_size(response_header_api_version)
                            .unwrap();
                        size += response.compute_size(header.request_api_version).unwrap();
                        response_buffer.put_u32(size as u32);
                        let _ = response_header
                            .encode(&mut response_buffer, response_header_api_version);
                        let _ = response.encode(&mut response_buffer, api_version);
                    }
                    _ => {
                        info!("This request of the kafka protocol is not yet covered. :(");
                    }
                }

                // dbg!(&response_buffer);
                stream.write(&response_buffer[..]).unwrap();
                stream.flush().unwrap();
            }
            Err(..) => {
                error!("Error");
                break;
            }
        }
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    let adress = "127.0.0.1:9001";

    info!("Create the topic");

    info!("Starting the TCP server. Listening on {:?}", adress);
    let listener = TcpListener::bind(adress)?;

    let mut topic_manager = Arc::new(TopicManager::new());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tm_clone = Arc::clone(&topic_manager);
                thread::spawn(|| handle_connection(stream, tm_clone));
            }
            Err(..) => {
                error!("Oh oh!");
            }
        }
    }
    Ok(())
}
