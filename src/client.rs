use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::thread::sleep;
use std::time::Duration;

use crate::kafka_api::create_fetch_request;
use crate::kafka_api::create_produce_request;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable};

fn create_request_header(request_api_key: i16, request_api_version: i16) -> RequestHeader {
    let mut header = RequestHeader::default();
    header.request_api_key = request_api_key;
    header.request_api_version = request_api_version;
    header
}

fn create_buffer(header: RequestHeader, request: impl Encodable) -> BytesMut {
    let mut size = header.compute_size(header.request_api_version).unwrap();
    size += request.compute_size(header.request_api_version).unwrap();

    let mut request_buffer = BytesMut::new();
    request_buffer.put_u32(size as u32);
    let _ = header.encode(&mut request_buffer, header.request_api_version);
    let _ = request.encode(&mut request_buffer, header.request_api_version);
    request_buffer
}

pub fn produce(broker: String, topic: String, message: String) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;
    let produce_request_api_version = 9;

    let header = create_request_header(ApiKey::Produce as i16, produce_request_api_version);
    let record = Bytes::from(message);
    let produce_request = create_produce_request(&topic, record);

    let request_buffer = create_buffer(header, produce_request);
    stream.write(&request_buffer)?;

    Ok(())
}

pub fn consume_continuos(broker: String, topic: String, max_messages: i32) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;
    let fetch_request_api_version = 1;

    let header = create_request_header(ApiKey::Fetch as i16, fetch_request_api_version);
    let fetch_request = create_fetch_request(&topic, max_messages);

    let request_buffer = create_buffer(header, fetch_request);

    loop {
        println!("Consumed records:");
        let records = get_records(&mut stream, &request_buffer, fetch_request_api_version)?;
        for record in records {
            println!("{:?}", std::str::from_utf8(&record).unwrap());
        }
        sleep(Duration::from_secs(2));
    }
}

pub fn consume(broker: String, topic: String, max_messages: i32) -> Result<Vec<Vec<u8>>> {
    let mut stream = TcpStream::connect(broker)?;
    let fetch_request_api_version = 1;

    let header = create_request_header(ApiKey::Fetch as i16, fetch_request_api_version);
    let fetch_request = create_fetch_request(&topic, max_messages);

    let request_buffer = create_buffer(header, fetch_request);

    get_records(&mut stream, &request_buffer, fetch_request_api_version)
}

fn get_records(
    stream: &mut TcpStream,
    request_buffer: &BytesMut,
    fetch_request_api_version: i16,
) -> Result<Vec<Vec<u8>>> {
    stream.write(&request_buffer)?;

    // Read response
    let mut buffer = [0; 512];
    stream.read(&mut buffer);

    // Decode response
    let mut new_buf = Bytes::from(Vec::from(&buffer[4..]));
    let header = ResponseHeader::decode(&mut new_buf, 1).unwrap();

    let fetch_response =
        FetchResponse::decode(&mut Bytes::from(new_buf), fetch_request_api_version).unwrap();
    let records = fetch_response
        .responses
        .get(0)
        .unwrap()
        .partitions
        .get(0)
        .unwrap()
        .records
        .clone()
        .unwrap();

    // split records
    let parts: Vec<Vec<u8>> = records.split(|b| *b == 0).map(|s| s.to_vec()).collect();
    Ok(parts)
}
