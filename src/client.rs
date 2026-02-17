use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::thread::sleep;
use std::time::Duration;

use crate::herbert_api::Request;
use crate::kafka_api::create_fetch_request;
use crate::kafka_api::create_offset_commit_request;
use crate::kafka_api::create_offset_fetch_request;
use crate::kafka_api::create_produce_request;
use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use byteorder::{BigEndian, WriteBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::OffsetFetchResponse;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable};

fn create_request_header(request_api_key: i16, request_api_version: i16) -> RequestHeader {
    let mut header = RequestHeader::default();
    header.request_api_key = request_api_key;
    header.request_api_version = request_api_version;
    header
}

fn create_buffer(header: &RequestHeader, request: impl Encodable) -> BytesMut {
    let mut size = header.compute_size(header.request_api_version).unwrap();
    size += request.compute_size(header.request_api_version).unwrap();

    let mut request_buffer = BytesMut::new();
    request_buffer.put_u32(size as u32);
    let _ = header.encode(&mut request_buffer, header.request_api_version);
    let _ = request.encode(&mut request_buffer, header.request_api_version);
    request_buffer
}

pub fn produce(broker: &str, topic: &str, message: &str) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;
    let produce_request_api_version = 9;

    let header = create_request_header(ApiKey::Produce as i16, produce_request_api_version);
    let record = Bytes::from(message.to_string());
    let produce_request = create_produce_request(&topic, record);

    let request_buffer = create_buffer(&header, produce_request);
    stream.write(&request_buffer)?;

    Ok(())
}

pub fn produce_record_batch(broker: &str, topic: &str, record_batch: &RecordBatch) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;
    let produce_request_api_version = 9;

    let header = create_request_header(ApiKey::Produce as i16, produce_request_api_version);

    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, &record_batch.schema())?;
    writer.write(&record_batch)?;
    writer.finish()?;

    let record = Bytes::from(buffer);
    let produce_request = create_produce_request(&topic, record);

    let request_buffer = create_buffer(&header, produce_request);
    stream.write(&request_buffer)?;

    Ok(())
}
pub fn consume_continuos(
    broker: &str,
    topic: &str,
    max_messages: i32,
    consumer_group: &str,
) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;

    let initial_offset = get_offset(&mut stream, consumer_group, topic)?;

    let fetch_request_api_version = 1;
    let header = create_request_header(ApiKey::Fetch as i16, fetch_request_api_version);

    let mut offset = initial_offset;
    loop {
        let fetch_request = create_fetch_request(&topic, max_messages, offset);
        let request_buffer = create_buffer(&header, fetch_request);
        println!("Consumed records:");
        let records = get_records(&mut stream, &request_buffer, fetch_request_api_version)?;
        if records.is_empty() {
            sleep(Duration::from_secs(2));
            continue;
        }

        // Arrow IPC streams start with 0xFFFFFFFF (continuation marker) or the "ARROW1" magic
        let magic = &records[..6.min(records.len())];
        if magic == b"ARROW1" || &records[..4] == &[0xFF, 0xFF, 0xFF, 0xFF] {
            let cursor = Cursor::new(records);
            let mut reader = StreamReader::try_new(cursor, None)?;

            let mut batches = Vec::new();
            while let Some(batch_result) = reader.next() {
                println!("{:?}", batch_result);
                batches.push(batch_result?);
            }
            // FIXME: offsets do not work.
            offset += 1;
        } else {
            let parts: Vec<Vec<u8>> = records.split(|b| *b == 0).map(|s| s.to_vec()).collect();
            for part in &parts {
                println!("{:?}", std::str::from_utf8(&part).unwrap());
            }
            offset += parts.len() as i64;
        }

        set_offset(&mut stream, consumer_group, topic, offset)?;
        sleep(Duration::from_secs(2));
    }
}

pub fn consume(
    broker: &str,
    topic: &str,
    max_messages: i32,
    consumer_group: &str,
) -> Result<Vec<Vec<u8>>> {
    let mut stream = TcpStream::connect(broker)?;
    let initial_offset = get_offset(&mut stream, consumer_group, topic)?;

    let fetch_request_api_version = 1;
    let header = create_request_header(ApiKey::Fetch as i16, fetch_request_api_version);
    let fetch_request = create_fetch_request(&topic, max_messages, initial_offset);

    let request_buffer = create_buffer(&header, fetch_request);

    let records = get_records(&mut stream, &request_buffer, fetch_request_api_version)?;

    let parts: Vec<Vec<u8>> = records.split(|b| *b == 0).map(|s| s.to_vec()).collect();
    let mut offset = initial_offset;

    offset += parts.len() as i64;
    set_offset(&mut stream, consumer_group, topic, offset)?;
    Ok(parts)
}

fn get_offset(stream: &mut TcpStream, consumer_group: &str, topic: &str) -> Result<i64> {
    let offset_fetch_request_api_version = 6;
    let offset_fetch_header =
        create_request_header(ApiKey::OffsetFetch as i16, offset_fetch_request_api_version);
    let offset_fetch_request = create_offset_fetch_request(consumer_group, topic);
    let offset_fetch_request_buffer = create_buffer(&offset_fetch_header, offset_fetch_request);

    stream.write(&offset_fetch_request_buffer)?;
    // Read response
    let mut buffer = [0; 512];
    stream.read(&mut buffer)?;
    // Decode response
    let mut new_buf = Bytes::from(Vec::from(&buffer[4..]));
    // Decode header if not accessed to consume bytes.
    let _header = ResponseHeader::decode(&mut new_buf, 1).unwrap();

    let offset_fetch_response =
        OffsetFetchResponse::decode(&mut Bytes::from(new_buf), offset_fetch_request_api_version)
            .unwrap();
    let got_offset = offset_fetch_response
        .topics
        .get(0)
        .unwrap()
        .partitions
        .get(0)
        .unwrap()
        .committed_offset
        .clone();
    Ok(got_offset)
}

fn get_records(
    stream: &mut TcpStream,
    request_buffer: &BytesMut,
    fetch_request_api_version: i16,
) -> Result<Bytes> {
    stream.write(&request_buffer)?;

    // Read response length
    let mut len_buf = [0; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buffer = vec![0u8; len];
    stream.read(&mut buffer)?;

    // Decode response
    let mut new_buf = Bytes::from(buffer);
    // Decode header if not accessed to consume bytes.
    let _header = ResponseHeader::decode(&mut new_buf, 1).unwrap();

    let fetch_response =
        FetchResponse::decode(&mut Bytes::from(new_buf), fetch_request_api_version).unwrap();
    let records: Bytes = fetch_response
        .responses
        .get(0)
        .unwrap()
        .partitions
        .get(0)
        .unwrap()
        .records
        .clone()
        .unwrap();

    Ok(records)
}

fn set_offset(
    stream: &mut TcpStream,
    consumer_group: &str,
    topic: &str,
    offset: i64,
) -> Result<()> {
    let offset_commit_request_api_version = 9;
    let offset_commit_header = create_request_header(
        ApiKey::OffsetCommit as i16,
        offset_commit_request_api_version,
    );
    let offset_commit_request = create_offset_commit_request(consumer_group, topic, offset);
    let offset_offset_request_buffer = create_buffer(&offset_commit_header, offset_commit_request);

    stream.write(&offset_offset_request_buffer)?;
    Ok(())
}

pub fn create_topic(broker: &str, topic: &str, schema: Option<Schema>) -> Result<()> {
    let mut stream = TcpStream::connect(broker)?;
    let request = Request::CreateTopic {
        topic: topic.into(),
        schema: schema,
    };
    let encoded = serde_json::to_vec(&request)?;
    let len = encoded.len() as u32;
    stream.write_u32::<BigEndian>(len)?;
    stream.write(&encoded)?;
    Ok(())
}
