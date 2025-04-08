use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::from_utf8;

use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, ProduceRequest, RequestHeader, TopicName, produce_request};
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::{error, info};

#[derive(Parser, Debug)]
#[command(name = "herbert-produce-cli")]
#[command(about = "This is herbert, say hello to him.", long_about = None)]
struct Args {
    /// Herbert broker address, e.g. 127.0.0.1:9092
    #[arg(short, long)]
    broker: String,

    /// The topic to which you want to produce to
    #[arg(short, long)]
    topic: String,

    /// The message you want to produce
    #[arg(short, long)]
    message: String,
}

fn create_request_header(request_api_key: i16, request_api_version: i16) -> RequestHeader {
    let mut header = RequestHeader::default();
    header.request_api_key = request_api_key;
    header.request_api_version = request_api_version;
    header
}

fn create_produce_request(topic: &str, record: Bytes) -> ProduceRequest {
    let mut produce_request = ProduceRequest::default();

    let mut topic_to_produce_to = TopicProduceData::default();
    topic_to_produce_to.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    let mut things_to_produce = PartitionProduceData::default();
    things_to_produce.records = Some(record);

    topic_to_produce_to.partition_data.push(things_to_produce);

    produce_request.topic_data.push(topic_to_produce_to);
    produce_request
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

fn main() -> std::io::Result<()> {
    env_logger::init();
    info!("Hello from the Herbert CLI");
    let args = Args::parse();

    let mut stream = TcpStream::connect(args.broker)?;
    let produce_request_api_version = 9;

    let header = create_request_header(ApiKey::Produce as i16, produce_request_api_version);
    let record = Bytes::from(args.message);
    let produce_request = create_produce_request(&args.topic, record);

    let request_buffer = create_buffer(header, produce_request);
    stream.write(&request_buffer)?;

    Ok(())
}
