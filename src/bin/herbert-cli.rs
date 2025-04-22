use std::io::Write;
use std::net::TcpStream;

use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use herbert::kafka_api::create_produce_request;
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, FetchRequest, ProduceRequest, RequestHeader, TopicName};
use kafka_protocol::protocol::{Encodable, StrBytes};

#[derive(Parser, Debug)]
#[command(name = "herbert--cli")]
#[command(about = "This is herbert, say hello to him.", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Produce a message to herbert
    Produce {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic to which you want to produce to
        #[arg(short, long)]
        topic: String,

        /// The message you want to produce
        #[arg(short, long)]
        message: String,
    },

    /// Consume message(s) from herbert
    Consume {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic from which you want to comsume to
        #[arg(short, long)]
        topic: String,

        /// The number of message you want to consume in maximum
        #[arg(short, long)]
        max_messages: i32,
    },
}

fn create_request_header(request_api_key: i16, request_api_version: i16) -> RequestHeader {
    let mut header = RequestHeader::default();
    header.request_api_key = request_api_key;
    header.request_api_version = request_api_version;
    header
}

fn create_fetch_request(topic: &str, max_messages: i32) -> FetchRequest {
    let mut fetch_request = FetchRequest::default();

    // FIXME: Max bytes does not work. Herbet can't decode it somehow.
    fetch_request.max_bytes = max_messages;

    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName::from(StrBytes::from_string(topic.to_string()));

    fetch_request.topics.push(fetch_topic);
    fetch_request
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
    let args = Args::parse();

    match args.command {
        Command::Produce {
            broker,
            topic,
            message,
        } => {
            let mut stream = TcpStream::connect(broker)?;
            let produce_request_api_version = 9;

            let header = create_request_header(ApiKey::Produce as i16, produce_request_api_version);
            let record = Bytes::from(message);
            let produce_request = create_produce_request(&topic, record);

            let request_buffer = create_buffer(header, produce_request);
            stream.write(&request_buffer)?;

            Ok(())
        }
        Command::Consume {
            broker,
            topic,
            max_messages,
        } => {
            let mut stream = TcpStream::connect(broker)?;
            let fetch_request_api_version = 5;

            let header = create_request_header(ApiKey::Fetch as i16, fetch_request_api_version);
            let fetch_request = create_fetch_request(&topic, max_messages);

            let request_buffer = create_buffer(header, fetch_request);
            stream.write(&request_buffer)?;

            Ok(())
        }
    }
}
