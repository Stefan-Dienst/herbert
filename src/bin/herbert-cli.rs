use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::thread::sleep;
use std::time::Duration;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use herbert::client::consume;
use herbert::client::consume_continuos;
use herbert::client::create_topic;
use herbert::kafka_api::create_fetch_request;
use herbert::kafka_api::create_produce_request;
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable};

use herbert::client::produce;

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

        /// The name of the consumer group that shall be used
        #[arg(short, long)]
        consumer_group: String,
    },

    /// Create a topic in Herbert
    CreateTopic {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic which shall be created
        #[arg(short, long)]
        topic: String,
    },
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    match args.command {
        Command::Produce {
            broker,
            topic,
            message,
        } => {
            let _ = produce(&broker, &topic, &message);
        }
        Command::Consume {
            broker,
            topic,
            max_messages,
            consumer_group,
        } => {
            let _ = consume_continuos(&broker, &topic, max_messages, &consumer_group);
        }
        Command::CreateTopic { broker, topic } => {
            let _ = create_topic(&broker, &topic);
        }
    };
}
